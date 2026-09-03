"""FastAPI dashboard for Telecrime."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shlex
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import cast

from fastapi import FastAPI, Form, Query, Request
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    RedirectResponse,
    Response,
    StreamingResponse,
)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from openpyxl import Workbook
from sqlalchemy import and_, case, extract, func, inspect, or_, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import selectinload

from telecrime.database import get_cached_engine, get_engine, get_session
from telecrime.fts import ensure_fts
from telecrime.models import (
    ArchiveGroup,
    ArchiveGroupPart,
    Conversation,
    DownloadArtifact,
    ExtractedOutput,
    ExtractionJob,
    FileAttachment,
    FirstSeenIndex,
    Message,
    ParsedCredential,
    PipelineRun,
    TelegramChannel,
)
from telecrime.models.password import PasswordCandidate
from telecrime.models.system_info import SystemInfoRecord
from telecrime.models.watchlist import WatchlistItem
from telecrime.pipeline.progress import read_progress
from telecrime.states import DownloadStatus, ExtractionStatus, GroupStatus, PasswordScope
from telecrime.utils.credential_dedup import soft_dedupe_credentials
from telecrime.web.exporting import _csv_stream, _export_value, _markdown_table, _serialize_row

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SearchResults:
    credentials: list[ParsedCredential]
    messages: list[Message]
    attachments: list[FileAttachment]
    archives: list[DownloadArtifact]
    extracted: list[ExtractedOutput]
    conversations: list[Conversation]
    channels: list[TelegramChannel]




_db_column_cache: dict[tuple[str, str, str], bool] = {}


def _has_db_column(session, table: str, column: str) -> bool:
    engine = session.get_bind()
    key = (str(engine.url), table, column)
    if key in _db_column_cache:
        return _db_column_cache[key]
    try:
        result = column in {
            col["name"] for col in inspect(engine).get_columns(table)
        }
    except Exception:
        result = False
    _db_column_cache[key] = result
    return result


def _credential_identity_sql(session, alias: str = "pc") -> str:
    if _has_db_column(session, "parsed_credentials", "soft_credential_hash"):
        return (
            f"COALESCE({alias}.soft_credential_hash, "
            f"{alias}.credential_hash, CAST({alias}.id AS TEXT))"
        )
    return f"COALESCE({alias}.credential_hash, CAST({alias}.id AS TEXT))"


def _templates_dir() -> Path:
    return Path(__file__).parent / "templates"


def _fts_escape(terms: str) -> str:
    """Quote each token for FTS5 so special chars like . are treated as literals."""
    tokens = terms.split()
    return " ".join(f'"{t.replace(chr(34), chr(34) + chr(34))}"' for t in tokens)


def _parse_since(value: str) -> datetime | None:
    """Parse a since: filter value into a datetime cutoff.

    Accepts:
      - 24h, 48h, 12h  — hours ago
      - 7d, 30d, 1d    — days ago
      - YYYY-MM-DD     — absolute date (start of day UTC)
    """
    value = value.strip().lower()
    try:
        if value.endswith("h"):
            hours = int(value[:-1])
            return datetime.now(UTC) - timedelta(hours=hours)
        if value.endswith("d"):
            days = int(value[:-1])
            return datetime.now(UTC) - timedelta(days=days)
        # Try YYYY-MM-DD
        dt = datetime.strptime(value, "%Y-%m-%d")
        return dt.replace(tzinfo=UTC)
    except (ValueError, OverflowError):
        return None


def _iso_age_seconds(value: object) -> float | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return (datetime.now(UTC) - dt).total_seconds()


def _errors_json_count(raw: str | None) -> int:
    """Count errors stored in a PipelineRun.errors_json without crashing on
    malformed/legacy values (null, partial JSON, plain text)."""
    if not raw:
        return 0
    try:
        parsed = json.loads(raw)
        return len(parsed) if isinstance(parsed, list) else 0
    except (ValueError, TypeError):
        return 0


def _parse_query(raw: str) -> tuple[str, dict[str, list[str]]]:
    try:
        tokens = shlex.split(raw)
    except ValueError:
        # Unbalanced quote (e.g. `foo"bar`) — treat the whole input as one
        # term instead of 500-ing every search/export endpoint.
        tokens = [raw]
    filters: dict[str, list[str]] = {}
    terms: list[str] = []
    allowed = {"domain", "stealer", "application", "email_domain", "username", "url", "since"}
    for token in tokens:
        if ":" in token:
            key, value = token.split(":", 1)
            key = key.lower().strip()
            value = value.strip()
            if key in allowed and value:
                filters.setdefault(key, []).append(value)
                continue
        terms.append(token)
    return " ".join(terms), filters


def _build_query(terms: str, filters: dict[str, list[str]]) -> str:
    parts: list[str] = []
    if terms:
        parts.append(terms)
    for key, values in filters.items():
        for value in values:
            if " " in value:
                value = f'"{value}"'
            parts.append(f"{key}:{value}")
    return " ".join(parts)


def _credential_filter_clause(filters: dict[str, list[str]]):
    mapping = {
        "domain": ParsedCredential.domain,
        "stealer": ParsedCredential.stealer_type,
        "application": ParsedCredential.application,
        "email_domain": ParsedCredential.email_domain,
        "username": ParsedCredential.username,
        "url": ParsedCredential.url,
    }
    clauses = []
    for key, values in filters.items():
        if key == "since":
            for value in values:
                cutoff = _parse_since(value)
                if cutoff:
                    clauses.append(ParsedCredential.created_at >= cutoff)
            continue
        col = mapping.get(key)
        if not col:
            continue
        ors = []
        for value in values:
            ors.append(func.lower(col).like(f"%{value.lower()}%"))
        if ors:
            clauses.append(or_(*ors))
    if not clauses:
        return None
    return and_(*clauses)


def _decorate_facets(
    facets: dict[str, list[tuple[str, int]]],
    terms: str,
    filters: dict[str, list[str]],
) -> dict[str, list[dict[str, str | int]]]:
    decorated: dict[str, list[dict[str, str | int]]] = {}
    key_map = {
        "domain": "domain",
        "stealer_type": "stealer",
        "application": "application",
        "email_domain": "email_domain",
    }
    for facet_key, items in facets.items():
        query_key = key_map.get(facet_key, facet_key)
        decorated[facet_key] = []
        for value, count in items:
            if not value:
                continue
            next_filters = {k: list(v) for k, v in filters.items()}
            next_filters.setdefault(query_key, [])
            if value not in next_filters[query_key]:
                next_filters[query_key].append(value)
            decorated[facet_key].append(
                {
                    "value": value,
                    "count": count,
                    "query": _build_query(terms, next_filters),
                }
            )
    return decorated


def _filter_pills(terms: str, filters: dict[str, list[str]]) -> list[dict[str, str]]:
    pills: list[dict[str, str]] = []
    for key, values in filters.items():
        for value in values:
            next_filters = {k: list(v) for k, v in filters.items()}
            try:
                next_filters[key].remove(value)
                if not next_filters[key]:
                    next_filters.pop(key, None)
            except (KeyError, ValueError):
                pass
            pills.append(
                {
                    "label": f"{key}:{value}",
                    "query": _build_query(terms, next_filters),
                }
            )
    return pills


def _get_exclude_names() -> list[str]:
    """Get names to exclude from search results via TELECRIME_EXCLUDE_NAMES env var.

    Set TELECRIME_EXCLUDE_NAMES to a comma-separated list of conversation/channel
    usernames or titles to hide from dashboard search (e.g. your self-chat for
    download notifications).
    """
    raw = os.environ.get("TELECRIME_EXCLUDE_NAMES", "")
    return [n.strip().lower() for n in raw.split(",") if n.strip()]


def _get_exclusions(session) -> tuple[set[int], set[int]]:
    names = _get_exclude_names()
    if not names:
        return set(), set()

    excluded_conversations = set(
        row[0]
        for row in session.query(Conversation.id)
        .filter(
            or_(
                func.lower(Conversation.title).in_(names),
                func.lower(Conversation.username).in_(names),
            )
        )
        .all()
    )
    excluded_channels = set(
        row[0]
        for row in session.query(TelegramChannel.id)
        .filter(
            or_(
                func.lower(TelegramChannel.title).in_(names),
                func.lower(TelegramChannel.username).in_(names),
            )
        )
        .all()
    )
    return excluded_conversations, excluded_channels


_PG_CREDENTIAL_SEARCH_COLUMNS = ("domain", "username", "email_domain")


def _pg_try_ids_for_column(
    session,
    *,
    column: str,
    pattern: str,
    limit: int,
    timeout_ms: int = 2500,
) -> list[int]:
    from sqlalchemy.exc import SQLAlchemyError

    bind = session.get_bind()
    try:
        with bind.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.exec_driver_sql("SET max_parallel_workers_per_gather = 0")
            conn.exec_driver_sql(f"SET statement_timeout = {int(timeout_ms)}")
            try:
                rows = conn.execute(
                    text(
                        f"SELECT id FROM parsed_credentials "
                        f"WHERE {column} ILIKE :q "
                        "LIMIT :limit"
                    ),
                    {"q": pattern, "limit": limit},
                ).fetchall()
                return [int(row[0]) for row in rows]
            finally:
                # Reset to server default so this connection is safe to reuse from the pool.
                try:
                    conn.exec_driver_sql("SET statement_timeout = DEFAULT")
                    conn.exec_driver_sql("SET max_parallel_workers_per_gather = DEFAULT")
                except Exception:
                    pass
    except SQLAlchemyError as exc:
        logger.warning("Credential search branch timed out for %s: %s", column, exc)
        return []


def _pg_bounded_candidate_ids(
    session,
    *,
    terms: str,
    limit: int,
    offset: int = 0,
    timeout_ms: int = 2500,
) -> list[int]:
    tokens = terms.split()
    if not tokens:
        return []
    # This bounded search is safe to run while the pipeline is active: every
    # per-column branch uses a trgm GIN index with LIMIT 500 and a 2.5s
    # statement_timeout (see _pg_try_ids_for_column), and the pipeline watchdog
    # additionally cancels any query on parsed_credentials running >15s during
    # the parse stage. There is deliberately NO blanket "pipeline running"
    # gate here — the pipeline runs back-to-back through thousands of archives,
    # so a whole-pipeline throttle would make credential search permanently
    # unavailable. Under heavy parse load the per-branch timeouts degrade the
    # search to empty gracefully instead of blocking the pipeline.

    branch_limit = 500
    per_token: list[set[int]] = []
    for token in tokens:
        pattern = f"%{token}%"
        token_ids: set[int] = set()
        for column in _PG_CREDENTIAL_SEARCH_COLUMNS:
            token_ids.update(
                _pg_try_ids_for_column(
                    session,
                    column=column,
                    pattern=pattern,
                    limit=branch_limit,
                    timeout_ms=timeout_ms,
                )
            )
        if not token_ids:
            return []
        per_token.append(token_ids)

    candidate_ids = set.intersection(*per_token) if len(per_token) > 1 else per_token[0]
    return sorted(candidate_ids, reverse=True)[offset : offset + limit]


_COUNT_CAP = 10_001  # Stop counting after this many rows; display as "10,000+"


def _credential_match_count(
    session, *, terms: str, filters: dict[str, list[str]], exclude_conversation_ids: set[int]
) -> int:
    if not terms:
        return 0

    if session.get_bind().dialect.name == "postgresql":
        candidate_ids = _pg_bounded_candidate_ids(
            session,
            terms=terms,
            limit=_COUNT_CAP,
            timeout_ms=2000,
        )
        if not candidate_ids:
            return 0
        params: dict[str, object] = {
            "cap": _COUNT_CAP,
            **{f"candidate_{idx}": row_id for idx, row_id in enumerate(candidate_ids)},
        }
        candidate_params = ", ".join(f":candidate_{idx}" for idx in range(len(candidate_ids)))
        where_parts: list[str] = []
        _apply_credential_filters_sql(
            where_parts,
            params,
            filters,
            exclude_conversation_ids,
            case_insensitive_op="ILIKE",
        )
        identity_sql = _credential_identity_sql(session)
        select_cols = "pc.credential_hash, pc.id"
        if _has_db_column(session, "parsed_credentials", "soft_credential_hash"):
            select_cols = "pc.soft_credential_hash, " + select_cols
        row = session.execute(
            text(

                    f"SELECT COUNT(DISTINCT {identity_sql}) FROM ("
                    + f"SELECT {select_cols} FROM parsed_credentials pc "
                    + f"WHERE pc.id IN ({candidate_params}) "
                    + (f"AND {' AND '.join(where_parts)} " if where_parts else "")
                    + "ORDER BY pc.id DESC "
                    + "LIMIT :cap"
                    + ") pc"

            ),
            params,
        ).fetchone()
        return int(row[0] if row else 0)

    where_parts = ["parsed_credentials_fts MATCH :q"]
    params = {"q": _fts_escape(terms)}
    _apply_credential_filters_sql(where_parts, params, filters, exclude_conversation_ids)

    identity_sql = _credential_identity_sql(session)
    row = session.execute(
        text(
            f"SELECT COUNT(DISTINCT {identity_sql}) "
            "FROM parsed_credentials_fts "
            "JOIN parsed_credentials pc ON pc.id = parsed_credentials_fts.rowid "
            f"WHERE {' AND '.join(where_parts)}"
        ),
        params,
    ).fetchone()
    return int(row[0] if row else 0)


def _apply_credential_filters_sql(
    sql_parts: list[str],
    params: dict[str, object],
    filters: dict[str, list[str]],
    exclude_conversation_ids: set[int],
    *,
    case_insensitive_op: str = "LIKE",
) -> None:
    column_map = {
        "domain": "pc.domain",
        "stealer": "pc.stealer_type",
        "application": "pc.application",
        "email_domain": "pc.email_domain",
        "username": "pc.username",
        "url": "pc.url",
    }
    for filter_key, values in filters.items():
        if filter_key == "since":
            for idx, value in enumerate(values):
                cutoff = _parse_since(value)
                if cutoff:
                    param_name = f"since_{idx}_{len(params)}"
                    sql_parts.append(f"pc.created_at >= :{param_name}")
                    params[param_name] = cutoff
            continue
        column_name = column_map.get(filter_key)
        if not column_name:
            continue
        value_parts = []
        for idx, value in enumerate(values):
            param_name = f"{filter_key}_{idx}_{len(params)}"
            if case_insensitive_op == "ILIKE":
                value_parts.append(f"{column_name} ILIKE :{param_name}")
                params[param_name] = f"%{value}%"
            else:
                value_parts.append(f"lower({column_name}) LIKE :{param_name}")
                params[param_name] = f"%{value.lower()}%"
        if value_parts:
            sql_parts.append("(" + " OR ".join(value_parts) + ")")

    if exclude_conversation_ids:
        excluded_params = []
        for idx, conversation_id in enumerate(sorted(exclude_conversation_ids)):
            param_name = f"excluded_conv_{idx}"
            excluded_params.append(f":{param_name}")
            params[param_name] = conversation_id
        sql_parts.append(
            "(pc.source_conversation_id IS NULL OR pc.source_conversation_id NOT IN ("
            + ", ".join(excluded_params)
            + "))"
        )


def _credential_ids_via_fts(
    session,
    *,
    terms: str,
    filters: dict[str, list[str]],
    exclude_conversation_ids: set[int],
    limit: int,
    offset: int = 0,
) -> list[int]:
    if session.get_bind().dialect.name == "postgresql":
        # Over-fetch candidates: filters (stealer:/app:/etc.) are applied AFTER
        # the candidate fetch, so a pool trimmed to `limit` may contain zero
        # rows that pass the filter. Fetch 5× the target so filtered searches
        # still return `limit` matches (mirrors the search route's page_size*5).
        fetch_limit = max(limit * 5, 50)
        candidate_ids = _pg_bounded_candidate_ids(
            session,
            terms=terms,
            limit=fetch_limit,
            offset=offset,
            timeout_ms=2500,
        )
        if not candidate_ids:
            return []
        params: dict[str, object] = {
            "limit": limit,
            "offset": offset,
            **{f"candidate_{idx}": row_id for idx, row_id in enumerate(candidate_ids)},
        }
        candidate_params = ", ".join(f":candidate_{idx}" for idx in range(len(candidate_ids)))
        where_parts: list[str] = []
        _apply_credential_filters_sql(
            where_parts,
            params,
            filters,
            exclude_conversation_ids,
            case_insensitive_op="ILIKE",
        )
        rows = session.execute(
            text(

                    "SELECT pc.id FROM parsed_credentials pc "
                    + f"WHERE pc.id IN ({candidate_params}) "
                    + (f"AND {' AND '.join(where_parts)} " if where_parts else "")
                    + "ORDER BY pc.id DESC LIMIT :limit OFFSET :offset"

            ),
            params,
        ).fetchall()
        return [row[0] for row in rows]

    fts_where_parts = ["parsed_credentials_fts MATCH :q"]
    fts_params: dict[str, object] = {"q": _fts_escape(terms), "limit": limit, "offset": offset}
    _apply_credential_filters_sql(fts_where_parts, fts_params, filters, exclude_conversation_ids)

    rows = session.execute(
        text(
            "SELECT pc.id "
            "FROM parsed_credentials_fts "
            "JOIN parsed_credentials pc ON pc.id = parsed_credentials_fts.rowid "
            f"WHERE {' AND '.join(fts_where_parts)} "
            "ORDER BY bm25(parsed_credentials_fts), pc.id "
            "LIMIT :limit OFFSET :offset"
        ),
        fts_params,
    ).fetchall()
    return [row[0] for row in rows]


def _message_ids_via_fts(
    session,
    *,
    terms: str,
    exclude_conversation_ids: set[int],
    limit: int,
) -> list[int]:
    if session.get_bind().dialect.name == "postgresql":
        params: dict[str, object] = {"q": f"%{terms}%", "limit": limit}
        pg_where = [
            "(lower(m.text) LIKE lower(:q) OR lower(m.caption) LIKE lower(:q) "
            "OR lower(m.post_author) LIKE lower(:q))"
        ]
        if exclude_conversation_ids:
            excluded_params = []
            for idx, conversation_id in enumerate(sorted(exclude_conversation_ids)):
                param_name = f"excluded_msg_conv_{idx}"
                excluded_params.append(f":{param_name}")
                params[param_name] = conversation_id
            pg_where.append("m.conversation_id NOT IN (" + ", ".join(excluded_params) + ")")
        rows = session.execute(
            text(
                "SELECT m.id FROM messages m "
                f"WHERE {' AND '.join(pg_where)} "
                "ORDER BY m.id DESC LIMIT :limit"
            ),
            params,
        ).fetchall()
        return [row[0] for row in rows]

    fts_where = ["messages_fts MATCH :q"]
    fts_params: dict[str, object] = {"q": _fts_escape(terms), "limit": limit}
    if exclude_conversation_ids:
        excluded_params = []
        for idx, conversation_id in enumerate(sorted(exclude_conversation_ids)):
            param_name = f"excluded_msg_conv_{idx}"
            excluded_params.append(f":{param_name}")
            fts_params[param_name] = conversation_id
        fts_where.append("m.conversation_id NOT IN (" + ", ".join(excluded_params) + ")")

    rows = session.execute(
        text(
            "SELECT m.id "
            "FROM messages_fts "
            "JOIN messages m ON m.id = messages_fts.rowid "
            f"WHERE {' AND '.join(fts_where)} "
            "ORDER BY bm25(messages_fts), m.id "
            "LIMIT :limit"
        ),
        fts_params,
    ).fetchall()
    return [row[0] for row in rows]


def _load_ordered_records(session, model, ids: list[int]):
    if not ids:
        return []
    rows = session.query(model).filter(model.id.in_(ids)).all()
    order = {row_id: idx for idx, row_id in enumerate(ids)}
    rows.sort(key=lambda row: order.get(row.id, len(ids)))
    return rows


def _search_for_export(
    session,
    terms: str,
    filters: dict[str, list[str]],
    regex: bool,
    fts_enabled: bool,
    exclude_conversation_ids: set[int],
    exclude_channel_ids: set[int],
    limit_credentials: int,
    limit_messages: int,
    limit_attachments: int,
    limit_archives: int,
    limit_extracted: int,
    limit_conversations: int,
    limit_channels: int,
) -> SearchResults:
    pattern = f"%{terms.lower()}%"

    def like_any(*cols):
        return or_(*[func.lower(col).like(pattern) for col in cols])

    filter_clause = _credential_filter_clause(filters)
    credentials = []
    if terms and fts_enabled and not regex:
        try:
            credential_ids = _credential_ids_via_fts(
                session,
                terms=terms,
                filters=filters,
                exclude_conversation_ids=exclude_conversation_ids,
                limit=limit_credentials,
            )
            if credential_ids:
                credentials = soft_dedupe_credentials(
                    _load_ordered_records(session, ParsedCredential, credential_ids),
                    limit=limit_credentials,
                )
        except Exception:
            credentials = []

    if not credentials:
        query_base = session.query(ParsedCredential)
        if terms:
            query_base = query_base.filter(
                like_any(
                    ParsedCredential.url,
                    ParsedCredential.domain,
                    ParsedCredential.username,
                    ParsedCredential.email_domain,
                    ParsedCredential.application,
                    ParsedCredential.source_archive,
                    ParsedCredential.source_file,
                    ParsedCredential.stealer_type,
                )
            )
        if filter_clause is not None:
            query_base = query_base.filter(filter_clause)
        if exclude_conversation_ids:
            query_base = query_base.filter(
                ParsedCredential.source_conversation_id.notin_(exclude_conversation_ids)
            )
        try:
            # Bound the LIKE fallback so exports degrade gracefully instead of
            # scanning the whole table (no trgm index on stealer_type/url/etc).
            if session.get_bind().dialect.name == "postgresql":
                session.execute(text("SET LOCAL statement_timeout = '30000'"))
            fallback_rows = (
                query_base.order_by(ParsedCredential.created_at.desc())
                .limit(limit_credentials * 5)
                .all()
            )
        except Exception as exc:
            try:
                session.rollback()
            except Exception:
                pass
            logger.warning("search export: credential LIKE fallback timed out: %r", exc)
            fallback_rows = []
        credentials = soft_dedupe_credentials(
            fallback_rows,
            limit=limit_credentials,
        )

    messages = []
    if terms and fts_enabled and not regex:
        try:
            message_ids = _message_ids_via_fts(
                session,
                terms=terms,
                exclude_conversation_ids=exclude_conversation_ids,
                limit=limit_messages,
            )
            messages = _load_ordered_records(session, Message, message_ids)
        except Exception:
            messages = []

    if not messages:
        messages = (
            session.query(Message)
            .filter(like_any(Message.text, Message.caption, Message.post_author))
            .order_by(Message.platform_timestamp.desc())
            .limit(limit_messages)
            .all()
        )
    if exclude_conversation_ids:
        messages = [m for m in messages if m.conversation_id not in exclude_conversation_ids]

    attachments = (
        session.query(FileAttachment)
        .join(Message, Message.id == FileAttachment.message_id)
        .filter(like_any(FileAttachment.filename, FileAttachment.mime_type))
        .order_by(FileAttachment.created_at.desc())
        .limit(limit_attachments)
        .all()
    )
    if exclude_conversation_ids:
        attachments = [
            a for a in attachments if a.message.conversation_id not in exclude_conversation_ids
        ]
    archives = (
        session.query(DownloadArtifact)
        .join(FileAttachment, FileAttachment.id == DownloadArtifact.attachment_id)
        .join(Message, Message.id == FileAttachment.message_id)
        .filter(like_any(DownloadArtifact.local_path, DownloadArtifact.temp_path))
        .order_by(DownloadArtifact.created_at.desc())
        .limit(limit_archives)
        .all()
    )
    if exclude_conversation_ids:
        archives = [
            a
            for a in archives
            if a.attachment.message.conversation_id not in exclude_conversation_ids
        ]
    extracted = (
        session.query(ExtractedOutput)
        .filter(like_any(ExtractedOutput.output_filename, ExtractedOutput.output_path))
        .order_by(ExtractedOutput.created_at.desc())
        .limit(limit_extracted)
        .all()
    )
    if exclude_conversation_ids:
        extracted = [
            e for e in extracted if e.source_conversation_id not in exclude_conversation_ids
        ]
    conversations = (
        session.query(Conversation)
        .filter(like_any(Conversation.title, Conversation.username, Conversation.notes))
        .order_by(Conversation.created_at.desc())
        .limit(limit_conversations)
        .all()
    )
    if exclude_conversation_ids:
        conversations = [c for c in conversations if c.id not in exclude_conversation_ids]
    channels = (
        session.query(TelegramChannel)
        .filter(like_any(TelegramChannel.username, TelegramChannel.title, TelegramChannel.notes))
        .order_by(TelegramChannel.discovered_at.desc())
        .limit(limit_channels)
        .all()
    )
    if exclude_channel_ids:
        channels = [c for c in channels if c.id not in exclude_channel_ids]

    if regex and terms:
        try:
            rx = re.compile(terms, re.IGNORECASE)
        except re.error:
            rx = None
        if rx:

            def _match(value):
                return value and rx.search(str(value))

            credentials = [
                c
                for c in credentials
                if any(
                    _match(v)
                    for v in [
                        c.url,
                        c.domain,
                        c.username,
                        c.email_domain,
                        c.application,
                        c.source_archive,
                        c.source_file,
                        c.stealer_type,
                    ]
                )
            ]
            messages = [
                m for m in messages if any(_match(v) for v in [m.text, m.caption, m.post_author])
            ]

    return SearchResults(
        credentials=credentials,
        messages=messages,
        attachments=attachments,
        archives=archives,
        extracted=extracted,
        conversations=conversations,
        channels=channels,
    )


def _triage_payload(session, *, limit: int = 50) -> dict[str, object]:
    """Build recent failure and retryability data for dashboard triage views."""
    failed_downloads = (
        session.query(DownloadArtifact)
        .options(
            selectinload(DownloadArtifact.attachment)
            .selectinload(FileAttachment.message),
        )
        .filter(
            DownloadArtifact.status.in_([DownloadStatus.FAILED, DownloadStatus.FAILED_TERMINAL])
        )
        .order_by(DownloadArtifact.updated_at.desc())
        .limit(limit)
        .all()
    )
    failed_extractions = (
        session.query(ExtractionJob)
        .options(
            selectinload(ExtractionJob.group)
            .selectinload(ArchiveGroup.parts)
            .selectinload(ArchiveGroupPart.artifact)
            .selectinload(DownloadArtifact.attachment)
            .selectinload(FileAttachment.message)
        )
        .filter(
            ExtractionJob.status.in_(
                [
                    ExtractionStatus.FAILED,
                    ExtractionStatus.FAILED_TERMINAL,
                    ExtractionStatus.PASSWORD_NEEDED,
                ]
            )
        )
        .order_by(ExtractionJob.updated_at.desc())
        .limit(limit)
        .all()
    )

    # Precompute message text for each failed extraction job
    extraction_message_texts: dict[int, str] = {}
    for job in failed_extractions:
        try:
            if job.group and job.group.parts:
                part = job.group.parts[0]
                if part.artifact and part.artifact.attachment:
                    msg = part.artifact.attachment.message
                    if msg:
                        text_val = msg.text or msg.caption
                        if text_val:
                            extraction_message_texts[job.id] = text_val
        except Exception:
            pass

    # Collect conversation IDs for failed extraction jobs to look up password candidates
    conv_ids_by_job: dict[int, int] = {}  # job_id → conversation_id
    for job in failed_extractions:
        try:
            if job.group and job.group.parts:
                part = job.group.parts[0]
                if part.artifact and part.artifact.attachment and part.artifact.attachment.message:
                    conv_ids_by_job[job.id] = part.artifact.attachment.message.conversation_id
        except Exception:
            pass

    # Bulk load password candidates for those conversations
    passwords_by_job: dict[int, list[str]] = {}
    if conv_ids_by_job:
        all_conv_ids = list(set(conv_ids_by_job.values()))
        candidates = (
            session.query(PasswordCandidate)
            .filter(PasswordCandidate.conversation_id.in_(all_conv_ids))
            .order_by(PasswordCandidate.times_succeeded.desc(), PasswordCandidate.id)
            .all()
        )
        # Build reverse map: conversation_id → list of candidate values
        cands_by_conv: dict[int, list[str]] = {}
        for c in candidates:
            cands_by_conv.setdefault(c.conversation_id, []).append(c.value)
        for job_id, conv_id in conv_ids_by_job.items():
            passwords_by_job[job_id] = cands_by_conv.get(conv_id, [])

    return {
        "failed_downloads": failed_downloads,
        "failed_extractions": failed_extractions,
        "extraction_message_texts": extraction_message_texts,
        "passwords_by_job": passwords_by_job,
        "summary": {
            "download_failures": len(failed_downloads),
            "extraction_failures": len(failed_extractions),
            "terminal_downloads": sum(
                1 for row in failed_downloads if row.status == DownloadStatus.FAILED_TERMINAL
            ),
            "terminal_extractions": sum(
                1 for row in failed_extractions if row.status == ExtractionStatus.FAILED_TERMINAL
            ),
            "password_needed": sum(
                1 for row in failed_extractions if row.status == ExtractionStatus.PASSWORD_NEEDED
            ),
        },
    }


def _ensure_search_infra_pg(engine) -> bool:
    """Create saved_searches table and enable pg_trgm for PostgreSQL."""
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS saved_searches (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL,
                        query TEXT NOT NULL,
                        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
        ensure_fts(engine)
        return True
    except Exception:
        return False


def _ensure_search_infra(engine) -> bool:
    """Create FTS tables/triggers and saved searches table. Returns True if FTS is available."""
    return _ensure_search_infra_pg(engine)


def _ensure_stats_indexes(engine) -> None:
    """Create any missing performance indexes without blocking active transactions.

    On PostgreSQL, uses CREATE INDEX CONCURRENTLY so the web container can
    start even while the pipeline is mid-transaction on the same tables.
    Each index is created individually in autocommit mode (CONCURRENTLY
    requires no enclosing transaction).  On SQLite the regular DDL path is
    used because CONCURRENTLY is not supported.
    """
    if _pipeline_running_for_heavy_web_work():
        return

    wanted = [
        ("ix_messages_platform_timestamp", "ON messages(platform_timestamp)"),
        ("ix_messages_conversation_timestamp", "ON messages(conversation_id, platform_timestamp)"),
        ("ix_messages_forwarded_from", "ON messages(forwarded_from_id)"),
        ("ix_messages_is_forwarded", "ON messages(is_forwarded)"),
        ("ix_file_attachments_message", "ON file_attachments(message_id)"),
        ("ix_file_attachments_archive_candidate", "ON file_attachments(is_archive_candidate)"),
        ("ix_download_artifacts_attachment", "ON download_artifacts(attachment_id)"),
        ("ix_download_artifacts_status", "ON download_artifacts(status)"),
        ("ix_parsed_credentials_created_at", "ON parsed_credentials(created_at)"),
        ("ix_parsed_credentials_source_conversation_id", "ON parsed_credentials(source_conversation_id)"),
        ("ix_extracted_outputs_created_at", "ON extracted_outputs(created_at)"),
        ("ix_extracted_outputs_source_conversation", "ON extracted_outputs(source_conversation_id)"),
        ("ix_telegram_channels_discovered", "ON telegram_channels(discovered_at)"),
        ("ix_telegram_channels_active", "ON telegram_channels(is_active)"),
    ]

    is_postgres = engine.dialect.name == "postgresql"

    # Check which indexes already exist in one query (fast path — skips
    # all DDL on normal restarts where everything is already in place).
    with engine.connect() as conn:
        if is_postgres:
            existing = {
                row[0]
                for row in conn.execute(
                    text("SELECT indexname FROM pg_indexes WHERE schemaname = 'public'")
                )
            }
        else:
            existing = set()  # SQLite: always attempt, DDL is cheap

    missing = [(name, defn) for name, defn in wanted if name not in existing]
    if not missing:
        return

    for name, defn in missing:
        try:
            if is_postgres:
                # CONCURRENTLY requires autocommit — won't block active writers.
                with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                    conn.execute(text(f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {name} {defn}"))
            else:
                with engine.begin() as conn:
                    conn.execute(text(f"CREATE INDEX IF NOT EXISTS {name} {defn}"))
        except Exception as exc:
            logger.warning("Could not create index %s: %s", name, exc)


def _stats_cache_path() -> Path:
    default_data_dir = Path(__file__).parent.parent.parent / "data"
    data_dir = Path(os.environ.get("TELECRIME_DATA_DIR", str(default_data_dir)))
    return data_dir / "stats_cache.json"


# Protects read-modify-write cycles on the shared stats cache JSON file.
# _stats_worker and _home_worker both update different keys in the same file;
# without a lock one thread's update silently overwrites the other's.
_stats_cache_lock = threading.Lock()


def _write_stats_cache(payload: dict[str, object]) -> None:
    path = _stats_cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, default=str))
    tmp.replace(path)


def _read_stats_cache() -> dict[str, object] | None:
    path = _stats_cache_path()
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def _pipeline_running_for_heavy_web_work() -> bool:
    """Return True when web background scans should yield to ingestion."""
    if os.environ.get("TELECRIME_WEB_STATS_DURING_PIPELINE") == "1":
        return False
    try:
        progress = read_progress() or {}
    except Exception:
        return False
    return bool(progress.get("running"))


def _stats_worker(engine_url: str, presets: list[tuple[int, int]], interval: int) -> None:
    # Short initial delay so the web process is fully up before we start queries.
    time.sleep(5)
    engine = get_cached_engine(engine_url)
    first_pass = True
    # When the pipeline is running, yield fully — stats GROUP BY on 157M rows competes
    # badly with bulk INSERT I/O and can block for hours. Re-check every 60s.
    while True:
        if _pipeline_running_for_heavy_web_work():
            time.sleep(min(interval, 60))
            continue
        for days, limit in presets:
            # Re-check inside the loop: pipeline may have started since the outer check.
            if _pipeline_running_for_heavy_web_work():
                logger.debug("Stats worker: pipeline started mid-cycle, aborting preset loop")
                break
            try:
                payload = _compute_stats_payload(engine, days, limit)
                with _stats_cache_lock:
                    cache = _read_stats_cache() or {"generated_at": None, "data": {}}
                    raw_data = cache.get("data")
                    data: dict[str, object] = raw_data if isinstance(raw_data, dict) else {}
                    data[f"{days}:{limit}"] = payload
                    cache["data"] = data
                    cache["generated_at"] = datetime.now(UTC).isoformat()
                    _write_stats_cache(cache)
            except Exception as e:
                logger.warning("Stats compute failed for preset %dd limit=%d: %s", days, limit, e)
            # On the first pass, run all presets back-to-back without sleeping so
            # the stats page shows data ASAP instead of waiting 510s.
            if not first_pass:
                time.sleep(interval)
        first_pass = False


def _home_cache() -> dict[str, object] | None:
    cache = _read_stats_cache()
    if not cache:
        return None
    home = cache.get("home")
    return home if isinstance(home, dict) else None


def _write_home_cache(payload: dict[str, object]) -> None:
    with _stats_cache_lock:
        cache = _read_stats_cache() or {"generated_at": None, "data": {}}
        cache["home"] = payload
        cache["generated_at"] = datetime.now(UTC).isoformat()
        _write_stats_cache(cache)


def _approx_count(session, table_name: str) -> int:
    """Return Postgres reltuples estimate — instant vs 50s COUNT(*) on 100M rows."""
    row = session.execute(
        text("SELECT reltuples::bigint FROM pg_class WHERE relname = :t"),
        {"t": table_name},
    ).scalar_one_or_none()
    return int(row) if row and row > 0 else 0


def _preferred_table_estimate(reltuples: object, n_live_tup: object) -> int:
    """Pick the least stale fast row estimate available for dashboard tiles."""
    estimates: list[int] = []
    for value in (reltuples, n_live_tup):
        if value is None:
            continue
        try:
            estimate = int(value)
        except (TypeError, ValueError):
            continue
        if estimate > 0:
            estimates.append(estimate)
    return max(estimates, default=0)


def _pg_fast_count_estimates(session, *table_names: str) -> dict[str, int]:
    if not table_names:
        return {}
    rows = session.execute(
        text(
            """
            SELECT c.relname, c.reltuples::bigint AS reltuples, s.n_live_tup::bigint AS n_live_tup
            FROM pg_class c
            LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
            WHERE c.relname = ANY(:table_names)
            """
        ),
        {"table_names": list(table_names)},
    ).mappings()
    return {
        str(row["relname"]): _preferred_table_estimate(row["reltuples"], row["n_live_tup"])
        for row in rows
    }


def _compute_home_cache_payload(engine) -> dict[str, object]:
    with get_session(engine) as session:
        if engine.dialect.name == "postgresql":
            session.execute(text("SET LOCAL statement_timeout = '90s'"))
        stats = {
            "conversations": session.query(Conversation).count(),
            "messages": _approx_count(session, "messages"),
            "attachments": _approx_count(session, "file_attachments"),
            "archives": _approx_count(session, "download_artifacts"),
            "archive_groups": session.query(ArchiveGroup).count(),
            "extractions": _approx_count(session, "extraction_jobs"),
            "extracted_outputs": _approx_count(session, "extracted_outputs"),
            "credentials": _approx_count(session, "parsed_credentials"),
            "channels": session.query(TelegramChannel).count(),
        }
        since_14d = datetime.now(UTC) - timedelta(days=14)
        daily_credentials_14d = (
            session.query(
                func.date(ParsedCredential.created_at).label("day"),
                func.count(ParsedCredential.id).label("count"),
            )
            .filter(ParsedCredential.created_at >= since_14d)
            .group_by(func.date(ParsedCredential.created_at))
            .order_by(func.date(ParsedCredential.created_at).asc())
            .all()
        )
    return {
        "stats": stats,
        "daily_credentials_14d": [
            {"day": str(row.day), "count": row.count} for row in daily_credentials_14d
        ],
    }


def _home_worker(engine_url: str, interval: int) -> None:
    time.sleep(2)
    engine = get_cached_engine(engine_url)
    while True:
        if _pipeline_running_for_heavy_web_work():
            time.sleep(min(interval, 60))
            continue
        try:
            _write_home_cache(_compute_home_cache_payload(engine))
        except Exception as e:
            logger.warning("Home cache update failed: %s", e)
        time.sleep(interval)


def _watchlist_worker(engine_url: str, interval: int) -> None:
    """Background worker to check watchlist items for new matches."""
    time.sleep(10)
    engine = get_cached_engine(engine_url)
    while True:
        try:
            _check_watchlist(
                engine,
                incremental_only=_pipeline_running_for_heavy_web_work(),
            )
        except Exception as e:
            logger.warning("Watchlist check failed: %s", e)
        time.sleep(interval)


def _check_watchlist(engine, *, incremental_only: bool = False) -> None:
    """Check all enabled watchlist items and update new_count.

    Uses an AUTOCOMMIT connection so each per-item query runs in its own
    transaction — a cancellation of one query does not poison the connection
    for the next item, and the long-lived connection avoids the
    `idle_in_transaction_session_timeout` set globally on the DB.
    """

    now = datetime.now(UTC)
    cancelled_count = 0
    failed_count = 0

    with get_session(engine) as session:
        items = session.query(WatchlistItem).filter(WatchlistItem.enabled == True).all()
        if not items:
            return
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            # Bound (not disable) the per-item timeout: a pathological item
            # (e.g. match_type "any" with a single-char query) would otherwise
            # hold a backend indefinitely with no external watchdog to cancel.
            try:
                conn.execute(text("SET statement_timeout = '300s'"))
            except Exception:
                pass
            for item in items:
                try:
                    if incremental_only:
                        if item.last_checked_at is None:
                            continue
                        delta = _watchlist_count(
                            conn, item.query, item.match_type, since=item.last_checked_at,
                        )
                        item.new_count = int(item.new_count or 0) + delta
                        item.last_known_count = int(item.last_known_count or 0) + delta
                    else:
                        count = _watchlist_count(conn, item.query, item.match_type)
                        item.new_count = max(0, count - item.last_known_count)
                        item.last_known_count = count
                    item.last_checked_at = now
                except Exception as e:
                    msg = str(e)
                    # Watchdog cancels these queries during heavy parse to
                    # protect bulk INSERT throughput — it's expected, not an
                    # error. Demote to DEBUG and tally for a single summary.
                    if "QueryCanceled" in type(e).__name__ or "canceling statement" in msg:
                        cancelled_count += 1
                        logger.debug(
                            "Watchlist item %r cancelled (watchdog/timeout): %s",
                            item.query, msg,
                        )
                    else:
                        failed_count += 1
                        logger.warning(
                            "Watchlist item check failed for %r: %s", item.query, e
                        )
        session.commit()

    if cancelled_count:
        logger.info(
            "Watchlist sweep: %d/%d items cancelled by watchdog/timeout (expected during heavy parse)",
            cancelled_count, len(items),
        )
    if failed_count:
        logger.warning("Watchlist sweep: %d/%d items failed", failed_count, len(items))


def _watchlist_count(
    conn,
    query: str,
    match_type: str,
    *,
    since: datetime | None = None,
) -> int:
    """Count credentials matching a watchlist query (PostgreSQL ILIKE + GIN trgm)."""
    q = f"%{query}%"
    params: dict[str, object] = {"q": q}
    since_sql = ""
    if since is not None:
        since_sql = "created_at >= :since AND "
        params["since"] = since
    if match_type == "domain":
        row = conn.execute(
            text(f"SELECT COUNT(*) FROM parsed_credentials WHERE {since_sql}domain ILIKE :q"),
            params,
        ).fetchone()
    elif match_type == "user":
        row = conn.execute(
            text(f"SELECT COUNT(*) FROM parsed_credentials WHERE {since_sql}username ILIKE :q"),
            params,
        ).fetchone()
    elif match_type == "url":
        row = conn.execute(
            text(f"SELECT COUNT(*) FROM parsed_credentials WHERE {since_sql}url ILIKE :q"),
            params,
        ).fetchone()
    else:
        # PostgreSQL cannot use multiple GIN trgm indexes with OR — run per-column
        # queries and UNION to deduplicate, letting each query use its own index.
        row = conn.execute(
            text(
                f"SELECT COUNT(*) FROM ("
                f"  SELECT id FROM parsed_credentials WHERE {since_sql}domain ILIKE :q"
                f"  UNION"
                f"  SELECT id FROM parsed_credentials WHERE {since_sql}username ILIKE :q"
                f"  UNION"
                f"  SELECT id FROM parsed_credentials WHERE {since_sql}url ILIKE :q"
                f") _wl"
            ),
            params,
        ).fetchone()
    return int(row[0] if row else 0)


def _witem_dict(item) -> dict:
    """Convert a WatchlistItem ORM object to a dict compatible with existing templates."""
    return {
        "id": item.id,
        "label": item.label,
        "query": item.query,
        "match_type": item.match_type,
        "enabled": 1 if item.enabled else 0,
        "last_checked_at": item.last_checked_at.isoformat() if item.last_checked_at else None,
        "last_known_count": item.last_known_count,
        "new_count": item.new_count,
        "last_viewed_at": item.last_viewed_at.isoformat() if item.last_viewed_at else None,
        "created_at": item.created_at.isoformat() if item.created_at else None,
    }


def _home_stats_fallback(session, credential_count: object) -> dict[str, object]:
    # The stats worker is disabled in production (TELECRIME_DISABLE_STATS_WORKER=1),
    # so this fallback runs on every page load. Real COUNT(*) on messages /
    # attachments / artifacts / extraction_jobs are multi-million-row seq scans
    # that compete with the parse stage's bulk INSERT I/O — use pg_class
    # estimates for the big tables and real counts only for the small ones.
    estimates = _pg_fast_count_estimates(
        session,
        "messages",
        "file_attachments",
        "download_artifacts",
        "extraction_jobs",
    )
    return {
        "conversations": session.query(Conversation).count(),
        "messages": estimates.get("messages") or 0,
        "attachments": estimates.get("file_attachments") or 0,
        "archives": estimates.get("download_artifacts") or 0,
        "archive_groups": session.query(ArchiveGroup).count(),
        "extractions": estimates.get("extraction_jobs") or 0,
        "extracted_outputs": 0,
        "credentials": credential_count or 0,
        "channels": session.query(TelegramChannel).count(),
    }


def _hours_between(ts1, ts2):
    """SQLAlchemy expression for (ts1 - ts2) in hours (PostgreSQL)."""
    return extract("EPOCH", ts1 - ts2) / 3600.0


def _compute_stats_payload(engine, days: int, limit: int) -> dict[str, object]:
    since: datetime | None
    if days == 0:
        since = None
    else:
        since = datetime.now(UTC) - timedelta(days=days)

    with get_session(engine) as session:
        # Prevent any single stats query from stalling for more than 90 s.
        # Stats queries are GROUP BY / COUNT scans on large tables; without a cap
        # they can run for minutes during parse and compete with bulk INSERT I/O.
        session.execute(text("SET LOCAL statement_timeout = '90s'"))
        excluded_conversations, excluded_channels = _get_exclusions(session)
        excluded_platform_ids = []
        if excluded_conversations:
            excluded_platform_ids = [
                row[0]
                for row in session.query(Conversation.platform_id)
                .filter(Conversation.id.in_(excluded_conversations))
                .all()
                if row[0] is not None
            ]
        message_filters = []
        if since is not None:
            message_filters.append(Message.platform_timestamp >= since)
        if excluded_conversations:
            message_filters.append(Message.conversation_id.notin_(excluded_conversations))
        credential_filters = []
        if since is not None:
            credential_filters.append(ParsedCredential.created_at >= since)
        if excluded_conversations:
            credential_filters.append(
                ParsedCredential.source_conversation_id.notin_(excluded_conversations)
            )

        top_by_messages_query = session.query(
            Conversation.id,
            Conversation.title,
            Conversation.username,
            func.count(Message.id).label("count"),
        ).join(Message, Message.conversation_id == Conversation.id)
        if message_filters:
            top_by_messages_query = top_by_messages_query.filter(*message_filters)
        top_by_messages = (
            top_by_messages_query.group_by(Conversation.id)
            .order_by(func.count(Message.id).desc())
            .limit(limit)
            .all()
        )

        top_by_attachments_query = (
            session.query(
                Conversation.id,
                Conversation.title,
                Conversation.username,
                func.count(FileAttachment.id).label("count"),
            )
            .join(Message, Message.conversation_id == Conversation.id)
            .join(FileAttachment, FileAttachment.message_id == Message.id)
        )
        if message_filters:
            top_by_attachments_query = top_by_attachments_query.filter(*message_filters)
        top_by_attachments = (
            top_by_attachments_query.group_by(Conversation.id)
            .order_by(func.count(FileAttachment.id).desc())
            .limit(limit)
            .all()
        )

        top_by_archives_query = (
            session.query(
                Conversation.id,
                Conversation.title,
                Conversation.username,
                func.count(FileAttachment.id).label("count"),
            )
            .join(Message, Message.conversation_id == Conversation.id)
            .join(FileAttachment, FileAttachment.message_id == Message.id)
            .filter(FileAttachment.is_archive_candidate == True)
        )
        if message_filters:
            top_by_archives_query = top_by_archives_query.filter(*message_filters)
        top_by_archives = (
            top_by_archives_query.group_by(Conversation.id)
            .order_by(func.count(FileAttachment.id).desc())
            .limit(limit)
            .all()
        )

        top_by_credentials_query = session.query(
            Conversation.id,
            Conversation.title,
            Conversation.username,
            func.count(ParsedCredential.id).label("count"),
        ).join(ParsedCredential, ParsedCredential.source_conversation_id == Conversation.id)
        if credential_filters:
            top_by_credentials_query = top_by_credentials_query.filter(*credential_filters)
        top_by_credentials = (
            top_by_credentials_query.group_by(Conversation.id)
            .order_by(func.count(ParsedCredential.id).desc())
            .limit(limit)
            .all()
        )

        top_forwarded_sources_query = session.query(
            Message.forwarded_from_id,
            func.count(Message.id).label("count"),
        ).filter(Message.is_forwarded == True)
        if message_filters:
            top_forwarded_sources_query = top_forwarded_sources_query.filter(*message_filters)
        if excluded_platform_ids:
            top_forwarded_sources_query = top_forwarded_sources_query.filter(
                Message.forwarded_from_id.notin_(excluded_platform_ids)
            )
        top_forwarded_sources = (
            top_forwarded_sources_query.group_by(Message.forwarded_from_id)
            .order_by(func.count(Message.id).desc())
            .limit(limit)
            .all()
        )

        source_lookup = {}
        if top_forwarded_sources:
            source_ids = [row[0] for row in top_forwarded_sources if row[0] is not None]
            if source_ids:
                for conv in (
                    session.query(Conversation)
                    .filter(Conversation.platform_id.in_(source_ids))
                    .all()
                ):
                    source_lookup[conv.platform_id] = conv

        top_forwarded_sources = [
            {
                "platform_id": row[0],
                "count": row[1],
                "title": source_lookup.get(row[0]).title if row[0] in source_lookup else None,
                "username": source_lookup.get(row[0]).username if row[0] in source_lookup else None,
            }
            for row in top_forwarded_sources
            if row[0] is not None
        ]

        top_forwarding_channels_query = (
            session.query(
                Conversation.id,
                Conversation.title,
                Conversation.username,
                func.count(Message.id).label("count"),
            )
            .join(Message, Message.conversation_id == Conversation.id)
            .filter(Message.is_forwarded == True)
        )
        if message_filters:
            top_forwarding_channels_query = top_forwarding_channels_query.filter(*message_filters)
        top_forwarding_channels = (
            top_forwarding_channels_query.group_by(Conversation.id)
            .order_by(func.count(Message.id).desc())
            .limit(limit)
            .all()
        )

        most_diverse_sources_query = (
            session.query(
                Conversation.id,
                Conversation.title,
                Conversation.username,
                func.count(func.distinct(Message.forwarded_from_id)).label("count"),
            )
            .join(Message, Message.conversation_id == Conversation.id)
            .filter(Message.is_forwarded == True)
            .filter(Message.forwarded_from_id.isnot(None))
        )
        if message_filters:
            most_diverse_sources_query = most_diverse_sources_query.filter(*message_filters)
        most_diverse_sources = (
            most_diverse_sources_query.group_by(Conversation.id)
            .order_by(func.count(func.distinct(Message.forwarded_from_id)).desc())
            .limit(limit)
            .all()
        )

        top_amplified_sources_query = (
            session.query(
                Conversation.id,
                Conversation.title,
                Conversation.username,
                func.count(Message.id).label("count"),
            )
            .join(Message, Message.forwarded_from_id == Conversation.platform_id)
            .filter(Message.is_forwarded == True)
        )
        if message_filters:
            top_amplified_sources_query = top_amplified_sources_query.filter(*message_filters)
        if excluded_conversations:
            top_amplified_sources_query = top_amplified_sources_query.filter(
                Conversation.id.notin_(excluded_conversations)
            )
        top_amplified_sources = (
            top_amplified_sources_query.group_by(Conversation.id)
            .order_by(func.count(Message.id).desc())
            .limit(limit)
            .all()
        )

        daily_messages_query = session.query(
            func.date(Message.platform_timestamp).label("day"),
            func.count(Message.id).label("count"),
        )
        if message_filters:
            daily_messages_query = daily_messages_query.filter(*message_filters)
        daily_messages_raw = (
            daily_messages_query.group_by(func.date(Message.platform_timestamp))
            .order_by(func.date(Message.platform_timestamp).desc())
            .limit(30)
            .all()
        )

        daily_credentials_query = session.query(
            func.date(ParsedCredential.created_at).label("day"),
            func.count(ParsedCredential.id).label("count"),
        )
        if credential_filters:
            daily_credentials_query = daily_credentials_query.filter(*credential_filters)
        daily_credentials_raw = (
            daily_credentials_query.group_by(func.date(ParsedCredential.created_at))
            .order_by(func.date(ParsedCredential.created_at).desc())
            .limit(30)
            .all()
        )

        def _series(rows):
            if not rows:
                return []
            max_count = max(row.count for row in rows)
            series = []
            for row in reversed(rows):
                pct = 0 if max_count == 0 else int((row.count / max_count) * 100)
                series.append({"day": row.day, "count": row.count, "pct": pct})
            return series

        daily_messages = _series(daily_messages_raw)
        daily_credentials = _series(daily_credentials_raw)

        forwarding_edges_query = (
            session.query(
                Conversation.id.label("dest_id"),
                Conversation.title.label("dest_title"),
                Conversation.username.label("dest_username"),
                Message.forwarded_from_id.label("source_platform_id"),
                func.count(Message.id).label("count"),
            )
            .join(Message, Message.conversation_id == Conversation.id)
            .filter(Message.is_forwarded == True)
            .filter(Message.forwarded_from_id.isnot(None))
        )
        if message_filters:
            forwarding_edges_query = forwarding_edges_query.filter(*message_filters)
        if excluded_platform_ids:
            forwarding_edges_query = forwarding_edges_query.filter(
                Message.forwarded_from_id.notin_(excluded_platform_ids)
            )
        forwarding_edges = (
            forwarding_edges_query.group_by(Conversation.id, Message.forwarded_from_id)
            .order_by(func.count(Message.id).desc())
            .limit(limit)
            .all()
        )

        source_ids = [row.source_platform_id for row in forwarding_edges]
        source_lookup = {
            conv.platform_id: conv
            for conv in session.query(Conversation)
            .filter(Conversation.platform_id.in_(source_ids))
            .all()
        }
        forwarding_matrix = [
            {
                "source_title": source_lookup.get(row.source_platform_id).title
                if row.source_platform_id in source_lookup
                else None,
                "source_username": source_lookup.get(row.source_platform_id).username
                if row.source_platform_id in source_lookup
                else None,
                "source_platform_id": row.source_platform_id,
                "dest_title": row.dest_title,
                "dest_username": row.dest_username,
                "dest_id": row.dest_id,
                "count": row.count,
            }
            for row in forwarding_edges
        ]

        archive_stats_query = (
            session.query(
                Conversation.id.label("conv_id"),
                Conversation.title.label("title"),
                Conversation.username.label("username"),
                func.count(func.distinct(FileAttachment.id)).label("archives"),
                func.count(
                    func.distinct(
                        case(
                            (
                                DownloadArtifact.status == DownloadStatus.COMPLETED,
                                DownloadArtifact.id,
                            ),
                            else_=None,
                        )
                    )
                ).label("downloads_completed"),
                func.count(
                    func.distinct(
                        case(
                            (ExtractionJob.status == ExtractionStatus.COMPLETED, ExtractionJob.id),
                            else_=None,
                        )
                    )
                ).label("extractions_completed"),
            )
            .join(Message, Message.conversation_id == Conversation.id)
            .join(FileAttachment, FileAttachment.message_id == Message.id)
            .outerjoin(DownloadArtifact, DownloadArtifact.attachment_id == FileAttachment.id)
            .outerjoin(ArchiveGroupPart, ArchiveGroupPart.artifact_id == DownloadArtifact.id)
            .outerjoin(ArchiveGroup, ArchiveGroup.id == ArchiveGroupPart.group_id)
            .outerjoin(ExtractionJob, ExtractionJob.group_id == ArchiveGroup.id)
            .filter(FileAttachment.is_archive_candidate == True)
        )
        if message_filters:
            archive_stats_query = archive_stats_query.filter(*message_filters)
        archive_stats_raw = (
            archive_stats_query.group_by(Conversation.id)
            .order_by(func.count(func.distinct(FileAttachment.id)).desc())
            .limit(limit)
            .all()
        )

        archive_stats = []
        for row in archive_stats_raw:
            archives = row.archives or 0
            downloads = row.downloads_completed or 0
            extractions = row.extractions_completed or 0
            download_rate = 0 if archives == 0 else int((downloads / archives) * 100)
            extraction_rate = 0 if archives == 0 else int((extractions / archives) * 100)
            archive_stats.append(
                {
                    "id": row.conv_id,
                    "title": row.title,
                    "username": row.username,
                    "archives": archives,
                    "downloads": downloads,
                    "extractions": extractions,
                    "download_rate": download_rate,
                    "extraction_rate": extraction_rate,
                }
            )

        repost_downloads_query = (
            session.query(
                Conversation.id.label("conv_id"),
                Conversation.title.label("title"),
                Conversation.username.label("username"),
                func.count(DownloadArtifact.id).label("total"),
                func.count(
                    case(
                        (
                            FirstSeenIndex.first_seen_conversation_id != Conversation.id,
                            DownloadArtifact.id,
                        ),
                        else_=None,
                    )
                ).label("reposts"),
            )
            .join(Message, Message.conversation_id == Conversation.id)
            .join(FileAttachment, FileAttachment.message_id == Message.id)
            .join(DownloadArtifact, DownloadArtifact.attachment_id == FileAttachment.id)
            .join(
                FirstSeenIndex,
                and_(
                    FirstSeenIndex.content_hash == DownloadArtifact.content_hash,
                    FirstSeenIndex.content_type == "download",
                ),
            )
        )
        if message_filters:
            repost_downloads_query = repost_downloads_query.filter(*message_filters)
        repost_downloads = (
            repost_downloads_query.group_by(Conversation.id)
            .order_by(func.count(DownloadArtifact.id).desc())
            .limit(limit)
            .all()
        )

        repost_extracted_q = (
            session.query(
                Conversation.id.label("conv_id"),
                Conversation.title.label("title"),
                Conversation.username.label("username"),
                func.count(ExtractedOutput.id).label("total"),
                func.count(
                    case(
                        (
                            FirstSeenIndex.first_seen_conversation_id != Conversation.id,
                            ExtractedOutput.id,
                        ),
                        else_=None,
                    )
                ).label("reposts"),
            )
            .join(ExtractedOutput, ExtractedOutput.source_conversation_id == Conversation.id)
            .join(
                FirstSeenIndex,
                and_(
                    FirstSeenIndex.content_hash == ExtractedOutput.output_hash,
                    FirstSeenIndex.content_type == "extracted",
                ),
            )
            .filter(ExtractedOutput.source_conversation_id.isnot(None))
        )
        if since is not None:
            repost_extracted_q = repost_extracted_q.filter(ExtractedOutput.created_at >= since)
        repost_extracted = (
            repost_extracted_q
            .group_by(Conversation.id)
            .order_by(func.count(ExtractedOutput.id).desc())
            .limit(limit)
            .all()
        )

        def _rate_rows(rows):
            data = []
            for row in rows:
                total = row.total or 0
                reposts = row.reposts or 0
                rate = 0 if total == 0 else int((reposts / total) * 100)
                data.append(
                    {
                        "id": row.conv_id,
                        "title": row.title,
                        "username": row.username,
                        "total": total,
                        "reposts": reposts,
                        "rate": rate,
                    }
                )
            return data

        repost_downloads = _rate_rows(repost_downloads)
        repost_extracted = _rate_rows(repost_extracted)

        failed_downloads_query = (
            session.query(
                Conversation.id,
                Conversation.title,
                Conversation.username,
                func.count(DownloadArtifact.id).label("count"),
            )
            .join(Message, Message.conversation_id == Conversation.id)
            .join(FileAttachment, FileAttachment.message_id == Message.id)
            .join(DownloadArtifact, DownloadArtifact.attachment_id == FileAttachment.id)
            .filter(
                DownloadArtifact.status.in_([DownloadStatus.FAILED, DownloadStatus.FAILED_TERMINAL])
            )
        )
        if message_filters:
            failed_downloads_query = failed_downloads_query.filter(*message_filters)
        failed_downloads = (
            failed_downloads_query.group_by(Conversation.id)
            .order_by(func.count(DownloadArtifact.id).desc())
            .limit(limit)
            .all()
        )

        failed_extractions_query = (
            session.query(
                Conversation.id,
                Conversation.title,
                Conversation.username,
                func.count(ExtractionJob.id).label("count"),
            )
            .join(Message, Message.conversation_id == Conversation.id)
            .join(FileAttachment, FileAttachment.message_id == Message.id)
            .join(DownloadArtifact, DownloadArtifact.attachment_id == FileAttachment.id)
            .join(ArchiveGroupPart, ArchiveGroupPart.artifact_id == DownloadArtifact.id)
            .join(ArchiveGroup, ArchiveGroup.id == ArchiveGroupPart.group_id)
            .join(ExtractionJob, ExtractionJob.group_id == ArchiveGroup.id)
            .filter(
                ExtractionJob.status.in_(
                    [ExtractionStatus.FAILED, ExtractionStatus.FAILED_TERMINAL]
                )
            )
        )
        if message_filters:
            failed_extractions_query = failed_extractions_query.filter(*message_filters)
        failed_extractions = (
            failed_extractions_query.group_by(Conversation.id)
            .order_by(func.count(ExtractionJob.id).desc())
            .limit(limit)
            .all()
        )

        _dl_hours = _hours_between(DownloadArtifact.updated_at, Message.platform_timestamp)
        avg_download_latency_query = (
            session.query(
                Conversation.id,
                Conversation.title,
                Conversation.username,
                func.avg(_dl_hours).label("hours"),
            )
            .join(Message, Message.conversation_id == Conversation.id)
            .join(FileAttachment, FileAttachment.message_id == Message.id)
            .join(DownloadArtifact, DownloadArtifact.attachment_id == FileAttachment.id)
            .filter(DownloadArtifact.status == DownloadStatus.COMPLETED)
        )
        if message_filters:
            avg_download_latency_query = avg_download_latency_query.filter(*message_filters)
        avg_download_latency = (
            avg_download_latency_query.group_by(Conversation.id)
            .order_by(func.avg(_dl_hours).desc())
            .limit(limit)
            .all()
        )

        _ex_hours = _hours_between(ExtractionJob.updated_at, Message.platform_timestamp)
        avg_extraction_latency_query = (
            session.query(
                Conversation.id,
                Conversation.title,
                Conversation.username,
                func.avg(_ex_hours).label("hours"),
            )
            .join(Message, Message.conversation_id == Conversation.id)
            .join(FileAttachment, FileAttachment.message_id == Message.id)
            .join(DownloadArtifact, DownloadArtifact.attachment_id == FileAttachment.id)
            .join(ArchiveGroupPart, ArchiveGroupPart.artifact_id == DownloadArtifact.id)
            .join(ArchiveGroup, ArchiveGroup.id == ArchiveGroupPart.group_id)
            .join(ExtractionJob, ExtractionJob.group_id == ArchiveGroup.id)
            .filter(ExtractionJob.status == ExtractionStatus.COMPLETED)
        )
        if message_filters:
            avg_extraction_latency_query = avg_extraction_latency_query.filter(*message_filters)
        avg_extraction_latency = (
            avg_extraction_latency_query.group_by(Conversation.id)
            .order_by(func.avg(_ex_hours).desc())
            .limit(limit)
            .all()
        )

        top_domains_query = session.query(
            ParsedCredential.domain,
            func.count(ParsedCredential.id).label("count"),
        ).filter(ParsedCredential.domain.isnot(None))
        if credential_filters:
            top_domains_query = top_domains_query.filter(*credential_filters)
        top_domains = (
            top_domains_query.group_by(ParsedCredential.domain)
            .order_by(func.count(ParsedCredential.id).desc())
            .limit(limit)
            .all()
        )

        top_stealers_query = session.query(
            ParsedCredential.stealer_type,
            func.count(ParsedCredential.id).label("count"),
        ).filter(ParsedCredential.stealer_type.isnot(None))
        if credential_filters:
            top_stealers_query = top_stealers_query.filter(*credential_filters)
        top_stealers = (
            top_stealers_query.group_by(ParsedCredential.stealer_type)
            .order_by(func.count(ParsedCredential.id).desc())
            .limit(limit)
            .all()
        )

        day_bucket = func.date(Message.platform_timestamp)
        channel_daily_query = session.query(
            Conversation.id.label("conv_id"),
            Conversation.title.label("title"),
            Conversation.username.label("username"),
            day_bucket.label("day"),
            func.count(Message.id).label("count"),
        ).join(Message, Message.conversation_id == Conversation.id)
        if message_filters:
            channel_daily_query = channel_daily_query.filter(*message_filters)
        channel_daily = (
            channel_daily_query.group_by(Conversation.id, day_bucket)
            .order_by(day_bucket.desc())
            .all()
        )

        spikes = {}
        for row in channel_daily:
            key = (row.conv_id, row.title, row.username)
            spikes.setdefault(key, []).append(row.count)
        bursty_channels = []
        for key, counts in spikes.items():
            avg = sum(counts) / max(len(counts), 1)
            max_day = max(counts)
            ratio = 0 if avg == 0 else max_day / avg
            if ratio >= 3 and max_day >= 10:
                bursty_channels.append(
                    {
                        "id": key[0],
                        "title": key[1],
                        "username": key[2],
                        "max_day": max_day,
                        "avg_day": round(avg, 2),
                        "ratio": round(ratio, 2),
                    }
                )
        bursty_channels.sort(key=lambda x: x["ratio"], reverse=True)
        bursty_channels = bursty_channels[:limit]

        funnel_query = (
            session.query(
                Conversation.id.label("conv_id"),
                Conversation.title.label("title"),
                Conversation.username.label("username"),
                func.count(func.distinct(FileAttachment.id)).label("discovered"),
                func.count(
                    func.distinct(
                        case(
                            (
                                DownloadArtifact.status == DownloadStatus.COMPLETED,
                                DownloadArtifact.id,
                            ),
                            else_=None,
                        )
                    )
                ).label("downloaded"),
                func.count(
                    func.distinct(
                        case(
                            (ExtractionJob.status == ExtractionStatus.COMPLETED, ExtractionJob.id),
                            else_=None,
                        )
                    )
                ).label("extracted"),
            )
            .join(Message, Message.conversation_id == Conversation.id)
            .join(FileAttachment, FileAttachment.message_id == Message.id)
            .outerjoin(DownloadArtifact, DownloadArtifact.attachment_id == FileAttachment.id)
            .outerjoin(ArchiveGroupPart, ArchiveGroupPart.artifact_id == DownloadArtifact.id)
            .outerjoin(ArchiveGroup, ArchiveGroup.id == ArchiveGroupPart.group_id)
            .outerjoin(ExtractionJob, ExtractionJob.group_id == ArchiveGroup.id)
            .filter(FileAttachment.is_archive_candidate == True)
        )
        if message_filters:
            funnel_query = funnel_query.filter(*message_filters)
        funnel = (
            funnel_query.group_by(Conversation.id)
            .order_by(func.count(func.distinct(FileAttachment.id)).desc())
            .limit(limit)
            .all()
        )

        channel_insights = (
            session.query(
                TelegramChannel.id,
                TelegramChannel.username,
                TelegramChannel.title,
                TelegramChannel.platform_id,
                TelegramChannel.discovered_at,
                TelegramChannel.is_active,
                TelegramChannel.is_accessible,
                TelegramChannel.source,
                TelegramChannel.last_checked,
            )
            .order_by(TelegramChannel.discovered_at.desc())
            .limit(limit)
            .all()
        )
        if excluded_channels:
            channel_insights = [row for row in channel_insights if row.id not in excluded_channels]

        deleted_channels = (
            session.query(
                TelegramChannel.id,
                TelegramChannel.username,
                TelegramChannel.title,
                TelegramChannel.platform_id,
                TelegramChannel.discovered_at,
                TelegramChannel.last_checked,
                TelegramChannel.check_error,
            )
            .filter(TelegramChannel.is_active == False)
            .order_by(TelegramChannel.discovered_at.desc())
            .limit(limit)
            .all()
        )
        if excluded_channels:
            deleted_channels = [row for row in deleted_channels if row.id not in excluded_channels]

        # --- top_countries (from SystemInfoRecord) ---
        top_countries_q = session.query(
            SystemInfoRecord.country,
            func.count(SystemInfoRecord.id).label("count"),
        ).filter(SystemInfoRecord.country.isnot(None))
        if since is not None:
            top_countries_q = top_countries_q.filter(SystemInfoRecord.created_at >= since)
        top_countries = [
            (row.country, row.count)
            for row in top_countries_q.group_by(SystemInfoRecord.country)
            .order_by(func.count(SystemInfoRecord.id).desc())
            .limit(limit)
            .all()
        ]

        # --- dork_channels (channels discovered via DuckDuckGo dorking) ---
        dork_channels = (
            session.query(TelegramChannel)
            .filter(TelegramChannel.source == "dork")
            .order_by(TelegramChannel.discovered_at.desc())
            .limit(limit)
            .all()
        )

        payload = {
            "days": days,
            "limit": limit,
            "top_by_messages": top_by_messages,
            "top_by_attachments": top_by_attachments,
            "top_by_archives": top_by_archives,
            "top_by_credentials": top_by_credentials,
            "top_forwarded_sources": top_forwarded_sources,
            "top_forwarding_channels": top_forwarding_channels,
            "most_diverse_sources": most_diverse_sources,
            "top_amplified_sources": top_amplified_sources,
            "daily_messages": daily_messages,
            "daily_credentials": daily_credentials,
            "forwarding_matrix": forwarding_matrix,
            "archive_stats": archive_stats,
            "repost_downloads": repost_downloads,
            "repost_extracted": repost_extracted,
            "failed_downloads": failed_downloads,
            "failed_extractions": failed_extractions,
            "avg_download_latency": avg_download_latency,
            "avg_extraction_latency": avg_extraction_latency,
            "top_domains": top_domains,
            "top_stealers": top_stealers,
            "bursty_channels": bursty_channels,
            "funnel": funnel,
            "channel_insights": channel_insights,
            "deleted_channels": deleted_channels,
            "top_countries": top_countries,
            "dork_channels": dork_channels,
        }

    return payload


def create_app(database_url: str | None = None) -> FastAPI:
    """Create FastAPI app bound to the Telecrime database."""
    engine = get_engine(database_url)
    # str(engine.url) masks the password; background workers must receive a
    # URL they can actually connect with when create_app() is called without
    # an explicit database_url.
    worker_database_url = database_url or engine.url.render_as_string(hide_password=False)

    @asynccontextmanager
    async def _lifespan(app: FastAPI):
        if not os.environ.get("TELECRIME_DISABLE_STATS_WORKER") == "1":
            stats_worker = threading.Thread(
                target=_stats_worker,
                args=(worker_database_url, app.state.stats_presets, 600),
                daemon=True,
            )
            stats_worker.start()
            home_worker = threading.Thread(
                target=_home_worker,
                args=(worker_database_url, 600),
                daemon=True,
            )
            home_worker.start()
            watchlist_thread = threading.Thread(
                target=_watchlist_worker,
                args=(worker_database_url, 1800),
                daemon=True,
            )
            watchlist_thread.start()
        yield

    app = FastAPI(title="Telecrime Dashboard", lifespan=_lifespan)
    templates = Jinja2Templates(directory=str(_templates_dir()))
    _static_dir = Path(__file__).parent / "static"
    if _static_dir.exists():
        app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")
    app.state.fts_enabled = _ensure_search_infra(engine)
    threading.Thread(target=_ensure_stats_indexes, args=(engine,), daemon=True).start()
    app.state.stats_presets = [(7, 20), (30, 20), (90, 20), (0, 20)]

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request):
        with get_session(engine) as session:
            excluded_conversations, excluded_channels = _get_exclusions(session)
            cached_home = (
                _home_cache() if not excluded_conversations and not excluded_channels else None
            )
            cached_stats = cached_home.get("stats") if cached_home else None
            stats = None
            if excluded_conversations or excluded_channels:
                stats = {
                    "conversations": session.query(Conversation)
                    .filter(Conversation.id.notin_(excluded_conversations))
                    .count()
                    if excluded_conversations
                    else session.query(Conversation).count(),
                    "messages": session.query(Message)
                    .filter(Message.conversation_id.notin_(excluded_conversations))
                    .count()
                    if excluded_conversations
                    else session.query(Message).count(),
                    "attachments": session.query(FileAttachment)
                    .join(Message, Message.id == FileAttachment.message_id)
                    .filter(Message.conversation_id.notin_(excluded_conversations))
                    .count()
                    if excluded_conversations
                    else session.query(FileAttachment).count(),
                    "archives": session.query(DownloadArtifact)
                    .join(FileAttachment, FileAttachment.id == DownloadArtifact.attachment_id)
                    .join(Message, Message.id == FileAttachment.message_id)
                    .filter(Message.conversation_id.notin_(excluded_conversations))
                    .count()
                    if excluded_conversations
                    else session.query(DownloadArtifact).count(),
                    "archive_groups": session.query(ArchiveGroup).count(),
                    "extractions": session.query(ExtractionJob).count(),
                    "extracted_outputs": session.query(ExtractedOutput)
                    .filter(ExtractedOutput.source_conversation_id.notin_(excluded_conversations))
                    .count()
                    if excluded_conversations
                    else session.query(ExtractedOutput).count(),
                    "credentials": session.query(ParsedCredential)
                    .filter(ParsedCredential.source_conversation_id.notin_(excluded_conversations))
                    .count()
                    if excluded_conversations
                    else session.query(ParsedCredential).count(),
                    "channels": session.query(TelegramChannel)
                    .filter(TelegramChannel.id.notin_(excluded_channels))
                    .count()
                    if excluded_channels
                    else session.query(TelegramChannel).count(),
                }
            elif isinstance(cached_stats, dict):
                stats = dict(cached_stats)
            else:
                stats = _home_stats_fallback(session, 0)

            # The cache only refreshes when the pipeline is idle, so on a busy
            # ingest it can lag by days. Use the larger fast estimate because
            # pg_stat counters can undercount after a database/container restart.
            if not excluded_conversations and not excluded_channels:
                try:
                    estimates = _pg_fast_count_estimates(session, "parsed_credentials", "messages")
                    if estimates.get("parsed_credentials"):
                        stats["credentials"] = estimates["parsed_credentials"]
                    if estimates.get("messages"):
                        stats["messages"] = estimates["messages"]
                except Exception:
                    pass

            recent_credentials_query = session.query(ParsedCredential)
            if excluded_conversations:
                recent_credentials_query = recent_credentials_query.filter(
                    ParsedCredential.source_conversation_id.notin_(excluded_conversations)
                )
            recent_credentials = (
                recent_credentials_query.order_by(ParsedCredential.created_at.desc())
                .limit(20)
                .all()
            )
            recent_runs = (
                session.query(PipelineRun).order_by(PipelineRun.started_at.desc()).limit(8).all()
            )
            recent_runs = [
                {
                    "id": run.id,
                    "mode": run.mode,
                    "status": run.status,
                    "started_at": run.started_at,
                    "duration_seconds": run.duration_seconds or 0,
                    "credentials_parsed": run.credentials_parsed,
                    "error_count": _errors_json_count(run.errors_json),
                }
                for run in recent_runs
            ]

            daily_credentials_14d: list[dict[str, object]] = []
            if excluded_conversations:
                since_14d = datetime.now(UTC) - timedelta(days=14)
                trend_query = session.query(
                    func.date(ParsedCredential.created_at).label("day"),
                    func.count(ParsedCredential.id).label("count"),
                ).filter(ParsedCredential.created_at >= since_14d)
                trend_query = trend_query.filter(
                    ParsedCredential.source_conversation_id.notin_(excluded_conversations)
                )
                daily_credentials_14d = [
                    {"day": str(row.day), "count": row.count}
                    for row in trend_query.group_by(func.date(ParsedCredential.created_at))
                    .order_by(func.date(ParsedCredential.created_at).asc())
                    .all()
                ]
            elif cached_home and isinstance(cached_home.get("daily_credentials_14d"), list):
                daily_credentials_14d = cast(
                    list[dict[str, object]], cached_home["daily_credentials_14d"]
                )

            # Watchlist alerts
            watchlist_alerts = []
            try:
                with get_session(engine) as _wsess:
                    watchlist_alerts = [
                        _witem_dict(i)
                        for i in _wsess.query(WatchlistItem)
                        .filter(WatchlistItem.enabled == True, WatchlistItem.new_count > 0)
                        .order_by(WatchlistItem.new_count.desc())
                        .limit(10)
                        .all()
                    ]
            except Exception:
                pass

            # Pipeline status summary
            last_run = recent_runs[0] if recent_runs else None

            return templates.TemplateResponse(
                "index.html",
                {
                    "request": request,
                    "stats": stats,
                    "recent_credentials": recent_credentials,
                    "recent_runs": recent_runs,
                    "daily_credentials_14d": daily_credentials_14d,
                    "watchlist_alerts": watchlist_alerts,
                    "last_run": last_run,
                },
            )

    _ops_cache: dict = {"ts": 0.0, "html": None}
    _ops_cache_lock = threading.Lock()
    # Background cache for the slow credential-count JOIN (229M+ rows).
    # Refreshed in a daemon thread so it never blocks a request.
    _cred_counts_cache: dict = {"ts": 0.0, "data": {}, "lock": None}
    _cred_counts_cache["lock"] = threading.Lock()

    def _refresh_cred_counts_bg(group_ids: list[int]) -> None:
        """Run the slow COUNT JOIN in a daemon thread; update _cred_counts_cache."""
        try:
            with get_session(engine) as s:
                if engine.dialect.name == "postgresql":
                    s.execute(text("SET LOCAL statement_timeout = '120s'"))
                rows = s.execute(
                    select(ExtractionJob.group_id, func.count(ParsedCredential.id).label("creds"))
                    .join(ParsedCredential, ParsedCredential.extraction_job_id == ExtractionJob.id)
                    .where(ExtractionJob.group_id.in_(group_ids))
                    .group_by(ExtractionJob.group_id)
                ).all()
                result = dict(rows)
        except Exception:
            result = {}
        with _cred_counts_cache["lock"]:
            _cred_counts_cache["data"] = result
            _cred_counts_cache["ts"] = time.monotonic()

    @app.get("/api/home/ops-fragment", response_class=HTMLResponse)
    def home_ops_fragment(request: Request):
        # Serve cached response to avoid hammering the DB on every 5-second HTMX poll.
        # Extend TTL to 120s when pipeline is running: CLEANED group counts are frozen
        # and bulk INSERT I/O competes badly with COUNT/JOIN scans on the 165M-row table.
        now = time.monotonic()
        cache_ttl = 120 if _pipeline_running_for_heavy_web_work() else 15
        with _ops_cache_lock:
            if _ops_cache["html"] is not None and now - _ops_cache["ts"] < cache_ttl:
                return HTMLResponse(_ops_cache["html"])
        with get_session(engine) as session:
            # Queue counts by status
            queue_rows = session.execute(
                select(ArchiveGroup.status, func.count(ArchiveGroup.id))
                .group_by(ArchiveGroup.status)
            ).all()
            queue = {row[0]: row[1] for row in queue_rows}

            # Active download
            downloading = session.execute(
                select(FileAttachment.filename, FileAttachment.size, DownloadArtifact.updated_at)
                .join(DownloadArtifact, DownloadArtifact.attachment_id == FileAttachment.id)
                .where(DownloadArtifact.status == DownloadStatus.DOWNLOADING)
                .limit(1)
            ).first()

            # Progress from pipeline file
            progress = read_progress() or {}
            is_running = bool(progress.get("running", False))
            progress_age_seconds = _iso_age_seconds(progress.get("updated_at"))
            progress_stale = bool(
                is_running
                and progress_age_seconds is not None
                and progress_age_seconds > 120
            )
            dl_pct = int(progress.get("dl_pct") or 0)
            dl_speed = progress.get("dl_speed") or progress.get("download_speed_mbps") or 0
            current_archive = progress.get("current_archive", "")
            current_stage = progress.get("current_stage", "")
            downloading_filename = downloading.filename if downloading else None
            progress_counter_stale = bool(
                is_running
                and downloading_filename
                and current_archive
                and downloading_filename != current_archive
            )
            runtime_note = progress.get("runtime_note", "")
            runtime_note_kind = progress.get("runtime_note_kind", "")
            archive_index = progress.get("archive_index", 0)
            archive_total = progress.get("archive_total", 0)
            recent_results = progress.get("recent_results", [])
            run_creds = int(progress.get("credentials") or 0)
            live_creds_10m = 0
            latest_credential_at = None
            if progress_stale or progress_counter_stale:
                live_row = session.execute(
                    select(
                        func.count(ParsedCredential.id),
                        func.max(ParsedCredential.created_at),
                    ).where(
                        ParsedCredential.created_at
                        >= datetime.now(UTC) - timedelta(minutes=10)
                    )
                ).first()
                if live_row:
                    live_creds_10m = int(live_row[0] or 0)
                    latest_credential_at = live_row[1]

            # Recent CLEANED groups — credential_count is denormalized at finalize time,
            # so no JOIN needed. Groups cleaned before this feature was added show 0
            # (covered by background cache fallback below).
            recent_groups = session.execute(
                select(
                    ArchiveGroup.id,
                    ArchiveGroup.base_name,
                    ArchiveGroup.updated_at,
                    ArchiveGroup.credential_count,
                )
                .where(ArchiveGroup.status == GroupStatus.CLEANED)
                .order_by(ArchiveGroup.updated_at.desc())
                .limit(8)
            ).all()
            if recent_groups:
                group_ids = [r.id for r in recent_groups]
                # Refresh background cache only for old groups (credential_count == 0)
                # that were cleaned before the denormalized column was added.
                with _cred_counts_cache["lock"]:
                    cred_counts = _cred_counts_cache["data"]
                    counts_age = now - _cred_counts_cache["ts"]
                needs_refresh = any(r.credential_count == 0 for r in recent_groups)
                counts_refresh_ttl = 90 if not is_running else 300
                if needs_refresh and not is_running and counts_age > counts_refresh_ttl:
                    t = threading.Thread(
                        target=_refresh_cred_counts_bg,
                        args=(group_ids,),
                        daemon=True,
                    )
                    t.start()
                from collections import namedtuple as _nt
                _Row = _nt("_Row", ["base_name", "updated_at", "creds"])
                recent = [
                    _Row(
                        r.base_name,
                        r.updated_at,
                        r.credential_count if r.credential_count else cred_counts.get(r.id, 0),
                    )
                    for r in recent_groups
                ]
            else:
                recent = []

            # Channel stats
            ch_subscribed = session.query(Conversation).count()
            ch_active = session.execute(
                select(func.count(TelegramChannel.id))
                .where(TelegramChannel.is_active == True)
            ).scalar() or 0
            today = date.today()
            ch_joined_today = session.execute(
                select(func.count(Conversation.id))
                .where(func.date(Conversation.created_at) == today)
            ).scalar() or 0

            # Approximate aggregate counts without scanning the largest tables.
            counts = _pg_fast_count_estimates(session, "parsed_credentials", "messages")
            total_creds = counts.get("parsed_credentials", 0)
            total_msgs = counts.get("messages", 0)

            resp = templates.TemplateResponse(
                "partials/home_ops.html",
                {
                    "request": request,
                    "queue": queue,
                    "downloading": downloading,
                    "is_running": is_running,
                    "dl_pct": dl_pct,
                    "dl_speed": dl_speed,
                    "current_archive": current_archive,
                    "current_stage": current_stage,
                    "runtime_note": runtime_note,
                    "runtime_note_kind": runtime_note_kind,
                    "progress_stale": progress_stale,
                    "progress_counter_stale": progress_counter_stale,
                    "progress_age_seconds": int(progress_age_seconds or 0),
                    "live_creds_10m": live_creds_10m,
                    "latest_credential_at": latest_credential_at,
                    "archive_index": archive_index,
                    "archive_total": archive_total,
                    "recent_results": recent_results,
                    "run_creds": run_creds,
                    "recent": recent,
                    "ch_subscribed": ch_subscribed,
                    "ch_active": ch_active,
                    "ch_joined_today": ch_joined_today,
                    "total_creds": total_creds,
                    "total_msgs": total_msgs,
                },
            )
            with _ops_cache_lock:
                _ops_cache["html"] = resp.body
                _ops_cache["ts"] = time.monotonic()
            return resp

    @app.get("/triage", response_class=HTMLResponse)
    def triage(request: Request, limit: int = Query(50, ge=1, le=200)):
        with get_session(engine) as session:
            payload = _triage_payload(session, limit=limit)
        payload["request"] = request
        payload["limit"] = limit
        return templates.TemplateResponse("triage.html", payload)

    @app.post("/triage/retry/download/{artifact_id}")
    def retry_download(artifact_id: int):
        with get_session(engine) as session:
            artifact = session.get(DownloadArtifact, artifact_id)
            if not artifact:
                return JSONResponse({"error": "Not found"}, status_code=404)
            if artifact.status not in (DownloadStatus.FAILED, DownloadStatus.FAILED_TERMINAL):
                return JSONResponse({"error": "Not in a failed state"}, status_code=409)
            artifact.status = DownloadStatus.PENDING
            artifact.error_message = None
            # The artifact's group is terminal (FAILED_TERMINAL/CLEANED): the
            # pickers exclude those groups, so the retried artifact would sit
            # PENDING forever. Reopen the group for download.
            part = session.execute(
                select(ArchiveGroupPart).where(
                    ArchiveGroupPart.artifact_id == artifact.id
                )
            ).scalar_one_or_none()
            if part is not None:
                group = session.get(ArchiveGroup, part.group_id)
                if group is not None and group.status in (
                    GroupStatus.FAILED_TERMINAL,
                    GroupStatus.FAILED,
                ):
                    group.status = GroupStatus.INCOMPLETE
            session.commit()
        return JSONResponse({"ok": True})

    @app.post("/triage/retry/extraction/{job_id}")
    def retry_extraction(job_id: int):
        with get_session(engine) as session:
            job = session.get(ExtractionJob, job_id)
            if not job:
                return JSONResponse({"error": "Not found"}, status_code=404)
            if job.status not in (
                ExtractionStatus.FAILED,
                ExtractionStatus.FAILED_TERMINAL,
                ExtractionStatus.PASSWORD_NEEDED,
            ):
                return JSONResponse({"error": "Not in a failed state"}, status_code=409)
            job.status = ExtractionStatus.PENDING
            job.last_error_code = None
            job.last_error_message = None
            group = session.get(ArchiveGroup, job.group_id)
            if group:
                group.status = GroupStatus.READY
            session.commit()
        return JSONResponse({"ok": True})

    @app.post("/triage/add-password/{job_id}")
    async def triage_add_password(job_id: int, request: Request):
        form = await request.form()
        password = (form.get("password") or "").strip()
        if not password:
            return JSONResponse({"error": "Password is required"}, status_code=400)
        with get_session(engine) as session:
            job = session.get(ExtractionJob, job_id)
            if not job:
                return JSONResponse({"error": "Not found"}, status_code=404)
            # Insert password candidate scoped to the job's conversation
            candidate = PasswordCandidate(
                value=password,
                scope=PasswordScope.CONVERSATION if job.source_conversation_id else PasswordScope.GLOBAL,
                conversation_id=job.source_conversation_id,
                extraction_method="manual",
                context_text=f"Added manually from triage for job {job_id}",
                confidence=0.95,
            )
            session.add(candidate)
            # Reset job to retry
            job.status = ExtractionStatus.PENDING
            job.last_error_code = None
            job.last_error_message = None
            group = session.get(ArchiveGroup, job.group_id)
            if group:
                group.status = GroupStatus.READY
            session.commit()
        return JSONResponse({"ok": True})

    @app.get("/stats", response_class=HTMLResponse)
    def stats(
        request: Request,
        days: int = Query(30, ge=0, le=3650),
        limit: int = Query(20, ge=5, le=100),
    ):
        if (days, limit) not in app.state.stats_presets:
            days, limit = 30, 20

        cache: dict[str, object] | None = _read_stats_cache()
        cache_key = f"{days}:{limit}"
        cached = None
        last_updated = None
        if isinstance(cache, dict):
            cache_data = cache.get("data")
            if isinstance(cache_data, dict):
                cached = cache_data.get(cache_key)
            last_updated = cache.get("generated_at")

        if not cached:
            payload = {
                "days": days,
                "limit": limit,
                "top_by_messages": [],
                "top_by_attachments": [],
                "top_by_archives": [],
                "top_by_credentials": [],
                "top_forwarded_sources": [],
                "top_forwarding_channels": [],
                "most_diverse_sources": [],
                "top_amplified_sources": [],
                "daily_messages": [],
                "daily_credentials": [],
                "forwarding_matrix": [],
                "archive_stats": [],
                "repost_downloads": [],
                "repost_extracted": [],
                "failed_downloads": [],
                "failed_extractions": [],
                "avg_download_latency": [],
                "avg_extraction_latency": [],
                "top_domains": [],
                "top_stealers": [],
                "bursty_channels": [],
                "funnel": [],
                "channel_insights": [],
                "deleted_channels": [],
                "top_countries": [],
                "dork_channels": [],
                "stats_note": "Stats are warming up. Please refresh in a minute.",
                "last_updated": None,
            }
        else:
            payload = dict(cached)
            payload["stats_note"] = None
            payload["last_updated"] = last_updated

        payload_with_request = dict(payload)
        payload_with_request["request"] = request
        payload_with_request["stats_presets"] = app.state.stats_presets
        return templates.TemplateResponse("stats.html", payload_with_request)

    @app.get("/search", response_class=HTMLResponse)
    def search(
        request: Request,
        q: str = Query("", min_length=0),
        limit: int = Query(50, ge=1, le=500),
        limit_messages: int = Query(0, ge=0, le=100),
        limit_attachments: int = Query(0, ge=0, le=100),
        limit_archives: int = Query(0, ge=0, le=100),
        limit_extracted: int = Query(0, ge=0, le=100),
        limit_conversations: int = Query(0, ge=0, le=100),
        limit_channels: int = Query(0, ge=0, le=100),
        page: int = Query(1, ge=1, le=1000),
        page_size: int = Query(50, ge=1, le=100),
        after_id: int = Query(0, ge=0),
        regex: bool = Query(False),
        facets: bool = Query(False),
        no_markdown: bool = Query(False),
        source_conv: int = Query(0, ge=0),
    ):
        debug = os.environ.get("TELECRIME_DEBUG_SEARCH") == "1"
        t0 = time.time()
        query = q.strip()
        terms, filters = _parse_query(query)
        results = SearchResults([], [], [], [], [], [], [])
        total_credentials = None
        facets_enabled = facets
        decorated_facets: dict[str, list[dict[str, str | int]]] = {}
        saved_searches: list[dict[str, str]] = []
        fts_used = False
        has_more = False
        active_filters: list[dict[str, str]] = []

        with engine.begin() as conn:
            saved_searches = [
                {"id": str(row[0]), "name": row[1], "query": row[2]}
                for row in conn.execute(
                    text(
                        "SELECT id, name, query FROM saved_searches ORDER BY created_at DESC LIMIT 20"
                    )
                ).fetchall()
            ]
        if debug:
            print(f"search: saved_searches {(time.time() - t0) * 1000:.1f} ms")

        cred_search_degraded = False
        if query or source_conv > 0:
            with get_session(engine) as session:
                excluded_conversations, excluded_channels = _get_exclusions(session)
                if debug:
                    print(f"search: exclusions {(time.time() - t0) * 1000:.1f} ms")
                pattern = f"%{terms.lower()}%"

                def like_any(*cols):
                    return or_(*[func.lower(col).like(pattern) for col in cols])

                credential_ids: list[int] | None = None
                filter_clause = _credential_filter_clause(filters)
                fts_available = app.state.fts_enabled and terms

                if fts_available:
                    # For regex mode, fetch more candidates from FTS then filter in Python
                    fts_fetch_limit = page_size * 5 if not regex else page_size * 8
                    try:
                        fts_used = True
                        offset = (page - 1) * page_size
                        credential_ids = _credential_ids_via_fts(
                            session,
                            terms=terms,
                            filters=filters,
                            exclude_conversation_ids=excluded_conversations,
                            limit=fts_fetch_limit,
                            offset=offset,
                        )
                        if not regex:
                            if len(credential_ids) > page_size:
                                has_more = True
                                credential_ids = credential_ids[:page_size]
                        if debug:
                            print(f"search: fts ids {(time.time() - t0) * 1000:.1f} ms")
                    except Exception as exc:
                        fts_available = False
                        fts_used = False
                        credential_ids = None
                        # A statement_timeout or connection error leaves the session in
                        # an aborted state. Roll back so subsequent queries can proceed.
                        try:
                            session.rollback()
                        except Exception:
                            pass
                        logger.warning("search: FTS query failed, falling back to LIKE: %r", exc)
                        if debug:
                            print(
                                f"search: fts error ({exc!r}), falling back to LIKE {(time.time() - t0) * 1000:.1f} ms"
                            )

                credential_query = None
                if not fts_available:
                    if terms:
                        credential_query = session.query(ParsedCredential).filter(
                            like_any(
                                ParsedCredential.url,
                                ParsedCredential.domain,
                                ParsedCredential.username,
                                ParsedCredential.email_domain,
                                ParsedCredential.application,
                                ParsedCredential.source_archive,
                                ParsedCredential.source_file,
                                ParsedCredential.stealer_type,
                            )
                        )
                    else:
                        credential_query = session.query(ParsedCredential)
                    if filter_clause is not None:
                        credential_query = credential_query.filter(filter_clause)
                    if excluded_conversations:
                        credential_query = credential_query.filter(
                            ParsedCredential.source_conversation_id.notin_(excluded_conversations)
                        )
                    if source_conv > 0:
                        credential_query = credential_query.filter(
                            ParsedCredential.source_conversation_id == source_conv
                        )

                credentials_page = []
                if fts_used and credential_ids is not None:
                    if credential_ids:
                        credentials_page = soft_dedupe_credentials(
                            _load_ordered_records(
                                session,
                                ParsedCredential,
                                credential_ids,
                            ),
                            limit=page_size + 1 if not regex else None,
                        )
                        if source_conv > 0:
                            credentials_page = [
                                c for c in credentials_page
                                if c.source_conversation_id == source_conv
                            ]
                        if not regex and len(credentials_page) > page_size:
                            has_more = True
                            credentials_page = credentials_page[:page_size]
                        if debug:
                            print(f"search: creds fetch {(time.time() - t0) * 1000:.1f} ms")
                elif credential_query is not None:
                    if after_id > 0:
                        credential_query = credential_query.filter(ParsedCredential.id < after_id)
                    try:
                        # Bound the LIKE fallback so it degrades to an empty page
                        # instead of scanning the whole 100M+ row table (no trgm
                        # index on stealer_type/application/url). 30s is generous
                        # for a live query; the parse watchdog cancels sooner.
                        if session.get_bind().dialect.name == "postgresql":
                            session.execute(text("SET LOCAL statement_timeout = '30000'"))
                        like_results = (
                            credential_query.order_by(ParsedCredential.id.desc())
                            .limit(page_size * 5)
                            .all()
                        )
                    except Exception as exc:
                        cred_search_degraded = True
                        try:
                            session.rollback()
                        except Exception:
                            pass
                        logger.warning("search: credential LIKE fallback timed out: %r", exc)
                        like_results = []
                    credentials_page = soft_dedupe_credentials(
                        like_results,
                        limit=page_size + 1,
                    )
                    if len(credentials_page) > page_size:
                        has_more = True
                        credentials_page = credentials_page[:page_size]
                    if debug:
                        print(f"search: creds like {(time.time() - t0) * 1000:.1f} ms")

                if total_credentials is None and terms:
                    try:
                        total_credentials = _credential_match_count(
                            session,
                            terms=terms,
                            filters=filters,
                            exclude_conversation_ids=excluded_conversations,
                        )
                    except Exception:
                        try:
                            session.rollback()
                        except Exception:
                            pass
                        total_credentials = None

                message_results = []
                if limit_messages > 0:
                    msg_fts_limit = limit_messages * 5 if regex else limit_messages
                    if app.state.fts_enabled and terms:
                        try:
                            message_ids = _message_ids_via_fts(
                                session,
                                terms=terms,
                                exclude_conversation_ids=excluded_conversations,
                                limit=msg_fts_limit,
                            )
                            if message_ids:
                                message_results = _load_ordered_records(
                                    session, Message, message_ids
                                )
                        except Exception:
                            pass
                    if not message_results and not app.state.fts_enabled:
                        message_query = session.query(Message).filter(
                            like_any(Message.text, Message.caption, Message.post_author)
                        )
                        if excluded_conversations:
                            message_query = message_query.filter(
                                Message.conversation_id.notin_(excluded_conversations)
                            )
                        message_results = (
                            message_query.order_by(Message.platform_timestamp.desc())
                            .limit(limit_messages)
                            .all()
                        )

                attachment_results = []
                if limit_attachments > 0:
                    attachment_query = (
                        session.query(FileAttachment)
                        .join(Message, Message.id == FileAttachment.message_id)
                        .filter(like_any(FileAttachment.filename, FileAttachment.mime_type))
                    )
                    if excluded_conversations:
                        attachment_query = attachment_query.filter(
                            Message.conversation_id.notin_(excluded_conversations)
                        )
                    attachment_results = (
                        attachment_query.order_by(FileAttachment.created_at.desc())
                        .limit(limit_attachments)
                        .all()
                    )

                archive_results = []
                if limit_archives > 0:
                    archive_query = (
                        session.query(DownloadArtifact)
                        .join(FileAttachment, FileAttachment.id == DownloadArtifact.attachment_id)
                        .join(Message, Message.id == FileAttachment.message_id)
                        .filter(like_any(DownloadArtifact.local_path, DownloadArtifact.temp_path))
                    )
                    if excluded_conversations:
                        archive_query = archive_query.filter(
                            Message.conversation_id.notin_(excluded_conversations)
                        )
                    archive_results = (
                        archive_query.order_by(DownloadArtifact.created_at.desc())
                        .limit(limit_archives)
                        .all()
                    )

                extracted_results = []
                if limit_extracted > 0:
                    extracted_query = session.query(ExtractedOutput).filter(
                        like_any(ExtractedOutput.output_filename, ExtractedOutput.output_path)
                    )
                    if excluded_conversations:
                        extracted_query = extracted_query.filter(
                            ExtractedOutput.source_conversation_id.notin_(excluded_conversations)
                        )
                    extracted_results = (
                        extracted_query.order_by(ExtractedOutput.created_at.desc())
                        .limit(limit_extracted)
                        .all()
                    )

                conversation_results = []
                if limit_conversations > 0:
                    conversation_query = session.query(Conversation).filter(
                        like_any(Conversation.title, Conversation.username, Conversation.notes)
                    )
                    if excluded_conversations:
                        conversation_query = conversation_query.filter(
                            Conversation.id.notin_(excluded_conversations)
                        )
                    conversation_results = (
                        conversation_query.order_by(Conversation.created_at.desc())
                        .limit(limit_conversations)
                        .all()
                    )

                channel_results = []
                if limit_channels > 0:
                    channel_query = session.query(TelegramChannel).filter(
                        like_any(
                            TelegramChannel.username, TelegramChannel.title, TelegramChannel.notes
                        )
                    )
                    if excluded_channels:
                        channel_query = channel_query.filter(
                            TelegramChannel.id.notin_(excluded_channels)
                        )
                    channel_results = (
                        channel_query.order_by(TelegramChannel.discovered_at.desc())
                        .limit(limit_channels)
                        .all()
                    )

                results = SearchResults(
                    credentials=credentials_page,
                    messages=message_results,
                    attachments=attachment_results,
                    archives=archive_results,
                    extracted=extracted_results,
                    conversations=conversation_results,
                    channels=channel_results,
                )
                if debug:
                    print(f"search: results built {(time.time() - t0) * 1000:.1f} ms")

                active_filters = _filter_pills(terms, filters)
                if debug:
                    print(f"search: filters {(time.time() - t0) * 1000:.1f} ms")

                if regex and terms:
                    regex_flags = re.IGNORECASE
                    try:
                        rx = re.compile(terms, regex_flags)
                    except re.error:
                        rx = None

                    if rx:

                        def cred_match(cred: ParsedCredential) -> bool:
                            fields = [
                                cred.url,
                                cred.domain,
                                cred.username,
                                cred.email_domain,
                                cred.application,
                                cred.source_archive,
                                cred.source_file,
                                cred.stealer_type,
                            ]
                            return any(f and rx.search(str(f)) for f in fields)

                        def msg_match(msg: Message) -> bool:
                            fields = [msg.text, msg.caption, msg.post_author]
                            return any(f and rx.search(str(f)) for f in fields)

                        results = SearchResults(
                            credentials=[c for c in results.credentials if cred_match(c)],
                            messages=[m for m in results.messages if msg_match(m)],
                            attachments=results.attachments,
                            archives=results.archives,
                            extracted=results.extracted,
                            conversations=results.conversations,
                            channels=results.channels,
                        )
                if debug:
                    print(f"search: done {(time.time() - t0) * 1000:.1f} ms")

        last_id = 0
        if results.credentials:
            last_id = results.credentials[-1].id

        result_counts = {
            "credentials": len(results.credentials),
            "messages": len(results.messages),
            "attachments": len(results.attachments),
            "archives": len(results.archives),
            "extracted": len(results.extracted),
            "conversations": len(results.conversations),
            "channels": len(results.channels),
        }

        # Resolve source_conv name for the filter banner
        source_conv_name: str = ""
        if source_conv > 0:
            with get_session(engine) as session:
                _sc = session.get(Conversation, source_conv)
                if _sc:
                    source_conv_name = _sc.title or _sc.username or str(source_conv)

        # Build channel name map for credential rows
        channel_map: dict[int, str] = {}
        if results.credentials:
            conv_ids = {
                c.source_conversation_id
                for c in results.credentials
                if c.source_conversation_id is not None
            }
            if conv_ids:
                with get_session(engine) as session:
                    for row in (
                        session.query(Conversation.id, Conversation.title, Conversation.username)
                        .filter(Conversation.id.in_(conv_ids))
                        .all()
                    ):
                        channel_map[row[0]] = row[2] or row[1] or str(row[0])

        # The bounded credential search only degrades (per-branch timeouts)
        # during the parse stage, which is the single window doing bulk INSERT
        # into parsed_credentials. Outside parse the search runs normally, so
        # an empty result just means "no matches". Show the banner only when
        # parse is actually in progress to avoid implying a false throttle.
        progress_now = read_progress() or {}
        pipeline_busy = bool(progress_now.get("running"))
        parsing_now = progress_now.get("current_stage") == "parse"
        search_reduced = (
            (parsing_now or cred_search_degraded)
            and bool(query)
            and not results.credentials
        )

        return templates.TemplateResponse(
            "search.html",
            {
                "request": request,
                "query": query,
                "terms": terms,
                "filters": filters,
                "limit": limit,
                "limit_messages": limit_messages,
                "limit_attachments": limit_attachments,
                "limit_archives": limit_archives,
                "limit_extracted": limit_extracted,
                "limit_conversations": limit_conversations,
                "limit_channels": limit_channels,
                "page": page,
                "page_size": page_size,
                "total_credentials": total_credentials,
                "has_more": has_more,
                "result_counts": result_counts,
                "last_id": last_id,
                "channel_map": channel_map,
                "facets": decorated_facets,
                "active_filters": active_filters,
                "saved_searches": saved_searches,
                "regex": regex,
                "no_markdown": no_markdown,
                "facets_enabled": facets_enabled,
                "fts_enabled": app.state.fts_enabled,
                "fts_used": fts_used,
                "results": results,
                "source_conv": source_conv,
                "source_conv_name": source_conv_name,
                "search_reduced": search_reduced,
                "pipeline_busy": pipeline_busy,
            },
        )

    @app.get("/search/facets")
    def search_facets(q: str = Query("", min_length=0)):
        """Async facets endpoint — returns JSON facet data for a search query."""
        query = q.strip()
        terms, filters = _parse_query(query)
        facet_counts: dict[str, list[tuple[str, int]]] = {
            "domain": [],
            "stealer_type": [],
            "application": [],
            "email_domain": [],
        }
        if not terms:
            return JSONResponse(_decorate_facets(facet_counts, terms, filters))

        facet_columns = {
            "domain": "domain",
            "stealer_type": "stealer_type",
            "application": "application",
            "email_domain": "email_domain",
        }

        with get_session(engine) as session:
            for facet_key, col_name in facet_columns.items():
                if app.state.fts_enabled and not False:
                    try:
                        rows = session.execute(
                            text(
                                f"SELECT pc.{col_name}, count(*) as cnt "
                                f"FROM parsed_credentials pc "
                                f"WHERE pc.{col_name} IS NOT NULL "
                                f"AND pc.id IN ("
                                f"  SELECT rowid FROM parsed_credentials_fts "
                                f"  WHERE parsed_credentials_fts MATCH :q"
                                f") "
                                f"GROUP BY pc.{col_name} ORDER BY cnt DESC LIMIT 10"
                            ),
                            {"q": _fts_escape(terms)},
                        ).fetchall()
                        facet_counts[facet_key] = [(row[0], row[1]) for row in rows]
                        continue
                    except Exception:
                        pass
                # LIKE fallback
                pattern = f"%{terms.lower()}%"
                rows = session.execute(
                    text(
                        f"SELECT pc.{col_name}, count(*) as cnt "
                        f"FROM parsed_credentials pc "
                        f"WHERE pc.{col_name} IS NOT NULL "
                        f"AND (lower(pc.url) LIKE :p OR lower(pc.domain) LIKE :p "
                        f"  OR lower(pc.username) LIKE :p OR lower(pc.email_domain) LIKE :p "
                        f"  OR lower(pc.application) LIKE :p OR lower(pc.source_archive) LIKE :p "
                        f"  OR lower(pc.source_file) LIKE :p OR lower(pc.stealer_type) LIKE :p) "
                        f"GROUP BY pc.{col_name} ORDER BY cnt DESC LIMIT 10"
                    ),
                    {"p": pattern},
                ).fetchall()
                facet_counts[facet_key] = [(row[0], row[1]) for row in rows]

        return JSONResponse(_decorate_facets(facet_counts, terms, filters))

    @app.get("/search/count")
    def search_count(q: str = Query("", min_length=0), regex: bool = Query(False)):
        query = q.strip()
        terms, filters = _parse_query(query)
        if not terms or regex:
            return JSONResponse({"query": query, "total_credentials": None})

        with get_session(engine) as session:
            excluded_conversations, _excluded_channels = _get_exclusions(session)
            total_credentials = _credential_match_count(
                session,
                terms=terms,
                filters=filters,
                exclude_conversation_ids=excluded_conversations,
            )

        capped = total_credentials >= _COUNT_CAP
        if capped:
            total_credentials = _COUNT_CAP - 1
        return JSONResponse({"query": query, "total_credentials": total_credentials, "capped": capped})

    @app.post("/search/save")
    def save_search(name: str = Form(...), query: str = Form(...)):
        payload = {"name": name.strip(), "query": query.strip()}
        for _attempt in range(3):
            try:
                with engine.begin() as conn:
                    conn.execute(
                        text("INSERT INTO saved_searches (name, query) VALUES (:name, :query)"),
                        payload,
                    )
                break
            except SQLAlchemyError:
                time.sleep(0.25)
        else:
            return JSONResponse({"error": "database busy"}, status_code=503)
        return RedirectResponse(url=f"/search?q={query.strip()}", status_code=303)

    @app.get("/search/credentials.csv")
    def export_credentials_csv(
        q: str = Query("", min_length=0),
        limit: int = Query(5000, ge=1, le=50000),
        regex: bool = Query(False),
        no_markdown: bool = Query(False),
    ):
        query = q.strip()
        terms, filters = _parse_query(query)

        def row_iter():
            header = (
                "id,url,domain,username,password,email_domain,application,profile,"
                "source_archive,source_file,stealer_type,created_at\n"
            )
            yield header
            if not query:
                return
            with get_session(engine) as session:
                excluded_conversations, _excluded_channels = _get_exclusions(session)
                pattern = f"%{terms.lower()}%"

                def like_any(*cols):
                    return or_(*[func.lower(col).like(pattern) for col in cols])

                filter_clause = _credential_filter_clause(filters)
                rows = []
                if terms and app.state.fts_enabled and not regex:
                    try:
                        credential_ids = _credential_ids_via_fts(
                            session,
                            terms=terms,
                            filters=filters,
                            exclude_conversation_ids=excluded_conversations,
                            limit=limit,
                        )
                        rows = _load_ordered_records(session, ParsedCredential, credential_ids)
                    except Exception:
                        rows = []

                if not rows:
                    query_base = session.query(ParsedCredential)
                    if terms:
                        query_base = query_base.filter(
                            like_any(
                                ParsedCredential.url,
                                ParsedCredential.domain,
                                ParsedCredential.username,
                                ParsedCredential.email_domain,
                                ParsedCredential.application,
                                ParsedCredential.source_archive,
                                ParsedCredential.source_file,
                                ParsedCredential.stealer_type,
                            )
                        )
                    if filter_clause is not None:
                        query_base = query_base.filter(filter_clause)
                    if excluded_conversations:
                        query_base = query_base.filter(
                            ParsedCredential.source_conversation_id.notin_(excluded_conversations)
                        )

                    rows = (
                        query_base.order_by(ParsedCredential.created_at.desc()).limit(limit).all()
                    )

                if regex and terms:
                    try:
                        rx = re.compile(terms, re.IGNORECASE)
                    except re.error:
                        rx = None
                    if rx:
                        rows = [
                            r
                            for r in rows
                            if any(
                                f and rx.search(str(f))
                                for f in [
                                    r.url,
                                    r.domain,
                                    r.username,
                                    r.email_domain,
                                    r.application,
                                    r.source_archive,
                                    r.source_file,
                                    r.stealer_type,
                                ]
                            )
                        ]

                for cred in rows:
                    values = [
                        cred.id,
                        cred.url,
                        cred.domain,
                        cred.username,
                        cred.password,
                        cred.email_domain,
                        cred.application,
                        cred.profile,
                        cred.source_archive,
                        cred.source_file,
                        cred.stealer_type,
                        cred.created_at,
                    ]
                    escaped = []
                    for value in values:
                        text = (
                            ""
                            if value is None
                            else str(_export_value(value, no_markdown=no_markdown))
                        )
                        text = text.replace('"', '""')
                        escaped.append(f'"{text}"')
                    yield ",".join(escaped) + "\n"

        filename = "telecrime_credentials.csv"
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return StreamingResponse(row_iter(), media_type="text/csv", headers=headers)

    @app.get("/search/export.json")
    def export_search_json(
        q: str = Query("", min_length=0),
        regex: bool = Query(False),
        no_markdown: bool = Query(False),
        limit_credentials: int = Query(5000, ge=1, le=50000),
        limit_messages: int = Query(1000, ge=1, le=20000),
        limit_attachments: int = Query(1000, ge=1, le=20000),
        limit_archives: int = Query(1000, ge=1, le=20000),
        limit_extracted: int = Query(1000, ge=1, le=20000),
        limit_conversations: int = Query(1000, ge=1, le=20000),
        limit_channels: int = Query(1000, ge=1, le=20000),
    ):
        query = q.strip()
        terms, filters = _parse_query(query)
        if not query:
            return JSONResponse({"query": query, "results": {}})

        with get_session(engine) as session:
            excluded_conversations, excluded_channels = _get_exclusions(session)
            results = _search_for_export(
                session,
                terms,
                filters,
                regex,
                app.state.fts_enabled,
                excluded_conversations,
                excluded_channels,
                limit_credentials,
                limit_messages,
                limit_attachments,
                limit_archives,
                limit_extracted,
                limit_conversations,
                limit_channels,
            )

            data = {
                "query": query,
                "terms": terms,
                "filters": filters,
                "limits": {
                    "credentials": limit_credentials,
                    "messages": limit_messages,
                    "attachments": limit_attachments,
                    "archives": limit_archives,
                    "extracted": limit_extracted,
                    "conversations": limit_conversations,
                    "channels": limit_channels,
                },
                "results": {
                    "credentials": [
                        _serialize_row(
                            c,
                            [
                                "id",
                                "url",
                                "domain",
                                "username",
                                "password",
                                "email_domain",
                                "application",
                                "profile",
                                "source_archive",
                                "source_file",
                                "stealer_type",
                                "created_at",
                            ],
                            no_markdown=no_markdown,
                        )
                        for c in results.credentials
                    ],
                    "messages": [
                        _serialize_row(
                            m,
                            [
                                "id",
                                "conversation_id",
                                "platform_id",
                                "platform_timestamp",
                                "text",
                                "caption",
                                "post_author",
                                "is_forwarded",
                                "forwarded_from_id",
                                "forwarded_from_name",
                            ],
                            no_markdown=no_markdown,
                        )
                        for m in results.messages
                    ],
                    "attachments": [
                        _serialize_row(
                            a,
                            [
                                "id",
                                "message_id",
                                "filename",
                                "mime_type",
                                "size",
                                "is_archive_candidate",
                                "archive_type",
                                "detected_part_number",
                                "detected_base_name",
                            ],
                            no_markdown=no_markdown,
                        )
                        for a in results.attachments
                    ],
                    "archives": [
                        _serialize_row(
                            a,
                            [
                                "id",
                                "attachment_id",
                                "local_path",
                                "temp_path",
                                "verified_size",
                                "content_hash",
                                "status",
                                "retry_count",
                                "is_deleted",
                                "created_at",
                                "updated_at",
                            ],
                            no_markdown=no_markdown,
                        )
                        for a in results.archives
                    ],
                    "extracted": [
                        _serialize_row(
                            e,
                            [
                                "id",
                                "job_id",
                                "output_path",
                                "output_filename",
                                "output_type",
                                "output_size",
                                "output_hash",
                                "source_conversation_id",
                                "source_message_id",
                                "created_at",
                                "updated_at",
                            ],
                            no_markdown=no_markdown,
                        )
                        for e in results.extracted
                    ],
                    "conversations": [
                        _serialize_row(
                            c,
                            [
                                "id",
                                "platform_id",
                                "title",
                                "username",
                                "conversation_type",
                                "is_member",
                                "is_accessible",
                                "last_ingested_message_id",
                                "last_ingested_at",
                            ],
                            no_markdown=no_markdown,
                        )
                        for c in results.conversations
                    ],
                    "channels": [
                        _serialize_row(
                            c,
                            [
                                "id",
                                "platform_id",
                                "username",
                                "title",
                                "invite_link",
                                "source",
                                "discovered_at",
                                "is_active",
                                "is_accessible",
                                "last_checked",
                                "check_error",
                            ],
                            no_markdown=no_markdown,
                        )
                        for c in results.channels
                    ],
                },
            }

        return JSONResponse(data)

    @app.get("/search/export.xlsx")
    def export_search_xlsx(
        q: str = Query("", min_length=0),
        regex: bool = Query(False),
        no_markdown: bool = Query(False),
        limit_credentials: int = Query(5000, ge=1, le=50000),
        limit_messages: int = Query(1000, ge=1, le=20000),
        limit_attachments: int = Query(1000, ge=1, le=20000),
        limit_archives: int = Query(1000, ge=1, le=20000),
        limit_extracted: int = Query(1000, ge=1, le=20000),
        limit_conversations: int = Query(1000, ge=1, le=20000),
        limit_channels: int = Query(1000, ge=1, le=20000),
    ):
        query = q.strip()
        terms, filters = _parse_query(query)
        if not query:
            return RedirectResponse(url="/search", status_code=303)

        with get_session(engine) as session:
            excluded_conversations, excluded_channels = _get_exclusions(session)
            results = _search_for_export(
                session,
                terms,
                filters,
                regex,
                app.state.fts_enabled,
                excluded_conversations,
                excluded_channels,
                limit_credentials,
                limit_messages,
                limit_attachments,
                limit_archives,
                limit_extracted,
                limit_conversations,
                limit_channels,
            )

        wb = Workbook()
        active_sheet = wb.active
        if active_sheet is not None:
            wb.remove(active_sheet)

        def add_sheet(name: str, headers: list[str], rows: list[list[object]]):
            ws = wb.create_sheet(title=name[:31])
            ws.append(headers)
            for row in rows:
                ws.append([_export_value(v, no_markdown=no_markdown) for v in row])

        add_sheet(
            "credentials",
            [
                "id",
                "url",
                "domain",
                "username",
                "password",
                "email_domain",
                "application",
                "profile",
                "source_archive",
                "source_file",
                "stealer_type",
                "created_at",
            ],
            [
                [
                    c.id,
                    c.url,
                    c.domain,
                    c.username,
                    c.password,
                    c.email_domain,
                    c.application,
                    c.profile,
                    c.source_archive,
                    c.source_file,
                    c.stealer_type,
                    c.created_at,
                ]
                for c in results.credentials
            ],
        )
        add_sheet(
            "messages",
            [
                "id",
                "conversation_id",
                "platform_id",
                "platform_timestamp",
                "text",
                "caption",
                "post_author",
                "is_forwarded",
                "forwarded_from_id",
                "forwarded_from_name",
            ],
            [
                [
                    m.id,
                    m.conversation_id,
                    m.platform_id,
                    m.platform_timestamp,
                    m.text,
                    m.caption,
                    m.post_author,
                    m.is_forwarded,
                    m.forwarded_from_id,
                    m.forwarded_from_name,
                ]
                for m in results.messages
            ],
        )
        add_sheet(
            "attachments",
            [
                "id",
                "message_id",
                "filename",
                "mime_type",
                "size",
                "is_archive_candidate",
                "archive_type",
                "detected_part_number",
                "detected_base_name",
            ],
            [
                [
                    a.id,
                    a.message_id,
                    a.filename,
                    a.mime_type,
                    a.size,
                    a.is_archive_candidate,
                    a.archive_type,
                    a.detected_part_number,
                    a.detected_base_name,
                ]
                for a in results.attachments
            ],
        )
        add_sheet(
            "archives",
            [
                "id",
                "attachment_id",
                "local_path",
                "temp_path",
                "verified_size",
                "content_hash",
                "status",
                "retry_count",
                "is_deleted",
                "created_at",
                "updated_at",
            ],
            [
                [
                    a.id,
                    a.attachment_id,
                    a.local_path,
                    a.temp_path,
                    a.verified_size,
                    a.content_hash,
                    a.status.value if a.status else None,
                    a.retry_count,
                    a.is_deleted,
                    a.created_at,
                    a.updated_at,
                ]
                for a in results.archives
            ],
        )
        add_sheet(
            "extracted",
            [
                "id",
                "job_id",
                "output_path",
                "output_filename",
                "output_type",
                "output_size",
                "output_hash",
                "source_conversation_id",
                "source_message_id",
                "created_at",
                "updated_at",
            ],
            [
                [
                    e.id,
                    e.job_id,
                    e.output_path,
                    e.output_filename,
                    e.output_type,
                    e.output_size,
                    e.output_hash,
                    e.source_conversation_id,
                    e.source_message_id,
                    e.created_at,
                    e.updated_at,
                ]
                for e in results.extracted
            ],
        )
        add_sheet(
            "conversations",
            [
                "id",
                "platform_id",
                "title",
                "username",
                "conversation_type",
                "is_member",
                "is_accessible",
                "last_ingested_message_id",
                "last_ingested_at",
            ],
            [
                [
                    c.id,
                    c.platform_id,
                    c.title,
                    c.username,
                    c.conversation_type,
                    c.is_member,
                    c.is_accessible,
                    c.last_ingested_message_id,
                    c.last_ingested_at,
                ]
                for c in results.conversations
            ],
        )
        add_sheet(
            "channels",
            [
                "id",
                "platform_id",
                "username",
                "title",
                "invite_link",
                "source",
                "discovered_at",
                "is_active",
                "is_accessible",
                "last_checked",
                "check_error",
            ],
            [
                [
                    c.id,
                    c.platform_id,
                    c.username,
                    c.title,
                    c.invite_link,
                    c.source,
                    c.discovered_at,
                    c.is_active,
                    c.is_accessible,
                    c.last_checked,
                    c.check_error,
                ]
                for c in results.channels
            ],
        )

        buf = BytesIO()
        wb.save(buf)
        buf.seek(0)
        filename = "telecrime_search.xlsx"
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return StreamingResponse(
            buf,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )

    @app.get("/search/export.md")
    def export_search_markdown(
        q: str = Query("", min_length=0),
        regex: bool = Query(False),
        no_markdown: bool = Query(False),
        limit_credentials: int = Query(5000, ge=1, le=50000),
        limit_messages: int = Query(1000, ge=1, le=20000),
        limit_attachments: int = Query(1000, ge=1, le=20000),
        limit_archives: int = Query(1000, ge=1, le=20000),
        limit_extracted: int = Query(1000, ge=1, le=20000),
        limit_conversations: int = Query(1000, ge=1, le=20000),
        limit_channels: int = Query(1000, ge=1, le=20000),
    ):
        query = q.strip()
        terms, filters = _parse_query(query)
        if not query:
            return RedirectResponse(url="/search", status_code=303)

        with get_session(engine) as session:
            excluded_conversations, excluded_channels = _get_exclusions(session)
            results = _search_for_export(
                session,
                terms,
                filters,
                regex,
                app.state.fts_enabled,
                excluded_conversations,
                excluded_channels,
                limit_credentials,
                limit_messages,
                limit_attachments,
                limit_archives,
                limit_extracted,
                limit_conversations,
                limit_channels,
            )

        sections: list[str] = [
            "# Telecrime Search Export",
            "",
            f"- Query: `{query}`",
            f"- Regex: {'yes' if regex else 'no'}",
            f"- Plain values: {'yes' if no_markdown else 'no'}",
            "",
        ]

        section_specs = [
            (
                "Credentials",
                [
                    "id",
                    "url",
                    "domain",
                    "username",
                    "password",
                    "email_domain",
                    "application",
                    "profile",
                    "source_archive",
                    "source_file",
                    "stealer_type",
                    "created_at",
                ],
                [
                    [
                        c.id,
                        c.url,
                        c.domain,
                        c.username,
                        c.password,
                        c.email_domain,
                        c.application,
                        c.profile,
                        c.source_archive,
                        c.source_file,
                        c.stealer_type,
                        c.created_at,
                    ]
                    for c in results.credentials
                ],
            ),
            (
                "Messages",
                [
                    "id",
                    "conversation_id",
                    "platform_id",
                    "platform_timestamp",
                    "text",
                    "caption",
                    "post_author",
                    "is_forwarded",
                    "forwarded_from_id",
                    "forwarded_from_name",
                ],
                [
                    [
                        m.id,
                        m.conversation_id,
                        m.platform_id,
                        m.platform_timestamp,
                        m.text,
                        m.caption,
                        m.post_author,
                        m.is_forwarded,
                        m.forwarded_from_id,
                        m.forwarded_from_name,
                    ]
                    for m in results.messages
                ],
            ),
            (
                "Attachments",
                [
                    "id",
                    "message_id",
                    "filename",
                    "mime_type",
                    "size",
                    "is_archive_candidate",
                    "archive_type",
                    "detected_part_number",
                    "detected_base_name",
                ],
                [
                    [
                        a.id,
                        a.message_id,
                        a.filename,
                        a.mime_type,
                        a.size,
                        a.is_archive_candidate,
                        a.archive_type,
                        a.detected_part_number,
                        a.detected_base_name,
                    ]
                    for a in results.attachments
                ],
            ),
            (
                "Archives",
                [
                    "id",
                    "attachment_id",
                    "local_path",
                    "temp_path",
                    "verified_size",
                    "content_hash",
                    "status",
                    "retry_count",
                    "is_deleted",
                    "created_at",
                    "updated_at",
                ],
                [
                    [
                        a.id,
                        a.attachment_id,
                        a.local_path,
                        a.temp_path,
                        a.verified_size,
                        a.content_hash,
                        a.status.value if a.status else None,
                        a.retry_count,
                        a.is_deleted,
                        a.created_at,
                        a.updated_at,
                    ]
                    for a in results.archives
                ],
            ),
            (
                "Extracted Outputs",
                [
                    "id",
                    "job_id",
                    "output_path",
                    "output_filename",
                    "output_type",
                    "output_size",
                    "output_hash",
                    "source_conversation_id",
                    "source_message_id",
                    "created_at",
                    "updated_at",
                ],
                [
                    [
                        e.id,
                        e.job_id,
                        e.output_path,
                        e.output_filename,
                        e.output_type,
                        e.output_size,
                        e.output_hash,
                        e.source_conversation_id,
                        e.source_message_id,
                        e.created_at,
                        e.updated_at,
                    ]
                    for e in results.extracted
                ],
            ),
            (
                "Conversations",
                [
                    "id",
                    "platform_id",
                    "title",
                    "username",
                    "conversation_type",
                    "is_member",
                    "is_accessible",
                    "last_ingested_message_id",
                    "last_ingested_at",
                ],
                [
                    [
                        c.id,
                        c.platform_id,
                        c.title,
                        c.username,
                        c.conversation_type,
                        c.is_member,
                        c.is_accessible,
                        c.last_ingested_message_id,
                        c.last_ingested_at,
                    ]
                    for c in results.conversations
                ],
            ),
            (
                "Channels",
                [
                    "id",
                    "platform_id",
                    "username",
                    "title",
                    "invite_link",
                    "source",
                    "discovered_at",
                    "is_active",
                    "is_accessible",
                    "last_checked",
                    "check_error",
                ],
                [
                    [
                        c.id,
                        c.platform_id,
                        c.username,
                        c.title,
                        c.invite_link,
                        c.source,
                        c.discovered_at,
                        c.is_active,
                        c.is_accessible,
                        c.last_checked,
                        c.check_error,
                    ]
                    for c in results.channels
                ],
            ),
        ]

        for title, headers, rows in section_specs:
            if rows:
                sections.append(_markdown_table(title, headers, rows, no_markdown=no_markdown))

        if len(sections) == 6:
            sections.extend(["No rows matched the export query.", ""])

        body = "\n".join(sections)
        headers = {"Content-Disposition": 'attachment; filename="telecrime_search.md"'}

        async def _body_stream():
            yield body

        return StreamingResponse(_body_stream(), media_type="text/markdown", headers=headers)

    @app.get("/search/messages.csv")
    def export_messages_csv(
        q: str = Query("", min_length=0),
        limit: int = Query(5000, ge=1, le=50000),
        regex: bool = Query(False),
        no_markdown: bool = Query(False),
    ):
        query = q.strip()
        terms, filters = _parse_query(query)
        if not query:
            return RedirectResponse(url="/search", status_code=303)
        with get_session(engine) as session:
            excluded_conversations, excluded_channels = _get_exclusions(session)
            results = _search_for_export(
                session,
                terms,
                filters,
                regex,
                app.state.fts_enabled,
                excluded_conversations,
                excluded_channels,
                limit_credentials=1,
                limit_messages=limit,
                limit_attachments=1,
                limit_archives=1,
                limit_extracted=1,
                limit_conversations=1,
                limit_channels=1,
            )
            rows = [
                [
                    m.id,
                    m.conversation_id,
                    m.platform_id,
                    m.platform_timestamp,
                    m.text,
                    m.caption,
                    m.post_author,
                    m.is_forwarded,
                    m.forwarded_from_id,
                    m.forwarded_from_name,
                ]
                for m in results.messages
            ]
        headers = [
            "id",
            "conversation_id",
            "platform_id",
            "platform_timestamp",
            "text",
            "caption",
            "post_author",
            "is_forwarded",
            "forwarded_from_id",
            "forwarded_from_name",
        ]
        return StreamingResponse(
            _csv_stream(headers, rows, no_markdown=no_markdown),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=telecrime_messages.csv"},
        )

    @app.get("/search/attachments.csv")
    def export_attachments_csv(
        q: str = Query("", min_length=0),
        limit: int = Query(5000, ge=1, le=50000),
        regex: bool = Query(False),
        no_markdown: bool = Query(False),
    ):
        query = q.strip()
        terms, filters = _parse_query(query)
        if not query:
            return RedirectResponse(url="/search", status_code=303)
        with get_session(engine) as session:
            excluded_conversations, excluded_channels = _get_exclusions(session)
            results = _search_for_export(
                session,
                terms,
                filters,
                regex,
                app.state.fts_enabled,
                excluded_conversations,
                excluded_channels,
                limit_credentials=1,
                limit_messages=1,
                limit_attachments=limit,
                limit_archives=1,
                limit_extracted=1,
                limit_conversations=1,
                limit_channels=1,
            )
            rows = [
                [
                    a.id,
                    a.message_id,
                    a.filename,
                    a.mime_type,
                    a.size,
                    a.is_archive_candidate,
                    a.archive_type,
                    a.detected_part_number,
                    a.detected_base_name,
                ]
                for a in results.attachments
            ]
        headers = [
            "id",
            "message_id",
            "filename",
            "mime_type",
            "size",
            "is_archive_candidate",
            "archive_type",
            "detected_part_number",
            "detected_base_name",
        ]
        return StreamingResponse(
            _csv_stream(headers, rows, no_markdown=no_markdown),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=telecrime_attachments.csv"},
        )

    @app.get("/search/archives.csv")
    def export_archives_csv(
        q: str = Query("", min_length=0),
        limit: int = Query(5000, ge=1, le=50000),
        regex: bool = Query(False),
        no_markdown: bool = Query(False),
    ):
        query = q.strip()
        terms, filters = _parse_query(query)
        if not query:
            return RedirectResponse(url="/search", status_code=303)
        with get_session(engine) as session:
            excluded_conversations, excluded_channels = _get_exclusions(session)
            results = _search_for_export(
                session,
                terms,
                filters,
                regex,
                app.state.fts_enabled,
                excluded_conversations,
                excluded_channels,
                limit_credentials=1,
                limit_messages=1,
                limit_attachments=1,
                limit_archives=limit,
                limit_extracted=1,
                limit_conversations=1,
                limit_channels=1,
            )
            rows = [
                [
                    a.id,
                    a.attachment_id,
                    a.local_path,
                    a.temp_path,
                    a.verified_size,
                    a.content_hash,
                    a.status.value if a.status else None,
                    a.retry_count,
                    a.is_deleted,
                    a.created_at,
                    a.updated_at,
                ]
                for a in results.archives
            ]
        headers = [
            "id",
            "attachment_id",
            "local_path",
            "temp_path",
            "verified_size",
            "content_hash",
            "status",
            "retry_count",
            "is_deleted",
            "created_at",
            "updated_at",
        ]
        return StreamingResponse(
            _csv_stream(headers, rows, no_markdown=no_markdown),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=telecrime_archives.csv"},
        )

    @app.get("/search/extracted.csv")
    def export_extracted_csv(
        q: str = Query("", min_length=0),
        limit: int = Query(5000, ge=1, le=50000),
        regex: bool = Query(False),
        no_markdown: bool = Query(False),
    ):
        query = q.strip()
        terms, filters = _parse_query(query)
        if not query:
            return RedirectResponse(url="/search", status_code=303)
        with get_session(engine) as session:
            excluded_conversations, excluded_channels = _get_exclusions(session)
            results = _search_for_export(
                session,
                terms,
                filters,
                regex,
                app.state.fts_enabled,
                excluded_conversations,
                excluded_channels,
                limit_credentials=1,
                limit_messages=1,
                limit_attachments=1,
                limit_archives=1,
                limit_extracted=limit,
                limit_conversations=1,
                limit_channels=1,
            )
            rows = [
                [
                    e.id,
                    e.job_id,
                    e.output_path,
                    e.output_filename,
                    e.output_type,
                    e.output_size,
                    e.output_hash,
                    e.source_conversation_id,
                    e.source_message_id,
                    e.created_at,
                    e.updated_at,
                ]
                for e in results.extracted
            ]
        headers = [
            "id",
            "job_id",
            "output_path",
            "output_filename",
            "output_type",
            "output_size",
            "output_hash",
            "source_conversation_id",
            "source_message_id",
            "created_at",
            "updated_at",
        ]
        return StreamingResponse(
            _csv_stream(headers, rows, no_markdown=no_markdown),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=telecrime_extracted.csv"},
        )

    @app.get("/search/conversations.csv")
    def export_conversations_csv(
        q: str = Query("", min_length=0),
        limit: int = Query(5000, ge=1, le=50000),
        regex: bool = Query(False),
        no_markdown: bool = Query(False),
    ):
        query = q.strip()
        terms, filters = _parse_query(query)
        if not query:
            return RedirectResponse(url="/search", status_code=303)
        with get_session(engine) as session:
            excluded_conversations, excluded_channels = _get_exclusions(session)
            results = _search_for_export(
                session,
                terms,
                filters,
                regex,
                app.state.fts_enabled,
                excluded_conversations,
                excluded_channels,
                limit_credentials=1,
                limit_messages=1,
                limit_attachments=1,
                limit_archives=1,
                limit_extracted=1,
                limit_conversations=limit,
                limit_channels=1,
            )
            rows = [
                [
                    c.id,
                    c.platform_id,
                    c.title,
                    c.username,
                    c.conversation_type,
                    c.is_member,
                    c.is_accessible,
                    c.last_ingested_message_id,
                    c.last_ingested_at,
                ]
                for c in results.conversations
            ]
        headers = [
            "id",
            "platform_id",
            "title",
            "username",
            "conversation_type",
            "is_member",
            "is_accessible",
            "last_ingested_message_id",
            "last_ingested_at",
        ]
        return StreamingResponse(
            _csv_stream(headers, rows, no_markdown=no_markdown),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=telecrime_conversations.csv"},
        )

    @app.get("/search/channels.csv")
    def export_channels_csv(
        q: str = Query("", min_length=0),
        limit: int = Query(5000, ge=1, le=50000),
        regex: bool = Query(False),
        no_markdown: bool = Query(False),
    ):
        query = q.strip()
        terms, filters = _parse_query(query)
        if not query:
            return RedirectResponse(url="/search", status_code=303)
        with get_session(engine) as session:
            excluded_conversations, excluded_channels = _get_exclusions(session)
            results = _search_for_export(
                session,
                terms,
                filters,
                regex,
                app.state.fts_enabled,
                excluded_conversations,
                excluded_channels,
                limit_credentials=1,
                limit_messages=1,
                limit_attachments=1,
                limit_archives=1,
                limit_extracted=1,
                limit_conversations=1,
                limit_channels=limit,
            )
            rows = [
                [
                    c.id,
                    c.platform_id,
                    c.username,
                    c.title,
                    c.invite_link,
                    c.source,
                    c.discovered_at,
                    c.is_active,
                    c.is_accessible,
                    c.last_checked,
                    c.check_error,
                ]
                for c in results.channels
            ]
        headers = [
            "id",
            "platform_id",
            "username",
            "title",
            "invite_link",
            "source",
            "discovered_at",
            "is_active",
            "is_accessible",
            "last_checked",
            "check_error",
        ]
        return StreamingResponse(
            _csv_stream(headers, rows, no_markdown=no_markdown),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=telecrime_channels.csv"},
        )

    # --- Watchlist ---

    @app.get("/watchlist", response_class=HTMLResponse)
    def watchlist_page(request: Request):
        with get_session(engine) as session:
            items = [
                _witem_dict(i)
                for i in session.query(WatchlistItem).order_by(WatchlistItem.created_at.desc()).all()
            ]
        total_new = sum(row["new_count"] for row in items if row["enabled"])
        return templates.TemplateResponse(
            "watchlist.html",
            {"request": request, "items": items, "total_new": total_new},
        )

    @app.get("/api/watchlist")
    def watchlist_list():
        with get_session(engine) as session:
            items = [
                _witem_dict(i)
                for i in session.query(WatchlistItem).order_by(WatchlistItem.created_at.desc()).all()
            ]
        return JSONResponse(items)

    def _watchlist_items_response(request: Request, items, total_new: int):
        return templates.TemplateResponse(
            "partials/watchlist_section.html",
            {"request": request, "items": items, "total_new": total_new},
        )

    def _watchlist_row_response(request: Request, item):
        return templates.TemplateResponse(
            "partials/watchlist_row.html",
            {"request": request, "item": item},
        )

    @app.post("/api/watchlist")
    async def watchlist_add(request: Request):
        """Add a watchlist item.

        The initial count is a synchronous full-table ILIKE COUNT — run it in
        the threadpool so it can't stall the event loop, with a 30s bound.
        """
        content_type = request.headers.get("content-type", "")
        if "application/json" in content_type:
            body = await request.json()
        else:
            form = await request.form()
            body = dict(form)
        label = (body.get("label") or "").strip()
        query = (body.get("query") or "").strip()
        match_type = body.get("match_type", "any")
        if not query:
            return JSONResponse({"error": "query is required"}, status_code=400)
        if not label:
            label = query
        if match_type not in ("any", "domain", "user", "url"):
            match_type = "any"

        def _initial_count() -> int:
            with engine.connect() as conn:
                conn.execute(text("SET LOCAL statement_timeout = '30s'"))
                try:
                    return _watchlist_count(conn, query, match_type)
                except Exception:
                    # Query too expensive for the 30s budget — accept the item
                    # with an unknown count; the background worker fills it in.
                    return 0

        count = await asyncio.to_thread(_initial_count)

        with get_session(engine) as session:
            session.add(WatchlistItem(
                label=label,
                query=query,
                match_type=match_type,
                last_known_count=count,
                new_count=0,
            ))
            session.commit()
            if request.headers.get("HX-Request"):
                items = [
                    _witem_dict(i)
                    for i in session.query(WatchlistItem)
                    .order_by(WatchlistItem.created_at.desc())
                    .all()
                ]
                total_new = sum(row["new_count"] for row in items if row["enabled"])
                return _watchlist_items_response(request, items, total_new)

        return JSONResponse({"ok": True})

    @app.delete("/api/watchlist/{item_id}")
    def watchlist_delete(request: Request, item_id: int):
        with get_session(engine) as session:
            item = session.get(WatchlistItem, item_id)
            if item:
                session.delete(item)
            session.commit()
        if request.headers.get("HX-Request"):
            return Response(status_code=200)
        return JSONResponse({"ok": True})

    @app.post("/api/watchlist/{item_id}/viewed")
    def watchlist_viewed(item_id: int):
        now = datetime.now(UTC)
        with get_session(engine) as session:
            item = session.get(WatchlistItem, item_id)
            if item:
                item.new_count = 0
                item.last_viewed_at = now
            session.commit()
        return JSONResponse({"ok": True})

    @app.patch("/api/watchlist/{item_id}")
    async def watchlist_update(item_id: int, request: Request):
        content_type = request.headers.get("content-type", "")
        if "application/json" in content_type:
            body = await request.json()
        else:
            form = await request.form()
            body = dict(form)

        with get_session(engine) as session:
            item = session.get(WatchlistItem, item_id)
            if not item:
                return JSONResponse({"error": "not found"}, status_code=404)
            changed = False
            if "enabled" in body:
                # HTMX sends form-urlencoded strings ("0"/"1"), and
                # bool("0") is True — the toggle could never disable an item.
                item.enabled = str(body["enabled"]).lower() in ("1", "true", "on")
                changed = True
            if "label" in body and str(body["label"]).strip():
                item.label = str(body["label"]).strip()
                changed = True
            if "query" in body and str(body["query"]).strip():
                item.query = str(body["query"]).strip()
                changed = True
            if "match_type" in body:
                item.match_type = body["match_type"]
                changed = True
            if not changed:
                return JSONResponse({"error": "nothing to update"}, status_code=400)
            session.commit()
            item_dict = _witem_dict(item)

        if request.headers.get("HX-Request"):
            return _watchlist_row_response(request, item_dict)

        return JSONResponse({"ok": True})

    @app.get("/api/watchlist/badge")
    def watchlist_badge():
        """Return total new count for nav badge."""
        with get_session(engine) as session:
            total = session.execute(
                select(func.coalesce(func.sum(WatchlistItem.new_count), 0))
                .where(WatchlistItem.enabled == True)
            ).scalar() or 0
        return JSONResponse({"new_count": int(total)})

    @app.get("/scheduler", response_class=HTMLResponse)
    def scheduler_page(request: Request, flash: str = "", flash_type: str = ""):
        from telecrime.scheduler import JOB_DEFS, read_status

        statuses = read_status()
        jobs = []
        for name, defn in JOB_DEFS.items():
            st = statuses.get(name)
            jobs.append(
                {
                    "name": name,
                    "description": defn["description"],
                    # Prefer the worker's runtime value (written to status file after CLI overrides)
                    # over the web process's stale JOB_DEFS code default.
                    "interval_hours": st.interval_hours if st else defn["interval_hours"],
                    "requires_telegram": defn["requires_telegram"],
                    "enabled": st is not None
                    and not (
                        st.last_error and "credentials not configured" in (st.last_error or "")
                    ),
                    "running": st.running if st else False,
                    "last_run": st.last_run if st else None,
                    "last_result": st.last_result if st else None,
                    "last_error": st.last_error if st else None,
                    "next_run": st.next_run if st else None,
                }
            )
        return templates.TemplateResponse(
            "scheduler.html",
            {"request": request, "jobs": jobs, "flash": flash, "flash_type": flash_type},
        )

    @app.post("/scheduler/run/{job_name}", response_class=HTMLResponse)
    def scheduler_run_now(request: Request, job_name: str):
        from telecrime.scheduler import (
            JOB_DEFS,
            _run_channel_join_job,
            _run_pipeline_job,
            _run_vacuum_job,
            _update_job,
        )

        if job_name not in JOB_DEFS:
            return RedirectResponse(
                f"/scheduler?flash=Unknown+job+{job_name}&flash_type=error", status_code=303
            )

        defn = JOB_DEFS[job_name]

        def _run_bg():
            from telecrime.config import load_config

            config = load_config()
            _update_job(job_name, running=True, last_run=datetime.now(UTC).isoformat())
            try:
                if job_name == "pipeline":
                    result = _run_pipeline_job(config, engine)
                elif job_name == "channel_join":
                    result = _run_channel_join_job(config, engine)
                elif job_name == "vacuum":
                    result = _run_vacuum_job(engine)
                else:
                    result = "unknown job"
                _update_job(job_name, running=False, last_result=result, last_error=None)
            except Exception as exc:
                _update_job(job_name, running=False, last_result=None, last_error=str(exc)[:500])

        t = threading.Thread(target=_run_bg, daemon=True)
        t.start()

        if request.headers.get("HX-Request"):
            from telecrime.scheduler import JOB_DEFS as _JOB_DEFS
            from telecrime.scheduler import read_status

            statuses = read_status()
            jobs = []
            for name, defn in _JOB_DEFS.items():
                st = statuses.get(name)
                jobs.append(
                    {
                        "name": name,
                        "description": defn["description"],
                        "interval_hours": defn["interval_hours"],
                        "requires_telegram": defn["requires_telegram"],
                        "enabled": st is not None
                        and not (
                            st.last_error and "credentials not configured" in (st.last_error or "")
                        ),
                        "running": st.running if st else False,
                        "last_run": st.last_run if st else None,
                        "last_result": st.last_result if st else None,
                        "last_error": st.last_error if st else None,
                        "next_run": st.next_run if st else None,
                    }
                )
            return templates.TemplateResponse(
                "partials/scheduler_jobs.html",
                {
                    "request": request,
                    "jobs": jobs,
                    "flash": f"Job {job_name} started in background",
                    "flash_type": "ok",
                },
            )

        return RedirectResponse(
            f"/scheduler?flash=Job+{job_name}+started+in+background&flash_type=ok",
            status_code=303,
        )

    @app.get("/scheduler/pipeline-status")
    def pipeline_status_api():
        data = read_progress() or {"running": False}
        return JSONResponse(data)

    @app.get("/scheduler/jobs-fragment", response_class=HTMLResponse)
    def scheduler_jobs_fragment(request: Request, flash: str = "", flash_type: str = ""):
        from telecrime.scheduler import JOB_DEFS, read_status

        statuses = read_status()
        jobs = []
        for name, defn in JOB_DEFS.items():
            st = statuses.get(name)
            jobs.append(
                {
                    "name": name,
                    "description": defn["description"],
                    "interval_hours": st.interval_hours if st else defn["interval_hours"],
                    "requires_telegram": defn["requires_telegram"],
                    "enabled": st is not None
                    and not (
                        st.last_error and "credentials not configured" in (st.last_error or "")
                    ),
                    "running": st.running if st else False,
                    "last_run": st.last_run if st else None,
                    "last_result": st.last_result if st else None,
                    "last_error": st.last_error if st else None,
                    "next_run": st.next_run if st else None,
                }
            )
        return templates.TemplateResponse(
            "partials/scheduler_jobs.html",
            {"request": request, "jobs": jobs, "flash": flash, "flash_type": flash_type},
        )

    @app.get("/credential/{credential_id}", response_class=HTMLResponse)
    def credential_detail(request: Request, credential_id: int):
        with get_session(engine) as session:
            cred = session.get(ParsedCredential, credential_id)
            if not cred:
                return HTMLResponse("<h1>Not found</h1>", status_code=404)

            conversation = None
            if cred.source_conversation_id:
                conversation = session.get(Conversation, cred.source_conversation_id)

            related = []
            if cred.domain:
                related = (
                    session.query(ParsedCredential)
                    .filter(
                        ParsedCredential.domain == cred.domain,
                        ParsedCredential.id != cred.id,
                    )
                    .order_by(ParsedCredential.created_at.desc())
                    .limit(10)
                    .all()
                )
            if len(related) < 10 and cred.source_archive:
                existing_ids = {r.id for r in related} | {cred.id}
                more = (
                    session.query(ParsedCredential)
                    .filter(
                        ParsedCredential.source_archive == cred.source_archive,
                        ParsedCredential.id.notin_(existing_ids),
                    )
                    .order_by(ParsedCredential.created_at.desc())
                    .limit(10 - len(related))
                    .all()
                )
                related.extend(more)

            return templates.TemplateResponse(
                "credential.html",
                {
                    "request": request,
                    "cred": cred,
                    "conversation": conversation,
                    "related": related,
                },
            )

    @app.get("/conversation/{conversation_id}", response_class=HTMLResponse)
    def conversation_detail(
        request: Request,
        conversation_id: int,
        msg_limit: int = Query(50, ge=1, le=500),
    ):
        with get_session(engine) as session:
            conv = session.get(Conversation, conversation_id)
            if not conv:
                return HTMLResponse("<h1>Not found</h1>", status_code=404)

            msg_count = (
                session.query(func.count(Message.id))
                .filter(Message.conversation_id == conversation_id)
                .scalar() or 0
            )
            attachment_count = (
                session.query(func.count(FileAttachment.id))
                .join(Message, Message.id == FileAttachment.message_id)
                .filter(Message.conversation_id == conversation_id)
                .scalar() or 0
            )
            cred_count = (
                session.query(func.count(ParsedCredential.id))
                .filter(ParsedCredential.source_conversation_id == conversation_id)
                .scalar() or 0
            )
            recent_messages = (
                session.query(Message)
                .filter(Message.conversation_id == conversation_id)
                .order_by(Message.platform_timestamp.desc())
                .limit(msg_limit)
                .all()
            )
            recent_creds = (
                session.query(ParsedCredential)
                .filter(ParsedCredential.source_conversation_id == conversation_id)
                .order_by(ParsedCredential.created_at.desc())
                .limit(10)
                .all()
            )

            return templates.TemplateResponse(
                "conversation.html",
                {
                    "request": request,
                    "conv": conv,
                    "msg_count": msg_count,
                    "attachment_count": attachment_count,
                    "cred_count": cred_count,
                    "recent_messages": recent_messages,
                    "recent_creds": recent_creds,
                    "msg_limit": msg_limit,
                },
            )

    @app.get("/conversations", response_class=HTMLResponse)
    def conversations_list(
        request: Request,
        page: int = Query(1, ge=1),
        limit: int = Query(50, ge=1, le=500),
    ):
        with get_session(engine) as session:
            excluded_conversations, _ = _get_exclusions(session)

            msg_counts = (
                session.query(
                    Message.conversation_id.label("cid"),
                    func.count(Message.id).label("n"),
                )
                .group_by(Message.conversation_id)
                .subquery()
            )
            cred_counts = (
                session.query(
                    ParsedCredential.source_conversation_id.label("cid"),
                    func.count(ParsedCredential.id).label("n"),
                )
                .group_by(ParsedCredential.source_conversation_id)
                .subquery()
            )

            base = (
                session.query(
                    Conversation,
                    func.coalesce(msg_counts.c.n, 0).label("msg_count"),
                    func.coalesce(cred_counts.c.n, 0).label("cred_count"),
                )
                .outerjoin(msg_counts, msg_counts.c.cid == Conversation.id)
                .outerjoin(cred_counts, cred_counts.c.cid == Conversation.id)
            )

            if excluded_conversations:
                base = base.filter(Conversation.id.notin_(excluded_conversations))

            total_q = session.query(Conversation)
            if excluded_conversations:
                total_q = total_q.filter(Conversation.id.notin_(excluded_conversations))
            total_convs = total_q.count()
            # Avoid full-table COUNT/GROUP BY over the 100M+ row tables — use
            # instant reltuples estimates like the home page does.
            counts = _pg_fast_count_estimates(session, "messages", "parsed_credentials")
            total_msgs = counts.get("messages", 0)
            total_creds = counts.get("parsed_credentials", 0)

            pages = max(1, (total_convs + limit - 1) // limit)
            offset = (page - 1) * limit
            rows = (
                base.order_by(func.coalesce(cred_counts.c.n, 0).desc())
                .offset(offset)
                .limit(limit)
                .all()
            )

            conversations = [
                {"conv": r.Conversation, "msg_count": r.msg_count, "cred_count": r.cred_count}
                for r in rows
            ]

            return templates.TemplateResponse(
                "conversations.html",
                {
                    "request": request,
                    "conversations": conversations,
                    "page": page,
                    "pages": pages,
                    "limit": limit,
                    "stats": {
                        "total_convs": total_convs,
                        "total_msgs": total_msgs,
                        "total_creds": total_creds,
                    },
                },
            )

    @app.get("/channels", response_class=HTMLResponse)
    def channels_list(
        request: Request,
        subscribed: str = Query(""),
        active: str = Query(""),
        source: str = Query(""),
        page: int = Query(1, ge=1),
        limit: int = Query(100, ge=1, le=500),
    ):
        with get_session(engine) as session:
            base = session.query(TelegramChannel)

            if subscribed == "1":
                base = base.filter(TelegramChannel.is_subscribed == True)
            elif subscribed == "0":
                base = base.filter(TelegramChannel.is_subscribed == False)

            if active == "1":
                base = base.filter(TelegramChannel.is_active == True)
            elif active == "0":
                base = base.filter(TelegramChannel.is_active == False)

            if source:
                base = base.filter(TelegramChannel.source == source)

            stats_row = base.with_entities(
                func.count(TelegramChannel.id),
                func.coalesce(
                    func.sum(case((TelegramChannel.is_subscribed == True, 1), else_=0)),
                    0,
                ),
                func.coalesce(
                    func.sum(case((TelegramChannel.is_active == True, 1), else_=0)),
                    0,
                ),
                func.coalesce(func.sum(TelegramChannel.credentials_extracted), 0),
            ).one()
            total = stats_row[0]
            n_subscribed = stats_row[1]
            n_active = stats_row[2]
            total_creds = stats_row[3]

            sources = [
                r[0]
                for r in session.query(TelegramChannel.source)
                .filter(TelegramChannel.source.isnot(None))
                .distinct()
                .all()
            ]

            pages = max(1, (total + limit - 1) // limit)
            offset = (page - 1) * limit
            channels = (
                base.order_by(TelegramChannel.credentials_extracted.desc())
                .offset(offset)
                .limit(limit)
                .all()
            )

            return templates.TemplateResponse(
                "channels.html",
                {
                    "request": request,
                    "channels": channels,
                    "page": page,
                    "pages": pages,
                    "limit": limit,
                    "stats": {
                        "total": total,
                        "subscribed": n_subscribed,
                        "active": n_active,
                        "total_creds": total_creds,
                    },
                    "sources": sorted(sources),
                    "filter_subscribed": subscribed,
                    "filter_active": active,
                    "filter_source": source,
                },
            )

    @app.get("/passwords", response_class=HTMLResponse)
    def passwords_page(
        request: Request,
        scope: str = Query(""),
        method: str = Query(""),
        sort: str = Query("success"),
        page: int = Query(1, ge=1),
        limit: int = Query(50, ge=1, le=200),
    ):
        with get_session(engine) as session:
            q = session.query(PasswordCandidate)

            if scope:
                q = q.filter(PasswordCandidate.scope == scope)
            if method:
                q = q.filter(PasswordCandidate.extraction_method == method)

            if sort == "success":
                q = q.order_by(
                    PasswordCandidate.times_succeeded.desc(),
                    PasswordCandidate.confidence.desc(),
                )
            elif sort == "confidence":
                q = q.order_by(PasswordCandidate.confidence.desc())
            elif sort == "failed":
                q = q.order_by(PasswordCandidate.times_failed.desc())
            else:
                q = q.order_by(PasswordCandidate.id.desc())

            total = q.count()
            offset = (page - 1) * limit
            candidates = q.offset(offset).limit(limit).all()

            # Stats
            stats_row = session.query(
                func.count(PasswordCandidate.id),
                func.avg(PasswordCandidate.confidence),
                func.sum(PasswordCandidate.times_succeeded),
                func.sum(PasswordCandidate.times_failed),
            ).one()
            total_count = stats_row[0] or 0
            avg_conf = round((stats_row[1] or 0) * 100)
            total_successes = stats_row[2] or 0
            total_failures = stats_row[3] or 0

            scopes = [r[0] for r in session.query(PasswordCandidate.scope).distinct().all()]
            methods = [r[0] for r in session.query(PasswordCandidate.extraction_method).distinct().all()]

            pages = max(1, (total + limit - 1) // limit)
            return templates.TemplateResponse(
                "passwords.html",
                {
                    "request": request,
                    "candidates": candidates,
                    "total": total,
                    "page": page,
                    "pages": pages,
                    "limit": limit,
                    "scope": scope,
                    "method": method,
                    "sort": sort,
                    "scopes": sorted(scopes),
                    "methods": sorted(methods),
                    "stats": {
                        "total": total_count,
                        "avg_conf": avg_conf,
                        "total_successes": total_successes,
                        "total_failures": total_failures,
                    },
                },
            )

    @app.get("/passwords.csv")
    def export_passwords_csv(
        scope: str = Query(""),
        method: str = Query(""),
        sort: str = Query("success"),
        limit: int = Query(5000, ge=1, le=50000),
    ):
        def row_iter():
            yield "id,value,scope,extraction_method,confidence,times_succeeded,times_failed,context_text,created_at\n"
            with get_session(engine) as session:
                q = session.query(PasswordCandidate)
                if scope:
                    q = q.filter(PasswordCandidate.scope == scope)
                if method:
                    q = q.filter(PasswordCandidate.extraction_method == method)
                if sort == "success":
                    q = q.order_by(
                        PasswordCandidate.times_succeeded.desc(),
                        PasswordCandidate.confidence.desc(),
                    )
                elif sort == "confidence":
                    q = q.order_by(PasswordCandidate.confidence.desc())
                elif sort == "failed":
                    q = q.order_by(PasswordCandidate.times_failed.desc())
                else:
                    q = q.order_by(PasswordCandidate.id.desc())
                for c in q.limit(limit):
                    values = [
                        c.id,
                        c.value,
                        c.scope.value if c.scope else "",
                        c.extraction_method,
                        round(c.confidence, 4),
                        c.times_succeeded,
                        c.times_failed,
                        c.context_text or "",
                        c.created_at,
                    ]
                    escaped = []
                    for v in values:
                        text = "" if v is None else str(v)
                        text = text.replace('"', '""')
                        escaped.append(f'"{text}"')
                    yield ",".join(escaped) + "\n"

        headers = {"Content-Disposition": 'attachment; filename="telecrime_passwords.csv"'}
        return StreamingResponse(row_iter(), media_type="text/csv", headers=headers)

    return app
