"""Tests for dashboard search helpers."""

import asyncio
import json
from datetime import UTC, datetime
from typing import Any, cast

from telecrime.database import get_session
from telecrime.models import (
    ArchiveGroup,
    Conversation,
    DownloadArtifact,
    ExtractionJob,
    FileAttachment,
    Message,
    ParsedCredential,
)
from telecrime.models.watchlist import WatchlistItem
from telecrime.states import DownloadStatus, ExtractionStatus
from telecrime.web.app import (
    _check_watchlist,
    _credential_ids_via_fts,
    _ensure_search_infra,
    _message_ids_via_fts,
    _pipeline_running_for_heavy_web_work,
    _preferred_table_estimate,
    _search_for_export,
    _stats_cache_path,
    _triage_payload,
    create_app,
)


def test_credential_fts_search_applies_filters_before_limit(pg_session):
    """FTS credential search applies structured filters in SQL before limiting."""
    from telecrime.fts import ensure_fts

    ensure_fts(pg_session.bind)
    pg_session.add_all(
        [
            ParsedCredential(
                url="https://accounts.google.com/login",
                domain="accounts.google.com",
                username="alice",
                password="secret",
                stealer_type="redline",
                credential_hash=ParsedCredential.compute_hash(
                    "accounts.google.com", "alice", "secret"
                ),
            ),
            ParsedCredential(
                url="https://accounts.google.com/login",
                domain="accounts.google.com",
                username="bob",
                password="secret",
                stealer_type="vidar",
                credential_hash=ParsedCredential.compute_hash(
                    "accounts.google.com", "bob", "secret"
                ),
            ),
        ]
    )
    pg_session.commit()

    ids = _credential_ids_via_fts(
        pg_session,
        terms="google",
        filters={"stealer": ["redline"]},
        exclude_conversation_ids=set(),
        limit=1,
    )
    results = _search_for_export(
        pg_session,
        "google",
        {"stealer": ["redline"]},
        False,
        True,
        set(),
        set(),
        10,
        10,
        10,
        10,
        10,
        10,
        10,
    )

    assert len(ids) == 1
    assert results.credentials and results.credentials[0].username == "alice"


def test_heavy_web_work_pauses_while_pipeline_running(monkeypatch):
    """Background stats workers should not compete with an active pipeline."""
    monkeypatch.delenv("TELECRIME_WEB_STATS_DURING_PIPELINE", raising=False)
    monkeypatch.setattr("telecrime.web.app.read_progress", lambda: {"running": True})

    assert _pipeline_running_for_heavy_web_work() is True


def test_stats_cache_path_uses_configured_data_dir(tmp_path, monkeypatch):
    data_dir = tmp_path / "runtime"
    monkeypatch.setenv("TELECRIME_DATA_DIR", str(data_dir))

    assert _stats_cache_path() == data_dir / "stats_cache.json"


def test_heavy_web_work_runs_when_pipeline_idle(monkeypatch):
    monkeypatch.delenv("TELECRIME_WEB_STATS_DURING_PIPELINE", raising=False)
    monkeypatch.setattr("telecrime.web.app.read_progress", lambda: {"running": False})

    assert _pipeline_running_for_heavy_web_work() is False


def test_heavy_web_work_override_allows_stats_during_pipeline(monkeypatch):
    """The operational escape hatch keeps manual stats refreshes possible."""
    monkeypatch.setenv("TELECRIME_WEB_STATS_DURING_PIPELINE", "1")
    monkeypatch.setattr("telecrime.web.app.read_progress", lambda: {"running": True})

    assert _pipeline_running_for_heavy_web_work() is False


def test_preferred_table_estimate_keeps_larger_fast_count():
    """Dashboard counts should not drop when pg_stat live tuples undercount."""
    assert _preferred_table_estimate(168_815_328, 8_836_812) == 168_815_328
    assert _preferred_table_estimate(None, 8_836_812) == 8_836_812
    assert _preferred_table_estimate(-1, None) == 0


def test_watchlist_incremental_check_counts_only_new_rows(pg_engine):
    """Watchlist can keep updating cheaply while ingestion is active."""
    from telecrime.database import get_session as _gs

    checked_at = datetime(2026, 4, 26, 7, 0, tzinfo=UTC)
    with _gs(pg_engine) as session:
        session.add(
            WatchlistItem(
                label="sec-consult",
                query="sec-consult",
                match_type="any",
                enabled=True,
                last_checked_at=checked_at,
                last_known_count=1,
                new_count=2,
            )
        )
        session.add_all(
            [
                ParsedCredential(
                    url="https://old.example/login",
                    domain="old.example",
                    username="sec-consult-old",
                    password="pw",
                    created_at=datetime(2026, 4, 26, 6, 0, tzinfo=UTC),
                    credential_hash=ParsedCredential.compute_hash(
                        "old.example", "sec-consult-old", "pw"
                    ),
                ),
                ParsedCredential(
                    url="https://new.example/login",
                    domain="new.example",
                    username="sec-consult-new",
                    password="pw",
                    created_at=datetime(2026, 4, 26, 7, 30, tzinfo=UTC),
                    credential_hash=ParsedCredential.compute_hash(
                        "new.example", "sec-consult-new", "pw"
                    ),
                ),
                ParsedCredential(
                    url="https://new.example/login",
                    domain="new.example",
                    username="other",
                    password="pw",
                    created_at=datetime(2026, 4, 26, 7, 45, tzinfo=UTC),
                    credential_hash=ParsedCredential.compute_hash("new.example", "other", "pw"),
                ),
            ]
        )

    _check_watchlist(pg_engine, incremental_only=True)

    with _gs(pg_engine) as session:
        item = session.query(WatchlistItem).one()
        assert item.new_count == 3
        assert item.last_known_count == 2
        assert item.last_checked_at is not None
        assert item.last_checked_at.replace(tzinfo=UTC) > checked_at


def test_message_fts_search_preserves_order_and_exclusions(pg_engine):
    """Message FTS helper returns ordered IDs and respects exclusions."""
    from telecrime.database import get_session as _gs

    with _gs(pg_engine) as session:
        session.add_all(
            [
                Conversation(platform_id=1, conversation_type="channel"),
                Conversation(platform_id=2, conversation_type="channel"),
            ]
        )
        session.flush()
        session.add_all(
            [
                Message(
                    conversation_id=1,
                    platform_id=10,
                    platform_timestamp=datetime(2026, 3, 10, tzinfo=UTC),
                    text="hello google",
                    caption=None,
                    is_forwarded=False,
                ),
                Message(
                    conversation_id=2,
                    platform_id=11,
                    platform_timestamp=datetime(2026, 3, 10, 0, 0, 1, tzinfo=UTC),
                    text="hello google again",
                    caption=None,
                    is_forwarded=False,
                ),
            ],
        )

    with _gs(pg_engine) as session:
        ids = _message_ids_via_fts(
            session,
            terms="google",
            exclude_conversation_ids={1},
            limit=10,
        )

    assert ids == [2]


def test_triage_payload_includes_recent_failures(pg_engine):
    """Dashboard triage payload includes failed downloads and extractions."""
    with get_session(pg_engine) as session:
        conv = Conversation(platform_id=1, conversation_type="channel")
        session.add(conv)
        session.flush()
        msg = Message(
            conversation_id=conv.id,
            platform_id=10,
            platform_timestamp=datetime.now(UTC),
            text="message",
        )
        session.add(msg)
        session.flush()
        attachment = FileAttachment(
            message_id=msg.id, platform_file_id="file1", filename="sample.zip"
        )
        session.add(attachment)
        session.flush()
        session.add(
            DownloadArtifact(
                attachment_id=attachment.id, status=DownloadStatus.FAILED, error_message="network"
            )
        )
        group = ArchiveGroup(
            fingerprint="triage-group",
            base_name="sample.zip",
            expected_part_count=1,
            detected_part_count=1,
        )
        session.add(group)
        session.flush()
        session.add(
            ExtractionJob(
                group_id=group.id,
                status=ExtractionStatus.FAILED_TERMINAL,
                last_error_code="CORRUPTED",
                last_error_message="corrupted archive",
                target_extensions=".txt",
            )
        )

    with get_session(pg_engine) as session:
        payload = _triage_payload(session, limit=20)
    payload = cast(dict[str, Any], payload)

    assert payload["summary"]["download_failures"] == 1
    assert payload["summary"]["extraction_failures"] == 1
    assert payload["failed_downloads"][0].error_message == "network"
    assert payload["failed_downloads"][0].attachment.filename == "sample.zip"
    assert payload["failed_extractions"][0].last_error_code == "CORRUPTED"
    assert payload["failed_extractions"][0].group.base_name == "sample.zip"


def test_search_count_endpoint_returns_total_matches(pg_engine):
    """Dashboard search count endpoint returns the soft-deduped credential match count."""
    from telecrime.database import get_session as _gs

    with _gs(pg_engine) as session:
        session.add_all(
            [
                ParsedCredential(
                    url="https://accounts.google.com/login",
                    domain="accounts.google.com",
                    username="alice",
                    password="secret1",
                    soft_credential_hash=ParsedCredential.compute_soft_hash(
                        "accounts.google.com", "alice", "secret1"
                    ),
                    credential_hash=ParsedCredential.compute_hash(
                        "accounts.google.com", "alice", "secret1"
                    ),
                ),
                ParsedCredential(
                    url="https://accounts.google.com/mail",
                    domain="accounts.google.com",
                    username="ALICE",
                    password="secret1",
                    soft_credential_hash=ParsedCredential.compute_soft_hash(
                        "accounts.google.com", "ALICE", "secret1"
                    ),
                    credential_hash=ParsedCredential.compute_hash(
                        "accounts.google.com", "ALICE", "secret1"
                    ),
                ),
            ]
        )

    app = create_app(pg_engine.url.render_as_string(hide_password=False))
    route = cast(Any, next(r for r in app.routes if getattr(r, "path", None) == "/search/count"))
    response = route.endpoint(q="google", regex=False)

    assert response.status_code == 200
    payload = cast(dict[str, Any], json.loads(response.body))
    assert payload["total_credentials"] == 1


def test_search_export_soft_dedupes_equivalent_credentials(pg_engine):
    assert _ensure_search_infra(pg_engine) is True

    with get_session(pg_engine) as session:
        session.add_all(
            [
                ParsedCredential(
                    url="https://example.com/login",
                    domain="example.com",
                    username="alice",
                    password="secret",
                    soft_credential_hash=ParsedCredential.compute_soft_hash(
                        "example.com", "alice", "secret"
                    ),
                    credential_hash=ParsedCredential.compute_hash(
                        "example.com", "alice", "secret"
                    ),
                ),
                ParsedCredential(
                    url="https://example.com/account",
                    domain="example.com",
                    username="ALICE",
                    password="secret",
                    soft_credential_hash=ParsedCredential.compute_soft_hash(
                        "example.com", "ALICE", "secret"
                    ),
                    credential_hash=ParsedCredential.compute_hash(
                        "example.com", "ALICE", "secret"
                    ),
                ),
            ]
        )

    with get_session(pg_engine) as session:
        results = _search_for_export(
            session,
            "example",
            {},
            False,
            True,
            set(),
            set(),
            10,
            10,
            10,
            10,
            10,
            10,
            10,
        )

    assert len(results.credentials) == 1


def test_export_json_supports_no_markdown(pg_engine):
    """Search export can strip simple markdown formatting from string fields."""
    assert _ensure_search_infra(pg_engine) is True

    with get_session(pg_engine) as session:
        session.add(
            ParsedCredential(
                url="https://example.com/login",
                domain="example.com",
                username="**alice**",
                password="`secret`",
                source_archive="[archive](https://example.com/archive)",
                credential_hash=ParsedCredential.compute_hash(
                    "example.com", "**alice**", "`secret`"
                ),
            )
        )

    app = create_app(pg_engine.url.render_as_string(hide_password=False))
    route = cast(
        Any, next(r for r in app.routes if getattr(r, "path", None) == "/search/export.json")
    )
    response = route.endpoint(
        q="example",
        regex=False,
        no_markdown=True,
        limit_credentials=5000,
        limit_messages=1000,
        limit_attachments=1000,
        limit_archives=1000,
        limit_extracted=1000,
        limit_conversations=1000,
        limit_channels=1000,
    )

    assert response.status_code == 200
    payload = cast(dict[str, Any], json.loads(response.body))
    first = payload["results"]["credentials"][0]
    assert first["username"] == "alice"
    assert first["password"] == "secret"
    assert first["source_archive"] == "archive"


def test_export_markdown_returns_markdown_tables(pg_engine):
    """Markdown export returns Markdown tables and supports plain-value export."""
    assert _ensure_search_infra(pg_engine) is True

    with get_session(pg_engine) as session:
        session.add(
            ParsedCredential(
                url="https://example.com/login",
                domain="example.com",
                username="**alice**",
                password="secret|pipe",
                source_archive="[archive](https://example.com/archive)",
                credential_hash=ParsedCredential.compute_hash(
                    "example.com", "**alice**", "secret|pipe"
                ),
            )
        )

    app = create_app(pg_engine.url.render_as_string(hide_password=False))
    route = cast(
        Any, next(r for r in app.routes if getattr(r, "path", None) == "/search/export.md")
    )
    response = route.endpoint(
        q="example",
        regex=False,
        no_markdown=True,
        limit_credentials=5000,
        limit_messages=1000,
        limit_attachments=1000,
        limit_archives=1000,
        limit_extracted=1000,
        limit_conversations=1000,
        limit_channels=1000,
    )

    assert response.status_code == 200
    body = asyncio.run(_read_streaming_body(response))
    assert "# Telecrime Search Export" in body
    assert "## Credentials" in body
    assert "| id | url | domain | username | password |" in body
    assert "alice" in body
    assert "secret\\|pipe" in body
    assert "archive" in body


async def _read_streaming_body(response) -> str:
    chunks: list[bytes] = []
    async for chunk in response.body_iterator:
        chunks.append(chunk if isinstance(chunk, bytes) else chunk.encode())
    return b"".join(chunks).decode()
