"""Tests for dashboard search helpers."""

import asyncio
import json
import threading
import time
from datetime import UTC, datetime
from typing import Any, cast
from urllib.parse import urlencode

from starlette.requests import Request

from telecrime.database import get_engine, get_session, init_db
from telecrime.models import (
    ArchiveGroup,
    ArchiveGroupPart,
    Conversation,
    DownloadArtifact,
    ExtractedOutput,
    ExtractionJob,
    FileAttachment,
    Message,
    ParsedCredential,
    PasswordCandidate,
    TelegramChannel,
)
from telecrime.models.watchlist import WatchlistItem
from telecrime.states import (
    DownloadStatus,
    ExtractionStatus,
    GroupStatus,
    PasswordScope,
)
from telecrime.web.app import (
    _check_watchlist,
    _claim_cred_counts_refresh,
    _credential_ids_via_fts,
    _ensure_search_infra,
    _message_ids_via_fts,
    _normalize_password_scope,
    _pipeline_running_for_heavy_web_work,
    _preferred_table_estimate,
    _search_for_export,
    _stats_cache_path,
    _triage_payload,
    _witem_dict,
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


def _seed_filter_only_rows(session):
    """Seed one unrelated conversation chain plus two credentials."""
    conv = Conversation(platform_id=1, conversation_type="channel", title="unrelated")
    session.add(conv)
    session.flush()
    msg = Message(
        conversation_id=conv.id,
        platform_id=100,
        platform_timestamp=datetime.now(UTC),
        text="completely unrelated message",
    )
    session.add(msg)
    session.flush()
    attachment = FileAttachment(
        message_id=msg.id, platform_file_id="unrelated-file", filename="unrelated.zip"
    )
    session.add(attachment)
    session.flush()
    artifact = DownloadArtifact(attachment_id=attachment.id, status=DownloadStatus.PENDING)
    group = ArchiveGroup(
        fingerprint="filter-only-search",
        base_name="unrelated.zip",
        expected_part_count=1,
        detected_part_count=1,
    )
    session.add_all([artifact, group])
    session.flush()
    job = ExtractionJob(
        group_id=group.id, status=ExtractionStatus.PENDING, target_extensions=".txt"
    )
    session.add(job)
    session.flush()
    session.add(
        ExtractedOutput(
            job_id=job.id,
            output_path="/tmp/unrelated/output.txt",
            output_filename="unrelated-output.txt",
            output_hash="0" * 64,
        )
    )
    session.add(
        TelegramChannel(
            platform_id=999, source="test", username="unrelatedchan", title="Unrelated"
        )
    )
    session.add_all(
        [
            ParsedCredential(
                url="https://example.com/login",
                domain="example.com",
                username="alice",
                password="secret",
                credential_hash=ParsedCredential.compute_hash("example.com", "alice", "secret"),
            ),
            ParsedCredential(
                url="https://other.test/login",
                domain="other.test",
                username="bob",
                password="secret",
                credential_hash=ParsedCredential.compute_hash("other.test", "bob", "secret"),
            ),
        ]
    )
    session.flush()


def test_filter_only_export_skips_unrelated_rows(session):
    """A domain: filter with no free text must not run "%%" sub-searches."""
    _seed_filter_only_rows(session)

    results = _search_for_export(
        session,
        "",
        {"domain": ["example.com"]},
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

    assert [c.username for c in results.credentials] == ["alice"]
    assert results.messages == []
    assert results.attachments == []
    assert results.archives == []
    assert results.extracted == []
    assert results.conversations == []
    assert results.channels == []


def test_filter_only_search_does_not_render_unrelated_rows(tmp_path):
    """The HTML search has the same %% bug for the non-credential sections."""
    app, seed_engine = _sqlite_app(tmp_path)
    with get_session(seed_engine) as session:
        _seed_filter_only_rows(session)

    response = _route(app, "/search").endpoint(
        request=_web_request(),
        q="domain:example.com",
        limit=50,
        limit_messages=10,
        limit_attachments=10,
        limit_archives=10,
        limit_extracted=10,
        limit_conversations=10,
        limit_channels=10,
        page=1,
        page_size=50,
        after_id=0,
        regex=False,
        facets=False,
        no_markdown=False,
        source_conv=0,
    )

    assert response.status_code == 200
    results = response.context["results"]
    assert [c.username for c in results.credentials] == ["alice"]
    assert results.messages == []
    assert results.attachments == []
    assert results.archives == []
    assert results.extracted == []
    assert results.conversations == []
    assert results.channels == []


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


def test_parse_query_unbalanced_quote_falls_back():
    """An unbalanced quote must not 500 — the whole input becomes one term
    (round-3 fix)."""
    from telecrime.web.app import _parse_query

    terms, filters = _parse_query('foo"bar')
    assert "foo\"bar" in terms
    assert filters == {}


def test_parse_query_extracts_filters():
    from telecrime.web.app import _parse_query

    terms, filters = _parse_query("admin domain:example.com stealer:redline")
    assert "admin" in terms
    assert filters.get("domain") == ["example.com"]
    assert filters.get("stealer") == ["redline"]


def test_errors_json_count_tolerates_malformed():
    """errors_json can be null/plain text/partial JSON — count must not crash
    (round-3 fix)."""
    from telecrime.web.app import _errors_json_count

    assert _errors_json_count(None) == 0
    assert _errors_json_count("") == 0
    assert _errors_json_count("not json") == 0
    assert _errors_json_count('{"a": 1}') == 0
    assert _errors_json_count('["one", "two"]') == 2


def _web_request(
    *, method: str = "GET", form: dict[str, str] | None = None
) -> Request:
    """Minimal ASGI request; `form` makes request.form() parsable."""
    body = urlencode(form or {}).encode()
    pending = [{"type": "http.request", "body": body, "more_body": False}]

    async def receive():
        return pending.pop(0) if pending else {"type": "http.disconnect"}

    headers = (
        [(b"content-type", b"application/x-www-form-urlencoded")] if form is not None else []
    )
    return Request(
        {
            "type": "http",
            "method": method,
            "path": "/",
            "query_string": b"",
            "headers": headers,
        },
        receive,
    )


def _route(app, path: str, method: str | None = None):
    routes = [r for r in app.routes if getattr(r, "path", None) == path]
    if method is not None:
        routes = [
            r for r in routes if method in (getattr(r, "methods", None) or set())
        ]
    return cast(Any, routes[0])


def _sqlite_app(tmp_path):
    """create_app bound to a file-backed SQLite DB, returning (app, engine)."""
    url = f"sqlite:///{tmp_path / 'webtest.db'}"
    seed_engine = get_engine(url)
    init_db(seed_engine)
    return create_app(url), seed_engine


def test_triage_add_password_scopes_candidate_to_source_conversation(tmp_path):
    """FIX 1: the route must not touch job.source_conversation_id (no such column)
    and must scope the candidate via the group chain."""
    app, seed_engine = _sqlite_app(tmp_path)
    with get_session(seed_engine) as session:
        conv = Conversation(platform_id=1, conversation_type="channel")
        session.add(conv)
        session.flush()
        msg = Message(
            conversation_id=conv.id,
            platform_id=10,
            platform_timestamp=datetime.now(UTC),
            text="archive password",
        )
        session.add(msg)
        session.flush()
        attachment = FileAttachment(
            message_id=msg.id, platform_file_id="file1", filename="sample.zip"
        )
        session.add(attachment)
        session.flush()
        artifact = DownloadArtifact(
            attachment_id=attachment.id, status=DownloadStatus.COMPLETED
        )
        session.add(artifact)
        session.flush()
        group = ArchiveGroup(
            fingerprint="pw-chain",
            base_name="sample.zip",
            expected_part_count=1,
            detected_part_count=1,
        )
        session.add(group)
        session.flush()
        session.add(
            ArchiveGroupPart(group_id=group.id, artifact_id=artifact.id, part_index=0)
        )
        job = ExtractionJob(
            group_id=group.id,
            status=ExtractionStatus.PASSWORD_NEEDED,
            target_extensions=".txt",
        )
        session.add(job)
        session.flush()
        job_id = job.id
        conversation_id = conv.id

    response = asyncio.run(
        _route(app, "/triage/add-password/{job_id}").endpoint(
            job_id=job_id,
            request=_web_request(method="POST", form={"password": "hunter2"}),
        )
    )
    assert response.status_code == 200

    with get_session(seed_engine) as session:
        candidate = session.query(PasswordCandidate).one()
        assert candidate.value == "hunter2"
        assert candidate.scope == PasswordScope.CONVERSATION
        assert candidate.conversation_id == conversation_id
        assert session.get(ExtractionJob, job_id).status == ExtractionStatus.PENDING
        assert session.get(ArchiveGroup, group.id).status == GroupStatus.READY


def test_triage_add_password_falls_back_to_global_without_chain(tmp_path):
    """FIX 1: a job whose group has no parts yields a GLOBAL candidate."""
    app, seed_engine = _sqlite_app(tmp_path)
    with get_session(seed_engine) as session:
        group = ArchiveGroup(
            fingerprint="pw-orphan",
            base_name=None,
            expected_part_count=1,
            detected_part_count=0,
        )
        session.add(group)
        session.flush()
        job = ExtractionJob(
            group_id=group.id,
            status=ExtractionStatus.FAILED_TERMINAL,
            target_extensions=".txt",
        )
        session.add(job)
        session.flush()
        job_id = job.id

    response = asyncio.run(
        _route(app, "/triage/add-password/{job_id}").endpoint(
            job_id=job_id,
            request=_web_request(method="POST", form={"password": "fallback-pw"}),
        )
    )
    assert response.status_code == 200

    with get_session(seed_engine) as session:
        candidate = session.query(PasswordCandidate).one()
        assert candidate.scope == PasswordScope.GLOBAL
        assert candidate.conversation_id is None


def test_credential_fts_pagination_page_two_returns_next_distinct_rows(pg_session):
    """FIX 2: offset must be applied exactly once; page 2 is not skipped."""
    from telecrime.fts import ensure_fts

    ensure_fts(pg_session.bind)
    pg_session.add_all(
        [
            ParsedCredential(
                url=f"https://site{i}.example/login",
                domain=f"site{i}.example",
                username=f"pagecheck{i}",
                password=f"pw{i}",
                credential_hash=ParsedCredential.compute_hash(
                    f"site{i}.example", f"pagecheck{i}", f"pw{i}"
                ),
            )
            for i in range(6)
        ]
    )
    pg_session.commit()

    page_size = 3
    page1 = _credential_ids_via_fts(
        pg_session,
        terms="pagecheck",
        filters={},
        exclude_conversation_ids=set(),
        limit=page_size,
        offset=0,
    )
    page2 = _credential_ids_via_fts(
        pg_session,
        terms="pagecheck",
        filters={},
        exclude_conversation_ids=set(),
        limit=page_size,
        offset=page_size,
    )

    all_ids = [
        row[0] for row in pg_session.query(ParsedCredential.id).order_by(ParsedCredential.id.desc())
    ]
    assert page1 == all_ids[:page_size]
    assert page2 == all_ids[page_size : page_size * 2]
    assert set(page1).isdisjoint(page2)


def test_watchlist_unknown_baseline_seeds_without_alerting(pg_engine):
    """FIX 3: -1 sentinel must seed the baseline instead of alerting history."""
    with get_session(pg_engine) as session:
        session.add(
            WatchlistItem(
                label="unknown",
                query="seedcheck",
                match_type="any",
                enabled=True,
                last_known_count=-1,
                new_count=0,
            )
        )
        session.add_all(
            [
                ParsedCredential(
                    url=f"https://seed{i}.example/login",
                    domain=f"seed{i}.example",
                    username=f"seedcheck{i}",
                    password="pw",
                    credential_hash=ParsedCredential.compute_hash(
                        f"seed{i}.example", f"seedcheck{i}", "pw"
                    ),
                )
                for i in range(2)
            ]
        )

    _check_watchlist(pg_engine)

    with get_session(pg_engine) as session:
        item = session.query(WatchlistItem).one()
        assert item.new_count == 0
        assert item.last_known_count == 2
        assert item.last_checked_at is not None


def test_watchlist_unknown_baseline_seeds_in_incremental_mode(pg_engine):
    """FIX 3: incremental checks treat -1 as unseeded, never as a baseline."""
    checked_at = datetime(2026, 4, 26, 7, 0, tzinfo=UTC)
    with get_session(pg_engine) as session:
        session.add(
            WatchlistItem(
                label="unknown",
                query="incrseed",
                match_type="any",
                enabled=True,
                last_checked_at=checked_at,
                last_known_count=-1,
                new_count=0,
            )
        )
        session.add_all(
            [
                ParsedCredential(
                    url=f"https://incr{i}.example/login",
                    domain=f"incr{i}.example",
                    username=f"incrseed{i}",
                    password="pw",
                    created_at=datetime(2026, 4, 26, 6, 30, tzinfo=UTC),
                    credential_hash=ParsedCredential.compute_hash(
                        f"incr{i}.example", f"incrseed{i}", "pw"
                    ),
                )
                for i in range(2)
            ]
        )

    _check_watchlist(pg_engine, incremental_only=True)

    with get_session(pg_engine) as session:
        item = session.query(WatchlistItem).one()
        assert item.new_count == 0
        assert item.last_known_count == 2


def test_watchlist_add_stores_unknown_sentinel_when_count_fails(pg_engine, monkeypatch):
    """FIX 3: explicit None would be stored as 0; the route stores -1 instead."""
    from telecrime.web import app as web_app

    app = create_app(pg_engine.url.render_as_string(hide_password=False))

    def _boom(*args, **kwargs):
        raise RuntimeError("count too slow")

    monkeypatch.setattr(web_app, "_watchlist_count", _boom)

    response = asyncio.run(
        _route(app, "/api/watchlist", "POST").endpoint(
            request=_web_request(method="POST", form={"query": "unknown-count"})
        )
    )
    assert response.status_code == 200

    with get_session(pg_engine) as session:
        item = session.query(WatchlistItem).one()
        assert item.last_known_count == -1
        assert _witem_dict(item)["last_known_count"] is None


def test_normalize_password_scope_accepts_value_and_repr():
    """FIX 4: templates used to render `PasswordScope.CONVERSATION`."""
    assert _normalize_password_scope("conversation") == PasswordScope.CONVERSATION
    assert (
        _normalize_password_scope("PasswordScope.CONVERSATION")
        == PasswordScope.CONVERSATION
    )
    assert _normalize_password_scope("  Conversation  ") == PasswordScope.CONVERSATION
    assert _normalize_password_scope("") is None
    assert _normalize_password_scope("bogus") is None


def test_passwords_scope_filter_matches_enum_value(tmp_path):
    """FIX 4: /passwords must filter by scope and highlight the selection."""
    app, seed_engine = _sqlite_app(tmp_path)
    with get_session(seed_engine) as session:
        session.add_all(
            [
                PasswordCandidate(
                    value="convo-pw",
                    scope=PasswordScope.CONVERSATION,
                    extraction_method="manual",
                    confidence=0.9,
                ),
                PasswordCandidate(
                    value="global-pw",
                    scope=PasswordScope.GLOBAL,
                    extraction_method="manual",
                    confidence=0.8,
                ),
            ]
        )

    route = _route(app, "/passwords")
    for raw in ("conversation", "PasswordScope.CONVERSATION"):
        response = route.endpoint(
            request=_web_request(),
            scope=raw,
            method="",
            sort="success",
            page=1,
            limit=50,
        )
        assert response.status_code == 200
        assert response.context["scope"] == "conversation"
        assert [c.value for c in response.context["candidates"]] == ["convo-pw"]

    body = bytes(response.body).decode()
    assert '<option value="conversation"' in body
    assert "PasswordScope.CONVERSATION" not in body


def test_conversations_list_counts_only_page_and_orders(tmp_path, monkeypatch):
    """Conversations are paged first, then counted for just the page ids."""
    from telecrime.web import app as web_app

    monkeypatch.setattr(web_app, "_pg_fast_count_estimates", lambda *args: {})
    app, seed_engine = _sqlite_app(tmp_path)
    with get_session(seed_engine) as session:
        for idx, cred_count in enumerate([1, 3, 0]):
            conv = Conversation(
                platform_id=idx + 1, conversation_type="channel", title=f"conv{idx}"
            )
            session.add(conv)
            session.flush()
            for msg_idx in range(2):
                session.add(
                    Message(
                        conversation_id=conv.id,
                        platform_id=idx * 100 + msg_idx,
                        platform_timestamp=datetime.now(UTC),
                        text="hello",
                    )
                )
            for cred_idx in range(cred_count):
                session.add(
                    ParsedCredential(
                        url=f"https://conv{idx}.example/login",
                        domain=f"conv{idx}.example",
                        username=f"user{cred_idx}",
                        password="pw",
                        source_conversation_id=conv.id,
                        credential_hash=ParsedCredential.compute_hash(
                            f"conv{idx}.example", f"user{cred_idx}", "pw"
                        ),
                    )
                )

    response = _route(app, "/conversations").endpoint(
        request=_web_request(), page=1, limit=50
    )

    assert response.status_code == 200
    conversations = response.context["conversations"]
    assert [row["cred_count"] for row in conversations] == [3, 1, 0]
    assert [row["msg_count"] for row in conversations] == [2, 2, 2]
    assert response.context["stats"]["total_convs"] == 3


def test_conversation_detail_caps_cred_count(tmp_path, monkeypatch):
    """The exact COUNT(*) is replaced by a LIMIT-capped count that renders as
    "N+" once the cap is reached."""
    from telecrime.web import app as web_app

    monkeypatch.setattr(web_app, "_COUNT_CAP", 3)
    app, seed_engine = _sqlite_app(tmp_path)
    with get_session(seed_engine) as session:
        conv = Conversation(platform_id=1, conversation_type="channel")
        session.add(conv)
        session.flush()
        conv_id = conv.id
        for idx in range(4):
            session.add(
                ParsedCredential(
                    url=f"https://cap{idx}.example/login",
                    domain=f"cap{idx}.example",
                    username=f"user{idx}",
                    password="pw",
                    source_conversation_id=conv.id,
                    credential_hash=ParsedCredential.compute_hash(
                        f"cap{idx}.example", f"user{idx}", "pw"
                    ),
                )
            )

    response = _route(app, "/conversation/{conversation_id}").endpoint(
        request=_web_request(), conversation_id=conv_id, msg_limit=50
    )

    assert response.status_code == 200
    assert response.context["cred_count"] == "2+"
    assert len(response.context["recent_creds"]) == 4


def test_home_exclusions_use_fast_estimates(tmp_path, monkeypatch):
    """With TELECRIME_EXCLUDE_NAMES set, big-table tiles must come from the
    reltuples estimates instead of real COUNT(*) scans."""
    from telecrime.web import app as web_app

    monkeypatch.setenv("TELECRIME_EXCLUDE_NAMES", "hidden")
    monkeypatch.setattr(
        web_app,
        "_pg_fast_count_estimates",
        lambda *tables: {"messages": 111, "parsed_credentials": 222},
    )
    app, seed_engine = _sqlite_app(tmp_path)
    with get_session(seed_engine) as session:
        session.add(Conversation(platform_id=1, conversation_type="channel", title="Hidden"))
        session.add(Conversation(platform_id=2, conversation_type="channel", title="Visible"))
        session.add(
            TelegramChannel(platform_id=3, source="test", username="hidden", title="Hidden")
        )
        session.add(TelegramChannel(platform_id=4, source="test", username="shown", title="Shown"))

    response = _route(app, "/").endpoint(request=_web_request())

    assert response.status_code == 200
    stats = response.context["stats"]
    assert stats["messages"] == 111
    assert stats["credentials"] == 222
    assert stats["conversations"] == 1
    assert stats["channels"] == 1


def test_claim_cred_counts_refresh_is_single_flight():
    """FIX 6: concurrent stale polls may start at most one refresh."""
    cache: dict = {"ts": 0.0, "data": {}, "refreshing": False, "lock": threading.Lock()}
    assert _claim_cred_counts_refresh(cache, 90) is True
    assert cache["refreshing"] is True
    assert _claim_cred_counts_refresh(cache, 90) is False  # refresh in flight
    assert _claim_cred_counts_refresh(cache, 90) is False

    with cache["lock"]:  # refresh finished and stamped fresh
        cache["refreshing"] = False
        cache["ts"] = time.monotonic()
    assert _claim_cred_counts_refresh(cache, 90) is False

    with cache["lock"]:  # stale again later
        cache["ts"] = 0.0
    assert _claim_cred_counts_refresh(cache, 90) is True
