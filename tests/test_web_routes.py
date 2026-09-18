"""Route-level coverage for web endpoints without dedicated tests.

`test_web_app.py` exercises search/export/triage/scheduler internals; the
watchlist, channels, password CSV and credential-detail routes were only
covered indirectly (or not at all). These call the route endpoints directly,
the same way the existing suite does, since httpx/TestClient is not a test
dependency.
"""

import asyncio
import json
from urllib.parse import urlencode

from sqlalchemy import text
from starlette.requests import Request

from telecrime.database import get_engine, get_session, init_db
from telecrime.models import Conversation, ParsedCredential, PasswordCandidate, TelegramChannel
from telecrime.models.watchlist import WatchlistItem
from telecrime.states import PasswordScope
from telecrime.web.app import create_app


def _web_request(*, method: str = "GET", form: dict[str, str] | None = None) -> Request:
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
        routes = [r for r in routes if method in (getattr(r, "methods", None) or set())]
    return routes[0]


def _sqlite_app(tmp_path):
    url = f"sqlite:///{tmp_path / 'routes.db'}"
    engine = get_engine(url)
    init_db(engine)
    return create_app(url), engine


def test_watchlist_page_json_and_badge_agree_on_enabled_counts(tmp_path):
    app, engine = _sqlite_app(tmp_path)
    with get_session(engine) as session:
        session.add_all(
            [
                WatchlistItem(
                    label="acme",
                    query="acme",
                    match_type="domain",
                    enabled=True,
                    new_count=3,
                    last_known_count=10,
                ),
                WatchlistItem(
                    label="off",
                    query="off",
                    match_type="any",
                    enabled=False,
                    new_count=5,
                    last_known_count=1,
                ),
            ]
        )

    page = _route(app, "/watchlist").endpoint(_web_request())
    assert page.status_code == 200
    assert {item["label"] for item in page.context["items"]} == {"acme", "off"}
    # Only enabled items contribute to the navigation badge total.
    assert page.context["total_new"] == 3

    listed = json.loads(_route(app, "/api/watchlist").endpoint().body)
    assert {row["label"] for row in listed} == {"acme", "off"}

    badge = json.loads(_route(app, "/api/watchlist/badge").endpoint().body)
    assert badge == {"new_count": 3}


def test_watchlist_viewed_and_delete_routes_update_db(tmp_path):
    app, engine = _sqlite_app(tmp_path)
    with get_session(engine) as session:
        item = WatchlistItem(label="acme", query="acme", new_count=4)
        session.add(item)
        session.flush()
        item_id = item.id

    viewed = _route(app, "/api/watchlist/{item_id}/viewed", "POST").endpoint(item_id=item_id)
    assert viewed.status_code == 200
    assert json.loads(viewed.body) == {"ok": True}
    with get_session(engine) as session:
        item = session.get(WatchlistItem, item_id)
        assert item is not None
        assert item.new_count == 0
        assert item.last_viewed_at is not None

    deleted = _route(app, "/api/watchlist/{item_id}", "DELETE").endpoint(
        _web_request(method="DELETE"), item_id=item_id
    )
    assert deleted.status_code == 200
    with get_session(engine) as session:
        assert session.get(WatchlistItem, item_id) is None


def test_passwords_csv_streams_filtered_rows(tmp_path):
    app, engine = _sqlite_app(tmp_path)
    with get_session(engine) as session:
        session.add_all(
            [
                PasswordCandidate(
                    value="conversation-pw",
                    scope=PasswordScope.CONVERSATION,
                    extraction_method="caption",
                    confidence=0.9,
                ),
                PasswordCandidate(
                    value="global-pw",
                    scope=PasswordScope.GLOBAL,
                    extraction_method="nearby",
                    confidence=0.4,
                ),
            ]
        )

    response = _route(app, "/passwords.csv").endpoint(
        scope="conversation", method="", sort="success", limit=5000
    )
    body = asyncio.run(_read_streaming_body(response))

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert "telecrime_passwords.csv" in response.headers["content-disposition"]
    lines = body.strip().splitlines()
    assert lines[0] == (
        "id,value,scope,extraction_method,confidence,times_succeeded,times_failed,"
        "context_text,created_at"
    )
    assert len(lines) == 2
    assert "conversation-pw" in lines[1]
    assert "conversation" in lines[1]
    assert "global-pw" not in body


async def _read_streaming_body(response) -> str:
    chunks: list[bytes] = []
    async for chunk in response.body_iterator:
        chunks.append(chunk if isinstance(chunk, bytes) else chunk.encode())
    return b"".join(chunks).decode()


def test_channels_page_filters_and_stats(tmp_path):
    app, engine = _sqlite_app(tmp_path)
    with get_session(engine) as session:
        session.add_all(
            [
                TelegramChannel(
                    platform_id=1,
                    source="discover",
                    username="sub",
                    title="Subscribed",
                    is_subscribed=True,
                    is_active=True,
                    credentials_extracted=7,
                ),
                TelegramChannel(
                    platform_id=2,
                    source="dork",
                    username="unsub",
                    title="Not subscribed",
                    is_subscribed=False,
                    is_active=True,
                    credentials_extracted=3,
                ),
            ]
        )

    page = _route(app, "/channels").endpoint(
        request=_web_request(), subscribed="1", active="", source="", page=1, limit=100
    )

    assert page.status_code == 200
    assert [c.username for c in page.context["channels"]] == ["sub"]
    assert page.context["stats"] == {
        "total": 1,
        "subscribed": 1,
        "active": 1,
        "total_creds": 7,
    }
    assert page.context["sources"] == ["discover", "dork"]


def test_credential_detail_404_and_related_rows(tmp_path):
    app, engine = _sqlite_app(tmp_path)
    with get_session(engine) as session:
        session.add_all(
            [
                ParsedCredential(
                    url="https://example.com/login",
                    domain="example.com",
                    username="alice",
                    password="secret",
                    source_archive="a.zip",
                    credential_hash=ParsedCredential.compute_hash(
                        "example.com", "alice", "secret"
                    ),
                ),
                ParsedCredential(
                    url="https://example.com/other",
                    domain="example.com",
                    username="bob",
                    password="secret",
                    credential_hash=ParsedCredential.compute_hash(
                        "example.com", "bob", "secret"
                    ),
                ),
            ]
        )

    missing = _route(app, "/credential/{credential_id}").endpoint(
        request=_web_request(), credential_id=999999
    )
    assert missing.status_code == 404

    with get_session(engine) as session:
        alice_id = (
            session.query(ParsedCredential.id)
            .filter(ParsedCredential.username == "alice")
            .scalar()
        )

    found = _route(app, "/credential/{credential_id}").endpoint(
        request=_web_request(), credential_id=alice_id
    )
    assert found.status_code == 200
    assert found.context["cred"].id == alice_id
    assert [c.username for c in found.context["related"]] == ["bob"]


def test_save_search_inserts_and_redirects(tmp_path):
    app, engine = _sqlite_app(tmp_path)

    response = _route(app, "/search/save", "POST").endpoint(
        name="my search", query="domain:example.com"
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/search?q=domain:example.com"
    with get_session(engine) as session:
        rows = session.execute(text("SELECT name, query FROM saved_searches")).all()
    assert rows == [("my search", "domain:example.com")]


def test_passwords_page_renders_seeded_candidate(tmp_path):
    app, engine = _sqlite_app(tmp_path)
    with get_session(engine) as session:
        session.add(
            PasswordCandidate(
                value="pinned-pw",
                scope=PasswordScope.CONVERSATION,
                extraction_method="pinned",
                confidence=0.8,
            )
        )

    page = _route(app, "/passwords").endpoint(
        request=_web_request(),
        scope="PasswordScope.CONVERSATION",
        method="pinned",
        sort="success",
        page=1,
        limit=50,
    )

    assert page.status_code == 200
    assert [c.value for c in page.context["candidates"]] == ["pinned-pw"]
    assert page.context["stats"]["total"] == 1
    assert page.context["scopes"] == ["conversation"]
    assert page.context["methods"] == ["pinned"]
    assert page.context["pages"] == 1


def test_conversations_page_lists_seeded_conversation(tmp_path, monkeypatch):
    # _pg_fast_count_estimates runs PG-only SQL (reltuples::bigint) that SQLite
    # cannot parse; the route's paging/aggregation behavior is what this test
    # covers, and the estimate path is out of scope (reported separately).
    from telecrime.web import app as web_app

    monkeypatch.setattr(web_app, "_pg_fast_count_estimates", lambda *args: {})
    app, engine = _sqlite_app(tmp_path)
    with get_session(engine) as session:
        session.add(
            Conversation(
                platform_id=7,
                conversation_type="channel",
                title="Seeded Channel",
            )
        )

    page = _route(app, "/conversations").endpoint(
        request=_web_request(), page=1, limit=50
    )

    assert page.status_code == 200
    assert [row["conv"].title for row in page.context["conversations"]] == [
        "Seeded Channel"
    ]
    assert page.context["conversations"][0]["cred_count"] == 0


def test_conversation_detail_renders_and_404s(tmp_path):
    app, engine = _sqlite_app(tmp_path)
    with get_session(engine) as session:
        conv = Conversation(
            platform_id=7,
            conversation_type="channel",
            title="Detail Channel",
        )
        session.add(conv)
        session.flush()
        conv_id = conv.id

    missing = _route(app, "/conversation/{conversation_id}").endpoint(
        request=_web_request(), conversation_id=999999, msg_limit=50
    )
    assert missing.status_code == 404

    found = _route(app, "/conversation/{conversation_id}").endpoint(
        request=_web_request(), conversation_id=conv_id, msg_limit=50
    )
    assert found.status_code == 200
    assert found.context["conv"].id == conv_id
    assert found.context["msg_count"] == 0


def test_pipeline_status_api_returns_progress_payload(tmp_path, monkeypatch):
    from telecrime.web import app as web_app

    app, _ = _sqlite_app(tmp_path)
    monkeypatch.setattr(
        web_app, "read_progress", lambda: {"running": True, "credentials": 5}
    )

    response = _route(app, "/scheduler/pipeline-status").endpoint()

    assert response.status_code == 200
    assert json.loads(response.body) == {"running": True, "credentials": 5}


def test_scheduler_jobs_fragment_builds_rows_from_job_defs(tmp_path):
    app, _ = _sqlite_app(tmp_path)

    response = _route(app, "/scheduler/jobs-fragment").endpoint(request=_web_request())

    assert response.status_code == 200
    jobs = {row["name"]: row for row in response.context["jobs"]}
    assert "pipeline" in jobs and "vacuum" in jobs
    assert jobs["pipeline"]["requires_telegram"] is True
    # No scheduler status was written, so no job is marked enabled yet.
    assert jobs["pipeline"]["enabled"] is False
    assert jobs["pipeline"]["running"] is False


def test_stats_page_serves_cached_payload(tmp_path, monkeypatch):
    app, _ = _sqlite_app(tmp_path)
    # _stats_cache_path follows TELECRIME_DATA_DIR.
    monkeypatch.setenv("TELECRIME_DATA_DIR", str(tmp_path))
    route = _route(app, "/stats")

    # Use the route's own no-cache payload as the full template schema, then
    # override one field so the cached read is distinguishable from a recompute.
    fallback = route.endpoint(request=_web_request(), days=90, limit=20)
    payload = {
        key: value
        for key, value in fallback.context.items()
        if key not in ("request", "stats_presets")
    }
    payload["top_domains"] = [{"domain": "example.com", "count": 2}]
    (tmp_path / "stats_cache.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-01-01T00:00:00+00:00",
                "data": {"90:20": payload},
            }
        )
    )

    response = route.endpoint(request=_web_request(), days=90, limit=20)

    assert response.status_code == 200
    assert response.context["top_domains"] == [{"domain": "example.com", "count": 2}]
    assert response.context["stats_note"] is None
    assert response.context["last_updated"] == "2026-01-01T00:00:00+00:00"


def test_stats_page_reports_warming_up_without_cache(tmp_path, monkeypatch):
    app, _ = _sqlite_app(tmp_path)
    monkeypatch.setenv("TELECRIME_DATA_DIR", str(tmp_path))
    monkeypatch.delenv("TELECRIME_DISABLE_STATS_WORKER", raising=False)

    response = _route(app, "/stats").endpoint(
        request=_web_request(), days=30, limit=20
    )

    assert response.status_code == 200
    assert "warming up" in response.context["stats_note"]
    assert response.context["top_domains"] == []


def test_pg_fast_count_estimates_returns_empty_on_sqlite(tmp_path):
    """The pg_class/pg_stat_user_tables query is PostgreSQL-only; on SQLite it
    must degrade to exact counts instead of raising and 500ing the route."""
    from telecrime.web.app import _pg_fast_count_estimates

    url = f"sqlite:///{tmp_path / 'stats.db'}"
    engine = get_engine(url)
    init_db(engine)
    with get_session(engine) as session:
        assert _pg_fast_count_estimates(session, "messages") == {}
