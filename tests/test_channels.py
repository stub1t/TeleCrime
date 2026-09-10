"""Tests for channel discovery helpers."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from telecrime.channels.discover import (
    DISCOVERY_CREDENTIAL_WATERMARK,
    DISCOVERY_MESSAGE_WATERMARK,
    DiscoveryScanResult,
    discover_channels_from_db,
    discover_channels_via_dork,
    persist_discovery_state,
    update_channel_stats,
)
from telecrime.channels.service import (
    build_subscription_query,
    mark_channel_check_failed,
    mark_channel_join_failed,
    mark_channel_join_result,
)
from telecrime.database import get_engine, get_session, init_db
from telecrime.models import Conversation, Message, ParsedCredential, PipelineState, TelegramChannel
from telecrime.pipeline.channel_discover import ChannelJoiner


def test_build_subscription_query_filters_candidates(tmp_path):
    """Only eligible stealer-log channels are returned as candidates."""
    engine = get_engine(f"sqlite:///{tmp_path / 'channels.db'}")
    init_db(engine)

    with get_session(engine) as session:
        session.add_all(
            [
                TelegramChannel(
                    username="best_logs_dump",
                    source="mentioned",
                    is_active=True,
                    is_accessible=True,
                    is_subscribed=False,
                ),
                TelegramChannel(
                    username="support_channel",
                    source="mentioned",
                    is_active=True,
                    is_accessible=True,
                    is_subscribed=False,
                ),
                TelegramChannel(username="private_clouds", source="mentioned", is_subscribed=True),
                TelegramChannel(username="vidar_leaks", source="mentioned", is_accessible=False),
            ]
        )

    with get_session(engine) as session:
        candidates = build_subscription_query(session).all()

    assert [channel.username for channel in candidates] == ["best_logs_dump"]


def test_update_channel_stats_uses_aggregates(tmp_path):
    """Channel stats are populated from grouped SQL queries."""
    engine = get_engine(f"sqlite:///{tmp_path / 'stats.db'}")
    init_db(engine)

    with get_session(engine) as session:
        conversation = Conversation(
            platform_id=123,
            title="Stealer Logs",
            username="stealerlogs",
            conversation_type="channel",
        )
        session.add(conversation)
        session.flush()

        session.add(TelegramChannel(platform_id=123, username="stealerlogs", source="subscribed"))
        session.add(
            Message(
                conversation_id=conversation.id,
                platform_id=1,
                platform_timestamp=datetime.now(UTC),
                text="hello",
            )
        )
        session.add(
            ParsedCredential(
                url="https://example.com/login",
                domain="example.com",
                username="alice",
                password="secret",
                source_conversation_id=conversation.id,
                credential_hash=ParsedCredential.compute_hash("example.com", "alice", "secret"),
            )
        )

    with get_session(engine) as session:
        update_channel_stats(session)

    with get_session(engine) as session:
        channel = session.query(TelegramChannel).filter_by(username="stealerlogs").one()

    assert channel.messages_seen == 1
    assert channel.credentials_extracted == 1


def test_channel_discovery_persists_and_uses_watermarks(tmp_path):
    """Incremental discovery scans only new messages and credentials."""
    engine = get_engine(f"sqlite:///{tmp_path / 'incremental.db'}")
    init_db(engine)

    with get_session(engine) as session:
        conversation = Conversation(
            platform_id=123,
            title="Stealer Logs",
            username="stealerlogs",
            conversation_type="channel",
        )
        session.add(conversation)
        session.flush()

        session.add(
            Message(
                conversation_id=conversation.id,
                platform_id=1,
                platform_timestamp=datetime.now(UTC),
                text="join @fresh_channel now",
            )
        )
        session.add(
            ParsedCredential(
                url="https://example.com/login",
                domain="example.com",
                username="alice",
                password="secret",
                source_archive="stealer_@archivechan.zip",
                credential_hash=ParsedCredential.compute_hash("example.com", "alice", "secret"),
            )
        )

    with get_session(engine) as session:
        first_scan = discover_channels_from_db(session)
        assert "@fresh_channel" in first_scan.channels
        persist_discovery_state(session, first_scan)

    with get_session(engine) as session:
        watermarks = {state.key: state.value_int for state in session.query(PipelineState).all()}

    assert watermarks[DISCOVERY_MESSAGE_WATERMARK] > 0
    assert watermarks[DISCOVERY_CREDENTIAL_WATERMARK] > 0

    with get_session(engine) as session:
        second_scan = discover_channels_from_db(session)

    assert "@fresh_channel" not in second_scan.channels
    assert "stealerlogs" in [
        channel.username for channel in second_scan.channels.values() if channel.username
    ]


# ---------------------------------------------------------------------------
# E5 — DuckDuckGo dorking tests (mocked HTTP)
# ---------------------------------------------------------------------------

_DDG_HTML_WITH_LINKS = """
<html><body>
<a href="https://t.me/+AbCdEf123456">Join Stealer Logs</a>
<a href="https://t.me/joinchat/XyZ789qrstUV">Another Channel</a>
<span>t.me/+DupeLink111</span>
</body></html>
"""

_DDG_HTML_NO_LINKS = "<html><body><p>No results found.</p></body></html>"


def _make_engine(tmp_path):
    engine = get_engine(f"sqlite:///{tmp_path / 'dork_test.db'}")
    init_db(engine)
    return engine


def test_dork_extracts_invite_links(tmp_path):
    """discover_channels_via_dork saves new invite-link channels."""
    engine = _make_engine(tmp_path)
    calls = []

    def mock_fetch(keyword):
        calls.append(keyword)
        return _DDG_HTML_WITH_LINKS

    with get_session(engine) as session:
        new_count, found = discover_channels_via_dork(
            session, keywords=["stealer log"], delay=0, _fetch_fn=mock_fetch
        )

    assert new_count == 3
    assert len(found) == 3
    assert calls == ["stealer log"]

    with get_session(engine) as session:
        saved = session.query(TelegramChannel).filter_by(source="dork").all()
    assert len(saved) == 3
    assert all(ch.invite_link.startswith("https://t.me/+") for ch in saved)


def test_dork_skips_existing_links(tmp_path):
    """discover_channels_via_dork does not duplicate already-saved invite links."""
    engine = _make_engine(tmp_path)

    # Pre-seed one of the links
    with get_session(engine) as session:
        session.add(TelegramChannel(invite_link="https://t.me/+AbCdEf123456", source="dork"))
        session.commit()

    with get_session(engine) as session:
        new_count, found = discover_channels_via_dork(
            session,
            keywords=["stealer log"],
            delay=0,
            _fetch_fn=lambda _: _DDG_HTML_WITH_LINKS,
        )

    assert new_count == 2  # one pre-seeded, two new


def test_dork_handles_empty_results(tmp_path):
    """discover_channels_via_dork handles pages with no invite links gracefully."""
    engine = _make_engine(tmp_path)
    with get_session(engine) as session:
        new_count, found = discover_channels_via_dork(
            session,
            keywords=["noresults"],
            delay=0,
            _fetch_fn=lambda _: _DDG_HTML_NO_LINKS,
        )
    assert new_count == 0
    assert found == []


def test_dork_handles_fetch_error(tmp_path):
    """discover_channels_via_dork continues on network error, returning 0 new channels."""
    engine = _make_engine(tmp_path)

    def failing_fetch(keyword):
        raise OSError("network unavailable")

    with get_session(engine) as session:
        new_count, found = discover_channels_via_dork(
            session, keywords=["stealer"], delay=0, _fetch_fn=failing_fetch
        )
    assert new_count == 0
    assert found == []


def test_dork_multiple_keywords(tmp_path):
    """discover_channels_via_dork queries each keyword and deduplicates across queries."""
    engine = _make_engine(tmp_path)
    fetched_keywords = []

    def mock_fetch(keyword):
        fetched_keywords.append(keyword)
        # Same links for all keywords — should only save 2 unique channels total
        return _DDG_HTML_WITH_LINKS

    with get_session(engine) as session:
        new_count, found = discover_channels_via_dork(
            session,
            keywords=["stealer log", "cloud ulp"],
            delay=0,
            _fetch_fn=mock_fetch,
        )

    assert fetched_keywords == ["stealer log", "cloud ulp"]
    assert new_count == 3  # deduped across both queries (3 unique links)
    assert len(found) == 6  # 3 per query × 2


# ---------------------------------------------------------------------------
# Transient failures must not permanently disqualify channels
# ---------------------------------------------------------------------------


def test_mark_channel_join_result_keeps_transient_failure_accessible():
    """A generic join failure (False, no error) stays retryable."""
    channel = TelegramChannel(
        username="vidar_logs", source="mentioned", is_accessible=True, is_subscribed=False
    )

    assert mark_channel_join_result(channel, False) == "failed"
    assert channel.is_accessible is True
    assert channel.is_subscribed is False
    assert channel.check_error == "Join failed"
    assert channel.last_checked is not None

    # Permanent classification still happens in mark_channel_join_failed.
    permanent = TelegramChannel(username="gone", source="mentioned", is_accessible=True)
    assert mark_channel_join_failed(permanent, "channel/supergroup not found") == "failed"
    assert permanent.is_accessible is False


def test_mark_channel_check_failed_none_entity_stays_accessible():
    """get_entity() returning None must not permanently disable a channel."""
    channel = TelegramChannel(
        username="maybe_gone", source="mentioned", is_active=True, is_accessible=True
    )

    mark_channel_check_failed(channel, "Entity not found")

    assert channel.is_active is True
    assert channel.is_accessible is True
    assert channel.check_error == "Entity not found"
    assert channel.last_checked is not None

    # Explicit Telegram "not found" errors still disable the channel.
    confirmed = TelegramChannel(
        username="really_gone", source="mentioned", is_active=True, is_accessible=True
    )
    mark_channel_check_failed(confirmed, 'No user has "really_gone" as username')
    assert confirmed.is_active is False
    assert confirmed.check_error == "Channel not found / deleted"


def test_discover_channels_can_skip_conversation_source(tmp_path):
    """Source #1 can be skipped while other incremental sources still run."""
    engine = get_engine(f"sqlite:///{tmp_path / 'skipconv.db'}")
    init_db(engine)

    with get_session(engine) as session:
        conv = Conversation(
            platform_id=321, username="subscribed_chan", conversation_type="channel"
        )
        session.add(conv)
        session.flush()
        session.add(
            Message(
                conversation_id=conv.id,
                platform_id=1,
                platform_timestamp=datetime.now(UTC),
                text="join @fresh_mention",
            )
        )
        session.commit()

    with get_session(engine) as session:
        result = discover_channels_from_db(session, include_conversations=False)

    sources = {channel.source for channel in result.channels.values()}
    assert "subscribed" not in sources
    assert "@fresh_mention" in result.channels


def test_discover_mention_does_not_duplicate_id_keyed_username(tmp_path):
    """A mention of a forwarded channel's username does not create a second key."""
    engine = get_engine(f"sqlite:///{tmp_path / 'idsuppress.db'}")
    init_db(engine)

    with get_session(engine) as session:
        conv = Conversation(platform_id=654, conversation_type="channel")
        session.add(conv)
        session.flush()
        session.add(
            Message(
                conversation_id=conv.id,
                platform_id=1,
                platform_timestamp=datetime.now(UTC),
                text="join @spamlogs",
                is_forwarded=True,
                forwarded_from_id=555,
                forwarded_from_name="@spamlogs",
            )
        )
        session.commit()

    with get_session(engine) as session:
        result = discover_channels_from_db(session)

    assert "id:555" in result.channels
    assert result.channels["id:555"].username == "spamlogs"
    assert "@spamlogs" not in result.channels


@pytest.mark.asyncio
async def test_channel_joiner_memoizes_conversation_scan(tmp_path, monkeypatch):
    """maybe_act() skips the full conversation scan until its signal changes."""
    import telecrime.pipeline.channel_discover as cd

    engine = get_engine(f"sqlite:///{tmp_path / 'joiner.db'}")
    init_db(engine)
    calls: list[bool] = []

    def fake_discover(session, *, include_conversations=True):
        calls.append(include_conversations)
        return DiscoveryScanResult(channels={}, last_message_id=0, last_credential_id=0)

    monkeypatch.setattr(cd, "discover_channels_from_db", fake_discover)

    with get_session(engine) as session:
        ctx = SimpleNamespace(session=session, adapter=MagicMock(), display=None)
        joiner = ChannelJoiner()
        await joiner.maybe_act(ctx)
        await joiner.maybe_act(ctx)

        # A new channel conversation changes the signal → full scan again.
        session.add(Conversation(platform_id=777, conversation_type="channel"))
        session.commit()
        await joiner.maybe_act(ctx)

    assert calls == [True, False, True]
