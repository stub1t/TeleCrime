"""Tests for the reworked Telegram notifier."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from telecrime.notify import (
    TelegramNotifier,
    _esc,
    _fmt_duration,
    _fmt_rate,
    _redact_password,
)


@pytest.fixture
def notifier() -> tuple[TelegramNotifier, AsyncMock]:
    client = MagicMock()
    client.get_me = AsyncMock(return_value=MagicMock(id=7))
    client.send_message = AsyncMock()
    n = TelegramNotifier(client=client, enabled=True)
    return n, client.send_message


# --------------------------------------------------------------------- helpers

def test_esc_quotes_html_special_chars():
    assert _esc("Tom & Jerry <i>nested</i>") == "Tom &amp; Jerry &lt;i&gt;nested&lt;/i&gt;"


def test_redact_password_short_and_long():
    assert _redact_password(None) == "—"
    assert _redact_password("") == "—"
    assert _redact_password("a") == "•"
    assert _redact_password("ab") == "••"
    out = _redact_password("hunter2")
    # First/last char visible, middle redacted, length disclosed.
    assert out.startswith("h") and "2" in out and "(7 chars)" in out


def test_fmt_duration():
    assert _fmt_duration(45) == "45s"
    assert _fmt_duration(125) == "2m 5s"
    assert _fmt_duration(3661) == "1h 1m"
    assert _fmt_duration(None) == "—"


def test_fmt_rate():
    assert _fmt_rate(1000, 5) == "200/s"
    assert _fmt_rate(0, 0) == "—"
    assert _fmt_rate("x", 1) == "—"


# ----------------------------------------------------------------- behaviour

@pytest.mark.asyncio
async def test_send_uses_html_parse_mode(notifier):
    n, send = notifier
    await n.send("hello <b>world</b>")
    send.assert_awaited_once()
    args, kwargs = send.call_args
    assert kwargs.get("parse_mode") == "html"


@pytest.mark.asyncio
async def test_stage_start_is_silent(notifier):
    n, send = notifier
    await n.stage_start("parse")
    send.assert_not_awaited()


@pytest.mark.asyncio
async def test_stage_complete_skips_noisy_stages(notifier):
    n, send = notifier
    await n.stage_complete("ingest")
    await n.stage_complete("plan")
    await n.stage_complete("discover")
    send.assert_not_awaited()


@pytest.mark.asyncio
async def test_stage_complete_announces_meaningful_stages(notifier):
    n, send = notifier
    await n.stage_complete("acquire", stats={"downloads": 7})
    send.assert_awaited_once()
    text = send.call_args.args[1]
    assert "<b>Stage complete: acquire</b>" in text
    assert "downloads" in text
    assert "7" in text


@pytest.mark.asyncio
async def test_downloading_small_files_suppressed(notifier):
    n, send = notifier
    await n.downloading("@chan", "tiny.zip", 12.5)
    send.assert_not_awaited()


@pytest.mark.asyncio
async def test_downloading_large_files_announced(notifier):
    n, send = notifier
    await n.downloading("@chan", "huge.zip", 250.0)
    text = send.call_args.args[1]
    assert "Downloading" in text
    assert "huge.zip" in text
    assert "250.0 MB" in text


@pytest.mark.asyncio
async def test_archive_parsed_skips_empty_archives(notifier):
    n, send = notifier
    await n.archive_parsed("empty.zip", 0, 0, 0)
    send.assert_not_awaited()


@pytest.mark.asyncio
async def test_archive_parsed_shows_dedup_pct(notifier):
    n, send = notifier
    await n.archive_parsed("a.zip", new_credentials=200, duplicates=800, unique_domains=15)
    text = send.call_args.args[1]
    assert "200" in text and "800" in text
    assert "80% dedup" in text


@pytest.mark.asyncio
async def test_archive_name_is_html_escaped(notifier):
    n, send = notifier
    # Adversarial archive name with HTML tags + ampersand.
    await n.archive_parsed("<script>x</script>&", 1, 0, 1)
    text = send.call_args.args[1]
    # Tag must be escaped — never appear verbatim.
    assert "<script>" not in text
    assert "&lt;script&gt;" in text
    assert "&amp;" in text


@pytest.mark.asyncio
async def test_error_renders_with_stage_and_traceback(notifier):
    n, send = notifier
    await n.error("psycopg2.OperationalError: connection closed", stage="parse")
    text = send.call_args.args[1]
    assert "Pipeline error" in text
    assert "parse" in text
    assert "<pre>" in text and "connection closed" in text


@pytest.mark.asyncio
async def test_pipeline_complete_shows_rate_and_duration(notifier):
    n, send = notifier
    await n.pipeline_complete({
        "archives_extracted": 12,
        "credentials_parsed": 50_000,
        "duplicates_skipped": 10_000,
        "errors": 0,
        "elapsed_seconds": 600,
    })
    text = send.call_args.args[1]
    assert "Pipeline complete" in text
    assert "50,000" in text and "10,000" in text
    assert "10m 0s" in text
    assert "83/s creds" in text  # 50000/600


@pytest.mark.asyncio
async def test_watchlist_alerts_redacts_passwords(notifier):
    n, send = notifier
    await n.watchlist_alerts([
        {
            "label": "demo",
            "query": "owlmail",
            "new_matches": 1,
            "hits": [
                {
                    "domain": "example.com",
                    "username": "owlmail@example.com",
                    "password": "hunter2-supersecret",
                    "source_archive": "dump.zip",
                },
            ],
        },
    ])
    text = send.call_args.args[1]
    # Clear-text password must NOT appear.
    assert "hunter2-supersecret" not in text
    # Length must be disclosed in the redaction marker.
    assert "(19 chars)" in text


@pytest.mark.asyncio
async def test_watchlist_empty_is_silent(notifier):
    n, send = notifier
    await n.watchlist_alerts([])
    send.assert_not_awaited()
