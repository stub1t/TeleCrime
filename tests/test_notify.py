"""Tests for the reworked Telegram notifier."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from telecrime.notify import (
    TelegramNotifier,
    _esc,
    _fmt_duration,
    _fmt_rate,
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
async def test_send_reconnects_via_adapter_when_disconnected():
    """A wedged client must not kill notifications: send() reconnects via the
    adapter (which replaces its client on reconnect) instead of sending on
    the dead instance captured at construction."""
    stale_client = MagicMock()
    stale_client.is_connected.return_value = False
    stale_client.send_message = AsyncMock()

    fresh_client = MagicMock()
    fresh_client.is_connected.return_value = True
    fresh_client.get_me = AsyncMock(return_value=MagicMock(id=7))
    fresh_client.send_message = AsyncMock()

    adapter = MagicMock()
    adapter._active_ops = 0
    adapter.client = stale_client

    async def _reconnect(*args, **kwargs):
        adapter.client = fresh_client

    adapter._ensure_connected = AsyncMock(side_effect=_reconnect)

    n = TelegramNotifier(client=stale_client, enabled=True, adapter=adapter)
    await n.send("hello <b>world</b>")

    adapter._ensure_connected.assert_awaited_once()
    fresh_client.send_message.assert_awaited_once()
    stale_client.send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_send_uses_adapter_client_when_connected():
    """With a live adapter client, no reconnect is attempted — the adapter's
    current client is used directly."""
    live_client = MagicMock()
    live_client.is_connected.return_value = True
    live_client.get_me = AsyncMock(return_value=MagicMock(id=7))
    live_client.send_message = AsyncMock()

    adapter = MagicMock()
    adapter.client = live_client
    adapter._ensure_connected = AsyncMock()

    n = TelegramNotifier(client=live_client, enabled=True, adapter=adapter)
    await n.send("hi")

    adapter._ensure_connected.assert_not_awaited()
    live_client.send_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_send_skips_quietly_when_reconnect_fails():
    """If reconnect fails, the notification is dropped with a warning, not an
    exception — a down Telegram link must not crash the pipeline."""
    dead_client = MagicMock()
    dead_client.is_connected.return_value = False

    adapter = MagicMock()
    adapter._active_ops = 0
    adapter.client = dead_client
    adapter._ensure_connected = AsyncMock(
        side_effect=ConnectionError("Telegram reconnect lock busy")
    )

    n = TelegramNotifier(client=dead_client, enabled=True, adapter=adapter)
    await n.send("boom")  # must not raise
    adapter._ensure_connected.assert_awaited_once()


@pytest.mark.asyncio
async def test_send_does_not_reconnect_while_adapter_busy():
    """While a download/extract op is in flight on the adapter, send() must
    NOT force a reconnect — disconnecting mid-op kills the download, and the
    new client collides with the retrying download on the same session file
    ('database is locked' / 'wrong session ID'). The message is dropped."""
    busy_client = MagicMock()
    busy_client.is_connected.return_value = False
    busy_client.send_message = AsyncMock()

    adapter = MagicMock()
    adapter._active_ops = 2  # two downloads in flight
    adapter.client = busy_client
    adapter._ensure_connected = AsyncMock()

    n = TelegramNotifier(client=busy_client, enabled=True, adapter=adapter)
    await n.send("drop me")

    adapter._ensure_connected.assert_not_awaited()
    busy_client.send_message.assert_not_awaited()


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
    assert "<b>Stage complete — acquire</b>" in text
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
async def test_archive_parsed_accumulates_until_flush(notifier):
    """Single archive_parsed must NOT send — results accumulate in the digest."""
    n, send = notifier
    await n.archive_parsed("empty.zip", 0, 0, 0)
    send.assert_not_awaited()


@pytest.mark.asyncio
async def test_digest_flushes_by_count(monkeypatch):
    """After the archive cap, archive_parsed auto-flushes the digest."""
    monkeypatch.setenv("TELECRIME_NOTIFY_DIGEST_ARCHIVES", "3")
    client = MagicMock()
    client.get_me = AsyncMock(return_value=MagicMock(id=7))
    client.send_message = AsyncMock()
    n = TelegramNotifier(client=client, enabled=True)
    for i in range(3):
        await n.archive_parsed(f"a{i}.zip", 100, 0, 1)
    client.send_message.assert_awaited_once()
    text = client.send_message.call_args.args[1]
    assert "Progress digest" in text
    assert "300" in text  # 3 × 100 new


@pytest.mark.asyncio
async def test_flush_noop_when_nothing_pending(notifier):
    n, send = notifier
    await n.flush()
    send.assert_not_awaited()


@pytest.mark.asyncio
async def test_pipeline_complete_flushes_digest_before_summary(notifier):
    """pipeline_complete emits the pending digest first, then the summary."""
    n, send = notifier
    await n.archive_parsed("a.zip", 50, 0, 1)
    await n.pipeline_complete({
        "archives_extracted": 12,
        "credentials_parsed": 50_000,
        "duplicates_skipped": 10_000,
        "errors": 0,
        "elapsed_seconds": 600,
    })
    assert send.await_count == 2
    first = send.call_args_list[0].args[1]
    second = send.call_args_list[1].args[1]
    assert "Progress digest" in first
    assert "Pipeline complete" in second


@pytest.mark.asyncio
async def test_archive_parsed_shows_dedup_pct(notifier):
    n, send = notifier
    # Per-archive results accumulate into a digest — flush to emit.
    await n.archive_parsed("a.zip", new_credentials=200, duplicates=800, unique_domains=15)
    await n.flush()
    text = send.call_args.args[1]
    assert "200" in text and "800" in text
    assert "80% dedup" in text
    assert "Progress digest" in text


@pytest.mark.asyncio
async def test_archive_name_is_html_escaped(notifier):
    n, send = notifier
    # Adversarial archive name with HTML tags + ampersand.
    await n.archive_parsed("<script>x</script>&", 1, 0, 1)
    await n.flush()
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
async def test_watchlist_alerts_include_full_credentials(notifier):
    """Watchlist hits carry the full url, username and password — the feed is
    Saved Messages (message-to-self), private by construction."""
    n, send = notifier
    await n.watchlist_alerts([
        {
            "label": "demo",
            "query": "owlmail",
            "new_matches": 1,
            "hits": [
                {
                    "url": "https://owlmail.example.com/login",
                    "domain": "owlmail.example.com",
                    "username": "owlmail@example.com",
                    "password": "hunter2-supersecret",
                    "source_archive": "dump.zip",
                },
            ],
        },
    ])
    text = send.call_args.args[1]
    assert "hunter2-supersecret" in text
    assert "owlmail@example.com" in text
    assert "https://owlmail.example.com/login" in text
    assert "dump.zip" in text


@pytest.mark.asyncio
async def test_watchlist_empty_is_silent(notifier):
    n, send = notifier
    await n.watchlist_alerts([])
    send.assert_not_awaited()


@pytest.mark.asyncio
async def test_digest_flushes_by_time(monkeypatch):
    """After the time cap, the next archive_parsed flushes the digest."""
    import asyncio

    monkeypatch.setenv("TELECRIME_NOTIFY_DIGEST_SECONDS", "30")
    client = MagicMock()
    client.get_me = AsyncMock(return_value=MagicMock(id=7))
    client.send_message = AsyncMock()
    n = TelegramNotifier(client=client, enabled=True)
    # Fast-forward the time window past the cap.
    n._digest_seconds_cap = 30
    n._digest_since = asyncio.get_event_loop().time() - 60
    await n.archive_parsed("a.zip", 10, 0, 1)
    client.send_message.assert_awaited_once()
    text = client.send_message.call_args.args[1]
    assert "Progress digest" in text


@pytest.mark.asyncio
async def test_pipeline_start_and_activity_summary_render(notifier):
    """pipeline_start / activity_summary / channels_discovered render headers."""
    n, send = notifier
    await n.pipeline_start([".txt"], queue_size=5, free_disk_gb=42.5)
    await n.activity_summary("Last hour", 1234)
    await n.channels_discovered(new_discovered=2, checked=5, joined=1)
    assert send.await_count == 3
    assert "Pipeline started" in send.call_args_list[0].args[1]
    assert "Last hour summary" in send.call_args_list[1].args[1]
    assert "Channels" in send.call_args_list[2].args[1]


@pytest.mark.asyncio
async def test_digest_includes_live_status_section():
    """The digest carries a 'Pipeline' section with live status."""
    client = MagicMock()
    client.get_me = AsyncMock(return_value=MagicMock(id=7))
    client.send_message = AsyncMock()
    n = TelegramNotifier(client=client, enabled=True)

    async def _status():
        return {
            "stage": "parse",
            "archive_index": 203,
            "archive_total": 3887,
            "current_archive": "big.txt",
            "pending": 450,
            "free_disk_gb": 342,
            "errors": 0,
        }

    n.status_provider = _status
    await n.archive_parsed("a.zip", 1000, 500, 3)
    await n.flush()
    text = client.send_message.call_args.args[1]
    assert "Progress digest" in text
    assert "Pipeline" in text
    assert "parse" in text
    assert "203 / 3,887" in text
    assert "450" in text  # queue
    assert "342 GB" in text


@pytest.mark.asyncio
async def test_flush_sends_watchlist_alerts_without_digest_content():
    """A multi-hour single-file parse must not silence watchlist alerts: the
    flusher checks them independently of digest accumulation."""
    client = MagicMock()
    client.is_connected.return_value = True
    client.get_me = AsyncMock(return_value=MagicMock(id=7))
    client.send_message = AsyncMock()
    n = TelegramNotifier(client=client, enabled=True)

    async def _wl():
        return [{
            "label": "paypal",
            "query": "paypal",
            "new_matches": 3,
            "hits": [{"url": "https://paypal.com/login",
                      "username": "a@b.com",
                      "password": "secret",
                      "source_archive": "x.zip"}],
        }]

    n.watchlist_provider = _wl
    await n.flush()  # empty digest — watchlist must still be checked
    assert client.send_message.await_count == 1
    text = client.send_message.call_args.args[1]
    assert "Watchlist hits" in text
    assert "paypal" in text


@pytest.mark.asyncio
async def test_flusher_loop_sends_status_only_when_no_digest(monkeypatch):
    """During a long parse with no archive completions, the background flusher
    sends a throttled live-status message instead of staying silent."""
    client = MagicMock()
    client.is_connected.return_value = True
    client.get_me = AsyncMock(return_value=MagicMock(id=7))
    client.send_message = AsyncMock()
    n = TelegramNotifier(client=client, enabled=True)

    async def _status():
        return {
            "stage": "parse",
            "archive_index": 42,
            "archive_total": 4537,
            "current_archive": "big.txt",
            "rate_per_min": 1200,
            "pending": 3,
            "free_disk_gb": 150,
            "errors": 0,
        }

    n.status_provider = _status
    await n._send_status_only()
    assert client.send_message.await_count == 1
    text = client.send_message.call_args.args[1]
    assert "Pipeline status" in text
    assert "parse" in text
    assert "42 / 4,537" in text
    assert "1,200 creds/min" in text
    """Watchlist hits ride on the digest flush via the provider."""
    client = MagicMock()
    client.get_me = AsyncMock(return_value=MagicMock(id=7))
    client.send_message = AsyncMock()
    n = TelegramNotifier(client=client, enabled=True)

    async def _wl():
        return [{
            "label": "paypal",
            "query": "paypal",
            "new_matches": 3,
            "hits": [{"domain": "paypal.com", "username": "a@b.com",
                      "password": "secret", "source_archive": "x.zip"}],
        }]

    n.watchlist_provider = _wl
    await n.archive_parsed("a.zip", 100, 0, 1)
    await n.flush()
    assert client.send_message.await_count == 2
    texts = [c.args[1] for c in client.send_message.call_args_list]
    # Watchlist check now runs FIRST (independent of digest content), then
    # the digest — assert both contents are present regardless of order.
    assert any("Watchlist hits" in t for t in texts)
    assert any("Progress digest" in t for t in texts)


@pytest.mark.asyncio
async def test_check_watchlist_throttled_inside_interval():
    """The watchlist provider is not called twice inside the throttle window;
    it runs again once the interval has elapsed (timestamp = fake clock)."""
    import asyncio

    client = MagicMock()
    client.is_connected.return_value = True
    client.get_me = AsyncMock(return_value=MagicMock(id=7))
    client.send_message = AsyncMock()
    n = TelegramNotifier(client=client, enabled=True)
    n._watchlist_interval = 900

    calls = 0

    async def _wl():
        nonlocal calls
        calls += 1
        return []

    n.watchlist_provider = _wl

    # First ever check is due immediately.
    await n._check_watchlist()
    assert calls == 1

    # Same clock instant: second call (flusher tick or digest flush) is
    # throttled and must not hit the provider / DB.
    await n._check_watchlist()
    await n._check_watchlist()
    assert calls == 1

    # Fast-forward the fake clock past the interval: due again.
    n._last_watchlist_check = asyncio.get_event_loop().time() - n._watchlist_interval - 1
    await n._check_watchlist()
    assert calls == 2


@pytest.mark.asyncio
async def test_failed_digest_flush_is_rate_limited():
    """A failed digest send keeps the accumulated results but must not retry
    from archive_parsed() on every archive (that blocked the parse hot path
    for tens of seconds during a Telegram outage). The retry resumes after
    the backoff elapses."""
    import asyncio

    client = MagicMock()
    client.is_connected.return_value = True
    client.get_me = AsyncMock(return_value=MagicMock(id=7))
    client.send_message = AsyncMock(side_effect=RuntimeError("telegram down"))
    n = TelegramNotifier(client=client, enabled=True)
    n._digest_seconds_cap = 30
    n._digest_since = asyncio.get_event_loop().time() - 60

    await n.archive_parsed("a.zip", 10, 0, 1)
    assert client.send_message.await_count == 1  # first flush attempt

    await n.archive_parsed("b.zip", 10, 0, 1)
    await n.archive_parsed("c.zip", 10, 0, 1)
    # Inside the backoff window: results accumulate, no second send attempt.
    assert client.send_message.await_count == 1
    assert n._digest_archives == 3
    assert n._digest_new == 30

    # Backoff elapsed: the next archive retries the flush (no data lost).
    n._digest_retry_after = asyncio.get_event_loop().time() - 1
    await n.archive_parsed("d.zip", 10, 0, 1)
    assert client.send_message.await_count == 2
    assert n._digest_archives == 4


# ---------------------------------------------------------------------------
# flush serialization + cancellation propagation + flusher resilience
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_flushes_send_digest_once():
    """archive_parsed + the background flusher can call flush() concurrently;
    the lock must prevent the same digest from being sent twice."""
    import asyncio

    client = MagicMock()
    client.is_connected.return_value = True
    client.get_me = AsyncMock(return_value=MagicMock(id=7))
    entered = asyncio.Event()
    gate = asyncio.Event()
    sent: list[str] = []

    async def _slow_send(*args, **kwargs):
        sent.append(args[1])
        entered.set()
        await gate.wait()

    client.send_message = _slow_send
    n = TelegramNotifier(client=client, enabled=True)
    await n.archive_parsed("a.zip", 10, 0, 1)

    first = asyncio.create_task(n.flush())
    await entered.wait()
    second = asyncio.create_task(n.flush())
    await asyncio.sleep(0)  # let the second flush reach the lock
    gate.set()
    await asyncio.gather(first, second)

    assert len(sent) == 1
    assert n._digest_archives == 0


@pytest.mark.asyncio
async def test_flush_reset_before_await_keeps_archives_arriving_during_send():
    """Counters are zeroed BEFORE the network await, so archives parsed while
    the digest is in flight accumulate into the next window instead of being
    wiped by a post-send reset."""
    import asyncio

    client = MagicMock()
    client.is_connected.return_value = True
    client.get_me = AsyncMock(return_value=MagicMock(id=7))
    entered = asyncio.Event()
    gate = asyncio.Event()

    async def _slow_send(*args, **kwargs):
        entered.set()
        await gate.wait()

    client.send_message = _slow_send
    n = TelegramNotifier(client=client, enabled=True)
    await n.archive_parsed("a.zip", 10, 0, 1)

    task = asyncio.create_task(n.flush())
    await entered.wait()
    await n.archive_parsed("b.zip", 5, 0, 1)
    gate.set()
    await task

    assert n._digest_archives == 1
    assert n._digest_new == 5
    assert n._digest_since is not None


@pytest.mark.asyncio
async def test_send_propagates_external_cancellation():
    """task.cancel() on a send must propagate (not be swallowed into False),
    so callers can actually stop the pipeline."""
    import asyncio

    client = MagicMock()
    client.is_connected.return_value = True
    client.get_me = AsyncMock(return_value=MagicMock(id=7))
    started = asyncio.Event()
    never = asyncio.Event()

    async def _hang(*args, **kwargs):
        started.set()
        await never.wait()

    client.send_message = _hang
    n = TelegramNotifier(client=client, enabled=True)

    task = asyncio.create_task(n.send("hello"))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_send_returns_false_for_connection_drop_cancellation():
    """A CancelledError raised by Telethon (no external task.cancel()) is a
    retryable connection drop: best-effort False, not an exception."""
    import asyncio

    client = MagicMock()
    client.is_connected.return_value = True
    client.get_me = AsyncMock(return_value=MagicMock(id=7))
    client.send_message = AsyncMock(side_effect=asyncio.CancelledError)
    n = TelegramNotifier(client=client, enabled=True)

    assert await n.send("hello") is False


@pytest.mark.asyncio
async def test_flusher_loop_continues_after_tick_error(monkeypatch):
    """One failing tick (DB/Telegram hiccup) must not kill the background
    flusher permanently."""
    import asyncio

    class _StopFlusher(BaseException):
        pass

    client = MagicMock()
    client.is_connected.return_value = True
    client.get_me = AsyncMock(return_value=MagicMock(id=7))
    client.send_message = AsyncMock()
    n = TelegramNotifier(client=client, enabled=True)
    n._watchlist_interval = 0

    calls = {"n": 0}

    async def _wl():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("db hiccup")
        return []

    n.watchlist_provider = _wl

    real_sleep = asyncio.sleep
    ticks = {"n": 0}

    async def _fast_sleep(_seconds):
        ticks["n"] += 1
        if ticks["n"] > 3:
            raise _StopFlusher
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", _fast_sleep)

    with pytest.raises(_StopFlusher):
        await n._flusher_loop()

    # The first tick raised; the loop kept ticking and called the provider again.
    assert calls["n"] >= 2
