"""Tests for the TelegramAdapter reconnect budget."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from telecrime.adapters.telegram import TelegramAdapter
from telecrime.config import Config, TelegramConfig


def _make_adapter() -> TelegramAdapter:
    cfg = Config(
        database_url="postgresql://x:y@db/z",
        telegram=TelegramConfig(api_id=1, api_hash="x"),
    )
    return TelegramAdapter(cfg)


@pytest.mark.asyncio
async def test_ensure_connected_aborts_after_budget(monkeypatch):
    """A stuck connect() must not hold _ensure_connected longer than the budget.

    Regression for the 2026-06-04 incident where a leaked Telethon client's
    auto_reconnect=True kept spinning for 9 hours, holding a PG transaction
    open and stalling the pipeline.
    """
    adapter = _make_adapter()
    adapter._ENSURE_CONNECTED_BUDGET_SECONDS = 1  # tighten for the test

    async def _hangs_forever(timeout: int = 30) -> None:
        # Simulate Telethon's stuck reconnect — the inner asyncio.wait_for in
        # connect() doesn't enforce the timeout because the spinning task
        # ignores cancellation.
        await asyncio.sleep(60)

    # connect is bound to the adapter; replace it with the hanging stub.
    monkeypatch.setattr(adapter, "connect", _hangs_forever)

    t = asyncio.get_event_loop().time()
    with pytest.raises(ConnectionError, match="budget"):
        await adapter._ensure_connected(timeout=1, reason="test")
    elapsed = asyncio.get_event_loop().time() - t

    # Must abort within budget + small slack; without the outer wait_for this
    # would block ~60s.
    assert elapsed < 5, f"_ensure_connected took {elapsed:.1f}s, expected < 5s"


@pytest.mark.asyncio
async def test_run_with_reconnect_aborts_on_stuck_factory(monkeypatch):
    """A Telethon op that hangs inside `await factory()` must not block the
    caller forever — the 2026-06-08 wedge symptom was a 10.3h-idle PG
    transaction held open by ChannelJoiner.join_conversation while Telethon
    was stuck. Budget enforces an upper bound per attempt.
    """
    adapter = _make_adapter()
    adapter._ENSURE_CONNECTED_BUDGET_SECONDS = 1
    adapter._RUN_WITH_RECONNECT_BUDGET_SECONDS = 1

    async def _ensure_ok(timeout=30, reason="t"):
        return None
    monkeypatch.setattr(adapter, "_ensure_connected", _ensure_ok)

    async def _hangs_forever():
        await asyncio.sleep(60)

    t = asyncio.get_event_loop().time()
    # With retries=0 to ensure deterministic single attempt; the outer
    # wait_for fires after the budget and the function raises
    # ConnectionError because asyncio.TimeoutError → CancelledError path is
    # caught and converted by the no-more-retries branch.  timeout=1 keeps
    # the budget at the shrunken 1s cap (budget = max(cap, explicit timeout)).
    with pytest.raises((ConnectionError, asyncio.TimeoutError)):
        await adapter._run_with_reconnect(
            "test_op", _hangs_forever, timeout=1, retries=0
        )
    elapsed = asyncio.get_event_loop().time() - t

    assert elapsed < 5, f"_run_with_reconnect took {elapsed:.1f}s, expected < 5s"


@pytest.mark.asyncio
async def test_run_with_reconnect_honors_large_explicit_timeout(monkeypatch):
    """A long operation (multi-GB download) must NOT be killed by the fixed
    budget cap — the explicit timeout extends it.

    Regression: downloads of >300 MB at ~1-2 MB/s exceed the 300s
    _RUN_WITH_RECONNECT_BUDGET_SECONDS cap, get cancelled and restarted in a
    loop that never completes (the 2026-08-16 "0 creds, 2.6GB file cycling"
    incident: 17h stuck on the same archive).
    """
    adapter = _make_adapter()
    adapter._RUN_WITH_RECONNECT_BUDGET_SECONDS = 1  # tight cap

    async def _ensure_ok(timeout=30, reason="t"):
        return None
    monkeypatch.setattr(adapter, "_ensure_connected", _ensure_ok)

    async def _long_op():
        # Would be cancelled after 1s under the old fixed cap; must survive
        # until the explicit 3s timeout instead.
        await asyncio.sleep(2)
        return "done"

    result = await adapter._run_with_reconnect(
        "long_download", _long_op, timeout=3, retries=0
    )
    assert result == "done"


class _FakeIter:
    """Emulates a Telethon iter_download for one striper range."""

    def __init__(self, base_offset: int, stride: int, count: int, chunk_size: int,
                 total: int):
        self._offset = base_offset
        self._stride = stride
        self._count = count
        self._chunk_size = chunk_size
        self._total = total
        self._i = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._i >= self._count:
            raise StopAsyncIteration
        start = self._offset + self._i * self._stride
        end = min(start + self._chunk_size, self._total)
        self._i += 1
        # Zero-filled chunk so every range can be verified independently.
        return bytes(end - start)


class _FakeMedia:
    pass


class _FakeMessage:
    def __init__(self, size: int):
        self.media = type("M", (), {})()
        self.media.document = type("D", (), {"size": size})()


class _FakeClient:
    """Stands in for the Telethon client: serves striper ranges on demand."""

    def __init__(self, total: int, chunk_size: int = 512 * 1024):
        self._total = total
        self._chunk_size = chunk_size
        self.iter_calls = []

    def iter_download(self, media, *, offset, stride, limit, chunk_size,
                      file_size):
        self.iter_calls.append((offset, stride, limit, chunk_size, file_size))
        return _FakeIter(offset, stride, limit, chunk_size, file_size)


@pytest.mark.asyncio
async def test_parallel_download_writes_all_bytes(tmp_path):
    """Parallel stripers cover the file exactly once, with no gaps/overlaps."""
    adapter = _make_adapter()
    size = 2 * 1024 * 1024 + 12345  # not a multiple of chunk size
    n = 8
    client = _FakeClient(size)
    msg = _FakeMessage(size)
    dest = tmp_path / "test_par_dl.bin"

    await adapter._download_media_parallel(client, msg, dest, n, size, None)

    with open(dest, "rb") as f:
        data = f.read()
    assert len(data) == size, f"expected {size} bytes, got {len(data)}"
    # All bytes must be present (zero-filled), proving every stripe wrote.
    assert data.count(b"\x00") == size
    # Only stripers whose range actually overlaps the file are requested.
    assert 0 < len(client.iter_calls) <= n
    for offset, stride, _limit, chunk_size, file_size in client.iter_calls:
        assert chunk_size == 512 * 1024
        assert file_size == size
        assert offset < size
        assert stride == n * 512 * 1024


@pytest.mark.asyncio
async def test_iter_messages_raises_on_cancelled():
    """A Telethon CancelledError (connection drop) must raise, not silently
    end iteration — the silent path let ingest advance its checkpoint past
    unfetched messages (round-5 fix)."""
    from telecrime.adapters.telegram import TelegramAdapter

    config = MagicMock()
    config.telegram.api_id = 1
    config.telegram.api_hash = "x"
    config.telegram.session_name = "test"
    adapter = TelegramAdapter(config)

    client = AsyncMock()
    adapter.client = client
    from datetime import UTC, datetime

    msg = MagicMock()
    msg.id = 1
    msg.date = datetime.now(UTC)
    msg.fwd_from = None
    msg.message = "x"

    async def _boom():
        yield msg
        raise asyncio.CancelledError

    client.iter_messages = lambda **kw: _boom()
    with pytest.raises(RuntimeError):
        async for _ in adapter.iter_messages(1, min_id=0):
            pass


@pytest.mark.asyncio
async def test_iter_messages_stalls_bounded():
    """A half-open socket (Telethon page fetch that never completes) must not
    hang ingest forever: the bounded iterator raises RuntimeError after the
    stall bound so the watchdog can restart the run with a fresh connection."""
    from telecrime.adapters.telegram import TelegramAdapter

    config = MagicMock()
    config.telegram.api_id = 1
    config.telegram.api_hash = "x"
    config.telegram.session_name = "test"
    adapter = TelegramAdapter(config)
    adapter._ITER_STALL_SECONDS = 1  # tighten for the test

    client = AsyncMock()
    adapter.client = client

    async def _hangs():
        await asyncio.sleep(60)
        yield  # never reached

    client.iter_messages = lambda **kw: _hangs()
    with pytest.raises(RuntimeError, match="stalled"):
        async for _ in adapter.iter_messages(1, min_id=0):
            pass


@pytest.mark.asyncio
async def test_iter_conversations_stalls_bounded():
    """Same stall bound for the conversation scan."""
    from telecrime.adapters.telegram import TelegramAdapter

    config = MagicMock()
    config.telegram.api_id = 1
    config.telegram.api_hash = "x"
    config.telegram.session_name = "test"
    adapter = TelegramAdapter(config)
    adapter._ITER_STALL_SECONDS = 1

    client = AsyncMock()
    adapter.client = client

    async def _hangs():
        await asyncio.sleep(60)
        yield  # never reached

    client.iter_dialogs = lambda: _hangs()
    with pytest.raises(RuntimeError, match="stalled"):
        async for _ in adapter.iter_conversations():
            pass


@pytest.mark.asyncio
async def test_run_with_reconnect_propagates_external_cancel(monkeypatch):
    """An EXTERNAL task.cancel() must not be swallowed into a reconnect+retry
    loop — that leak left _run_with_reconnect tasks alive after the download
    monitor cancelled them, hammering reconnect on a wedged session for hours."""
    adapter = _make_adapter()

    async def _ensure_ok(timeout=30, reason="t"):
        return None
    monkeypatch.setattr(adapter, "_ensure_connected", _ensure_ok)
    monkeypatch.setattr(adapter, "_reconnect_blocking", AsyncMock())

    async def _never_called():
        await asyncio.sleep(3600)
        raise AssertionError("factory must not be retried after external cancel")

    task = asyncio.create_task(
        adapter._run_with_reconnect("dl", _never_called, timeout=5, retries=2)
    )
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    adapter._reconnect_blocking.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_with_reconnect_retries_telethon_drop_cancel(monkeypatch):
    """A CancelledError raised by Telethon's own machinery (connection drop,
    not an external cancel) must still go through the reconnect+retry path."""
    adapter = _make_adapter()

    async def _ensure_ok(timeout=30, reason="t"):
        return None
    monkeypatch.setattr(adapter, "_ensure_connected", _ensure_ok)
    monkeypatch.setattr(adapter, "_reconnect_blocking", AsyncMock())

    calls = {"n": 0}

    async def _drops():
        calls["n"] += 1
        if calls["n"] == 1:
            raise asyncio.CancelledError  # simulates Telethon drop-cancel
        return "ok"

    result = await adapter._run_with_reconnect("dl", _drops, timeout=5, retries=2)
    assert result == "ok"
    assert calls["n"] == 2
    adapter._reconnect_blocking.assert_awaited_once()


@pytest.mark.asyncio
async def test_request_unsuccessful_is_retryable():
    """Telethon's 'Request was unsuccessful N time(s)' (given up after its own
    retries on a dropped connection) must trigger the adapter reconnect path."""
    from telecrime.adapters.telegram import TelegramAdapter

    assert TelegramAdapter._is_retryable_connection_error(
        ValueError("Request was unsuccessful 3 time(s)")
    )


@pytest.mark.asyncio
async def test_ensure_connected_forces_reconnect_when_suspect(monkeypatch):
    """A half-open socket leaves is_connected() True after a drop; the suspect
    flag must force a full teardown + fresh connect instead of trusting it —
    otherwise every op burns its budget on a dead client (observed: 5h crawl
    with 'waiting for Telegram reconnect' note and zero recoveries)."""
    adapter = _make_adapter()

    calls = {"connects": 0}
    connect_ok = {"value": False}

    async def _ensure_ok(timeout=30, reason="t"):
        return None
    monkeypatch.setattr(adapter, "_ensure_connected", _ensure_ok)

    async def _hangs():
        await asyncio.sleep(60)
        return "never"

    # Budget fires → TimeoutError → suspect flag set, op re-raises.
    adapter._RUN_WITH_RECONNECT_BUDGET_SECONDS = 1
    with pytest.raises(asyncio.TimeoutError):
        await adapter._run_with_reconnect("dl", _hangs, timeout=1, retries=0)
    assert adapter._connection_suspect is True

    # A successful op clears the flag.
    async def _ok():
        return "done"
    result = await adapter._run_with_reconnect("dl", _ok, timeout=1, retries=0)
    assert result == "done"
    assert adapter._connection_suspect is False


@pytest.mark.asyncio
async def test_reconnect_blocking_clears_suspect(monkeypatch):
    """A fresh connect via _reconnect_blocking clears the suspect flag."""
    adapter = _make_adapter()
    adapter._connection_suspect = True

    async def _connect_ok(timeout=30):
        client = MagicMock()
        client.is_connected = lambda: True
        adapter.client = client
        adapter._clear_runtime_note()

    monkeypatch.setattr(adapter, "connect", _connect_ok)
    await adapter._reconnect_blocking("test", timeout=10)
    assert adapter._connection_suspect is False


@pytest.mark.asyncio
async def test_client_created_with_auto_reconnect_disabled(monkeypatch, tmp_path):
    """Regression: auto_reconnect=True let Telethon's internal reconnect loop
    race the adapter's own reconnect, wedge the client permanently, and leak
    zombie clients reconnecting on the same session file ('wrong session ID')."""
    import sqlite3

    from telecrime.config import Config, TelegramConfig

    cfg = Config(
        database_url="postgresql://x:y@db/z",
        telegram=TelegramConfig(api_id=1, api_hash="x", session_name="sess"),
    )
    session_path = tmp_path / "sess.session"
    sqlite3.connect(str(session_path)).close()

    created = {}

    def _fake_client(*args, **kwargs):
        created.update(kwargs)
        client = MagicMock()
        client.connect = AsyncMock()
        client.is_user_authorized = AsyncMock(return_value=True)
        client.disconnect = AsyncMock()
        return client

    monkeypatch.setattr(
        "telecrime.adapters.telegram.TelegramClient", _fake_client
    )
    adapter = TelegramAdapter(cfg)
    await adapter.connect(timeout=5)
    assert created.get("auto_reconnect") is False
