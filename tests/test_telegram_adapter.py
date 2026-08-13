"""Tests for the TelegramAdapter reconnect budget."""

from __future__ import annotations

import asyncio

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
    # caught and converted by the no-more-retries branch.
    with pytest.raises((ConnectionError, asyncio.TimeoutError)):
        await adapter._run_with_reconnect("test_op", _hangs_forever, retries=0)
    elapsed = asyncio.get_event_loop().time() - t

    assert elapsed < 5, f"_run_with_reconnect took {elapsed:.1f}s, expected < 5s"
