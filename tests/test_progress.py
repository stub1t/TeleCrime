"""Tests for PipelineProgressWriter heartbeat behavior."""

import time

from telecrime.pipeline.progress import PipelineProgressWriter


def _run_one_heartbeat_tick(w: PipelineProgressWriter) -> None:
    """Force exactly one heartbeat-loop iteration without waiting 30s.

    The loop is `while not self._stop_event.wait(30): tick()`. Stub the
    Event with a single-shot fake that returns False once (run tick) then
    True (exit loop).
    """
    calls = {"n": 0}

    class _Once:
        def wait(self, timeout):
            calls["n"] += 1
            return calls["n"] > 1  # False on first call, True on second
        def is_set(self):
            return calls["n"] > 1
        def set(self):
            calls["n"] = 99

    w._stop_event = _Once()
    # Run the loop body once in this thread (the original heartbeat thread is
    # still waiting on the original event; we drive a synchronous tick here).
    w._heartbeat_loop()


def test_heartbeat_marks_progress_for_ingest_stage(tmp_path, monkeypatch):
    """Regression: heartbeat must update last_progress_at for stages other
    than extract/parse, so the watchdog doesn't kill a long ingest/discover."""
    monkeypatch.setenv("TELECRIME_PROGRESS_FILE", str(tmp_path / "p.json"))
    w = PipelineProgressWriter()
    try:
        w._current_stage = "ingest"
        baseline = w._last_progress_at
        time.sleep(0.01)  # ensure clock can move forward
        _run_one_heartbeat_tick(w)
        assert w._last_progress_at > baseline, (
            "heartbeat should have advanced last_progress_at during ingest"
        )
    finally:
        try:
            w.finish()
        except Exception:
            pass


def test_heartbeat_marks_progress_for_none_stage(tmp_path, monkeypatch):
    """Heartbeat must keep last_progress_at fresh even when stage is None
    (inter-stage transitions, startup recovery, post-final-stage cleanup)."""
    monkeypatch.setenv("TELECRIME_PROGRESS_FILE", str(tmp_path / "p.json"))
    w = PipelineProgressWriter()
    try:
        w._current_stage = None
        baseline = w._last_progress_at
        time.sleep(0.01)
        _run_one_heartbeat_tick(w)
        assert w._last_progress_at > baseline, (
            "heartbeat should have advanced last_progress_at when stage is None"
        )
    finally:
        try:
            w.finish()
        except Exception:
            pass
