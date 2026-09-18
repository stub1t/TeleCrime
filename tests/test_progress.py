"""Tests for PipelineProgressWriter heartbeat behavior."""

import json
import time

from telecrime.pipeline.progress import PipelineProgressWriter, _progress_path


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


def test_progress_path_uses_configured_data_dir(tmp_path, monkeypatch):
    monkeypatch.delenv("TELECRIME_PROGRESS_FILE", raising=False)
    data_dir = tmp_path / "runtime"
    monkeypatch.setenv("TELECRIME_DATA_DIR", str(data_dir))

    assert _progress_path() == data_dir / "pipeline_progress.json"


def test_failed_stage_not_promoted_to_completed(tmp_path, monkeypatch):
    """Regression: stage_error() left _current_stage set, so the next
    stage_start() promoted the FAILED stage into stages_completed —
    reporting it as both failed and completed."""
    monkeypatch.setenv("TELECRIME_PROGRESS_FILE", str(tmp_path / "p.json"))
    w = PipelineProgressWriter()
    try:
        w.stage_start("extract")
        w.stage_error("extract")

        assert w._current_stage is None
        w.stage_start("parse")

        assert "extract" in w._stages_failed
        assert "extract" not in w._stages_completed
        assert w._current_stage == "parse"
    finally:
        try:
            w.finish()
        except Exception:
            pass


def test_stage_start_does_not_promote_failed_stage(tmp_path, monkeypatch):
    """Defense in depth: a stage already recorded as failed is never appended
    to stages_completed when a new stage starts."""
    monkeypatch.setenv("TELECRIME_PROGRESS_FILE", str(tmp_path / "p.json"))
    w = PipelineProgressWriter()
    try:
        w._current_stage = "extract"
        w._stages_failed.append("extract")
        w.stage_start("parse")

        assert "extract" not in w._stages_completed
    finally:
        try:
            w.finish()
        except Exception:
            pass


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


def test_write_after_stop_never_reports_running(tmp_path, monkeypatch):
    """A late heartbeat write must not resurrect running=True after finish().

    Regression: the heartbeat thread could be preempted between its wait() and
    _write() by finish(), so its write landed after finish()'s running=False
    and left a phantom live pipeline in the progress file.
    """
    progress_file = tmp_path / "p.json"
    monkeypatch.setenv("TELECRIME_PROGRESS_FILE", str(progress_file))
    w = PipelineProgressWriter()
    try:
        w._stop_event.set()
        w._write()
        assert json.loads(progress_file.read_text())["running"] is False
    finally:
        try:
            w.finish()
        except Exception:
            pass


def test_update_errors_writes_authoritative_count(tmp_path, monkeypatch):
    """Stages append to ctx.errors without add_error(); the writer must be
    able to sync the progress file's error counter to that total."""
    progress_file = tmp_path / "p.json"
    monkeypatch.setenv("TELECRIME_PROGRESS_FILE", str(progress_file))
    w = PipelineProgressWriter()
    try:
        w.add_error()
        w.update_errors(3)
        assert json.loads(progress_file.read_text())["errors"] == 3
    finally:
        try:
            w.finish()
        except Exception:
            pass


def test_finish_pipeline_run_syncs_display_errors(session, test_config):
    """_finish_pipeline_run must mirror ctx.errors into the display.

    Regression: pipeline_progress.json showed errors=0 while pipeline_runs
    recorded failures because extract/acquire/parse/finalize append directly
    to ctx.errors without touching the display counter.
    """
    from unittest.mock import MagicMock

    from telecrime.pipeline.orchestrator import (
        PipelineContext,
        _finish_pipeline_run,
        _start_pipeline_run,
    )

    class _RecordingDisplay:
        def __init__(self):
            self.counts: list[int] = []

        def update_errors(self, count: int) -> None:
            self.counts.append(count)

    display = _RecordingDisplay()
    ctx = PipelineContext(
        config=test_config,
        session=session,
        adapter=MagicMock(),
        display=display,
    )
    ctx.errors.append("acquire: download error")
    ctx.errors.append("parse: boom")

    run = _start_pipeline_run(session, mode="sequential", dry_run=False)
    _finish_pipeline_run(
        session, run, ctx, stages_completed=[], stages_failed=[]
    )

    assert display.counts == [2]
