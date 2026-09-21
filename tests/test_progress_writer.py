"""Focused tests for the progress-file write/merge API in progress.py.

`test_progress.py` covers the heartbeat; this module covers the file-level
contract the dashboard and watchdog depend on: atomic writes, temp-file
cleanup on failure, throttled failure warnings, note overrides, and the full
JSON schema emitted by PipelineProgressWriter.
"""

import json
import logging

from telecrime.pipeline.progress import (
    PipelineProgressWriter,
    _write_progress_data,
    mark_progress_stopped,
    patch_progress,
    read_progress,
)


def _progress_file(tmp_path, monkeypatch):
    path = tmp_path / "pipeline_progress.json"
    monkeypatch.setenv("TELECRIME_PROGRESS_FILE", str(path))
    return path


def test_read_progress_missing_corrupt_and_valid(tmp_path, monkeypatch):
    path = _progress_file(tmp_path, monkeypatch)

    assert read_progress() is None

    path.write_text("not json {")
    assert read_progress() is None

    payload = {"running": True, "credentials": 4}
    path.write_text(json.dumps(payload))
    assert read_progress() == payload


def test_patch_progress_merges_without_clobbering_other_fields(tmp_path, monkeypatch):
    path = _progress_file(tmp_path, monkeypatch)
    path.write_text(json.dumps({"credentials": 5, "running": True}))

    patch_progress(credentials=7, runtime_note="paused")

    data = json.loads(path.read_text())
    assert data["credentials"] == 7
    assert data["running"] is True
    assert data["runtime_note"] == "paused"


def test_mark_progress_stopped_keeps_counters(tmp_path, monkeypatch):
    path = _progress_file(tmp_path, monkeypatch)
    patch_progress(credentials=9, duplicates=2, running=True, current_stage="parse")

    mark_progress_stopped("watchdog kill")

    data = json.loads(path.read_text())
    assert data["running"] is False
    assert data["dl_active"] is False
    assert data["current_stage"] is None
    assert data["runtime_note"] == "watchdog kill"
    assert data["runtime_note_kind"] == "stopped"
    assert data["runtime_note_since"]
    assert data["updated_at"]
    # Last counters must survive for post-mortem display.
    assert data["credentials"] == 9
    assert data["duplicates"] == 2


def test_writer_note_overrides_survive_counter_writes(tmp_path, monkeypatch):
    """TelegramAdapter's runtime note must not be erased by update_creds()."""
    path = _progress_file(tmp_path, monkeypatch)
    w = PipelineProgressWriter()
    try:
        patch_progress(runtime_note="receiving shutdown", runtime_note_kind="stopped")

        w.update_creds(3)  # would overwrite the note without _NOTE_OVERRIDES

        data = json.loads(path.read_text())
        assert data["runtime_note"] == "receiving shutdown"
        assert data["runtime_note_kind"] == "stopped"
        assert data["credentials"] == 3
    finally:
        w.finish()

    data = json.loads(path.read_text())
    assert data["running"] is False
    assert data["runtime_note"] is None
    assert data["runtime_note_kind"] is None
    assert data["credentials"] == 3


def test_writer_serializes_full_state(tmp_path, monkeypatch):
    path = _progress_file(tmp_path, monkeypatch)
    w = PipelineProgressWriter()
    try:
        w.stage_start("parse")
        w.set_archive_total(2)
        w.archive_start("logs.zip")
        w.download_start("logs.zip", 12.5)
        w.download_progress(37.5, 1.25, "00:10")
        w.download_complete()
        w.update_counts(11, 4)
        w.add_error()
        w.channels_update(3)
        w.set_shutdown_state(
            True,
            mode="finish_archive",
            requested_at="2026-01-01T00:00:00+00:00",
            state="draining",
        )
        w.archive_complete("logs.zip", 11, 4)
        w.stage_complete("parse")

        data = json.loads(path.read_text())
        assert data["running"] is True
        assert data["current_stage"] is None
        assert data["stages_completed"] == ["parse"]
        assert data["stages_failed"] == []
        assert data["archive_total"] == 2
        assert data["archive_index"] == 1
        assert data["current_archive"] == "logs.zip"
        assert data["dl_active"] is False
        assert data["dl_pct"] == 100.0
        assert data["dl_speed"] == 1.25
        assert data["credentials"] == 11
        assert data["duplicates"] == 4
        assert data["errors"] == 1
        assert data["channels_joined"] == 3
        assert data["shutdown_requested"] is True
        assert data["shutdown_mode"] == "finish_archive"
        assert data["shutdown_requested_at"] == "2026-01-01T00:00:00+00:00"
        assert data["shutdown_state"] == "draining"
        assert data["recent_results"] == ["logs.zip: +11 new, 4 dups"]
        assert data["started_at"]
        assert data["updated_at"]
        assert data["elapsed_seconds"] >= 0
        assert data["last_progress_at"]
        # Atomic write must not leave temp files behind.
        assert list(tmp_path.glob(".progress-*.tmp")) == []
    finally:
        w.finish()


def test_archive_complete_truncates_long_name_and_caps_history(tmp_path, monkeypatch):
    path = _progress_file(tmp_path, monkeypatch)
    w = PipelineProgressWriter()
    try:
        long_name = "x" * 80
        for i in range(7):
            w.archive_complete(f"{long_name}{i}", creds=i, dups=0)

        data = json.loads(path.read_text())
        assert len(data["recent_results"]) == 5  # deque(maxlen=5)
        assert data["recent_results"][0] == f"{'x' * 37}...: +2 new, 0 dups"
        assert data["recent_results"][-1] == f"{'x' * 37}...: +6 new, 0 dups"
    finally:
        w.finish()


def test_write_failure_warns_once_and_cleans_temp_file(tmp_path, monkeypatch, caplog):
    target = tmp_path / "pipeline_progress.json"

    def _boom(_src, _dst):
        raise OSError("no space left on device")

    monkeypatch.setattr("telecrime.pipeline.progress.os.replace", _boom)

    with caplog.at_level(logging.WARNING, logger="telecrime.pipeline.progress"):
        _write_progress_data({"credentials": 1}, target)
        _write_progress_data({"credentials": 2}, target)

    assert not target.exists()
    warnings = [r for r in caplog.records if "Progress file write failed" in r.getMessage()]
    # Second failure inside the 5-minute throttle window stays silent.
    assert len(warnings) == 1
    assert "no space left on device" in warnings[0].getMessage()
    assert list(tmp_path.glob(".progress-*.tmp")) == []


def test_progress_mirror_is_written_even_when_primary_fails(tmp_path, monkeypatch):
    """The local mirror must stay fresh when the data-drive write fails/hangs.

    A wedged data volume blocks the primary write in D-state; the watchdog
    reads the mirror (on the local filesystem) so monitoring survives.
    """
    import os

    primary = tmp_path / "pipeline_progress.json"
    mirror = tmp_path / "pipeline_progress.mirror.json"
    monkeypatch.setenv("TELECRIME_PROGRESS_FILE", str(primary))
    monkeypatch.setenv("TELECRIME_PROGRESS_MIRROR_FILE", str(mirror))

    real_replace = os.replace

    def _replace(src, dst):
        if str(dst) == str(primary):
            raise OSError("data drive wedged")
        real_replace(src, dst)

    monkeypatch.setattr("telecrime.pipeline.progress.os.replace", _replace)

    _write_progress_data({"running": True, "credentials": 7})

    assert not primary.exists()
    assert json.loads(mirror.read_text())["credentials"] == 7
    assert list(tmp_path.glob(".progress-*.tmp")) == []
