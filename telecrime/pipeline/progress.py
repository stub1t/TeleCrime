"""Write real-time pipeline progress to a JSON file for dashboard display."""

import json
import os
import tempfile
import threading
import time
from collections import deque
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

_DEFAULT_DATA_DIR = Path(__file__).parent.parent.parent / "data"

# Runtime-note fields patched via patch_progress() (e.g. from TelegramAdapter) are stored
# here so that PipelineProgressWriter._write() can merge them on every write.  Without this,
# a _write() from update_creds() or _heartbeat_loop() would overwrite whatever patch_progress
# had just written (because _write() uses self._runtime_note_* which are in-memory and not
# updated by patch_progress).
_NOTE_KEYS: frozenset[str] = frozenset(
    {"runtime_note", "runtime_note_kind", "runtime_note_since"}
)
_NOTE_OVERRIDES: dict[str, object] = {}
_NOTE_LOCK = threading.Lock()

STAGE_ORDER = [
    "ingest",
    "channel_discover",
    "discover",
    "plan",
    "acquire",
    "enrich",
    "extract",
    "parse",
    "finalize",
]


def _progress_path() -> Path:
    default_data_dir = Path(os.environ.get("TELECRIME_DATA_DIR", str(_DEFAULT_DATA_DIR)))
    default_path = default_data_dir / "pipeline_progress.json"
    return Path(os.environ.get("TELECRIME_PROGRESS_FILE", str(default_path)))


def _write_progress_data(data: dict[str, object]) -> None:
    path = _progress_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        # Unique temp name per write: the pipeline's heartbeat thread and the
        # scheduler process (patch_progress) write this file concurrently, and
        # a shared ".tmp" name let two writers clobber each other's temp —
        # worst case renaming a torn file into place (readers then see no
        # heartbeat and the watchdog kills a healthy pipeline).
        fd, tmp = tempfile.mkstemp(
            dir=str(path.parent), prefix=".progress-", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f)
            os.replace(tmp, path)
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
    except Exception:
        pass


def patch_progress(**updates: object) -> None:
    """Patch selected progress-file fields without disturbing other state."""
    note_updates = {k: v for k, v in updates.items() if k in _NOTE_KEYS}
    if note_updates:
        with _NOTE_LOCK:
            _NOTE_OVERRIDES.update(note_updates)
    data = read_progress() or {}
    data.update(updates)
    _write_progress_data(data)


def mark_progress_stopped(reason: str | None = None) -> None:
    """Mark a stale progress file as stopped without losing last counters."""
    updates: dict[str, object] = {
        "running": False,
        "dl_active": False,
        "current_stage": None,
        "runtime_note": reason,
        "runtime_note_kind": "stopped",
        "runtime_note_since": datetime.now(tz=UTC).isoformat(),
        "updated_at": datetime.now(tz=UTC).isoformat(),
    }
    patch_progress(**updates)


class PipelineProgressWriter:
    """Writes pipeline state to a JSON file as the pipeline runs.

    Implements the same public API as PipelineDisplay so it can be passed
    as the `display` argument to run_sequential_pipeline.
    """

    def __init__(self, path: Path | None = None):
        self._path = path or _progress_path()
        self._write_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._start_time = time.time()
        self._current_stage: str | None = None
        self._stages_completed: list[str] = []
        self._stages_failed: list[str] = []
        self._archive_index = 0
        self._archive_total = 0
        self._archive_name = ""
        self._dl_active = False
        self._dl_pct = 0.0
        self._dl_speed = 0.0
        self._credentials = 0
        self._duplicates = 0
        self._messages = 0
        self._errors = 0
        self._channels_joined = 0
        self._recent_results: deque[str] = deque(maxlen=5)
        self._last_progress_at = datetime.now(tz=UTC)
        self._shutdown_requested = False
        self._shutdown_mode: str | None = None
        self._shutdown_requested_at: str | None = None
        self._shutdown_state: str | None = None
        # Fresh subprocess start — discard any note overrides from the previous run.
        # TelegramAdapter calls patch_progress() to set/clear notes; _NOTE_OVERRIDES
        # keeps those in sync with _write() so they survive update_creds() overwrites.
        with _NOTE_LOCK:
            _NOTE_OVERRIDES.clear()
        self._runtime_note: str | None = None
        self._runtime_note_kind: str | None = None
        self._runtime_note_since: str | None = None
        self._write(running=True)
        self._heartbeat = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat.start()

    def _heartbeat_loop(self) -> None:
        """Refresh the progress file while the pipeline is active.

        Marks meaningful progress for the watchdog on every tick. Previously
        only extract/parse marked progress, which left ingest, channel_discover,
        discover, plan, finalize, the inter-stage transition windows, and
        startup recovery vulnerable to the watchdog killing a healthy
        subprocess for "no progress" — even though the python heartbeat thread
        proves the subprocess is alive. The watchdog still detects truly dead
        pipelines via the PID-liveness check.
        """
        while not self._stop_event.wait(30):
            self._mark_progress()
            self._write()

    def _write(self, running: bool = True) -> None:
        with self._write_lock:
            data = {
                "running": running,
                "started_at": datetime.fromtimestamp(
                    self._start_time, tz=UTC
                ).isoformat(),
                "elapsed_seconds": round(time.time() - self._start_time),
                "current_stage": self._current_stage,
                "stages_completed": self._stages_completed,
                "stages_failed": self._stages_failed,
                "current_archive": self._archive_name,
                "archive_index": self._archive_index,
                "archive_total": self._archive_total,
                "dl_active": self._dl_active,
                "dl_pct": self._dl_pct,
                "dl_speed": self._dl_speed,
                "credentials": self._credentials,
                "duplicates": self._duplicates,
                "messages": self._messages,
                "errors": self._errors,
                "channels_joined": self._channels_joined,
                "recent_results": list(self._recent_results),
                "last_progress_at": self._last_progress_at.isoformat(),
                "shutdown_requested": self._shutdown_requested,
                "shutdown_mode": self._shutdown_mode,
                "shutdown_requested_at": self._shutdown_requested_at,
                "shutdown_state": self._shutdown_state,
                "runtime_note": self._runtime_note,
                "runtime_note_kind": self._runtime_note_kind,
                "runtime_note_since": self._runtime_note_since,
                "updated_at": datetime.now(tz=UTC).isoformat(),
            }
            # Apply in-process note overrides from patch_progress() (e.g. TelegramAdapter).
            # This ensures note clears survive subsequent _write() calls from update_creds etc.
            with _NOTE_LOCK:
                data.update(_NOTE_OVERRIDES)
            _write_progress_data(data)

    def finish(self) -> None:
        """Mark pipeline as not running (call after pipeline exits)."""
        self._stop_event.set()
        if self._heartbeat.is_alive():
            self._heartbeat.join(timeout=1.0)
        self._current_stage = None
        self._runtime_note = None
        self._runtime_note_kind = None
        self._runtime_note_since = None
        with _NOTE_LOCK:
            _NOTE_OVERRIDES.clear()
        self._write(running=False)

    # --- PipelineDisplay-compatible API ---

    def _mark_progress(self) -> None:
        self._last_progress_at = datetime.now(tz=UTC)

    def stage_start(self, name: str) -> None:
        if self._current_stage and self._current_stage != name:
            if self._current_stage not in self._stages_completed:
                self._stages_completed.append(self._current_stage)
        self._current_stage = name
        self._dl_active = False
        self._mark_progress()
        self._write()

    def stage_complete(self, name: str) -> None:
        if name not in self._stages_completed:
            self._stages_completed.append(name)
        if self._current_stage == name:
            self._current_stage = None
        self._mark_progress()
        self._write()

    def stage_error(self, name: str) -> None:
        if name not in self._stages_failed:
            self._stages_failed.append(name)
        self._errors += 1
        self._mark_progress()
        self._write()

    def set_archive_total(self, total: int) -> None:
        self._archive_total = total
        self._mark_progress()
        self._write()

    def archive_start(self, name: str) -> None:
        self._archive_index += 1
        self._archive_name = name
        self._mark_progress()
        self._write()

    def archive_complete(self, name: str, creds: int, dups: int = 0) -> None:
        if self._current_stage:
            if self._current_stage not in self._stages_completed:
                self._stages_completed.append(self._current_stage)
            self._current_stage = None
        # Do NOT update _duplicates here — update_dups() already maintains the
        # cumulative count from ctx.duplicates_skipped during parse. Adding dups
        # again would double-count them between the archive_complete call and the
        # next update_dups() reset.
        # _credentials is already up-to-date from update_creds() calls during parse.
        self._dl_active = False
        short = name if len(name) <= 40 else name[:37] + "..."
        self._recent_results.append(f"{short}: +{creds:,} new, {dups:,} dups")
        self._mark_progress()
        self._write()

    def download_start(self, filename: str, size_mb: float) -> None:
        if filename and filename != self._archive_name:
            self._archive_name = filename
        self._dl_active = True
        self._dl_pct = 0.0
        self._dl_speed = 0.0
        self._mark_progress()
        self._write()

    def download_progress(self, pct: float, speed_mbps: float, eta: str) -> None:
        if pct > self._dl_pct or speed_mbps > 0:
            self._mark_progress()
        self._dl_pct = pct
        self._dl_speed = speed_mbps
        self._write()

    def download_complete(self) -> None:
        self._dl_active = False
        self._dl_pct = 100.0
        self._mark_progress()
        self._write()

    def update_messages(self, count: int) -> None:
        if count != self._messages:
            self._mark_progress()
        self._messages = count
        self._write()

    def update_creds(self, count: int) -> None:
        if count != self._credentials:
            self._mark_progress()
        self._credentials = count
        self._write()

    def update_counts(self, creds: int, dups: int) -> None:
        """Update both counters in a single progress-file write.

        Use from the parse hot loop instead of `update_creds` + `update_dups`
        back-to-back — halves the tmp-file write/rename I/O per batch.
        """
        if creds != self._credentials or dups != self._duplicates:
            self._mark_progress()
        self._credentials = creds
        self._duplicates = dups
        self._write()

    def add_error(self) -> None:
        self._errors += 1
        self._mark_progress()
        self._write()

    def channels_update(self, joined: int) -> None:
        if joined != self._channels_joined:
            self._mark_progress()
        self._channels_joined = joined
        self._write()

    def set_shutdown_state(
        self,
        requested: bool,
        mode: str | None = None,
        requested_at: str | None = None,
        state: str | None = None,
    ) -> None:
        self._shutdown_requested = requested
        self._shutdown_mode = mode
        self._shutdown_requested_at = requested_at
        self._shutdown_state = state
        self._mark_progress()
        self._write()


def read_progress() -> dict[str, Any] | None:
    """Read the current pipeline progress file. Returns None if unavailable."""
    path = _progress_path()
    if not path.exists():
        return None
    try:
        return cast(dict[str, Any], json.loads(path.read_text()))
    except Exception:
        return None
