"""Cross-process pipeline run lock."""

from __future__ import annotations

import fcntl
import threading
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import TextIO


class PipelineAlreadyRunningError(RuntimeError):
    """Raised when another pipeline process already holds the run lock."""


_HELD_LOCKS: dict[Path, tuple[TextIO, int]] = {}
_HELD_LOCK_GUARD = threading.Lock()


@contextmanager
def pipeline_run_lock(data_dir: Path) -> Generator[TextIO, None, None]:
    """Acquire an exclusive non-blocking pipeline lock file.

    Re-entrant within the process: nested acquisitions bump a count. The exit
    of the ORIGINAL acquisition (count reaching 0) releases the flock and
    closes the handle — the previous version deleted the registry entry
    without unlocking when a re-entrant acquisition exited, leaking the flock
    and causing spurious PipelineAlreadyRunningError on the next acquire.

    NOTE: the guard protects only the registry mutations — it must NOT wrap
    the yield (threading.Lock is non-reentrant; a nested acquisition during
    the body would self-deadlock).
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    lock_path = data_dir / "pipeline.lock"

    with _HELD_LOCK_GUARD:
        existing = _HELD_LOCKS.get(lock_path)
        if existing is not None:
            handle, count = existing
            _HELD_LOCKS[lock_path] = (handle, count + 1)
        else:
            handle = lock_path.open("w")
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                handle.close()
                raise PipelineAlreadyRunningError(
                    "Another pipeline run is already active"
                ) from exc
            _HELD_LOCKS[lock_path] = (handle, 1)

    try:
        yield handle
    finally:
        with _HELD_LOCK_GUARD:
            handle, held_count = _HELD_LOCKS[lock_path]
            if held_count <= 1:
                del _HELD_LOCKS[lock_path]
                release_flock = True
            else:
                _HELD_LOCKS[lock_path] = (handle, held_count - 1)
                release_flock = False
        if release_flock:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()