"""Cross-process pipeline run lock."""

from __future__ import annotations

import fcntl
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import TextIO


class PipelineAlreadyRunningError(RuntimeError):
    """Raised when another pipeline process already holds the run lock."""


_HELD_LOCKS: dict[Path, tuple[TextIO, int]] = {}


@contextmanager
def pipeline_run_lock(data_dir: Path) -> Generator[TextIO, None, None]:
    """Acquire an exclusive non-blocking pipeline lock file."""
    data_dir.mkdir(parents=True, exist_ok=True)
    lock_path = data_dir / "pipeline.lock"

    existing = _HELD_LOCKS.get(lock_path)
    if existing is not None:
        handle, count = existing
        _HELD_LOCKS[lock_path] = (handle, count + 1)
        try:
            yield handle
        finally:
            handle, held_count = _HELD_LOCKS[lock_path]
            if held_count <= 1:
                del _HELD_LOCKS[lock_path]
            else:
                _HELD_LOCKS[lock_path] = (handle, held_count - 1)
        return

    handle = lock_path.open("w")
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise PipelineAlreadyRunningError("Another pipeline run is already active") from exc
        _HELD_LOCKS[lock_path] = (handle, 1)
        yield handle
    finally:
        try:
            if lock_path in _HELD_LOCKS:
                del _HELD_LOCKS[lock_path]
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()
