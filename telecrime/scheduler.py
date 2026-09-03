"""Background worker scheduler for periodic telecrime tasks."""

from __future__ import annotations

import asyncio
import concurrent.futures as _futures
import json
import logging
import os
import platform
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from sqlalchemy.engine import CursorResult

from telecrime.pipeline.progress import mark_progress_stopped, read_progress

logger = logging.getLogger(__name__)

_status_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Status persistence
# ---------------------------------------------------------------------------

_DEFAULT_DATA_DIR = Path(__file__).parent.parent / "data"

JOB_DEFS: dict[str, dict] = {
    "pipeline": {
        "description": "Full Telegram pipeline (ingest → download → extract → parse)",
        "interval_hours": 4,
        "requires_telegram": True,
    },
    "pipeline_watchdog": {
        "description": "Detect stale pipeline runs, kill them, and restart cleanly",
        "interval_hours": 1,
        "interval_minutes": 10,
        "requires_telegram": False,
    },
    "pipeline_health": {
        "description": "Check pipeline health and progress",
        "interval_hours": 2,
        "requires_telegram": False,
    },
    "vacuum": {
        "description": "SQLite VACUUM to reclaim space and optimise read performance",
        "interval_hours": 168,
        "requires_telegram": False,
    },
    "channel_join": {
        "description": "Discover and join new stealer-log Telegram channels",
        "interval_hours": 1,
        "requires_telegram": True,
        "startup_delay_seconds": 90,
    },
    "watchlist_notify": {
        "description": "Alert on new watchlist matches",
        "interval_hours": 1,
        "interval_minutes": 15,
        "requires_telegram": True,
        "startup_delay_seconds": 0,
    },
    "hourly_summary": {
        "description": "Send hourly unique-credential summary to Telegram",
        "interval_hours": 1,
        "requires_telegram": True,
        "startup_delay_seconds": 30,
    },
    "daily_summary": {
        "description": "Send daily unique-credential summary to Telegram",
        "interval_hours": 24,
        "requires_telegram": True,
        "startup_delay_seconds": 60,
    },
    "reparse_stealers": {
        "description": "Backfill stealer_type on credentials where it is NULL",
        "interval_hours": 24,
        "requires_telegram": False,
    },
    "channel_export": {
        "description": "Regenerate channels.md and channels.txt and push to git remote",
        "interval_hours": 168,
        "requires_telegram": False,
    },
}


@dataclass
class JobStatus:
    name: str
    description: str
    interval_hours: int
    last_run: str | None = None
    last_result: str | None = None
    last_error: str | None = None
    next_run: str | None = None
    running: bool = False
    shutdown_requested: bool = False
    shutdown_mode: str | None = None
    shutdown_requested_at: str | None = None
    shutdown_state: str | None = None


@dataclass
class PipelineHealth:
    reasons: list[str]
    progress_summary: str
    disk_status: str = ""

    @property
    def healthy(self) -> bool:
        return not self.reasons

    def result(self) -> str:
        status = "healthy" if self.healthy else f"unhealthy: {'; '.join(self.reasons)}"
        parts = [status, self.progress_summary]
        if self.disk_status:
            parts.append(self.disk_status)
        return "; ".join(part for part in parts if part)


def _iso_age_seconds(value: str | None) -> float | None:
    if not value:
        return None
    try:
        ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return (datetime.now(UTC) - ts).total_seconds()


def _status_path() -> Path:
    data_dir = Path(os.environ.get("TELECRIME_DATA_DIR", str(_DEFAULT_DATA_DIR)))
    return Path(os.environ.get("TELECRIME_STATUS_FILE", str(data_dir / "scheduler_status.json")))


def _pipeline_pid_path() -> Path:
    data_dir = Path(os.environ.get("TELECRIME_DATA_DIR", str(_DEFAULT_DATA_DIR)))
    return Path(os.environ.get("TELECRIME_PIPELINE_PID_FILE", str(data_dir / "pipeline.pid")))


def _shutdown_request_path() -> Path:
    data_dir = Path(os.environ.get("TELECRIME_DATA_DIR", str(_DEFAULT_DATA_DIR)))
    return Path(
        os.environ.get(
            "TELECRIME_SHUTDOWN_REQUEST_FILE",
            str(data_dir / "pipeline_shutdown_request.json"),
        )
    )


def read_shutdown_request() -> dict[str, str] | None:
    path = _shutdown_request_path()
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text())
    except Exception:
        return None
    if not isinstance(raw, dict):
        return None
    requested_at = raw.get("requested_at")
    mode = raw.get("mode")
    reason = raw.get("reason")
    if not isinstance(requested_at, str) or not isinstance(mode, str):
        return None
    result: dict[str, str] = {"requested_at": requested_at, "mode": mode}
    if isinstance(reason, str) and reason:
        result["reason"] = reason
    return result


def write_shutdown_request(mode: str = "finish_archive", reason: str | None = None) -> dict[str, str]:
    request = {
        "requested_at": datetime.now(UTC).isoformat(),
        "mode": mode,
    }
    if reason:
        request["reason"] = reason
    path = _shutdown_request_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        os.write(fd, json.dumps(request).encode())
        os.close(fd)
        os.replace(tmp_path, path)
    except BaseException:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
    return request


def clear_shutdown_request() -> None:
    try:
        _shutdown_request_path().unlink(missing_ok=True)
    except OSError:
        pass


def _shutdown_state(config) -> str | None:
    request = read_shutdown_request()
    if not request:
        return None
    progress_running = False
    try:
        progress = read_progress() or {}
        progress_running = bool(progress.get("running"))
    except Exception:
        progress_running = False

    pid = _read_pipeline_pid()
    lock_held = _pipeline_lock_is_held(config.data_dir)
    if progress_running or (pid is not None and _pid_is_alive(pid)) or lock_held:
        return "draining"
    return "drained"


def send_desktop_notification(title: str, message: str) -> str:
    """Send a best-effort desktop notification without adding dependencies."""
    system = platform.system()
    try:
        if system == "Linux":
            if not shutil.which("notify-send"):
                return "skipped: notify-send not found"
            subprocess.run(
                ["notify-send", title, message],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return "sent"
        if system == "Darwin":
            script = (
                "display notification "
                f"{json.dumps(message)} with title {json.dumps(title)}"
            )
            subprocess.run(
                ["osascript", "-e", script],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return "sent"
        if system == "Windows":
            subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-Command",
                    (
                        "$ws = New-Object -ComObject WScript.Shell; "
                        f"$ws.Popup({json.dumps(message)}, 10, {json.dumps(title)}, 0x40)"
                    ),
                ],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return "sent"
    except Exception as exc:
        logger.warning("Desktop notification failed: %s", exc)
        return f"failed: {type(exc).__name__}"
    return f"skipped: unsupported platform {system or 'unknown'}"


def _apply_shutdown_status(config) -> None:
    request = read_shutdown_request()
    with _status_lock:
        statuses = read_status()
        state = _shutdown_state(config) if request else None
        for name, defn in JOB_DEFS.items():
            if name not in statuses:
                statuses[name] = JobStatus(
                    name=name,
                    description=defn.get("description", name),
                    interval_hours=defn.get("interval_hours", 0),
                )
            statuses[name].shutdown_requested = bool(request)
            statuses[name].shutdown_mode = request.get("mode") if request else None
            statuses[name].shutdown_requested_at = (
                request.get("requested_at") if request else None
            )
            statuses[name].shutdown_state = state
        _write_status(statuses)


def _write_pipeline_pid(pid: int) -> None:
    path = _pipeline_pid_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(pid))


def _read_pipeline_pid() -> int | None:
    path = _pipeline_pid_path()
    if not path.exists():
        return None
    try:
        return int(path.read_text().strip())
    except (OSError, ValueError):
        return None


def _clear_pipeline_pid() -> None:
    try:
        _pipeline_pid_path().unlink(missing_ok=True)
    except OSError:
        pass


def _pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _pipeline_lock_is_held(data_dir: Path) -> bool:
    from telecrime.pipeline.lock import PipelineAlreadyRunningError, pipeline_run_lock

    try:
        data_path = Path(data_dir)
    except TypeError:
        logger.warning("Cannot inspect pipeline lock: invalid data_dir %r", data_dir)
        return False

    try:
        with pipeline_run_lock(data_path):
            return False
    except PipelineAlreadyRunningError:
        return True
    except (OSError, TypeError) as exc:
        logger.warning("Cannot inspect pipeline lock in %s: %s", data_path, exc)
        return False


def _pipeline_stale_seconds() -> int:
    return int(os.environ.get("TELECRIME_PIPELINE_STALE_SECONDS", "1200"))


def _progress_age_seconds(progress: dict[str, object], key: str) -> float | None:
    raw = progress.get(key)
    if not isinstance(raw, str):
        return None
    try:
        updated_at = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return (datetime.now(UTC) - updated_at).total_seconds()


def _terminate_pipeline_process(pid: int, grace_seconds: int = 15) -> str:
    """Terminate a supervised pipeline subprocess."""
    if pid <= 0:
        return "no pipeline pid"

    try:
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        _clear_pipeline_pid()
        return f"pipeline pid {pid} already exited"

    deadline = time.time() + grace_seconds
    while time.time() < deadline:
        if not _pid_is_alive(pid):
            _clear_pipeline_pid()
            return f"terminated pipeline pid {pid}"
        time.sleep(0.5)

    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass

    _clear_pipeline_pid()
    return f"killed pipeline pid {pid}"


def read_status() -> dict[str, JobStatus]:
    """Read scheduler job statuses from the status file."""
    path = _status_path()
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text())
        return {k: JobStatus(**v) for k, v in raw.items()}
    except Exception:
        return {}


def _write_status(statuses: dict[str, JobStatus]) -> None:
    path = _status_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        os.write(fd, json.dumps({k: asdict(v) for k, v in statuses.items()}, default=str).encode())
        os.close(fd)
        os.replace(tmp_path, path)
    except BaseException:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _update_job(name: str, **kwargs) -> None:
    with _status_lock:
        statuses = read_status()
        defn = JOB_DEFS.get(name, {})
        if name not in statuses:
            statuses[name] = JobStatus(
                name=name,
                description=defn.get("description", name),
                interval_hours=defn.get("interval_hours", 0),
            )
        for k, v in kwargs.items():
            setattr(statuses[name], k, v)
        _write_status(statuses)


# ---------------------------------------------------------------------------
# Job implementations
# ---------------------------------------------------------------------------

def _run_pipeline_job(config, engine) -> str:
    """Run the full pipeline in a supervised subprocess.

    Running the pipeline out-of-process lets the watchdog terminate and recover
    a wedged Telegram session or stuck download without restarting the worker.
    """
    del engine  # Unused; kept for call-site compatibility.

    request = read_shutdown_request()
    if request:
        state = _shutdown_state(config)
        return (
            f"skipped, shutdown requested at {request['requested_at']} "
            f"(mode={request['mode']}, state={state})"
        )

    disk_status = _check_disk_status(config)
    if disk_status:
        return f"skipped, {disk_status}"

    cmd = [sys.executable, "-m", "telecrime", "run"]
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    # Save the previous PID so we can restore it if this subprocess turns out
    # to be a "skipped" duplicate (rc=75 — lock already held by another run).
    prev_pid = _read_pipeline_pid()
    proc = subprocess.Popen(
        cmd,
        cwd=Path(__file__).parent.parent,
        env=env,
        start_new_session=True,
        stderr=subprocess.PIPE,
    )
    _write_pipeline_pid(proc.pid)

    # Drain stderr in a background thread so the pipe never blocks. Also tee
    # the lines to data_dir/pipeline_run.log so pipeline logs are inspectable
    # WHILE the run is active — previously they were buffered in memory and
    # only surfaced if the subprocess exited non-zero, which made diagnosing
    # slow/stuck runs (e.g. the 17h download-restart loop) nearly impossible.
    stderr_lines: list[str] = []
    _log_file = config.data_dir / "pipeline_run.log"
    try:
        # Truncate before the subprocess starts: each run starts fresh, and the
        # drain thread below opens append-mode so no first lines are lost.
        _log_file.open("wb").close()
    except Exception:
        _log_file = None

    def _drain_stderr() -> None:
        stderr = getattr(proc, "stderr", None)
        if stderr is None:
            return
        lf = None
        if _log_file is not None:
            try:
                lf = open(_log_file, "ab", buffering=0)
            except Exception:
                lf = None
        try:
            for raw in stderr:
                line = raw.decode(errors="replace").rstrip()
                stderr_lines.append(line)
                if len(stderr_lines) > 200:
                    stderr_lines.pop(0)
                if lf is not None:
                    try:
                        lf.write(raw)
                    except Exception:
                        pass
        finally:
            if lf is not None:
                try:
                    lf.close()
                except Exception:
                    pass

    _stderr_thread = threading.Thread(target=_drain_stderr, daemon=True)
    _stderr_thread.start()

    try:
        while True:
            rc = proc.poll()
            if rc is not None:
                break
            time.sleep(5)
        _stderr_thread.join(timeout=5)
        if rc == 75:  # EX_TEMPFAIL — pipeline lock held by another process
            # Our subprocess was a no-op; restore the real pipeline's PID so
            # the watchdog can still monitor and kill it if needed.
            if prev_pid is not None:
                _write_pipeline_pid(prev_pid)
            else:
                _clear_pipeline_pid()
            return "skipped — another pipeline run is already active"
        if rc != 0:
            tail = "\n".join(stderr_lines[-30:]) if stderr_lines else "(no stderr)"
            logger.error(
                "Pipeline subprocess exited with code %d. Last stderr:\n%s", rc, tail
            )
            raise RuntimeError(f"pipeline subprocess exited with code {rc}")
    finally:
        # Only clear the PID file if it still points to our subprocess.
        # The "skipped" (rc=75) branch above already restored prev_pid.
        if _read_pipeline_pid() == proc.pid:
            _clear_pipeline_pid()

    progress = read_progress() or {}
    creds = int(progress.get("credentials", 0) or 0)
    dups = int(progress.get("duplicates", 0) or 0)
    return f"completed, {creds:,} creds, {dups:,} dups"


def _format_pipeline_progress(progress: dict[str, Any], *, pid: int | None) -> str:
    if not progress:
        return "no progress file"

    running = bool(progress.get("running"))
    state = "running" if running else "stopped"
    stage = progress.get("current_stage") or "none"
    archive = progress.get("current_archive") or "none"
    archive_index = int(progress.get("archive_index") or 0)
    archive_total = int(progress.get("archive_total") or 0)
    credentials = int(progress.get("credentials") or 0)
    duplicates = int(progress.get("duplicates") or 0)
    updated_age = _progress_age_seconds(progress, "updated_at")
    age = f", heartbeat {int(updated_age)}s ago" if updated_age is not None else ""
    pid_text = f", pid {pid}" if pid is not None else ""
    return (
        f"progress: {state}{pid_text}, stage={stage}, archive={archive} "
        f"({archive_index}/{archive_total}), {credentials:,} creds, {duplicates:,} dups{age}"
    )


_DISK_CHECK_EXECUTOR: _futures.ThreadPoolExecutor | None = None
_DISK_CHECK_EXECUTOR_LOCK = threading.Lock()


def _check_disk_status(config, timeout_seconds: float = 4.0) -> str:
    """Check free disk on the data dir.

    Runs statvfs in a worker thread with a timeout: on a wedged data drive
    (D-state dm-crypt), shutil.disk_usage blocks indefinitely — and the
    scheduler runs jobs sequentially in one thread, so the whole job loop
    (health, notify, channel_join, pipeline starts) froze for the entire
    wedge duration. Returns "" on success; a "disk critical" string when
    low; "disk wedged/unavailable" when the check could not complete.
    """
    global _DISK_CHECK_EXECUTOR
    with _DISK_CHECK_EXECUTOR_LOCK:
        if _DISK_CHECK_EXECUTOR is None:
            _DISK_CHECK_EXECUTOR = _futures.ThreadPoolExecutor(max_workers=1)

    def _usage() -> str:
        usage = shutil.disk_usage(config.data_dir)
        free_gb = usage.free / (1024 ** 3)
        total_gb = usage.total / (1024 ** 3)
        pct_used = 100 * (usage.used / usage.total)
        threshold = getattr(config.extraction, "scheduler_min_free_disk_gb", 10.0)
        if free_gb >= threshold:
            return ""
        return (
            f"disk critical: {free_gb:.1f} GB free of "
            f"{total_gb:.0f} GB ({pct_used:.0f}% used)"
        )

    try:
        # NO `with` block: ThreadPoolExecutor.__exit__ calls shutdown(wait=True)
        # which joins the worker — on a wedged drive the statvfs thread is in
        # D-state forever, so the timeout would never fire and the caller would
        # block for the entire wedge anyway. Reuse ONE module-level executor so
        # wedged statvfs threads don't accumulate (each 10-min health check
        # would otherwise leak a thread until APScheduler's pool exhausts).
        result = _DISK_CHECK_EXECUTOR.submit(_usage).result(timeout=timeout_seconds)
        return result
    except _futures.TimeoutError:
        logger.warning(
            "Disk space check timed out (%ss) — data dir likely wedged",
            timeout_seconds,
        )
        return "disk wedged/unavailable"
    except Exception as exc:
        logger.warning("Disk space check failed: %s", exc)
        return ""


def _check_pipeline_health(config) -> PipelineHealth:
    statuses = read_status()
    pipeline_status = statuses.get("pipeline")
    progress = read_progress() or {}
    pid = _read_pipeline_pid()
    lock_held = _pipeline_lock_is_held(config.data_dir)
    stale_threshold = _pipeline_stale_seconds()

    updated_age = _progress_age_seconds(progress, "updated_at")
    meaningful_age = _progress_age_seconds(progress, "last_progress_at")
    runtime_note_kind = progress.get("runtime_note_kind")
    runtime_note_since_age = _progress_age_seconds(progress, "runtime_note_since")
    if meaningful_age is None:
        meaningful_age = updated_age

    reasons: list[str] = []
    # Skip the pid-disappeared check when the lock is held: the exclusive flock
    # on pipeline.lock proves a process is running even if its PID is invisible
    # across Docker container namespaces (e.g. pipeline triggered from web container).
    if pid is not None and not _pid_is_alive(pid) and not lock_held:
        reasons.append(f"pipeline subprocess pid {pid} disappeared")
    if bool(progress.get("running")) and updated_age is not None and updated_age > stale_threshold:
        reasons.append(f"progress heartbeat stale for {int(updated_age)}s")
    if (
        bool(progress.get("running"))
        and meaningful_age is not None
        and meaningful_age > stale_threshold
    ):
        stage = progress.get("current_stage") or "unknown"
        archive = progress.get("current_archive") or "unknown"
        reasons.append(
            f"no meaningful progress for {int(meaningful_age)}s "
            f"(stage={stage}, archive={archive})"
        )
    if (
        bool(progress.get("running"))
        and runtime_note_kind == "telegram_reconnect"
        and runtime_note_since_age is not None
        and runtime_note_since_age > min(stale_threshold, 600)
        # Skip the telegram_reconnect signal when last_progress_at is fresh —
        # the note is stale (was never cleared by adapter._clear_runtime_note)
        # and the pipeline is otherwise making progress (parse/finalize/etc).
        # Without this guard the watchdog keeps trying to kill a healthy
        # subprocess for a note that hasn't been valid for hours.
        # Fail-safe: an UNKNOWN age must not trigger the kill either.
        and meaningful_age is not None
        and meaningful_age > min(stale_threshold, 600)
    ):
        reasons.append(f"telegram reconnect wait exceeded {int(runtime_note_since_age)}s")
    pipeline_started_age = _iso_age_seconds(pipeline_status.last_run) if pipeline_status else None
    pipeline_starting = (
        bool(pipeline_status and pipeline_status.running)
        and pipeline_started_age is not None
        and pipeline_started_age < 120
    )
    if (
        pipeline_status
        and pipeline_status.running
        and not lock_held
        and pid is None
        and not pipeline_starting
    ):
        reasons.append("scheduler marked pipeline running but no lock or pid exists")

    disk_status = _check_disk_status(config)
    if disk_status.startswith("disk wedged"):
        # The data drive is wedged (D-state): the pipeline cannot write
        # progress, so every liveness signal looks stale — killing and
        # restarting into the wedge only churns (reset downloads, fresh run
        # into degraded I/O). Suppress kill reasons until the drive recovers.
        logger.warning(
            "Suppressing pipeline health-kill: %s", disk_status
        )
        reasons = []

    return PipelineHealth(
        reasons=reasons,
        progress_summary=_format_pipeline_progress(progress, pid=pid),
        disk_status=disk_status,
    )


def _run_pipeline_health_job(config, engine) -> str:
    health = _check_pipeline_health(config)
    if health.healthy:
        return health.result()

    result = _recover_stuck_pipeline(config, engine, "; ".join(health.reasons))
    if health.disk_status:
        result = f"{result}; {health.disk_status}"
    return result


def _run_channel_join_job(config, engine, max_joins: int = 1) -> str:
    """Discover channels from DB and join up to max_joins new ones.

    Wraps the Telegram-touching second step so a routine session-file lock
    or brief connection failure is recorded as "skipped" rather than a hard
    job failure (mirrors `_send_telegram_notification`'s tolerance).
    """
    from telecrime.adapters.telegram import TelegramAdapter
    from telecrime.channels.discover import (
        discover_channels_from_db,
        persist_discovery_state,
        save_discovered_channels,
        update_channel_stats,
    )
    from telecrime.channels.service import (
        build_subscription_query,
        mark_channel_join_failed,
        mark_channel_join_result,
    )
    from telecrime.database import get_session

    async def _run():
        # Step 1: discover from DB (no Telegram needed)
        with get_session(engine) as session:
            scan_result = discover_channels_from_db(session)
            new_count, updated_count = save_discovered_channels(session, scan_result.channels)
            persist_discovery_state(session, scan_result)
            _prog = read_progress()
            if _prog and _prog.get("running"):
                logger.debug("update_channel_stats skipped — pipeline running")
            else:
                try:
                    update_channel_stats(session)
                except Exception as e:
                    logger.warning("update_channel_stats failed (DB busy?): %s", e)
                    session.rollback()
            logger.info("Channel discovery: %d new, %d updated", new_count, updated_count)

        # Step 2: join via Telegram (aux session if configured). Swallow
        # transient Telegram/session errors so the job reports "skipped"
        # instead of "failed" — same policy as _send_telegram_notification.
        # Also defer while the pipeline subprocess is live: it holds the main
        # session (a second client on the same session file invalidates both).
        try:
            if read_progress().get("running"):
                return (
                    f"discovered {new_count} new, "
                    f"skipped Telegram step: pipeline running (session in use)"
                )
        except Exception:
            pass
        adapter = TelegramAdapter(config.with_aux_telegram_session())
        joined = skipped = failed = 0
        try:
            try:
                await adapter.connect()
            except Exception as exc:
                if _is_telegram_transient(exc):
                    logger.warning(
                        "channel_join skipped — transient %s: %s",
                        type(exc).__name__, exc,
                    )
                    return (
                        f"discovered {new_count} new, "
                        f"skipped Telegram step: transient {type(exc).__name__}"
                    )
                raise

            with get_session(engine) as session:
                # Verify a batch of channels first (oldest-checked first) so
                # deleted/private channels drop out of the public channel list
                # even if the pipeline rarely runs. Bounded to respect Telegram
                # rate limits. TELECRIME_CHANNEL_CHECK_BATCH overrides.
                import os as _os

                check_batch = int(
                    _os.environ.get("TELECRIME_CHANNEL_CHECK_BATCH", "20")
                )
                if check_batch > 0:
                    from telecrime.channels.service import (
                        mark_channel_check_failed,
                        mark_channel_checked,
                    )
                    from telecrime.models import TelegramChannel

                    to_check = (
                        session.query(TelegramChannel)
                        .filter(
                            TelegramChannel.last_checked.isnot(None),
                            TelegramChannel.is_active.is_(True),
                            TelegramChannel.is_accessible.is_(True),
                            (TelegramChannel.username.isnot(None)) | (TelegramChannel.platform_id.isnot(None)),
                        )
                        .order_by(TelegramChannel.last_checked.asc().nulls_last())
                        .limit(check_batch)
                        .all()
                    )
                    checked_ok = checked_dead = 0
                    for channel in to_check:
                        target = (
                            f"@{channel.username}"
                            if channel.username
                            else channel.platform_id
                        )
                        try:
                            entity = await adapter.get_entity(target)
                            if entity is not None:
                                mark_channel_checked(channel, entity)
                                checked_ok += 1
                            else:
                                mark_channel_check_failed(channel, "Entity not found")
                                checked_dead += 1
                        except Exception as e:
                            mark_channel_check_failed(channel, str(e))
                            checked_dead += 1
                        await asyncio.sleep(1)  # gentle pacing, stay inside rate limits
                    session.commit()
                    if checked_ok or checked_dead:
                        logger.info(
                            "channel_join: verified %d channels (%d ok, %d removed)",
                            len(to_check), checked_ok, checked_dead,
                        )

                candidates = build_subscription_query(session).limit(max_joins).all()

                for channel in candidates:
                    target = channel.username or channel.invite_link
                    try:
                        success = await adapter.join_conversation(
                            channel.platform_id or 0, username=target
                        )
                        if mark_channel_join_result(channel, success) == "joined":
                            joined += 1
                        else:
                            failed += 1
                    except (Exception, asyncio.CancelledError) as e:
                        result = mark_channel_join_failed(channel, str(e))
                        if result == "already":
                            skipped += 1
                        else:
                            failed += 1
                        await asyncio.sleep(2)

                session.commit()

        finally:
            try:
                await adapter.disconnect()
            except Exception:
                pass

        return f"discovered {new_count} new, joined {joined}, skipped {skipped}, failed {failed}"

    return asyncio.run(_run())


def _cancel_parse_competing_queries(engine) -> int:
    """Cancel web-layer COUNT/JOIN queries on parsed_credentials that compete with bulk INSERT.

    During parse, the ops-fragment endpoint runs a COUNT(id) JOIN between
    extraction_jobs and parsed_credentials (165M rows) every 15 seconds.  Each
    query takes 30-60 s and competes with the bulk-INSERT GIN-flush I/O, halving
    parse throughput.  Cancel any such query running longer than 15 s.

    Returns the number of queries cancelled.
    """
    from sqlalchemy import text

    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT pg_cancel_backend(pid) "
                    "FROM pg_stat_activity "
                    "WHERE state = 'active' "
                    "  AND pid != pg_backend_pid() "
                    "  AND query ILIKE '%parsed_credentials%' "
                    "  AND (query ILIKE '%count%' OR query ILIKE '%join%extraction_jobs%') "
                    "  AND now() - query_start > interval '15 seconds'"
                )
            )
            cancelled = sum(1 for row in result if row[0])
        return cancelled
    except Exception as exc:
        logger.debug("Cancel parse competitors failed: %s", exc)
        return 0


def _run_vacuum_job(engine) -> str:
    """Run VACUUM ANALYZE to reclaim space (PostgreSQL).

    Also prunes stale extracted_output rows for CLEANED groups — these are
    kept only until finalize, after which they serve no purpose.
    """
    import sqlalchemy as _sa

    from telecrime.database import get_session
    from telecrime.states import GroupStatus

    # Prune extracted_output rows for already-CLEANED groups.
    # New groups are pruned at finalize time; this catches rows that
    # accumulated before that cleanup was added.
    # NB: the PostgreSQL enum stores the member NAME ("CLEANED"), not the
    # lowercase .value — a raw 'cleaned' literal raises InvalidTextRepresentation.
    with get_session(engine) as session:
        result = session.execute(_sa.text("""
            DELETE FROM extracted_outputs
            WHERE job_id IN (
                SELECT ej.id FROM extraction_jobs ej
                JOIN archive_groups ag ON ag.id = ej.group_id
                WHERE ag.status = :cleaned_status
            )
        """), {"cleaned_status": GroupStatus.CLEANED.name})
        pruned = cast(CursorResult[Any], result).rowcount
        session.commit()

    # VACUUM cannot run inside a transaction block on PostgreSQL, and the DB's
    # statement_timeout (5 min) would cancel a full-table VACUUM ANALYZE on
    # this 240GB database mid-flight (verified: "canceling statement due to
    # statement timeout while vacuuming first_seen_index"). Disable the
    # timeout on this connection only.
    with engine.execution_options(isolation_level="AUTOCOMMIT").connect() as conn:
        conn.execute(_sa.text("SET statement_timeout = 0"))
        conn.execute(_sa.text("VACUUM ANALYZE"))
    return f"VACUUM completed, pruned {pruned:,} stale extracted_output rows"


def _reparse_stealers_impl(engine, limit: int | None = None, dry_run: bool = False) -> str:
    """Backfill stealer_type on ParsedCredential rows where it is NULL.

    For each ExtractionJob that has NULL-stealer_type credentials:
    1. Check the persisted SystemInfoRecord for a stealer_name (no file I/O).
    2. Fallback: try to read SystemInfo.txt if the extracted file still exists.
    3. Detect stealer_type from output filenames.
    4. Bulk UPDATE credentials for that job.

    Args:
        engine: SQLAlchemy engine.
        limit: Max number of ExtractionJobs to process (None = all).
        dry_run: If True, count affected rows without writing.

    Returns:
        Summary string.
    """
    import sqlalchemy as _sa
    from sqlalchemy.orm import selectinload

    from telecrime.database import get_session
    from telecrime.models import ExtractionJob
    from telecrime.models.credential import ParsedCredential
    from telecrime.models.system_info import SystemInfoRecord
    from telecrime.stealer.parser import parse_system_info
    from telecrime.stealer.patterns import detect_stealer_type, is_system_info_file

    updated_total = 0
    jobs_processed = 0

    with get_session(engine) as session:
        # Find distinct jobs that have at least one NULL stealer_type credential
        q = (
            _sa.select(_sa.func.distinct(ParsedCredential.extraction_job_id))
            .where(
                ParsedCredential.stealer_type.is_(None),
                ParsedCredential.extraction_job_id.isnot(None),
            )
        )
        if limit is not None:
            q = q.limit(limit)
        job_ids = list(session.execute(q).scalars())

        if dry_run:
            count = session.execute(
                _sa.select(_sa.func.count(ParsedCredential.id)).where(
                    ParsedCredential.stealer_type.is_(None)
                )
            ).scalar() or 0
            return f"dry-run: {count:,} credentials with NULL stealer_type across {len(job_ids)} jobs"

        # Batch-fetch jobs (with outputs) and SystemInfoRecords to avoid N+1 queries
        jobs = {
            j.id: j
            for j in session.execute(
                _sa.select(ExtractionJob)
                .where(ExtractionJob.id.in_(job_ids))
                .options(selectinload(ExtractionJob.outputs))
            ).scalars()
        }
        sysinfo_map = {
            r.extraction_job_id: r
            for r in session.execute(
                _sa.select(SystemInfoRecord).where(
                    SystemInfoRecord.extraction_job_id.in_(job_ids)
                )
            ).scalars()
        }

        for job_id in job_ids:
            job = jobs.get(job_id)
            if not job:
                continue

            # 1. Check persisted SystemInfoRecord (no file I/O)
            sysinfo_record = sysinfo_map.get(job_id)
            sysinfo_stealer: str | None = sysinfo_record.stealer_name if sysinfo_record else None

            # 2. Fallback: try reading file if still on disk
            all_filenames = [o.output_filename for o in job.outputs]
            if not sysinfo_stealer:
                for output in job.outputs:
                    if is_system_info_file(output.output_filename):
                        p = Path(output.output_path)
                        if p.exists():
                            try:
                                sysinfo = parse_system_info(p.read_text(errors="replace"))
                                sysinfo_stealer = sysinfo.stealer_name
                            except Exception:
                                pass
                        break

            stealer_type = detect_stealer_type(all_filenames, sysinfo_stealer=sysinfo_stealer)
            if not stealer_type:
                continue

            # 3. Bulk UPDATE
            rows = cast(
                CursorResult[Any],
                session.execute(
                    _sa.update(ParsedCredential)
                    .where(
                        ParsedCredential.extraction_job_id == job_id,
                        ParsedCredential.stealer_type.is_(None),
                    )
                    .values(stealer_type=stealer_type)
                ),
            ).rowcount
            updated_total += rows
            jobs_processed += 1
            # Commit every 100 jobs to keep WAL growth bounded and release
            # the write lock so other processes can write between batches.
            if jobs_processed % 100 == 0:
                session.commit()

        session.commit()

    return f"backfilled stealer_type on {updated_total:,} credentials across {jobs_processed} jobs"


def _run_reparse_stealers_job(config, engine) -> str:
    """Scheduler entry point for reparse_stealers job."""
    return _reparse_stealers_impl(engine)


def _run_channel_export_job(config, engine) -> str:
    """Regenerate channels.md + channels.txt in the configured data directory."""
    from telecrime.channels.export import export_reports
    from telecrime.database import get_session

    # Write to the mounted data volume so the host can pick them up.
    # Git commit/push is handled by the host cron script (scripts/update-channels.sh)
    # because the Docker container does not have host git credentials.
    output_dir = config.data_dir
    with get_session(engine) as session:
        md_path, txt_path = export_reports(session, output_dir)

    return f"exported to {output_dir}: {md_path.name}, {txt_path.name}"


def _recover_stuck_pipeline(config, engine, reason: str) -> str:
    """Reset the pipeline to a recoverable state after a stale run."""
    from telecrime.database import get_session
    from telecrime.pipeline.acquire import AcquireStage

    parts: list[str] = [reason]
    pid = _read_pipeline_pid()
    if pid is not None:
        parts.append(_terminate_pipeline_process(pid))
    else:
        _clear_pipeline_pid()

    # If the lock is still held after the kill attempt, the subprocess may be running
    # in a different container (e.g. triggered from web UI — different PID namespace so
    # os.killpg had no effect).  In that case skip marking runs failed and progress
    # stopped, since the process is still alive and healthy.
    if _pipeline_lock_is_held(config.data_dir):
        parts.append("lock still held after kill — subprocess may be in another container; skipping run cleanup")
        logger.warning(
            "Pipeline lock still held after kill attempt (pid=%s). "
            "Subprocess may be running in a different container. "
            "Recovery is incomplete; watchdog will retry next cycle.",
            pid,
        )
        return ", ".join(parts)

    recovered = 0
    with get_session(engine) as session:
        recovered = AcquireStage().recover_stuck_downloads(session, config.downloads_dir)
        stale_runs = _mark_stale_pipeline_runs_failed(session, reason)
    if recovered:
        parts.append(f"reset {recovered} stuck downloads")
    if stale_runs:
        parts.append(f"closed {stale_runs} stale pipeline runs")

    mark_progress_stopped(reason)
    _update_job("pipeline", running=False, last_error=reason, last_result=None)
    return ", ".join(parts)


def _mark_stale_pipeline_runs_failed(session, reason: str, skip_latest: bool = False) -> int:
    from telecrime.models import PipelineRun

    now = datetime.now(UTC)
    # Exclude runs that started in the last 90 seconds — a freshly-spawned
    # pipeline subprocess may have already inserted its run record before the
    # cleanup code runs (APScheduler fires all coalesced jobs concurrently at
    # startup), and we must not mark the new live run as failed.
    cutoff = now - timedelta(seconds=90)
    runs = (
        session.query(PipelineRun)
        .filter(PipelineRun.status == "running")
        .filter(PipelineRun.started_at < cutoff)
        .order_by(PipelineRun.started_at.asc())
        .all()
    )
    # When the pipeline is currently healthy (running), the most recent "running"
    # record is the live run — skip it so we only remove truly stale ones.
    if skip_latest and runs:
        runs = runs[:-1]
    for run in runs:
        run.status = "failed"
        run.finished_at = now
        started_at = run.started_at
        if started_at is not None and started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=UTC)
        run.duration_seconds = (
            int((now - started_at).total_seconds()) if started_at is not None else None
        )
        existing = []
        if run.errors_json:
            try:
                loaded = json.loads(run.errors_json)
                if isinstance(loaded, list):
                    existing = [str(item) for item in loaded]
            except Exception:
                existing = [str(run.errors_json)]
        existing.append(reason)
        run.errors_json = json.dumps(existing)
    session.commit()
    return len(runs)


def repair_stale_runtime_state(config, engine) -> str:
    """Clear stale pipeline status when no supervised process is alive."""
    pid = _read_pipeline_pid()
    lock_held = _pipeline_lock_is_held(config.data_dir)
    pid_alive = pid is not None and _pid_is_alive(pid)

    if pid_alive or lock_held:
        return "pipeline appears active; no runtime repair applied"

    statuses = read_status()
    pipeline_status = statuses.get("pipeline")
    progress = read_progress() or {}
    if not pid and not bool(progress.get("running")) and not (
        pipeline_status and pipeline_status.running
    ):
        return "runtime state already clean"

    reason = "stale pipeline runtime state repaired; no live pid or lock"
    return _recover_stuck_pipeline(config, engine, reason)


# Schema introspection cache — schema is constant for the lifetime of the process.
_soft_hash_col_cache: dict[str, bool] = {}


def _credential_identity_expr(engine):
    import sqlalchemy as _sa

    from telecrime.models.credential import ParsedCredential

    url = str(engine.url)
    if url not in _soft_hash_col_cache:
        try:
            _soft_hash_col_cache[url] = "soft_credential_hash" in {
                col["name"] for col in _sa.inspect(engine).get_columns("parsed_credentials")
            }
        except Exception:
            _soft_hash_col_cache[url] = False
    has_soft_hash = _soft_hash_col_cache[url]

    if not has_soft_hash:
        return _sa.func.coalesce(
            ParsedCredential.credential_hash,
            _sa.cast(ParsedCredential.id, _sa.String),
        )

    return _sa.func.coalesce(
        ParsedCredential.soft_credential_hash,
        ParsedCredential.credential_hash,
        _sa.cast(ParsedCredential.id, _sa.String),
    )


def _count_recent_unique_credentials(engine, hours: int) -> int:
    from sqlalchemy import func

    from telecrime.database import get_session
    from telecrime.models.credential import ParsedCredential

    since = datetime.now(UTC) - timedelta(hours=hours)
    identity = _credential_identity_expr(engine)

    with get_session(engine) as session:
        count = (
            session.query(func.count(func.distinct(identity)))
            .filter(ParsedCredential.created_at >= since)
            .scalar()
        )
    return int(count or 0)


def _watchlist_match_filter(item):
    from telecrime.models.credential import ParsedCredential

    query = f"%{item.query}%"
    if item.match_type == "domain":
        return ParsedCredential.domain.ilike(query)
    if item.match_type == "user":
        return ParsedCredential.username.ilike(query)
    if item.match_type == "url":
        return ParsedCredential.url.ilike(query)
    return (
        ParsedCredential.domain.ilike(query)
        | ParsedCredential.username.ilike(query)
    )


def _credential_identity_value(credential) -> str | int:
    return (
        getattr(credential, "soft_credential_hash", None)
        or getattr(credential, "credential_hash", None)
        or credential.id
    )


def _watchlist_hit_match_values(item, credential) -> list[str]:
    query = (item.query or "").casefold()
    checks: list[tuple[str, object]] = []
    if item.match_type == "domain":
        checks = [("domain", credential.domain)]
    elif item.match_type == "user":
        checks = [("username", credential.username)]
    elif item.match_type == "url":
        checks = [("url", credential.url)]
    else:
        checks = [
            ("domain", credential.domain),
            ("username", credential.username),
            ("email_domain", getattr(credential, "email_domain", None)),
        ]

    matches: list[str] = []
    for field, value in checks:
        if value and query in str(value).casefold():
            matches.append(f"{field}: {value}")
    return matches


def _watchlist_new_hits(session, item, limit: int) -> list[dict[str, object]]:
    from sqlalchemy import text

    from telecrime.models.credential import ParsedCredential

    if limit <= 0:
        return []
    if session.get_bind().dialect.name == "postgresql":
        session.execute(text("SET LOCAL statement_timeout = '5s'"))
        session.execute(text("SET LOCAL max_parallel_workers_per_gather = 0"))

    # Cap the lookback window to 7 days — hits are sample examples, recent ones
    # are more useful, and a bounded window keeps the scan manageable.
    date_filter = None
    if item.last_alerted_at:
        seven_days_ago = datetime.now(UTC) - timedelta(days=7)
        cutoff = max(item.last_alerted_at, seven_days_ago)
        date_filter = ParsedCredential.created_at >= cutoff

    fetch = max(limit * 5, limit)

    def _col_query(col_filter):
        q = session.query(ParsedCredential).filter(col_filter)
        if date_filter is not None:
            q = q.filter(date_filter)
        return (
            q.order_by(ParsedCredential.created_at.desc(), ParsedCredential.id.desc())
            .limit(fetch)
            .all()
        )

    # For single-column match types run one query; for "all" run per-column queries
    # separately so each trgm index is used independently instead of a slow OR merge.
    if item.match_type in ("domain", "user", "url"):
        candidates = _col_query(_watchlist_match_filter(item))
    else:
        query = f"%{item.query}%"
        seen_ids: set[int] = set()
        candidates = []
        for col_filter in (
            ParsedCredential.domain.ilike(query),
            ParsedCredential.username.ilike(query),
            ParsedCredential.email_domain.ilike(query),
        ):
            for row in _col_query(col_filter):
                if row.id not in seen_ids:
                    seen_ids.add(row.id)
                    candidates.append(row)
        # Sort merged candidates so the most recent hits appear first regardless
        # of which column they matched.
        candidates.sort(
            key=lambda c: (c.created_at or datetime.min.replace(tzinfo=UTC), c.id),
            reverse=True,
        )

    hits: list[dict[str, object]] = []
    seen: set[str | int] = set()
    for credential in candidates:
        identity = _credential_identity_value(credential)
        if identity in seen:
            continue
        seen.add(identity)
        hits.append(
            {
                "id": credential.id,
                "matched_values": _watchlist_hit_match_values(item, credential),
                "url": credential.url,
                "domain": credential.domain,
                "username": credential.username,
                "password": credential.password,
                "source_archive": credential.source_archive,
                "source_file": credential.source_file,
                "created_at": credential.created_at.isoformat()
                if credential.created_at
                else None,
            }
        )
        if len(hits) >= limit:
            break
    return hits


def _collect_watchlist_alerts(engine) -> list[dict]:
    from sqlalchemy import text

    from telecrime.database import get_session
    from telecrime.models.credential import ParsedCredential
    from telecrime.models.watchlist import WatchlistItem

    now = datetime.now(UTC)
    alerts: list[dict] = []
    # Load item IDs in a short-lived session to avoid a long-held transaction.
    with get_session(engine) as session:
        item_ids = [
            row[0]
            for row in session.query(WatchlistItem.id)
            .filter(WatchlistItem.enabled == True)
            .order_by(WatchlistItem.id.asc())
            .all()
        ]

    # Process each item in its own session so a rollback on one item
    # (e.g. statement_timeout on a large ILIKE scan) doesn't cascade.
    for item_id in item_ids:
        try:
            with get_session(engine) as session:
                item = session.get(WatchlistItem, item_id)
                if item is None or not item.enabled:
                    continue

                is_pg = session.get_bind().dialect.name == "postgresql"

                if item.last_alerted_at is None:
                    if is_pg:
                        # First run on PostgreSQL: skip the full scan (would time out on
                        # 100M+ rows).  Set last_alerted_at = now so the next run is
                        # incremental.  On SQLite (tests) the full count is fast enough.
                        item.last_alerted_at = now
                        session.commit()
                        continue
                    # First run on SQLite / small DB: full count to catch pre-existing matches.
                    current = (
                        session.query(ParsedCredential)
                        .filter(_watchlist_match_filter(item))
                        .count()
                    )
                    previous = int(item.last_alerted_count or 0)
                    if current > previous:
                        new_matches = current - previous
                        try:
                            hits = _watchlist_new_hits(session, item, min(new_matches, 5))
                        except Exception as hits_exc:
                            logger.warning("Hits fetch timed out for item %d: %s", item.id, hits_exc)
                            session.rollback()
                            hits = []
                        alerts.append(
                            {
                                "id": item.id,
                                "label": item.label,
                                "query": item.query,
                                "new_matches": new_matches,
                                "total_matches": current,
                                "hits": hits,
                            }
                        )
                        item.last_alerted_count = current
                else:
                    # Incremental path: only count credentials created since last alert.
                    # Disable statement_timeout — ILIKE + large date ranges can exceed 5m.
                    if is_pg:
                        session.execute(text("SET LOCAL statement_timeout = 0"))
                    new_matches = (
                        session.query(ParsedCredential)
                        .filter(
                            _watchlist_match_filter(item),
                            ParsedCredential.created_at >= item.last_alerted_at,
                        )
                        .count()
                    )
                    if new_matches > 0:
                        total = int(item.last_alerted_count or 0) + new_matches
                        try:
                            hits = _watchlist_new_hits(session, item, min(new_matches, 5))
                        except Exception as hits_exc:
                            logger.warning("Hits fetch timed out for item %d: %s", item.id, hits_exc)
                            session.rollback()
                            hits = []
                        alerts.append(
                            {
                                "id": item.id,
                                "label": item.label,
                                "query": item.query,
                                "new_matches": new_matches,
                                "total_matches": total,
                                "hits": hits,
                            }
                        )
                        item.last_alerted_count = total
                item.last_alerted_at = now
                session.commit()
        except Exception as exc:
            logger.warning("Watchlist alert check failed for item %d: %s", item_id, exc)

    return alerts


_TELEGRAM_TRANSIENT_MSG_FRAGMENTS = (
    "connection to telegram failed",
    "database is locked",
    "timed out",
    "connection reset",
    "server closed the connection",
    "while disconnected",
)


def _is_telegram_transient(exc: BaseException) -> bool:
    """True when `exc` is a routine Telegram/session-file failure that the
    next interval will recover from on its own — used by all Telegram-using
    scheduler jobs so transient noise doesn't get recorded as a hard failure.
    """
    import sqlite3 as _sqlite3
    if isinstance(exc, (ConnectionError, _sqlite3.OperationalError, asyncio.TimeoutError)):
        return True
    msg = str(exc).lower()
    return any(frag in msg for frag in _TELEGRAM_TRANSIENT_MSG_FRAGMENTS)


def _send_telegram_notification(config, callback) -> str:
    """Send a Telegram notification, swallowing expected transient errors.

    Returns a short status string instead of raising so the APScheduler job
    is recorded as "executed successfully" rather than "failed" for routine
    issues (brief Telegram unreachability, session-file lock contention with
    a parallel job). Persistent failures still surface via the returned status
    and the warning log.
    """
    from telecrime.adapters.telegram import TelegramAdapter
    from telecrime.notify import TelegramNotifier

    async def _run() -> str:
        # The pipeline subprocess holds the main Telegram session for the whole
        # run (downloads, password extraction). Opening a second Telethon
        # client on the same session file mid-run makes Telegram invalidate
        # both connections ("Server replied with a wrong session ID") and can
        # wedge the pipeline's extract stage indefinitely. Defer the
        # notification until the pipeline is idle.
        try:
            if read_progress().get("running"):
                return "skipped: pipeline running (session in use)"
        except Exception:
            pass
        adapter = TelegramAdapter(config.with_aux_telegram_session())
        try:
            await adapter.connect()
            notifier = TelegramNotifier(adapter.client, enabled=True)
            await callback(notifier)
            return "ok"
        except asyncio.CancelledError:
            logger.warning("Telegram notification cancelled (connection drop)")
            return "cancelled (will retry next interval)"
        except Exception as exc:
            if _is_telegram_transient(exc):
                logger.warning(
                    "Telegram notification skipped — transient %s: %s",
                    type(exc).__name__, exc,
                )
                return f"skipped: transient {type(exc).__name__}"
            raise
        finally:
            try:
                await adapter.disconnect()
            except Exception:
                pass

    return asyncio.run(_run())


def _run_watchlist_notify_job(config, engine) -> str:
    alerts = _collect_watchlist_alerts(engine)
    if not alerts:
        return "no new watchlist hits"

    _send_telegram_notification(config, lambda notifier: notifier.watchlist_alerts(alerts))
    new_total = sum(int(alert["new_matches"]) for alert in alerts)
    return f"alerted on {new_total:,} new watchlist matches across {len(alerts)} items"


def _run_summary_job(config, engine, hours: int) -> str:
    count = _count_recent_unique_credentials(engine, hours)
    if hours == 1 and count <= 0:
        return "no new unique credentials in last hour"

    label = "Last hour" if hours == 1 else f"Last {hours} hours"
    _send_telegram_notification(
        config,
        lambda notifier: notifier.activity_summary(label, count),
    )
    return f"sent {label.lower()} summary: {count:,} new unique credentials"


# ---------------------------------------------------------------------------
# Scheduler class
# ---------------------------------------------------------------------------


class TelecrimeWorker:
    """Runs periodic background jobs using APScheduler."""

    def __init__(self, config, engine):
        self.config = config
        self.engine = engine
        self._locks: dict[str, threading.Lock] = {name: threading.Lock() for name in JOB_DEFS}
        # Shared lock so only one job at a time touches the Telethon session file.
        # Both pipeline (ingest) and channel_join open telecrime.session; SQLite
        # cannot handle concurrent writers, causing "database is locked" errors.
        self._telegram_lock = threading.Lock()
        self._scheduler = None
        self._shutdown_requested = threading.Event()
        if read_shutdown_request():
            self._shutdown_requested.set()

    def _has_telegram(self) -> bool:
        return bool(
            getattr(self.config, "telegram", None)
            and self.config.telegram.api_id
            and self.config.telegram.api_hash
        )

    def _is_pipeline_running(self) -> bool:
        """Check if the pipeline job is currently running."""
        lock = self._locks.get("pipeline")
        if lock and not lock.acquire(blocking=False):
            return True
        if lock:
            lock.release()
        return False

    def _any_job_running(self) -> bool:
        for lock in self._locks.values():
            if lock.acquire(blocking=False):
                lock.release()
                continue
            return True
        return False

    def is_shutdown_requested(self) -> bool:
        return self._shutdown_requested.is_set() or bool(read_shutdown_request())

    def request_shutdown(
        self, mode: str = "finish_archive", reason: str | None = None
    ) -> dict[str, str]:
        request = write_shutdown_request(mode=mode, reason=reason)
        self._shutdown_requested.set()
        if self._scheduler is not None:
            try:
                self._scheduler.pause()
            except Exception:
                logger.exception("Failed to pause scheduler during shutdown request")
        _apply_shutdown_status(self.config)
        logger.info(
            "Graceful shutdown requested at %s (mode=%s, reason=%s)",
            request["requested_at"],
            request["mode"],
            reason or "",
        )
        return request

    def clear_shutdown(self) -> None:
        clear_shutdown_request()
        self._shutdown_requested.clear()
        if self._scheduler is not None:
            try:
                self._scheduler.resume()
            except Exception:
                logger.exception("Failed to resume scheduler after clearing shutdown request")
        _apply_shutdown_status(self.config)

    def can_exit(self) -> bool:
        if self._any_job_running():
            return False
        pid = _read_pipeline_pid()
        if pid is not None and _pid_is_alive(pid):
            return False
        if _pipeline_lock_is_held(self.config.data_dir):
            return False
        return True

    def wait_until_drained(
        self,
        poll_seconds: float = 5.0,
        timeout_seconds: float | None = None,
    ) -> bool:
        deadline = time.time() + timeout_seconds if timeout_seconds is not None else None
        while not self.can_exit():
            if deadline is not None and time.time() >= deadline:
                return False
            time.sleep(poll_seconds)
        return True

    def _make_job_func(self, name: str):
        """Return a wrapped job function that handles status tracking."""
        lock = self._locks[name]
        requires_telegram = JOB_DEFS[name]["requires_telegram"]
        # The pipeline itself runs in a subprocess; holding this in-process lock
        # for the entire subprocess would block notifications while only local
        # extraction/parsing is happening. Other Telegram jobs check progress
        # below and only defer while the pipeline is actually using Telegram.
        telegram_lock = self._telegram_lock if requires_telegram and name != "pipeline" else None

        def _job():
            if self.is_shutdown_requested():
                state = _shutdown_state(self.config)
                logger.info("Job %s skipped — shutdown requested (%s)", name, state)
                _update_job(
                    name,
                    running=False,
                    last_result=(
                        "Skipped: shutdown requested"
                        + (f" ({state})" if state else "")
                    ),
                )
                self._refresh_next_run(name)
                return

            if not lock.acquire(blocking=False):
                logger.info("Job %s already running, skipping", name)
                return

            # On SQLite (single-writer), skip heavy jobs while the pipeline holds
            # the write lock. On PostgreSQL, VACUUM ANALYZE is safe during
            # inserts (AUTOCOMMIT) and has been starved for months by this
            # gate — the analyze drift was visible on the live table. Exempt
            # vacuum here; it re-enables on SQLite via the pipeline-lock check
            # inside _run_vacuum_job.
            _heavy_jobs = (
                {
                    "channel_join",
                    "reparse_stealers",
                    "watchlist_notify",
                    "hourly_summary",
                    "daily_summary",
                }
            )
            _blocks = name in _heavy_jobs
            if (
                name not in {"pipeline", "pipeline_watchdog", "pipeline_health"}
                and _blocks
                and (
                    self._is_pipeline_running()
                    or bool((read_progress() or {}).get("running"))
                )
            ):
                lock.release()
                logger.info("Job %s skipped — pipeline is running", name)
                _update_job(name, last_error="Skipped: pipeline running")
                self._refresh_next_run(name)
                return

            if (
                requires_telegram
                and name != "pipeline"
                and not self.config.telegram.aux_session_name
                and self._is_pipeline_running()
            ):
                lock.release()
                logger.info("Job %s skipped — Telegram session busy (pipeline running)", name)
                _update_job(name, last_error="Skipped: Telegram session busy (pipeline running)")
                self._refresh_next_run(name)
                return

            # Serialize Telegram session access — only one job at a time may
            # open telecrime.session to avoid SQLite "database is locked" errors.
            # Use a 60s timeout so channel_join doesn't stall indefinitely while
            # the pipeline is running (it will simply retry next hour).
            if telegram_lock is not None:
                if not telegram_lock.acquire(timeout=60):
                    lock.release()
                    logger.info(
                        "Job %s skipped — Telegram session busy (another Telegram job is running)", name
                    )
                    _update_job(name, last_error="Skipped: Telegram session busy")
                    self._refresh_next_run(name)
                    return

            _update_job(name, running=True, last_run=datetime.now(UTC).isoformat())
            try:
                if name == "pipeline":
                    result = _run_pipeline_job(self.config, self.engine)
                elif name == "pipeline_watchdog":
                    result = self._run_pipeline_watchdog_job()
                elif name == "pipeline_health":
                    result = _run_pipeline_health_job(self.config, self.engine)
                elif name == "vacuum":
                    result = _run_vacuum_job(self.engine)
                elif name == "channel_join":
                    result = _run_channel_join_job(self.config, self.engine)
                elif name == "watchlist_notify":
                    result = _run_watchlist_notify_job(self.config, self.engine)
                elif name == "hourly_summary":
                    result = _run_summary_job(self.config, self.engine, hours=1)
                elif name == "daily_summary":
                    result = _run_summary_job(self.config, self.engine, hours=24)
                elif name == "reparse_stealers":
                    result = _run_reparse_stealers_job(self.config, self.engine)
                elif name == "channel_export":
                    result = _run_channel_export_job(self.config, self.engine)
                else:
                    result = f"unknown job: {name}"
                _update_job(name, running=False, last_result=result, last_error=None)
                logger.info("Job %s done: %s", name, result)
            except (Exception, asyncio.CancelledError) as exc:
                logger.exception("Job %s failed", name)
                _update_job(name, running=False, last_result=None, last_error=str(exc)[:500])
            finally:
                if telegram_lock is not None:
                    telegram_lock.release()
                lock.release()
                # Update next_run from scheduler
                self._refresh_next_run(name)

        return _job

    def _refresh_next_run(self, name: str) -> None:
        if self._scheduler is None:
            return
        job = self._scheduler.get_job(name)
        if job and job.next_run_time:
            _update_job(name, next_run=job.next_run_time.isoformat())

    def _run_pipeline_watchdog_job(self) -> str:
        """Detect and recover stale pipeline runs."""
        if self.is_shutdown_requested():
            state = _shutdown_state(self.config)
            _apply_shutdown_status(self.config)
            return f"shutdown requested ({state or 'requested'})"

        health = _check_pipeline_health(self.config)
        if health.disk_status and self._has_telegram():
            # Telethon holds the SQLite session file locked for the subprocess's entire
            # lifetime (not just during active downloads).  Without an aux session,
            # opening a second client while the pipeline runs causes "database is locked".
            has_aux = bool(
                getattr(self.config, "telegram", None)
                and self.config.telegram.aux_session_name
            )
            session_busy = has_aux is False and self._is_pipeline_running()
            if not session_busy:
                try:
                    _send_telegram_notification(
                        self.config,
                        lambda n, msg=health.disk_status: n.send(
                            (
                                f"⚠️ Data drive status: {msg}. "
                                "Pipeline may stall."
                                if msg.startswith("disk wedged")
                                else f"Disk space critical: {msg}. Pipeline may stall."
                            ),
                            force=True,
                        ),
                    )
                except Exception as exc:
                    logger.warning("Disk alert notification failed: %s", exc)
            else:
                logger.warning(
                    "Disk critical (%s) — skipping Telegram alert: pipeline holds session "
                    "(configure aux_session_name to enable concurrent notifications)",
                    health.disk_status,
                )

        # Sweep stale "running" run records left by previous killed/crashed runs.
        # When the pipeline IS healthy, preserve the most recent "running" record
        # (that's the current live run). When NOT healthy, all "running" records
        # are stale and can be marked failed.
        from telecrime.database import get_session
        with get_session(self.engine) as _sess:
            stale = _mark_stale_pipeline_runs_failed(
                _sess, "process no longer alive", skip_latest=health.healthy
            )
            if stale:
                logger.info("Watchdog cleaned up %d stale pipeline run record(s)", stale)

        if health.healthy:
            # Cancel any web-layer COUNT queries competing with bulk INSERT I/O.
            # These run every 15s from the ops-fragment HTMX poll and stall parse
            # throughput when they overlap with GIN flushes on the 165M-row table.
            progress = read_progress() or {}
            if progress.get("current_stage") == "parse":
                cancelled = _cancel_parse_competing_queries(self.engine)
                if cancelled:
                    logger.info("Watchdog cancelled %d query(s) competing with parse", cancelled)
            return health.result()

        result = _recover_stuck_pipeline(self.config, self.engine, "; ".join(health.reasons))
        logger.warning("Pipeline watchdog recovered stale run: %s", result)
        restarted = self.run_now("pipeline")
        if restarted:
            result = f"{result}, scheduled immediate rerun"
        if health.disk_status:
            result = f"{result}; {health.disk_status}"
        return result

    def start(self) -> None:
        from apscheduler.schedulers.background import BackgroundScheduler

        # PIDs written by a previous worker container refer to that container's
        # PID namespace. After a container restart they are meaningless (a PID
        # may even alias an unrelated process), so the watchdog could see a
        # healthy newly spawned pipeline as "disappeared" and kill it. Drop the
        # stale PID before the watchdog runs; the pipeline job rewrites it on
        # the next spawn.
        _clear_pipeline_pid()

        self._scheduler = BackgroundScheduler()
        has_tg = self._has_telegram()

        for name, defn in JOB_DEFS.items():
            _update_job(
                name,
                description=defn["description"],
                interval_hours=defn["interval_hours"],
                running=False,
                last_error=None,
            )
        _apply_shutdown_status(self.config)

        for name, defn in JOB_DEFS.items():
            if defn["requires_telegram"] and not has_tg:
                logger.warning("Skipping job %s: Telegram credentials not configured", name)
                _update_job(
                    name,
                    last_error="Telegram credentials not configured — job disabled",
                    next_run=None,
                )
                continue

            hours = defn["interval_hours"]
            minutes = defn.get("interval_minutes")
            startup_delay = defn.get("startup_delay_seconds", 0)
            first_run = datetime.now(UTC) + timedelta(seconds=startup_delay)
            self._scheduler.add_job(
                self._make_job_func(name),
                trigger="interval",
                hours=hours if minutes is None else 0,
                minutes=minutes if minutes is not None else 0,
                id=name,
                replace_existing=True,
                max_instances=1,
                next_run_time=first_run,
            )
            if minutes is not None:
                logger.info("Scheduled job %s every %dm", name, minutes)
            else:
                logger.info("Scheduled job %s every %dh", name, hours)

        self._scheduler.start()

        # Only pause the scheduler if THIS process already requested a shutdown
        # (i.e. _shutdown_requested was set in-process before start() was called).
        # Do NOT check read_shutdown_request() here — that file may be stale from
        # a previous process and will be cleared by cli.py's startup code.
        if self._shutdown_requested.is_set():
            try:
                self._scheduler.pause()
            except Exception:
                logger.exception("Failed to pause scheduler on startup")

        # Persist initial next_run times
        for name in JOB_DEFS:
            self._refresh_next_run(name)
        _apply_shutdown_status(self.config)

        logger.info("TelecrimeWorker started")

    def stop(self) -> None:
        _apply_shutdown_status(self.config)
        if self._scheduler:
            self._scheduler.shutdown(wait=False)

    def run_now(self, job_name: str) -> bool:
        """Trigger a job to run immediately. Returns False if job not found."""
        if self._scheduler is None:
            return False
        job = self._scheduler.get_job(job_name)
        if job is None:
            return False
        job.modify(next_run_time=datetime.now(UTC))
        return True
