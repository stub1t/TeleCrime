"""Tests for telecrime.scheduler module."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from telecrime.scheduler import (
    JOB_DEFS,
    JobStatus,
    TelecrimeWorker,
    _check_pipeline_health,
    _clear_pipeline_pid,
    _collect_watchlist_alerts,
    _count_recent_unique_credentials,
    _mark_stale_pipeline_runs_failed,
    _pipeline_pid_path,
    _progress_age_seconds,
    _run_pipeline_health_job,
    _run_pipeline_job,
    _send_telegram_notification,
    _shutdown_request_path,
    _shutdown_state,
    _status_path,
    _update_job,
    _write_status,
    clear_shutdown_request,
    read_shutdown_request,
    read_status,
    send_desktop_notification,
    write_shutdown_request,
)


@pytest.fixture
def status_file(tmp_path):
    """Patch the status file path to a temp file."""
    status_path = tmp_path / "scheduler_status.json"
    with patch("telecrime.scheduler._status_path", return_value=status_path):
        yield status_path


def test_job_defs_have_required_keys():
    for name, defn in JOB_DEFS.items():
        assert "description" in defn
        assert "interval_hours" in defn
        assert "requires_telegram" in defn
        assert defn["interval_hours"] > 0


def test_read_status_missing_file(status_file):
    result = read_status()
    assert result == {}


def test_write_and_read_status(status_file):
    statuses = {
        "pipeline": JobStatus(
            name="pipeline",
            description="test",
            interval_hours=4,
            last_result="ok",
        )
    }
    _write_status(statuses)
    loaded = read_status()
    assert "pipeline" in loaded
    assert loaded["pipeline"].last_result == "ok"
    assert loaded["pipeline"].interval_hours == 4


def test_update_job_creates_entry(status_file):
    _update_job("pipeline", last_result="run completed")
    statuses = read_status()
    assert "pipeline" in statuses
    assert statuses["pipeline"].last_result == "run completed"
    assert statuses["pipeline"].running is False


def test_update_job_updates_existing(status_file):
    _update_job("pipeline", last_result="5 creds")
    _update_job("pipeline", running=True)
    statuses = read_status()
    assert statuses["pipeline"].last_result == "5 creds"
    assert statuses["pipeline"].running is True


def test_write_status_atomic(status_file):
    """Writes via tmp file so partial writes don't corrupt."""
    statuses = {"pipeline": JobStatus(name="pipeline", description="d", interval_hours=4)}
    _write_status(statuses)
    # .tmp file should not exist after write
    assert not status_file.with_suffix(".tmp").exists()
    assert status_file.exists()


def test_runtime_files_follow_configured_data_dir(tmp_path, monkeypatch):
    data_dir = tmp_path / "runtime"
    monkeypatch.setenv("TELECRIME_DATA_DIR", str(data_dir))
    monkeypatch.delenv("TELECRIME_STATUS_FILE", raising=False)
    monkeypatch.delenv("TELECRIME_PIPELINE_PID_FILE", raising=False)
    monkeypatch.delenv("TELECRIME_SHUTDOWN_REQUEST_FILE", raising=False)

    assert _status_path() == data_dir / "scheduler_status.json"
    assert _pipeline_pid_path() == data_dir / "pipeline.pid"
    assert _shutdown_request_path() == data_dir / "pipeline_shutdown_request.json"


def test_worker_skips_telegram_jobs_when_no_creds(status_file):
    """When no Telegram creds are configured, only non-Telegram jobs are scheduled."""
    config = MagicMock()
    config.telegram.api_id = None
    config.telegram.api_hash = None
    engine = MagicMock()

    worker = TelecrimeWorker(config, engine)

    with patch("apscheduler.schedulers.background.BackgroundScheduler") as MockSched:
        mock_sched = MagicMock()
        MockSched.return_value = mock_sched
        mock_sched.get_job.return_value = None

        worker.start()

        # Only vacuum (requires_telegram=False) should be scheduled
        non_tg_jobs = [n for n, d in JOB_DEFS.items() if not d["requires_telegram"]]
        assert mock_sched.add_job.call_count == len(non_tg_jobs)

        worker.stop()


def test_worker_with_telegram_creds_schedules_all_jobs(status_file):
    """When Telegram creds are present, all jobs are scheduled."""
    config = MagicMock()
    config.telegram.api_id = "12345"
    config.telegram.api_hash = "abc"
    engine = MagicMock()

    worker = TelecrimeWorker(config, engine)

    with patch("apscheduler.schedulers.background.BackgroundScheduler") as MockSched:
        mock_sched = MagicMock()
        MockSched.return_value = mock_sched
        mock_sched.get_job.return_value = None

        worker.start()

        assert mock_sched.add_job.call_count == len(JOB_DEFS)
        worker.stop()


def test_worker_run_now_returns_false_when_not_started(status_file):
    config = MagicMock()
    engine = MagicMock()
    worker = TelecrimeWorker(config, engine)
    assert worker.run_now("pipeline") is False


def test_worker_run_now_returns_false_for_unknown_job(status_file):
    config = MagicMock()
    config.telegram.api_id = "x"
    config.telegram.api_hash = "y"
    engine = MagicMock()
    worker = TelecrimeWorker(config, engine)

    with patch("apscheduler.schedulers.background.BackgroundScheduler") as MockSched:
        mock_sched = MagicMock()
        MockSched.return_value = mock_sched
        mock_sched.get_job.return_value = None
        worker.start()
        assert worker.run_now("nonexistent_job") is False
        worker.stop()


def test_vacuum_job_executes_vacuum(pg_engine):
    """_run_vacuum_job runs the cleanup DELETE and VACUUM ANALYZE on PG."""
    from telecrime.scheduler import _run_vacuum_job

    result = _run_vacuum_job(pg_engine)

    assert "VACUUM" in result
    assert "completed" in result.lower() or "done" in result.lower()


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("reparse_stealers", {"requires_telegram": False, "interval_hours": 24}),
        ("pipeline_watchdog", {"requires_telegram": False, "interval_minutes": 10}),
        ("pipeline_health", {"requires_telegram": False, "interval_hours": 2}),
        ("watchlist_notify", {"requires_telegram": True, "interval_minutes": 15}),
        ("hourly_summary", {"requires_telegram": True, "interval_hours": 1}),
        ("daily_summary", {"requires_telegram": True, "interval_hours": 24}),
    ],
)
def test_job_defs_registers_background_jobs(name, expected):
    """Background jobs expose the scheduler properties they require."""
    assert name in JOB_DEFS
    for key, value in expected.items():
        assert JOB_DEFS[name][key] == value


def test_progress_age_seconds_handles_valid_and_invalid_values():
    assert _progress_age_seconds({"updated_at": "not-a-date"}, "updated_at") is None
    age = _progress_age_seconds(
        {"updated_at": "2026-04-20T20:00:00+00:00"},
        "updated_at",
    )
    assert age is not None


def test_check_pipeline_health_reports_progress(status_file, tmp_path, monkeypatch):
    config = MagicMock()
    config.data_dir = tmp_path

    monkeypatch.setattr("telecrime.scheduler._read_pipeline_pid", lambda: None)
    monkeypatch.setattr("telecrime.scheduler._pipeline_lock_is_held", lambda _data_dir: False)
    monkeypatch.setattr("telecrime.scheduler._check_disk_status", lambda _config: "")
    monkeypatch.setattr(
        "telecrime.scheduler.read_progress",
        lambda: {
            "running": True,
            "current_stage": "parse",
            "current_archive": "logs.zip",
            "archive_index": 2,
            "archive_total": 5,
            "credentials": 10,
            "duplicates": 1,
        },
    )

    health = _check_pipeline_health(config)

    assert health.healthy is True
    assert "stage=parse" in health.progress_summary
    assert "logs.zip" in health.progress_summary


def test_check_pipeline_health_allows_startup_grace(status_file, tmp_path, monkeypatch):
    config = MagicMock()
    config.data_dir = tmp_path
    _write_status(
        {
            "pipeline": JobStatus(
                name="pipeline",
                description="test",
                interval_hours=4,
                last_run=datetime.now(UTC).isoformat(),
                running=True,
            )
        }
    )

    monkeypatch.setattr("telecrime.scheduler._read_pipeline_pid", lambda: None)
    monkeypatch.setattr("telecrime.scheduler._pipeline_lock_is_held", lambda _data_dir: False)
    monkeypatch.setattr("telecrime.scheduler._check_disk_status", lambda _config: "")
    monkeypatch.setattr("telecrime.scheduler.read_progress", lambda: {"running": False})

    health = _check_pipeline_health(config)

    assert health.healthy is True


def test_run_pipeline_health_job_recovers_unhealthy_pipeline(
    status_file, tmp_path, monkeypatch
):
    config = MagicMock()
    config.data_dir = tmp_path
    config.downloads_dir = tmp_path / "downloads"
    config.downloads_dir.mkdir()

    monkeypatch.setattr("telecrime.scheduler._read_pipeline_pid", lambda: None)
    monkeypatch.setattr("telecrime.scheduler._pipeline_lock_is_held", lambda _data_dir: False)
    monkeypatch.setattr("telecrime.scheduler._check_disk_status", lambda _config: "")
    monkeypatch.setattr("telecrime.scheduler._pipeline_stale_seconds", lambda: 60)
    monkeypatch.setattr(
        "telecrime.scheduler.read_progress",
        lambda: {
            "running": True,
            "updated_at": "2026-04-20T20:00:00+00:00",
            "current_stage": "parse",
            "current_archive": "logs.zip",
        },
    )
    recover = MagicMock(return_value="recovered stale run")
    monkeypatch.setattr("telecrime.scheduler._recover_stuck_pipeline", recover)

    result = _run_pipeline_health_job(config, MagicMock())

    assert result == "recovered stale run"
    recover.assert_called_once()


def test_mark_stale_pipeline_runs_failed(pg_engine):
    from datetime import datetime

    from telecrime.database import get_session
    from telecrime.models import PipelineRun

    with get_session(pg_engine) as session:
        session.add(
            PipelineRun(
                mode="sequential",
                status="running",
                dry_run=0,
                started_at=datetime(2026, 5, 1, tzinfo=UTC),
            )
        )
        session.commit()

    with get_session(pg_engine) as session:
        count = _mark_stale_pipeline_runs_failed(session, "stale heartbeat")

    assert count == 1
    with get_session(pg_engine) as session:
        run = session.query(PipelineRun).one()
        assert run.status == "failed"
        assert run.finished_at is not None
        assert run.duration_seconds is not None
        assert "stale heartbeat" in (run.errors_json or "")


def test_run_pipeline_job_uses_subprocess(tmp_path, monkeypatch):
    class DummyProc:
        pid = 43210

        def __init__(self):
            self._polls = 0

        def poll(self):
            self._polls += 1
            return 0 if self._polls > 1 else None

    dummy = DummyProc()
    monkeypatch.setenv(
        "TELECRIME_SHUTDOWN_REQUEST_FILE", str(tmp_path / "pipeline_shutdown_request.json")
    )
    monkeypatch.setenv("TELECRIME_PIPELINE_PID_FILE", str(tmp_path / "pipeline.pid"))
    monkeypatch.setattr("subprocess.Popen", lambda *args, **kwargs: dummy)
    monkeypatch.setattr("time.sleep", lambda _: None)
    monkeypatch.setattr("telecrime.scheduler._check_disk_status", lambda _config: "")
    monkeypatch.setattr(
        "telecrime.scheduler.read_progress",
        lambda: {"credentials": 12, "duplicates": 3},
    )

    result = _run_pipeline_job(MagicMock(), MagicMock())

    assert "12 creds" in result
    assert not _pipeline_pid_path().exists()
    _clear_pipeline_pid()


def test_shutdown_request_round_trip(tmp_path, monkeypatch):
    monkeypatch.setenv(
        "TELECRIME_SHUTDOWN_REQUEST_FILE", str(tmp_path / "pipeline_shutdown_request.json")
    )

    request = write_shutdown_request(reason="maintenance")
    loaded = read_shutdown_request()

    assert loaded is not None
    assert loaded["mode"] == "finish_archive"
    assert loaded["reason"] == "maintenance"
    assert loaded["requested_at"] == request["requested_at"]

    clear_shutdown_request()
    assert read_shutdown_request() is None


def test_run_pipeline_job_skips_when_shutdown_requested(tmp_path, monkeypatch):
    monkeypatch.setenv(
        "TELECRIME_SHUTDOWN_REQUEST_FILE", str(tmp_path / "pipeline_shutdown_request.json")
    )
    write_shutdown_request(reason="drain")

    config = MagicMock()
    config.data_dir = tmp_path / "data"
    config.data_dir.mkdir()

    result = _run_pipeline_job(config, MagicMock())

    assert "shutdown requested" in result
    assert "finish_archive" in result


def test_run_pipeline_job_skips_when_disk_critical(tmp_path, monkeypatch):
    monkeypatch.setenv(
        "TELECRIME_SHUTDOWN_REQUEST_FILE", str(tmp_path / "pipeline_shutdown_request.json")
    )
    config = MagicMock()
    config.data_dir = tmp_path / "data"
    config.data_dir.mkdir()

    popen = MagicMock()
    monkeypatch.setattr("telecrime.scheduler._check_disk_status", lambda _config: "disk critical: 9.0 GB free")
    monkeypatch.setattr("subprocess.Popen", popen)

    result = _run_pipeline_job(config, MagicMock())

    assert result == "skipped, disk critical: 9.0 GB free"
    popen.assert_not_called()


def test_shutdown_state_reports_drained_when_request_exists(tmp_path, monkeypatch):
    monkeypatch.setenv(
        "TELECRIME_SHUTDOWN_REQUEST_FILE", str(tmp_path / "pipeline_shutdown_request.json")
    )
    write_shutdown_request(reason="maintenance")

    config = MagicMock()
    config.data_dir = tmp_path / "data"
    config.data_dir.mkdir()

    monkeypatch.setattr("telecrime.scheduler._read_pipeline_pid", lambda: None)
    monkeypatch.setattr("telecrime.scheduler._pipeline_lock_is_held", lambda _data_dir: False)
    monkeypatch.setattr("telecrime.scheduler.read_progress", lambda: {"running": False})

    assert _shutdown_state(config) == "drained"


def test_desktop_notification_uses_notify_send_on_linux(monkeypatch):
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append((cmd, kwargs))
        return MagicMock(returncode=0)

    monkeypatch.setattr("platform.system", lambda: "Linux")
    monkeypatch.setattr("shutil.which", lambda name: "/usr/bin/notify-send")
    monkeypatch.setattr("subprocess.run", fake_run)

    result = send_desktop_notification("Done", "Pipeline stopped")

    assert result == "sent"
    assert calls[0][0] == ["notify-send", "Done", "Pipeline stopped"]


def test_desktop_notification_skips_when_notify_send_missing(monkeypatch):
    monkeypatch.setattr("platform.system", lambda: "Linux")
    monkeypatch.setattr("shutil.which", lambda name: None)

    result = send_desktop_notification("Done", "Pipeline stopped")

    assert result == "skipped: notify-send not found"


def test_worker_start_pauses_scheduler_when_shutdown_requested(status_file, tmp_path, monkeypatch):
    monkeypatch.setenv(
        "TELECRIME_SHUTDOWN_REQUEST_FILE", str(tmp_path / "pipeline_shutdown_request.json")
    )
    write_shutdown_request(reason="maintenance")

    config = MagicMock()
    config.telegram.api_id = "12345"
    config.telegram.api_hash = "abc"
    config.data_dir = tmp_path / "data"
    config.data_dir.mkdir()
    engine = MagicMock()

    worker = TelecrimeWorker(config, engine)

    with patch("apscheduler.schedulers.background.BackgroundScheduler") as MockSched:
        mock_sched = MagicMock()
        MockSched.return_value = mock_sched
        mock_sched.get_job.return_value = None

        worker.start()

        mock_sched.pause.assert_called_once()
        statuses = read_status()
        assert statuses["pipeline"].shutdown_requested is True
        assert statuses["pipeline"].shutdown_state in {"draining", "drained"}


def test_reparse_stealers_impl_dry_run(pg_engine):
    """_reparse_stealers_impl dry_run returns count string without writing."""
    from telecrime.database import get_session
    from telecrime.models import ExtractionJob
    from telecrime.models.archive_group import ArchiveGroup
    from telecrime.models.credential import ParsedCredential
    from telecrime.scheduler import _reparse_stealers_impl
    from telecrime.states import ExtractionStatus, GroupStatus

    with get_session(pg_engine) as session:
        group = ArchiveGroup(
            fingerprint="test.zip",
            base_name="test.zip",
            status=GroupStatus.INCOMPLETE,
            expected_part_count=1,
            detected_part_count=1,
        )
        session.add(group)
        session.flush()
        job = ExtractionJob(group_id=group.id, status=ExtractionStatus.COMPLETED)
        session.add(job)
        session.flush()
        # Add credentials with NULL stealer_type
        for i in range(3):
            session.add(ParsedCredential(
                url=f"https://example{i}.com/login",
                domain=f"example{i}.com",
                username=f"user{i}",
                password="pass",
                extraction_job_id=job.id,
                credential_hash=ParsedCredential.compute_hash(f"example{i}.com", f"user{i}", "pass"),
            ))
        session.commit()

    result = _reparse_stealers_impl(pg_engine, dry_run=True)
    assert "dry-run" in result
    assert "3" in result

    # Verify no stealer_type was written
    with get_session(pg_engine) as session:
        nulls = session.query(ParsedCredential).filter(
            ParsedCredential.stealer_type.is_(None)
        ).count()
    assert nulls == 3


def test_reparse_stealers_impl_backfills(pg_engine):
    """_reparse_stealers_impl updates stealer_type for jobs with known filenames."""
    from telecrime.database import get_session
    from telecrime.models import ExtractionJob
    from telecrime.models.archive_group import ArchiveGroup
    from telecrime.models.credential import ParsedCredential
    from telecrime.models.extraction import ExtractedOutput
    from telecrime.scheduler import _reparse_stealers_impl
    from telecrime.states import ExtractionStatus, GroupStatus

    with get_session(pg_engine) as session:
        group = ArchiveGroup(
            fingerprint="redline.zip",
            base_name="redline.zip",
            status=GroupStatus.INCOMPLETE,
            expected_part_count=1,
            detected_part_count=1,
        )
        session.add(group)
        session.flush()
        job = ExtractionJob(group_id=group.id, status=ExtractionStatus.COMPLETED)
        session.add(job)
        session.flush()
        # Add a RedLine signature file
        session.add(ExtractedOutput(
            job_id=job.id,
            output_path="/tmp/DomainDetects.txt",
            output_filename="DomainDetects.txt",
            output_hash="abc123",
        ))
        # Add a credential with NULL stealer_type
        session.add(ParsedCredential(
            url="https://example.com/login",
            domain="example.com",
            username="user",
            password="pass",
            extraction_job_id=job.id,
            credential_hash=ParsedCredential.compute_hash("example.com", "user", "pass"),
        ))
        session.commit()

    result = _reparse_stealers_impl(pg_engine)
    assert "1" in result  # 1 credential updated

    with get_session(pg_engine) as session:
        cred = session.query(ParsedCredential).first()
    assert cred.stealer_type == "redline"


def test_count_recent_unique_credentials_uses_soft_dedup(pg_engine):
    from telecrime.database import get_session
    from telecrime.models.credential import ParsedCredential

    with get_session(pg_engine) as session:
        session.add_all(
            [
                ParsedCredential(
                    url="https://a.example/login",
                    domain="a.example",
                    username="alice",
                    password="secret",
                    credential_hash="hard-1",
                    soft_credential_hash="soft-1",
                ),
                ParsedCredential(
                    url="https://b.example/login",
                    domain="b.example",
                    username="alice",
                    password="secret",
                    credential_hash="hard-2",
                    soft_credential_hash="soft-1",
                ),
            ]
        )
        session.commit()

    assert _count_recent_unique_credentials(pg_engine, hours=24) == 1


def test_collect_watchlist_alerts_only_returns_new_matches(pg_engine):
    """Incremental watchlist alerts cover only rows created after the watermark.

    On PostgreSQL the very first run deliberately seeds last_alerted_at without
    scanning (a full-table scan would time out on production-sized data), so
    this test seeds the watermark itself and asserts on the incremental path.
    """
    from datetime import timedelta

    from telecrime.database import get_session
    from telecrime.models.credential import ParsedCredential
    from telecrime.models.watchlist import WatchlistItem

    watermarked_at = datetime.now(UTC) - timedelta(hours=2)

    with get_session(pg_engine) as session:
        session.add(
            WatchlistItem(
                label="Example",
                query="example.com",
                match_type="domain",
                enabled=True,
                last_alerted_count=1,
                last_alerted_at=watermarked_at,
            )
        )
        session.add_all(
            [
                ParsedCredential(
                    url="https://example.com/login",
                    domain="example.com",
                    username="alice",
                    password="secret",
                    credential_hash="hash-1",
                    created_at=watermarked_at - timedelta(minutes=30),
                ),
                ParsedCredential(
                    url="https://example.com/portal",
                    domain="example.com",
                    username="bob",
                    password="secret",
                    credential_hash="hash-2",
                ),
            ]
        )
        session.commit()

    alerts = _collect_watchlist_alerts(pg_engine)

    assert len(alerts) == 1
    assert alerts[0]["new_matches"] == 1
    assert alerts[0]["total_matches"] == 2
    assert len(alerts[0]["hits"]) == 1
    assert alerts[0]["hits"][0]["matched_values"] == ["domain: example.com"]
    assert alerts[0]["hits"][0]["username"] == "bob"
    assert alerts[0]["hits"][0]["password"] == "secret"

    with get_session(pg_engine) as session:
        item = session.query(WatchlistItem).one()
        assert item.last_alerted_count == 2
        assert item.last_alerted_at is not None
        assert item.last_alerted_at.replace(tzinfo=UTC) > watermarked_at


def test_check_disk_status_uses_config_threshold(tmp_path, monkeypatch):
    """Disk check respects the configurable scheduler threshold."""
    from telecrime.scheduler import _check_disk_status

    config = MagicMock()
    config.data_dir = tmp_path

    # 15 GB free, default threshold is 10 GB → should pass
    config.extraction = MagicMock()
    config.extraction.scheduler_min_free_disk_gb = 10.0
    monkeypatch.setattr(
        "telecrime.scheduler.shutil.disk_usage",
        lambda _path: MagicMock(free=15 * 1024 ** 3, total=100 * 1024 ** 3, used=85 * 1024 ** 3),
    )
    assert _check_disk_status(config) == ""

    # 15 GB free, threshold raised to 20 GB → should fail
    config.extraction.scheduler_min_free_disk_gb = 20.0
    result = _check_disk_status(config)
    assert "disk critical" in result
    assert "15.0 GB free" in result

    # 8 GB free, threshold is 10 GB → should fail
    config.extraction.scheduler_min_free_disk_gb = 10.0
    monkeypatch.setattr(
        "telecrime.scheduler.shutil.disk_usage",
        lambda _path: MagicMock(free=8 * 1024 ** 3, total=100 * 1024 ** 3, used=92 * 1024 ** 3),
    )
    result = _check_disk_status(config)
    assert "disk critical" in result
    assert "8.0 GB free" in result


def test_check_disk_status_handles_exception(monkeypatch):
    """Disk check returns empty string when disk_usage raises."""
    from telecrime.scheduler import _check_disk_status

    config = MagicMock()
    config.data_dir = Path("/nonexistent")
    monkeypatch.setattr(
        "telecrime.scheduler.shutil.disk_usage",
        lambda _path: (_ for _ in ()).throw(OSError("boom")),
    )
    assert _check_disk_status(config) == ""


# ---------------------------------------------------------------------------
# _send_telegram_notification transient-error tolerance
# ---------------------------------------------------------------------------


def _make_config_with_telegram():
    cfg = MagicMock()
    cfg.with_aux_telegram_session.return_value = cfg
    cfg.telegram.api_id = 1
    cfg.telegram.api_hash = "abc"
    cfg.telegram.session_name = "test"
    return cfg


def test_send_telegram_notification_swallows_connection_error():
    """A Telegram ConnectionError is transient — return "skipped", don't raise."""
    import asyncio
    cfg = _make_config_with_telegram()

    with patch("telecrime.adapters.telegram.TelegramAdapter") as MockAdapter:
        instance = MockAdapter.return_value
        async def _conn():
            raise ConnectionError("Connection to Telegram failed 5 time(s)")
        instance.connect = _conn
        async def _disconn():
            return None
        instance.disconnect = _disconn

        result = _send_telegram_notification(cfg, lambda n: asyncio.sleep(0))

    assert "transient" in result.lower() or "skipped" in result.lower()


def test_send_telegram_notification_swallows_session_lock():
    """Telethon session-file lock is transient — return "skipped", don't raise."""
    import asyncio
    import sqlite3
    cfg = _make_config_with_telegram()

    with patch("telecrime.adapters.telegram.TelegramAdapter") as MockAdapter:
        instance = MockAdapter.return_value
        async def _conn():
            raise sqlite3.OperationalError("database is locked")
        instance.connect = _conn
        async def _disconn():
            return None
        instance.disconnect = _disconn

        result = _send_telegram_notification(cfg, lambda n: asyncio.sleep(0))

    assert "transient" in result.lower() or "skipped" in result.lower()


def test_send_telegram_notification_propagates_unexpected_errors():
    """Non-transient errors (e.g. logic bug in callback) still raise."""
    cfg = _make_config_with_telegram()

    with patch("telecrime.adapters.telegram.TelegramAdapter") as MockAdapter:
        instance = MockAdapter.return_value
        async def _conn():
            return None
        instance.connect = _conn
        async def _disconn():
            return None
        instance.disconnect = _disconn
        instance.client = MagicMock()

        async def _bad_callback(notifier):
            raise ValueError("logic bug in caller")

        with pytest.raises(ValueError, match="logic bug"):
            _send_telegram_notification(cfg, _bad_callback)


def test_send_telegram_notification_returns_ok_on_success():
    cfg = _make_config_with_telegram()

    with patch("telecrime.adapters.telegram.TelegramAdapter") as MockAdapter:
        instance = MockAdapter.return_value
        async def _conn():
            return None
        instance.connect = _conn
        async def _disconn():
            return None
        instance.disconnect = _disconn
        instance.client = MagicMock()

        called = []
        async def _cb(notifier):
            called.append(notifier)

        result = _send_telegram_notification(cfg, _cb)

    assert result == "ok"
    assert len(called) == 1
