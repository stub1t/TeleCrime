"""Tests for CLI module."""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from telecrime import __version__
from telecrime.cli import app
from telecrime.database import get_engine, get_session, init_db
from telecrime.fts import ensure_fts
from telecrime.models import ArchiveGroup, ExtractionJob, ParsedCredential, PipelineRun
from telecrime.states import ExtractionStatus, GroupStatus

runner = CliRunner()


class TestCliVersion:
    """Tests for version command."""

    def test_version_in_help(self):
        """Test version appears in help output."""
        result = runner.invoke(app, ["--help"])

        # Help should work
        assert result.exit_code == 0
        assert "telecrime" in result.stdout.lower()

    def test_version_constant_exists(self):
        """Test version constant is defined."""
        assert __version__ == "0.1.0"


class TestCliInit:
    """Tests for init command."""

    def test_init_creates_database(self, tmp_path):
        """Test init command creates database."""
        config_path = tmp_path / "config.toml"

        with patch("telecrime.cli.get_config_and_engine") as mock_get:
            mock_config = MagicMock()
            mock_config.database_url = f"sqlite:///{tmp_path / 'test.db'}"
            mock_engine = MagicMock()
            mock_get.return_value = (mock_config, mock_engine)

            with patch("telecrime.cli.init_db"):
                with patch("telecrime.cli.save_config"):
                    with patch.object(Path, "exists", return_value=False):
                        result = runner.invoke(app, ["init", "--config", str(config_path)])

        assert result.exit_code == 0
        assert "initialized" in result.stdout.lower()


class TestCliStatus:
    """Tests for status command."""

    def test_status_shows_counts(self, tmp_path):
        """Test status command shows entity counts."""
        with patch("telecrime.cli.get_config_and_engine") as mock_get:
            mock_config = MagicMock()
            mock_engine = MagicMock()
            mock_get.return_value = (mock_config, mock_engine)

            with patch("telecrime.cli.get_session") as mock_session:
                # Create mock session context manager
                mock_sess = MagicMock()
                mock_sess.query.return_value.count.return_value = 0
                mock_sess.query.return_value.filter.return_value.count.return_value = 0
                mock_session.return_value.__enter__ = MagicMock(return_value=mock_sess)
                mock_session.return_value.__exit__ = MagicMock(return_value=False)

                result = runner.invoke(app, ["status"])

        assert result.exit_code == 0
        # Should show table with entities
        assert "Conversations" in result.stdout or "conversations" in result.stdout.lower()


class TestCliDiagnostics:
    """Tests for diagnostics command."""

    def test_diagnostics_runs(self):
        with patch("telecrime.cli.get_config_and_engine") as mock_get:
            mock_get.return_value = (MagicMock(), MagicMock())

            with patch("telecrime.cli.get_session") as mock_session:
                mock_sess = MagicMock()
                mock_query = MagicMock()
                mock_query.filter.return_value.count.return_value = 0
                mock_query.group_by.return_value.order_by.return_value.limit.return_value.all.return_value = []
                mock_query.count.return_value = 0
                mock_sess.query.return_value = mock_query
                mock_session.return_value.__enter__ = MagicMock(return_value=mock_sess)
                mock_session.return_value.__exit__ = MagicMock(return_value=False)

                result = runner.invoke(app, ["diagnostics"])

        assert result.exit_code == 0
        assert "pipeline diagnostics" in result.stdout.lower()
        assert "failure summary" in result.stdout.lower()

    def test_diagnostics_shows_recent_runs(self, tmp_path):
        db_path = tmp_path / "diag.db"
        engine = get_engine(f"sqlite:///{db_path}")
        init_db(engine)

        with get_session(engine) as session:
            session.add(
                PipelineRun(
                    mode="batch",
                    status="completed",
                    dry_run=0,
                    started_at=datetime.now(UTC),
                    duration_seconds=3,
                    credentials_parsed=10,
                    errors_json="[]",
                )
            )

        with patch("telecrime.cli.get_config_and_engine") as mock_get:
            mock_get.return_value = (MagicMock(), engine)
            result = runner.invoke(app, ["diagnostics"])

        assert result.exit_code == 0
        assert "recent pipeline runs" in result.stdout.lower()


class TestCliRetry:
    """Tests for retry command."""

    def test_retry_resets_failed_jobs(self, tmp_path):
        """Test retry command resets failed jobs."""
        with patch("telecrime.cli.get_config_and_engine") as mock_get:
            mock_config = MagicMock()
            mock_engine = MagicMock()
            mock_get.return_value = (mock_config, mock_engine)

            with patch("telecrime.cli.get_session") as mock_session:
                mock_sess = MagicMock()
                mock_sess.query.return_value.filter.return_value.all.return_value = []
                mock_session.return_value.__enter__ = MagicMock(return_value=mock_sess)
                mock_session.return_value.__exit__ = MagicMock(return_value=False)

                result = runner.invoke(app, ["retry"])

        assert result.exit_code == 0
        assert "reset" in result.stdout.lower()

    def test_retry_can_include_terminal_failures(self):
        """Retry command can include terminal failures."""
        with patch("telecrime.cli.get_config_and_engine") as mock_get:
            mock_get.return_value = (MagicMock(), MagicMock())

            with patch("telecrime.cli.get_session") as mock_session:
                mock_sess = MagicMock()
                mock_filter = MagicMock()
                mock_filter.all.return_value = []
                mock_sess.query.return_value.filter.return_value = mock_filter
                mock_session.return_value.__enter__ = MagicMock(return_value=mock_sess)
                mock_session.return_value.__exit__ = MagicMock(return_value=False)

                result = runner.invoke(app, ["retry", "--downloads", "--terminal"])

        assert result.exit_code == 0


class TestCliShutdownRequest:
    def test_shutdown_request_writes_file(self, tmp_path, monkeypatch):
        shutdown_path = tmp_path / "pipeline_shutdown_request.json"
        monkeypatch.setenv("TELECRIME_SHUTDOWN_REQUEST_FILE", str(shutdown_path))

        with patch("telecrime.cli.get_config_and_engine") as mock_get:
            mock_config = MagicMock()
            mock_config.data_dir = tmp_path / "data"
            mock_config.data_dir.mkdir()
            mock_get.return_value = (mock_config, MagicMock())

            result = runner.invoke(
                app,
                ["shutdown-request", "--reason", "maintenance window"],
            )

        assert result.exit_code == 0
        assert shutdown_path.exists()
        assert "Graceful shutdown requested" in result.stdout

    def test_shutdown_request_clear_removes_file(self, tmp_path, monkeypatch):
        shutdown_path = tmp_path / "pipeline_shutdown_request.json"
        shutdown_path.write_text('{"requested_at":"2026-04-22T00:00:00+00:00","mode":"finish_archive"}')
        monkeypatch.setenv("TELECRIME_SHUTDOWN_REQUEST_FILE", str(shutdown_path))

        with patch("telecrime.cli.get_config_and_engine") as mock_get:
            mock_config = MagicMock()
            mock_config.data_dir = tmp_path / "data"
            mock_config.data_dir.mkdir()
            mock_get.return_value = (mock_config, MagicMock())

            result = runner.invoke(app, ["shutdown-request", "--clear"])

        assert result.exit_code == 0
        assert not shutdown_path.exists()
        assert "Cleared shutdown request" in result.stdout

    def test_shutdown_request_waits_and_notifies(self, tmp_path, monkeypatch):
        shutdown_path = tmp_path / "pipeline_shutdown_request.json"
        monkeypatch.setenv("TELECRIME_SHUTDOWN_REQUEST_FILE", str(shutdown_path))

        worker = MagicMock()
        worker.wait_until_drained.return_value = True

        with patch("telecrime.cli.get_config_and_engine") as mock_get, patch(
            "telecrime.scheduler.TelecrimeWorker", return_value=worker
        ), patch("telecrime.scheduler.send_desktop_notification", return_value="sent") as notify:
            mock_config = MagicMock()
            mock_config.data_dir = tmp_path / "data"
            mock_config.data_dir.mkdir()
            engine = MagicMock()
            mock_get.return_value = (mock_config, engine)

            result = runner.invoke(app, ["shutdown-request", "--wait", "--timeout", "1"])

        assert result.exit_code == 0
        worker.wait_until_drained.assert_called_once_with(timeout_seconds=1)
        notify.assert_called_once()
        assert "Pipeline shutdown complete" in result.stdout

    def test_shutdown_request_wait_timeout_exits_124(self, tmp_path, monkeypatch):
        shutdown_path = tmp_path / "pipeline_shutdown_request.json"
        monkeypatch.setenv("TELECRIME_SHUTDOWN_REQUEST_FILE", str(shutdown_path))

        worker = MagicMock()
        worker.wait_until_drained.return_value = False

        with patch("telecrime.cli.get_config_and_engine") as mock_get, patch(
            "telecrime.scheduler.TelecrimeWorker", return_value=worker
        ), patch("telecrime.scheduler.send_desktop_notification") as notify:
            mock_config = MagicMock()
            mock_config.data_dir = tmp_path / "data"
            mock_config.data_dir.mkdir()
            mock_get.return_value = (mock_config, MagicMock())

            result = runner.invoke(app, ["shutdown-request", "--wait", "--timeout", "1"])

        assert result.exit_code == 124
        notify.assert_not_called()
        assert "Timed out waiting" in result.stdout


class TestCliFailures:
    """Tests for failures command."""

    def test_failures_command_runs(self):
        with patch("telecrime.cli.get_config_and_engine") as mock_get:
            mock_get.return_value = (MagicMock(), MagicMock())

            with patch("telecrime.cli.get_session") as mock_session:
                mock_sess = MagicMock()
                mock_query = MagicMock()
                mock_query.filter.return_value.order_by.return_value.limit.return_value.all.return_value = []
                mock_sess.query.return_value = mock_query
                mock_session.return_value.__enter__ = MagicMock(return_value=mock_sess)
                mock_session.return_value.__exit__ = MagicMock(return_value=False)

                result = runner.invoke(app, ["failures"])

        assert result.exit_code == 0
        assert "failed downloads" in result.stdout.lower()
        assert "failed extractions" in result.stdout.lower()


class TestCliReprocess:
    """Tests for reprocess command."""

    def test_reprocess_requires_target(self):
        result = runner.invoke(app, ["reprocess"])
        assert result.exit_code == 1

    def test_reprocess_parse_resets_group(self, tmp_path):
        db_path = tmp_path / "reprocess.db"
        engine = get_engine(f"sqlite:///{db_path}")
        init_db(engine)

        with get_session(engine) as session:
            group = ArchiveGroup(
                fingerprint="group1",
                base_name="sample.zip",
                expected_part_count=1,
                detected_part_count=1,
                status=GroupStatus.CLEANED,
            )
            session.add(group)
            session.flush()
            job = ExtractionJob(group_id=group.id, status=ExtractionStatus.COMPLETED)
            session.add(job)
            session.flush()
            session.add(
                ParsedCredential(
                    url="https://example.com",
                    domain="example.com",
                    username="alice",
                    password="secret",
                    extraction_job_id=job.id,
                    credential_hash=ParsedCredential.compute_hash("example.com", "alice", "secret"),
                )
            )

        with patch("telecrime.cli.get_config_and_engine") as mock_get:
            mock_config = MagicMock()
            mock_config.database_url = f"sqlite:///{db_path}"
            mock_get.return_value = (mock_config, engine)

            result = runner.invoke(app, ["reprocess", "--group-id", "1", "--stage", "parse"])

        assert result.exit_code == 0
        with get_session(engine) as session:
            group = session.query(ArchiveGroup).filter_by(id=1).one()
            assert group.status == GroupStatus.EXTRACTED
            assert session.query(ParsedCredential).count() == 0


class TestCliClean:
    """Tests for clean command."""

    def test_clean_requires_flags(self, tmp_path):
        """Test clean command requires --downloads."""
        with patch("telecrime.cli.get_config_and_engine") as mock_get:
            mock_config = MagicMock()
            mock_engine = MagicMock()
            mock_get.return_value = (mock_config, mock_engine)

            result = runner.invoke(app, ["clean"])

        assert result.exit_code == 1
        assert "specify" in result.stdout.lower()

    def test_clean_with_force(self, tmp_path):
        """Test clean command with --force skips confirmation."""
        downloads_dir = tmp_path / "downloads"
        downloads_dir.mkdir()
        (downloads_dir / "test.zip").touch()

        with patch("telecrime.cli.get_config_and_engine") as mock_get:
            mock_config = MagicMock()
            mock_config.downloads_dir = downloads_dir
            mock_engine = MagicMock()
            mock_get.return_value = (mock_config, mock_engine)

            result = runner.invoke(app, ["clean", "--downloads", "--force"])

        assert result.exit_code == 0
        assert "cleaned" in result.stdout.lower()

    def test_clean_asks_confirmation(self, tmp_path):
        """Test clean command asks for confirmation without --force."""
        with patch("telecrime.cli.get_config_and_engine") as mock_get:
            mock_config = MagicMock()
            mock_config.downloads_dir = tmp_path / "downloads"
            mock_engine = MagicMock()
            mock_get.return_value = (mock_config, mock_engine)

            # Say no to confirmation
            result = runner.invoke(app, ["clean", "--downloads"], input="n\n")

        # Should exit without cleaning
        assert result.exit_code == 0 or result.exit_code == 1


class TestCliRun:
    """Tests for run command."""

    def test_run_requires_credentials(self):
        """Test run command requires Telegram credentials."""
        with patch("telecrime.cli.get_config_and_engine") as mock_get:
            mock_config = MagicMock()
            mock_config.telegram.api_id = None
            mock_config.telegram.api_hash = None
            mock_engine = MagicMock()
            mock_get.return_value = (mock_config, mock_engine)

            result = runner.invoke(app, ["run"])

        assert result.exit_code == 1
        assert "credentials" in result.stdout.lower() or "configured" in result.stdout.lower()

    def test_run_dry_run_flag(self):
        """Test run command accepts --dry-run flag."""
        import asyncio

        with patch("telecrime.cli.get_config_and_engine") as mock_get:
            mock_config = MagicMock()
            mock_config.telegram.api_id = 12345
            mock_config.telegram.api_hash = "test"
            mock_config.database_url = "sqlite:///:memory:"
            mock_config.extraction.target_extensions = [".epub"]
            mock_engine = MagicMock()
            mock_get.return_value = (mock_config, mock_engine)

            with patch.object(asyncio, "run") as mock_run:
                mock_run.return_value = None
                result = runner.invoke(app, ["run", "--dry-run"])

        # Should mention dry run mode (case insensitive)
        output = result.stdout.lower()
        assert "dry run" in output or "dry_run" in output or "dry-run" in output

    def test_run_reports_existing_pipeline_lock(self):
        """Run command exits cleanly when another pipeline is already active."""
        import asyncio

        from telecrime.pipeline.lock import PipelineAlreadyRunningError

        with patch("telecrime.cli.get_config_and_engine") as mock_get:
            mock_config = MagicMock()
            mock_config.telegram.api_id = 12345
            mock_config.telegram.api_hash = "test"
            mock_config.database_url = "sqlite:///:memory:"
            mock_config.extraction.target_extensions = [".txt"]
            mock_engine = MagicMock()
            mock_get.return_value = (mock_config, mock_engine)

            with patch.object(
                asyncio,
                "run",
                side_effect=PipelineAlreadyRunningError("Another pipeline run is already active"),
            ):
                result = runner.invoke(app, ["run"])

        assert result.exit_code == 75  # EX_TEMPFAIL — not an error, just busy
        assert "already active" in result.stdout.lower()


class TestCliProcess:
    """Tests for process command."""

    def test_process_forwards_config_path(self, tmp_path):
        folder = tmp_path / "archives"
        folder.mkdir()
        config_path = tmp_path / "config.toml"

        with patch("telecrime.cli.get_config_and_engine") as mock_get:
            mock_config = MagicMock()
            mock_config.database_url = "sqlite:///ignored.db"
            mock_get.return_value = (mock_config, MagicMock())

            with patch("subprocess.run") as mock_run:
                mock_run.return_value.returncode = 0
                result = runner.invoke(
                    app,
                    ["process", str(folder), "--config", str(config_path)],
                )

        assert result.exit_code == 0
        command = mock_run.call_args.args[0]
        assert "--config" in command
        assert str(config_path) in command

    def test_process_falls_back_to_database_url(self, tmp_path):
        folder = tmp_path / "archives"
        folder.mkdir()

        with patch("telecrime.cli.get_config_and_engine") as mock_get:
            mock_config = MagicMock()
            mock_config.database_url = "sqlite:///telecrime.db"
            mock_get.return_value = (mock_config, MagicMock())

            with patch("subprocess.run") as mock_run:
                mock_run.return_value.returncode = 0
                result = runner.invoke(app, ["process", str(folder)])

        assert result.exit_code == 0
        command = mock_run.call_args.args[0]
        assert "--database" in command
        assert "sqlite:///telecrime.db" in command


class TestCliFts:
    """Tests for FTS commands."""

    def test_rebuild_fts_command(self):
        with patch("telecrime.cli.get_config_and_engine") as mock_get:
            mock_get.return_value = (MagicMock(), MagicMock())

            with patch("telecrime.fts.ensure_fts", return_value=True) as mock_ensure:
                result = runner.invoke(app, ["fts", "rebuild"])

        assert result.exit_code == 0
        assert "fts index ready" in result.stdout.lower()
        mock_ensure.assert_called_once()


class TestCliSearch:
    """Tests for search command."""

    @pytest.mark.skip(reason="exercises removed SQLite FTS5 path; production is PG-only")
    def test_search_uses_fts_filters(self, tmp_path):
        db_path = tmp_path / "search.db"
        engine = get_engine(f"sqlite:///{db_path}")
        init_db(engine)

        with get_session(engine) as session:
            session.add_all(
                [
                    ParsedCredential(
                        url="https://accounts.google.com/login",
                        domain="accounts.google.com",
                        username="alice",
                        password="secret",
                        stealer_type="redline",
                        credential_hash=ParsedCredential.compute_hash(
                            "accounts.google.com",
                            "alice",
                            "secret",
                        ),
                    ),
                    ParsedCredential(
                        url="https://accounts.google.com/login",
                        domain="accounts.google.com",
                        username="bob",
                        password="secret",
                        stealer_type="vidar",
                        credential_hash=ParsedCredential.compute_hash(
                            "accounts.google.com",
                            "bob",
                            "secret",
                        ),
                    ),
                ]
            )

        assert ensure_fts(engine, rebuild=True) is True

        with patch("telecrime.cli.get_config_and_engine") as mock_get:
            mock_config = MagicMock()
            mock_config.database_url = f"sqlite:///{db_path}"
            mock_get.return_value = (mock_config, engine)

            result = runner.invoke(app, ["search", "google", "--domain", "--stealer", "redline"])

        assert result.exit_code == 0
        assert "alice" in result.stdout
        assert "bob" not in result.stdout


class TestCliHelp:
    """Tests for help output."""

    def test_help_shows_commands(self):
        """Test help shows all available commands."""
        result = runner.invoke(app, ["--help"])

        assert result.exit_code == 0
        assert "init" in result.stdout
        assert "run" in result.stdout
        assert "status" in result.stdout
        assert "retry" in result.stdout
        assert "clean" in result.stdout
        assert "fts" in result.stdout

    def test_command_help(self):
        """Test individual command help."""
        for cmd in ["init", "run", "status", "retry", "clean", "process"]:
            result = runner.invoke(app, [cmd, "--help"])
            assert result.exit_code == 0
