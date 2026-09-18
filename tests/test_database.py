"""Tests for database module."""

from datetime import UTC

import pytest
from sqlalchemy import inspect, text

from telecrime.database import (
    _pg_connect_args,
    ensure_runtime_schema,
    get_cached_engine,
    get_engine,
    get_session,
    get_session_factory,
    init_db,
)
from telecrime.models import Conversation


class TestPgConnectArgs:
    """application_name keeps the watchdog's PGAPPNAME contract intact."""

    def test_default_application_name(self, monkeypatch):
        monkeypatch.delenv("PGAPPNAME", raising=False)
        assert _pg_connect_args() == {"application_name": "telecrime"}

    def test_pipeline_tag_is_preserved(self, monkeypatch):
        monkeypatch.setenv("PGAPPNAME", "telecrime-pipeline")
        assert _pg_connect_args()["application_name"] == "telecrime-pipeline"


class TestGetEngine:
    """Tests for get_engine function."""

    def test_creates_engine_with_url(self, pg_engine):
        engine = get_engine(pg_engine.url.render_as_string(hide_password=False))
        assert engine is not None
        try:
            with engine.connect() as conn:
                assert conn.execute(text("SELECT 1")).scalar() == 1
        finally:
            engine.dispose()

    def test_requires_database_url(self):
        with pytest.raises(RuntimeError, match="database_url is required"):
            get_engine()


class TestGetSessionFactory:
    def test_creates_session_factory(self, pg_engine):
        factory = get_session_factory(pg_engine)
        assert factory is not None
        session = factory()
        try:
            # A factory that merely exists proves nothing; the session it
            # yields must actually be bound to the engine.
            assert session.execute(text("SELECT 1")).scalar() == 1
        finally:
            session.close()


class TestGetSession:
    def test_session_commits_on_success(self, pg_engine):
        with get_session(pg_engine) as session:
            session.add(Conversation(platform_id=123, conversation_type="channel"))
        with get_session(pg_engine) as session:
            assert session.query(Conversation).filter_by(platform_id=123).first() is not None

    def test_session_rollbacks_on_error(self, pg_engine):
        try:
            with get_session(pg_engine) as session:
                session.add(Conversation(platform_id=456, conversation_type="channel"))
                raise ValueError("boom")
        except ValueError:
            pass
        with get_session(pg_engine) as session:
            assert session.query(Conversation).filter_by(platform_id=456).first() is None

    def test_session_rollbacks_on_base_exception(self, tmp_path):
        """KeyboardInterrupt/CancelledError must not leak a transaction."""
        engine = get_engine(f"sqlite:///{tmp_path / 'baseexc.db'}")
        init_db(engine)
        try:
            with pytest.raises(KeyboardInterrupt):
                with get_session(engine) as session:
                    session.add(
                        Conversation(platform_id=789, conversation_type="channel")
                    )
                    raise KeyboardInterrupt
        finally:
            with get_session(engine) as session:
                assert (
                    session.query(Conversation).filter_by(platform_id=789).first()
                    is None
                )
            engine.dispose()


class TestInitDb:
    def test_creates_tables_and_is_idempotent(self, pg_engine):
        init_db(pg_engine)
        init_db(pg_engine)
        tables = set(inspect(pg_engine).get_table_names())

        for name in (
            "conversations",
            "messages",
            "file_attachments",
            "download_artifacts",
            "archive_groups",
            "extraction_jobs",
            "pipeline_state",
            "pipeline_runs",
        ):
            assert name in tables
        with get_session(pg_engine) as session:
            assert session.query(Conversation).count() == 0


class TestDatabaseIntegration:
    def test_full_workflow(self, pg_session):
        from datetime import datetime

        from telecrime.models import (
            ArchiveGroup,
            ArchiveGroupPart,
            Conversation,
            DownloadArtifact,
            FileAttachment,
            Message,
        )
        from telecrime.states import DownloadStatus, GroupStatus

        conv = Conversation(platform_id=1000, title="Test Channel", conversation_type="channel")
        pg_session.add(conv)
        pg_session.flush()
        msg = Message(
            conversation_id=conv.id,
            platform_id=100,
            platform_timestamp=datetime.now(UTC),
            text="Test message",
        )
        pg_session.add(msg)
        pg_session.flush()
        attachment = FileAttachment(
            message_id=msg.id,
            platform_file_id="file123",
            filename="test.zip",
            is_archive_candidate=True,
        )
        pg_session.add(attachment)
        pg_session.flush()
        artifact = DownloadArtifact(
            attachment_id=attachment.id,
            status=DownloadStatus.COMPLETED,
            content_hash="abc123",
        )
        pg_session.add(artifact)
        pg_session.flush()
        group = ArchiveGroup(fingerprint="group123", expected_part_count=1, status=GroupStatus.READY)
        pg_session.add(group)
        pg_session.flush()
        part = ArchiveGroupPart(group_id=group.id, artifact_id=artifact.id, part_index=0)
        pg_session.add(part)
        pg_session.commit()
        assert conv.messages == [msg]
        assert msg.attachments == [attachment]
        assert attachment.download_artifact == artifact
        assert artifact.group_part == part
        assert part.group == group

    def test_message_identity_constraint(self, pg_session):
        from datetime import datetime

        from sqlalchemy.exc import IntegrityError

        from telecrime.models import Conversation, Message

        conv = Conversation(platform_id=3000, conversation_type="channel")
        pg_session.add(conv)
        pg_session.flush()
        pg_session.add(
            Message(
                conversation_id=conv.id,
                platform_id=100,
                platform_timestamp=datetime.now(UTC),
                text="First",
            )
        )
        pg_session.flush()
        pg_session.add(
            Message(
                conversation_id=conv.id,
                platform_id=100,
                platform_timestamp=datetime.now(UTC),
                text="Duplicate",
            )
        )
        with pytest.raises(IntegrityError):
            pg_session.flush()

    def test_cascade_delete(self, pg_session):
        from datetime import datetime

        from telecrime.models import Conversation, FileAttachment, Message

        conv = Conversation(platform_id=2000, conversation_type="channel")
        pg_session.add(conv)
        pg_session.flush()
        msg = Message(
            conversation_id=conv.id,
            platform_id=200,
            platform_timestamp=datetime.now(UTC),
        )
        pg_session.add(msg)
        pg_session.flush()
        attachment = FileAttachment(message_id=msg.id, platform_file_id="file456")
        pg_session.add(attachment)
        pg_session.commit()
        pg_session.delete(conv)
        pg_session.commit()
        assert pg_session.query(Message).filter_by(platform_id=200).first() is None
        assert pg_session.query(FileAttachment).filter_by(platform_file_id="file456").first() is None


class TestDestructiveTestDatabaseGuard:
    """Regression: pytest must never drop the production schema again.

    On 2026-09-10 ``pytest tests/`` ran with TELECRIME_TEST_DATABASE_URL set to
    the production database; the pg_engine fixture's DROP SCHEMA public CASCADE
    wiped the live dataset.
    """

    def test_refuses_production_database(self):
        from tests.conftest import _assert_safe_test_database

        with pytest.raises(RuntimeError, match="Refusing to DROP SCHEMA"):
            _assert_safe_test_database(
                "postgresql://telecrime:telecrime@localhost:5432/telecrime"
            )

    def test_allows_test_databases(self):
        from tests.conftest import _assert_safe_test_database

        _assert_safe_test_database(
            "postgresql://telecrime:telecrime@localhost:5432/telecrime_test"
        )
        _assert_safe_test_database("postgresql://telecrime:telecrime@localhost:5432/testdb")

    def test_override_allows_production_database(self, monkeypatch):
        from tests.conftest import _assert_safe_test_database

        monkeypatch.setenv("TELECRIME_ALLOW_DESTRUCTIVE_TESTS", "1")
        _assert_safe_test_database(
            "postgresql://telecrime:telecrime@localhost:5432/telecrime"
        )


class TestGetCachedEngine:
    """URL-keyed engine cache used by the web background workers."""

    def test_same_url_reuses_engine_and_clear_rebuilds(self, tmp_path):
        url = f"sqlite:///{tmp_path / 'cached.db'}"
        first = get_cached_engine(url)
        try:
            assert get_cached_engine(url) is first
            get_cached_engine.cache_clear()
            second = get_cached_engine(url)
            assert second is not first
            second.dispose()
        finally:
            get_cached_engine.cache_clear()
            first.dispose()


class TestEnsureRuntimeSchema:
    """Forward-compatible repairs for databases created before Alembic ran.

    The function is the CLI's startup repair path; before this class it had
    zero coverage even though a bug here means a live DB missing alert columns.
    """

    @staticmethod
    def _legacy_engine(tmp_path, *, version: str | None = "m3n4o5p6q7r8"):
        """A pre-soft-hash schema: parsed_credentials/watchlist_items lack the
        columns the repair adds, plus an alembic_version row."""
        engine = get_engine(f"sqlite:///{tmp_path / 'legacy.db'}")
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE parsed_credentials (id INTEGER PRIMARY KEY)"))
            conn.execute(text("CREATE TABLE watchlist_items (id INTEGER PRIMARY KEY)"))
            conn.execute(
                text("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)")
            )
            if version is not None:
                conn.execute(
                    text("INSERT INTO alembic_version (version_num) VALUES (:v)"),
                    {"v": version},
                )
        return engine

    def test_repairs_columns_and_advances_old_alembic_version(self, tmp_path):
        engine = self._legacy_engine(tmp_path)
        try:
            changes = ensure_runtime_schema(engine)

            assert changes == [
                "added parsed_credentials.soft_credential_hash",
                "added watchlist_items.last_alerted_at",
                "added watchlist_items.last_alerted_count",
                "advanced alembic_version to n4o5p6q7r8s9",
            ]
            with engine.connect() as conn:
                parsed_cols = {
                    row[1]
                    for row in conn.execute(text("PRAGMA table_info(parsed_credentials)"))
                }
                watch_cols = {
                    row[1]
                    for row in conn.execute(text("PRAGMA table_info(watchlist_items)"))
                }
                version = conn.execute(
                    text("SELECT version_num FROM alembic_version")
                ).scalar()
            assert "soft_credential_hash" in parsed_cols
            assert {"last_alerted_at", "last_alerted_count"} <= watch_cols
            assert version == "n4o5p6q7r8s9"
        finally:
            engine.dispose()

    def test_second_run_is_a_noop(self, tmp_path):
        engine = self._legacy_engine(tmp_path)
        try:
            assert ensure_runtime_schema(engine)  # first run repairs
            assert ensure_runtime_schema(engine) == []
        finally:
            engine.dispose()

    def test_unknown_alembic_version_is_not_rewritten(self, tmp_path):
        engine = self._legacy_engine(tmp_path, version="z9y8x7w6v5u4")
        try:
            changes = ensure_runtime_schema(engine)
            assert "advanced alembic_version to n4o5p6q7r8s9" not in changes
            with engine.connect() as conn:
                assert conn.execute(
                    text("SELECT version_num FROM alembic_version")
                ).scalar() == "z9y8x7w6v5u4"
        finally:
            engine.dispose()

    def test_partial_schema_repairs_but_does_not_advance_version(self, tmp_path):
        """Without watchlist_items there is no alert schema to declare ready,
        so the version must not be bumped."""
        engine = get_engine(f"sqlite:///{tmp_path / 'partial.db'}")
        try:
            with engine.begin() as conn:
                conn.execute(
                    text("CREATE TABLE parsed_credentials (id INTEGER PRIMARY KEY)")
                )
                conn.execute(
                    text("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)")
                )
                conn.execute(
                    text("INSERT INTO alembic_version (version_num) VALUES ('m3n4o5p6q7r8')")
                )

            changes = ensure_runtime_schema(engine)

            assert changes == ["added parsed_credentials.soft_credential_hash"]
        finally:
            engine.dispose()

    def test_empty_database_is_untouched(self, tmp_path):
        engine = get_engine(f"sqlite:///{tmp_path / 'empty.db'}")
        try:
            assert ensure_runtime_schema(engine) == []
        finally:
            engine.dispose()

    def test_fresh_schema_needs_no_repairs(self, tmp_path):
        engine = get_engine(f"sqlite:///{tmp_path / 'fresh.db'}")
        try:
            init_db(engine)
            assert ensure_runtime_schema(engine) == []
        finally:
            engine.dispose()
