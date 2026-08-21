"""Tests for database module."""

from datetime import UTC

import pytest
from sqlalchemy import inspect, text

from telecrime.database import (
    get_engine,
    get_session,
    get_session_factory,
    init_db,
)
from telecrime.models import Conversation


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
        with pytest.raises(RuntimeError):
            get_engine()


class TestGetSessionFactory:
    def test_creates_session_factory(self, pg_engine):
        factory = get_session_factory(pg_engine)
        assert factory is not None
        s = factory()
        assert s is not None
        s.close()


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
