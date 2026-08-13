"""Tests for database module."""

from datetime import UTC

import pytest
from sqlalchemy import text

from telecrime.database import (
    get_engine,
    get_session,
    get_session_factory,
    init_db,
)
from telecrime.models import Conversation


class TestGetEngine:
    """Tests for get_engine function."""

    def test_creates_engine_with_url(self, tmp_path):
        db_path = tmp_path / "test.db"
        engine = get_engine(f"sqlite:///{db_path}")
        assert engine is not None
        with engine.connect() as conn:
            assert conn.execute(text("SELECT 1")).scalar() == 1

    def test_requires_database_url(self):
        with pytest.raises(RuntimeError):
            get_engine()


class TestGetSessionFactory:
    def test_creates_session_factory(self, in_memory_engine):
        factory = get_session_factory(in_memory_engine)
        assert factory is not None
        s = factory()
        assert s is not None
        s.close()


class TestGetSession:
    def test_session_commits_on_success(self, in_memory_engine):
        with get_session(in_memory_engine) as session:
            session.add(Conversation(platform_id=123, conversation_type="channel"))
        with get_session(in_memory_engine) as session:
            assert session.query(Conversation).filter_by(platform_id=123).first() is not None

    def test_session_rollbacks_on_error(self, in_memory_engine):
        try:
            with get_session(in_memory_engine) as session:
                session.add(Conversation(platform_id=456, conversation_type="channel"))
                raise ValueError("boom")
        except ValueError:
            pass
        with get_session(in_memory_engine) as session:
            assert session.query(Conversation).filter_by(platform_id=456).first() is None


class TestInitDb:
    def test_creates_tables(self, tmp_path):
        db_path = tmp_path / "init_test.db"
        engine = get_engine(f"sqlite:///{db_path}")
        init_db(engine)
        with engine.connect() as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table'")
                )
            }
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

    def test_idempotent(self, tmp_path):
        db_path = tmp_path / "idempotent_test.db"
        engine = get_engine(f"sqlite:///{db_path}")
        init_db(engine)
        init_db(engine)
        init_db(engine)
        with get_session(engine) as session:
            assert session.query(Conversation).count() == 0


class TestDatabaseIntegration:
    def test_full_workflow(self, session):
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
        session.add(conv)
        session.flush()
        msg = Message(
            conversation_id=conv.id,
            platform_id=100,
            platform_timestamp=datetime.now(UTC),
            text="Test message",
        )
        session.add(msg)
        session.flush()
        attachment = FileAttachment(
            message_id=msg.id,
            platform_file_id="file123",
            filename="test.zip",
            is_archive_candidate=True,
        )
        session.add(attachment)
        session.flush()
        artifact = DownloadArtifact(
            attachment_id=attachment.id,
            status=DownloadStatus.COMPLETED,
            content_hash="abc123",
        )
        session.add(artifact)
        session.flush()
        group = ArchiveGroup(fingerprint="group123", expected_part_count=1, status=GroupStatus.READY)
        session.add(group)
        session.flush()
        part = ArchiveGroupPart(group_id=group.id, artifact_id=artifact.id, part_index=0)
        session.add(part)
        session.commit()
        assert conv.messages == [msg]
        assert msg.attachments == [attachment]
        assert attachment.download_artifact == artifact
        assert artifact.group_part == part
        assert part.group == group

    def test_message_identity_constraint(self, session):
        from datetime import datetime

        from sqlalchemy.exc import IntegrityError

        from telecrime.models import Conversation, Message

        conv = Conversation(platform_id=3000, conversation_type="channel")
        session.add(conv)
        session.flush()
        session.add(
            Message(
                conversation_id=conv.id,
                platform_id=100,
                platform_timestamp=datetime.now(UTC),
                text="First",
            )
        )
        session.flush()
        session.add(
            Message(
                conversation_id=conv.id,
                platform_id=100,
                platform_timestamp=datetime.now(UTC),
                text="Duplicate",
            )
        )
        with pytest.raises(IntegrityError):
            session.flush()

    def test_cascade_delete(self, session):
        from datetime import datetime

        from telecrime.models import Conversation, FileAttachment, Message

        conv = Conversation(platform_id=2000, conversation_type="channel")
        session.add(conv)
        session.flush()
        msg = Message(
            conversation_id=conv.id,
            platform_id=200,
            platform_timestamp=datetime.now(UTC),
        )
        session.add(msg)
        session.flush()
        attachment = FileAttachment(message_id=msg.id, platform_file_id="file456")
        session.add(attachment)
        session.commit()
        session.delete(conv)
        session.commit()
        assert session.query(Message).filter_by(platform_id=200).first() is None
        assert session.query(FileAttachment).filter_by(platform_file_id="file456").first() is None
