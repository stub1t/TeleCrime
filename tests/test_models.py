"""Tests for database models."""

from datetime import UTC, datetime

from telecrime.models import (
    ArchiveGroup,
    Conversation,
    DownloadArtifact,
    FileAttachment,
    FirstSeenIndex,
    Message,
    PasswordCandidate,
)
from telecrime.states import (
    DownloadStatus,
    GroupStatus,
    PasswordScope,
)


class TestConversation:
    def test_create_conversation(self, session):
        """Test creating a conversation."""
        conv = Conversation(
            platform_id=123456789,
            title="Test Channel",
            conversation_type="channel",
        )
        session.add(conv)
        session.commit()

        assert conv.id is not None
        assert conv.platform_id == 123456789
        assert conv.title == "Test Channel"
        assert conv.is_member is True
        assert conv.is_accessible is True


class TestMessage:
    def test_create_message_with_conversation(self, session):
        """Test creating a message linked to a conversation."""
        conv = Conversation(
            platform_id=123456789,
            title="Test Channel",
            conversation_type="channel",
        )
        session.add(conv)
        session.flush()

        msg = Message(
            conversation_id=conv.id,
            platform_id=100,
            platform_timestamp=datetime.now(UTC),
            text="Hello world",
        )
        session.add(msg)
        session.commit()

        assert msg.id is not None
        assert msg.conversation_id == conv.id
        assert msg.conversation == conv
        assert conv.messages == [msg]


class TestFileAttachment:
    def test_create_attachment(self, session):
        """Test creating a file attachment."""
        conv = Conversation(platform_id=1, conversation_type="channel")
        session.add(conv)
        session.flush()

        msg = Message(
            conversation_id=conv.id,
            platform_id=100,
            platform_timestamp=datetime.now(UTC),
        )
        session.add(msg)
        session.flush()

        attachment = FileAttachment(
            message_id=msg.id,
            platform_file_id="abc123",
            filename="test.zip",
            mime_type="application/zip",
            size=1024,
            is_archive_candidate=True,
            archive_type="zip",
        )
        session.add(attachment)
        session.commit()

        assert attachment.id is not None
        assert attachment.is_archive_candidate is True
        assert attachment.message == msg


class TestDownloadArtifact:
    def test_download_status_tracking(self, session):
        """Test download status state machine."""
        conv = Conversation(platform_id=1, conversation_type="channel")
        msg = Message(
            conversation_id=1,
            platform_id=100,
            platform_timestamp=datetime.now(UTC),
        )
        attachment = FileAttachment(
            message_id=1,
            platform_file_id="abc123",
        )

        session.add(conv)
        session.flush()
        msg.conversation_id = conv.id
        session.add(msg)
        session.flush()
        attachment.message_id = msg.id
        session.add(attachment)
        session.flush()

        artifact = DownloadArtifact(
            attachment_id=attachment.id,
            status=DownloadStatus.PENDING,
        )
        session.add(artifact)
        session.commit()

        assert artifact.status == DownloadStatus.PENDING

        artifact.status = DownloadStatus.DOWNLOADING
        session.commit()
        assert artifact.status == DownloadStatus.DOWNLOADING

        artifact.status = DownloadStatus.COMPLETED
        artifact.content_hash = "abc123def456"
        session.commit()
        assert artifact.status == DownloadStatus.COMPLETED


class TestArchiveGroup:
    def test_archive_group_with_parts(self, session):
        """Test archive group with multiple parts."""
        # Create prerequisite objects
        conv = Conversation(platform_id=1, conversation_type="channel")
        session.add(conv)
        session.flush()

        group = ArchiveGroup(
            fingerprint="abc123",
            base_name="archive",
            expected_part_count=3,
            status=GroupStatus.INCOMPLETE,
        )
        session.add(group)
        session.commit()

        assert group.id is not None
        assert group.status == GroupStatus.INCOMPLETE
        assert group.expected_part_count == 3


class TestPasswordCandidate:
    def test_password_scopes(self, session):
        """Test different password scopes."""
        for scope in PasswordScope:
            pwd = PasswordCandidate(
                value="secret123",
                scope=scope,
                extraction_method="test",
                confidence=0.8,
            )
            session.add(pwd)
            session.commit()
            assert pwd.scope == scope


class TestFirstSeenIndex:
    def test_first_seen_tracking(self, session):
        """Test first seen index for duplicate detection."""
        first_seen = FirstSeenIndex(
            content_hash="deadbeef" * 8,
            content_type="download",
            first_seen_timestamp=datetime.now(UTC),
        )
        session.add(first_seen)
        session.commit()

        # Try to find duplicate
        existing = session.query(FirstSeenIndex).filter_by(
            content_hash="deadbeef" * 8
        ).first()
        assert existing is not None
        assert existing.duplicate_count == 0

        existing.duplicate_count += 1
        session.commit()
        assert existing.duplicate_count == 1
