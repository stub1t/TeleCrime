"""Tests for state definitions."""

from datetime import UTC, datetime

import pytest
from sqlalchemy import text

from telecrime.models import (
    ArchiveGroup,
    Conversation,
    DownloadArtifact,
    ExtractionJob,
    FileAttachment,
    Message,
    PasswordCandidate,
)
from telecrime.states import (
    DownloadStatus,
    ExtractionStatus,
    GroupStatus,
    PasswordScope,
)


def test_download_status_string_values():
    """Download statuses serialize to stable DB strings."""
    assert DownloadStatus.PENDING.value == "pending"
    assert DownloadStatus.DOWNLOADING.value == "downloading"
    assert DownloadStatus.COMPLETED.value == "completed"
    assert DownloadStatus.FAILED.value == "failed"
    assert DownloadStatus.FAILED_TERMINAL.value == "failed_terminal"


def test_group_status_string_values():
    """Group statuses serialize to stable DB strings."""
    assert GroupStatus.INCOMPLETE.value == "incomplete"
    assert GroupStatus.READY.value == "ready"
    assert GroupStatus.EXTRACTING.value == "extracting"
    assert GroupStatus.EXTRACTED.value == "extracted"
    assert GroupStatus.CLEANED.value == "cleaned"
    assert GroupStatus.FAILED.value == "failed"
    assert GroupStatus.FAILED_TERMINAL.value == "failed_terminal"


def test_extraction_status_string_values():
    """Extraction statuses serialize to stable DB strings."""
    assert ExtractionStatus.PENDING.value == "pending"
    assert ExtractionStatus.IN_PROGRESS.value == "in_progress"
    assert ExtractionStatus.PASSWORD_NEEDED.value == "password_needed"
    assert ExtractionStatus.COMPLETED.value == "completed"
    assert ExtractionStatus.FAILED.value == "failed"
    assert ExtractionStatus.FAILED_TERMINAL.value == "failed_terminal"


def test_password_scope_order_matches_priority():
    """Scopes are ordered by expected priority (MESSAGE highest, GLOBAL lowest)."""
    scopes = list(PasswordScope)
    assert scopes[0] == PasswordScope.MESSAGE
    assert scopes[-1] == PasswordScope.GLOBAL
    # MESSAGE -> NEARBY -> CONVERSATION -> LEARNED -> GLOBAL
    assert PasswordScope.MESSAGE.value == "message"
    assert PasswordScope.NEARBY.value == "nearby"
    assert PasswordScope.CONVERSATION.value == "conversation"
    assert PasswordScope.LEARNED.value == "learned"
    assert PasswordScope.GLOBAL.value == "global"


# --- ORM persistence round-trips --------------------------------------------
# SQLAlchemy's Enum column stores the member NAME ("FAILED_TERMINAL"), not the
# lowercase .value. Raw SQL in the scheduler/vacuum paths depends on that
# (see _run_vacuum_job), so a mapping change must fail here rather than in
# production. Each enum member is written, read back and checked against the
# raw stored string.


def _attachment(session, tag: str) -> FileAttachment:
    conv = Conversation(platform_id=1, conversation_type="channel")
    session.add(conv)
    session.flush()
    msg = Message(
        conversation_id=conv.id,
        platform_id=1,
        platform_timestamp=datetime.now(UTC),
    )
    session.add(msg)
    session.flush()
    attachment = FileAttachment(message_id=msg.id, platform_file_id=f"rt-{tag}")
    session.add(attachment)
    session.flush()
    return attachment


@pytest.mark.parametrize("status", list(DownloadStatus), ids=lambda s: s.value)
def test_download_status_round_trips_through_orm(session, status):
    attachment = _attachment(session, status.value)
    artifact = DownloadArtifact(attachment_id=attachment.id, status=status)
    session.add(artifact)
    session.commit()
    artifact_id = artifact.id
    session.expunge_all()

    loaded = session.get(DownloadArtifact, artifact_id)
    assert loaded is not None
    assert loaded.status is status
    stored = session.execute(
        text("SELECT status FROM download_artifacts WHERE id = :id"), {"id": artifact_id}
    ).scalar()
    assert stored == status.name


@pytest.mark.parametrize("status", list(GroupStatus), ids=lambda s: s.value)
def test_group_status_round_trips_through_orm(session, status):
    group = ArchiveGroup(
        fingerprint=f"rt-{status.value}",
        base_name="archive.zip",
        expected_part_count=1,
        status=status,
    )
    session.add(group)
    session.commit()
    group_id = group.id
    session.expunge_all()

    loaded = session.get(ArchiveGroup, group_id)
    assert loaded is not None
    assert loaded.status is status
    stored = session.execute(
        text("SELECT status FROM archive_groups WHERE id = :id"), {"id": group_id}
    ).scalar()
    assert stored == status.name


@pytest.mark.parametrize("status", list(ExtractionStatus), ids=lambda s: s.value)
def test_extraction_status_round_trips_through_orm(session, status):
    group = ArchiveGroup(
        fingerprint=f"rt-job-{status.value}",
        base_name="archive.zip",
        expected_part_count=1,
    )
    session.add(group)
    session.flush()
    job = ExtractionJob(group_id=group.id, status=status)
    session.add(job)
    session.commit()
    job_id = job.id
    session.expunge_all()

    loaded = session.get(ExtractionJob, job_id)
    assert loaded is not None
    assert loaded.status is status
    stored = session.execute(
        text("SELECT status FROM extraction_jobs WHERE id = :id"), {"id": job_id}
    ).scalar()
    assert stored == status.name


@pytest.mark.parametrize("scope", list(PasswordScope), ids=lambda s: s.value)
def test_password_scope_round_trips_through_orm(session, scope):
    candidate = PasswordCandidate(
        value=f"pw-{scope.value}", scope=scope, extraction_method="caption"
    )
    session.add(candidate)
    session.commit()
    candidate_id = candidate.id
    session.expunge_all()

    loaded = session.get(PasswordCandidate, candidate_id)
    assert loaded is not None
    assert loaded.scope is scope
    stored = session.execute(
        text("SELECT scope FROM password_candidates WHERE id = :id"), {"id": candidate_id}
    ).scalar()
    assert stored == scope.name
