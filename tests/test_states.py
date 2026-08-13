"""Tests for state definitions."""

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
