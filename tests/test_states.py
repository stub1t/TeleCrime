"""Tests for state machine definitions."""


from telecrime.states import (
    DownloadStatus,
    ExtractionStatus,
    GroupStatus,
    PasswordScope,
)


class TestDownloadStatus:
    """Tests for DownloadStatus enum."""

    def test_all_statuses_exist(self):
        """Test all expected download statuses exist."""
        assert DownloadStatus.PENDING
        assert DownloadStatus.DOWNLOADING
        assert DownloadStatus.COMPLETED
        assert DownloadStatus.FAILED
        assert DownloadStatus.FAILED_TERMINAL

    def test_string_values(self):
        """Test status string values."""
        assert DownloadStatus.PENDING.value == "pending"
        assert DownloadStatus.COMPLETED.value == "completed"


class TestGroupStatus:
    """Tests for GroupStatus enum."""

    def test_all_statuses_exist(self):
        """Test all expected group statuses exist."""
        assert GroupStatus.INCOMPLETE
        assert GroupStatus.READY
        assert GroupStatus.EXTRACTING
        assert GroupStatus.EXTRACTED
        assert GroupStatus.CLEANED
        assert GroupStatus.FAILED
        assert GroupStatus.FAILED_TERMINAL


class TestExtractionStatus:
    """Tests for ExtractionStatus enum."""

    def test_all_statuses_exist(self):
        """Test all expected extraction statuses exist."""
        assert ExtractionStatus.PENDING
        assert ExtractionStatus.IN_PROGRESS
        assert ExtractionStatus.PASSWORD_NEEDED
        assert ExtractionStatus.COMPLETED
        assert ExtractionStatus.FAILED
        assert ExtractionStatus.FAILED_TERMINAL


class TestPasswordScope:
    """Tests for PasswordScope enum."""

    def test_all_scopes_exist(self):
        """Test all expected password scopes exist."""
        assert PasswordScope.MESSAGE
        assert PasswordScope.NEARBY
        assert PasswordScope.CONVERSATION
        assert PasswordScope.LEARNED
        assert PasswordScope.GLOBAL

    def test_scope_order_matches_priority(self):
        """Test scopes are ordered by expected priority."""
        # MESSAGE should be highest priority (from same message)
        # GLOBAL should be lowest priority
        scopes = list(PasswordScope)
        assert scopes[0] == PasswordScope.MESSAGE
        assert scopes[-1] == PasswordScope.GLOBAL
