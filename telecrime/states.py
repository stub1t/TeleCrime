"""State machine definitions for Telecrime pipeline."""

from enum import Enum


class DownloadStatus(str, Enum):
    """Status of a download artifact."""

    PENDING = "pending"           # Not yet started
    DOWNLOADING = "downloading"   # Currently downloading
    COMPLETED = "completed"       # Downloaded and verified
    FAILED = "failed"             # Download failed (retryable)
    FAILED_TERMINAL = "failed_terminal"  # Download failed permanently


class GroupStatus(str, Enum):
    """Status of an archive group."""

    INCOMPLETE = "incomplete"     # Missing parts
    READY = "ready"               # All parts present and verified
    EXTRACTING = "extracting"     # Extraction in progress
    EXTRACTED = "extracted"       # Extraction completed
    CLEANED = "cleaned"           # Archive parts deleted after extraction
    FAILED = "failed"             # Extraction failed (retryable)
    FAILED_TERMINAL = "failed_terminal"  # Failed permanently


class ExtractionStatus(str, Enum):
    """Status of an extraction job."""

    PENDING = "pending"           # Not yet started
    IN_PROGRESS = "in_progress"   # Currently extracting
    PASSWORD_NEEDED = "password_needed"  # Needs password, none worked yet
    COMPLETED = "completed"       # Extraction succeeded
    FAILED = "failed"             # Extraction failed (retryable)
    FAILED_TERMINAL = "failed_terminal"  # Failed permanently (corrupt, etc.)


class PasswordScope(str, Enum):
    """Scope of a password candidate."""

    MESSAGE = "message"           # From the same message as the file
    NEARBY = "nearby"             # From nearby messages in conversation
    CONVERSATION = "conversation" # Conversation-level (pinned, etc.)
    LEARNED = "learned"           # Previously successful in this conversation
    GLOBAL = "global"             # Global default passwords
