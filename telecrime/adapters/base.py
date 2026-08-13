"""Base adapter interface for platform-specific implementations."""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class ConversationInfo:
    """Platform-agnostic conversation information."""

    platform_id: int
    access_hash: int | None
    title: str | None
    username: str | None
    conversation_type: str  # user, group, supergroup, channel
    is_member: bool
    is_accessible: bool


@dataclass
class MessageInfo:
    """Platform-agnostic message information."""

    platform_id: int
    conversation_id: int
    timestamp: datetime
    text: str | None
    caption: str | None
    is_forwarded: bool
    forwarded_from_id: int | None
    forwarded_from_name: str | None
    forwarded_message_id: int | None

    # Additional metadata
    views: int | None = None
    forwards: int | None = None
    edit_date: datetime | None = None
    post_author: str | None = None
    grouped_id: int | None = None


@dataclass
class FileInfo:
    """Platform-agnostic file attachment information."""

    platform_file_id: str
    platform_file_unique_id: str | None
    access_hash: int | None
    filename: str | None
    mime_type: str | None
    size: int | None


class BaseAdapter(ABC):
    """Abstract base class for platform adapters."""

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the platform."""
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the platform."""
        ...

    @abstractmethod
    def iter_conversations(self) -> AsyncIterator[ConversationInfo]:
        """Iterate over all accessible conversations."""
        ...

    @abstractmethod
    def iter_messages(
        self,
        conversation_id: int,
        min_id: int | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[tuple[MessageInfo, list[FileInfo]]]:
        """Iterate over messages in a conversation.

        Args:
            conversation_id: Platform conversation ID
            min_id: Only return messages with ID > min_id (for incremental fetching)
            limit: Maximum number of messages to return

        Yields:
            Tuple of (message_info, list of file attachments)
        """
        ...

    @abstractmethod
    async def download_message_media(
        self,
        conversation_id: int,
        message_id: int,
        destination: Path,
        progress_callback: Callable[[int, int], None] | None = None,
        timeout_seconds: int | None = None,
        stall_seconds: int = 300,
    ) -> bool:
        """Download media from a specific message."""
        ...

    @abstractmethod
    async def resolve_forwarded_source(
        self,
        forwarded_from_id: int,
    ) -> ConversationInfo | None:
        """Resolve the source conversation for a forwarded message.

        Args:
            forwarded_from_id: Platform ID of the forwarded source

        Returns:
            ConversationInfo if resolvable, None otherwise
        """
        ...

    @abstractmethod
    async def join_conversation(
        self,
        conversation_id: int,
        username: str | None = None,
    ) -> bool:
        """Attempt to join a conversation.

        Args:
            conversation_id: Platform conversation ID
            username: Optional username/invite link

        Returns:
            True if join succeeded, False otherwise
        """
        ...

    @abstractmethod
    async def get_entity(self, target: int | str) -> object | None:
        """Return the raw platform entity for a username, invite, or platform ID."""
        ...
