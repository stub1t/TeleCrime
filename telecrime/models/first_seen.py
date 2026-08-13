"""FirstSeenIndex model - duplicate detection by content hash."""

from datetime import datetime

from sqlalchemy import BigInteger, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from telecrime.models.base import Base, TimestampMixin


class FirstSeenIndex(Base, TimestampMixin):
    """Index for tracking first appearance of content by hash."""

    __tablename__ = "first_seen_index"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Content identification
    content_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)  # SHA256
    content_type: Mapped[str] = mapped_column(String(50))  # download, extracted

    # First seen location
    first_seen_timestamp: Mapped[datetime] = mapped_column()
    first_seen_conversation_id: Mapped[int | None] = mapped_column(
        ForeignKey("conversations.id", ondelete="SET NULL"), nullable=True
    )
    first_seen_message_id: Mapped[int | None] = mapped_column(
        ForeignKey("messages.id", ondelete="SET NULL"), nullable=True
    )
    first_seen_message_platform_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # Duplicate count
    duplicate_count: Mapped[int] = mapped_column(default=0)

    def __repr__(self) -> str:
        return f"<FirstSeenIndex(hash={self.content_hash[:8]}..., first={self.first_seen_timestamp})>"
