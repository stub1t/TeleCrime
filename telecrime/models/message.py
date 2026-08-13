"""Message model - represents a single message in a conversation."""

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from telecrime.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from telecrime.models.attachment import FileAttachment
    from telecrime.models.conversation import Conversation
    from telecrime.models.password import PasswordCandidate


class Message(Base, TimestampMixin):
    """A message within a conversation."""

    __tablename__ = "messages"
    __table_args__ = (
        UniqueConstraint(
            "conversation_id", "platform_id", name="uq_messages_conversation_platform"
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign key to conversation
    conversation_id: Mapped[int] = mapped_column(
        ForeignKey("conversations.id", ondelete="CASCADE"), index=True
    )

    # Telegram-specific identifiers
    platform_id: Mapped[int] = mapped_column(BigInteger, index=True)
    platform_timestamp: Mapped[datetime] = mapped_column(index=True)

    # Message content
    text: Mapped[str | None] = mapped_column(Text, nullable=True)
    caption: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Telegram metadata
    views: Mapped[int | None] = mapped_column(Integer, nullable=True)
    forwards: Mapped[int | None] = mapped_column(Integer, nullable=True)
    edit_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    post_author: Mapped[str | None] = mapped_column(String(255), nullable=True)
    grouped_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # Forwarded message info
    is_forwarded: Mapped[bool] = mapped_column(Boolean, default=False)
    forwarded_from_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    forwarded_from_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    forwarded_message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # Processing state
    is_processed: Mapped[bool] = mapped_column(Boolean, default=False, index=True)

    # Relationships
    conversation: Mapped["Conversation"] = relationship("Conversation", back_populates="messages")
    attachments: Mapped[list["FileAttachment"]] = relationship(
        "FileAttachment", back_populates="message", cascade="all, delete-orphan"
    )
    password_candidates: Mapped[list["PasswordCandidate"]] = relationship(
        "PasswordCandidate",
        back_populates="source_message",
        foreign_keys="PasswordCandidate.source_message_id",
    )

    def __repr__(self) -> str:
        return (
            f"<Message(id={self.id}, platform_id={self.platform_id}, conv={self.conversation_id})>"
        )
