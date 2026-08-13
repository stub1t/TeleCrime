"""Conversation model - represents a Telegram chat/channel."""

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, Boolean, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from telecrime.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from telecrime.models.message import Message


class Conversation(Base, TimestampMixin):
    """A Telegram conversation (chat, group, or channel)."""

    __tablename__ = "conversations"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Telegram-specific identifiers
    platform_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)
    access_hash: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # Conversation metadata
    title: Mapped[str | None] = mapped_column(String(255), nullable=True)
    username: Mapped[str | None] = mapped_column(String(255), nullable=True)
    conversation_type: Mapped[str] = mapped_column(String(50))  # user, group, supergroup, channel

    # Access state
    is_member: Mapped[bool] = mapped_column(Boolean, default=True)
    is_accessible: Mapped[bool] = mapped_column(Boolean, default=True)
    join_attempted: Mapped[bool] = mapped_column(Boolean, default=False)
    join_succeeded: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    # Ingestion cursor - last processed message ID for incremental runs
    last_ingested_message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    last_ingested_at: Mapped[datetime | None] = mapped_column(nullable=True)

    # Notes
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Relationships
    messages: Mapped[list["Message"]] = relationship(
        "Message", back_populates="conversation", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Conversation(id={self.id}, platform_id={self.platform_id}, title={self.title!r})>"
