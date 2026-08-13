"""Telegram channel tracking model."""

from datetime import UTC, datetime

from sqlalchemy import BigInteger, Boolean, DateTime, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from telecrime.models.base import Base


class TelegramChannel(Base):
    """Track Telegram channels that post stealer logs."""

    __tablename__ = "telegram_channels"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # Channel identification
    platform_id: Mapped[int | None] = mapped_column(
        BigInteger, unique=True, nullable=True, index=True
    )
    username: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    title: Mapped[str | None] = mapped_column(String(500), nullable=True)
    invite_link: Mapped[str | None] = mapped_column(String(500), nullable=True)

    # Discovery info
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    discovered_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(UTC)
    )
    discovered_from: Mapped[str | None] = mapped_column(String(500), nullable=True)

    # Status tracking
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    is_accessible: Mapped[bool] = mapped_column(Boolean, default=True)
    is_subscribed: Mapped[bool] = mapped_column(Boolean, default=False)
    last_checked: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    check_error: Mapped[str | None] = mapped_column(String(500), nullable=True)

    # Metadata
    member_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Stats from our data
    messages_seen: Mapped[int] = mapped_column(Integer, default=0)
    archives_seen: Mapped[int] = mapped_column(Integer, default=0)
    credentials_extracted: Mapped[int] = mapped_column(Integer, default=0)

    __table_args__ = (Index("ix_telegram_channels_username_lower", "username"),)

    def __repr__(self) -> str:
        username = getattr(self, "username")
        platform_id = getattr(self, "platform_id")
        handle = f"@{username}" if username else f"id:{platform_id}"
        return f"<TelegramChannel {handle} ({self.source})>"

    @property
    def display_name(self) -> str:
        """Get display name for the channel."""
        username = getattr(self, "username")
        title = getattr(self, "title")
        platform_id = getattr(self, "platform_id")
        invite_link = getattr(self, "invite_link")
        if username:
            return f"@{username}"
        if title:
            return title
        if platform_id:
            return f"id:{platform_id}"
        if invite_link:
            return invite_link
        return "(unknown)"

    @property
    def telegram_link(self) -> str | None:
        """Get t.me link for the channel."""
        username = getattr(self, "username")
        invite_link = getattr(self, "invite_link")
        if username:
            return f"https://t.me/{username}"
        if invite_link:
            return invite_link
        return None
