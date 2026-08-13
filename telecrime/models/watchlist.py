"""WatchlistItem model — persisted in the main database."""

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from telecrime.models.base import Base, TimestampMixin


class WatchlistItem(Base, TimestampMixin):
    """A saved search query that is checked periodically for new matches."""

    __tablename__ = "watchlist_items"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    label: Mapped[str] = mapped_column(String(500), nullable=False)
    query: Mapped[str] = mapped_column(String(1000), nullable=False)
    match_type: Mapped[str] = mapped_column(String(20), nullable=False, default="any")
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, index=True)
    last_checked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_known_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    new_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_viewed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_alerted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_alerted_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    def __repr__(self) -> str:
        return f"<WatchlistItem(id={self.id}, label={self.label!r}, query={self.query!r})>"
