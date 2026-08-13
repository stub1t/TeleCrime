"""PasswordCandidate model - inferred passwords from message context."""

from typing import TYPE_CHECKING, Optional

from sqlalchemy import Enum, Float, ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from telecrime.models.base import Base, TimestampMixin
from telecrime.states import PasswordScope

if TYPE_CHECKING:
    from telecrime.models.message import Message


class PasswordCandidate(Base, TimestampMixin):
    """A password candidate extracted from message context."""

    __tablename__ = "password_candidates"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Password value (plaintext for local use)
    value: Mapped[str] = mapped_column(String(255))

    # Scope and source
    scope: Mapped[PasswordScope] = mapped_column(Enum(PasswordScope), index=True)
    source_message_id: Mapped[int | None] = mapped_column(
        ForeignKey("messages.id", ondelete="SET NULL"), nullable=True, index=True
    )
    conversation_id: Mapped[int | None] = mapped_column(
        ForeignKey("conversations.id", ondelete="SET NULL"), nullable=True, index=True
    )

    # Extraction context
    extraction_method: Mapped[str] = mapped_column(String(50))  # caption, nearby, pinned, learned
    context_text: Mapped[str | None] = mapped_column(Text, nullable=True)  # Surrounding text

    # Ranking
    confidence: Mapped[float] = mapped_column(Float, default=0.5)

    # Usage tracking
    times_succeeded: Mapped[int] = mapped_column(default=0)
    times_failed: Mapped[int] = mapped_column(default=0)

    # Relationships
    source_message: Mapped[Optional["Message"]] = relationship(
        "Message",
        back_populates="password_candidates",
        foreign_keys=[source_message_id],
    )

    def __repr__(self) -> str:
        return f"<PasswordCandidate(id={self.id}, scope={self.scope}, confidence={self.confidence:.2f})>"
