"""FileAttachment model - represents a file attached to a message."""

from typing import TYPE_CHECKING, Optional

from sqlalchemy import BigInteger, Boolean, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from telecrime.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from telecrime.models.artifact import DownloadArtifact
    from telecrime.models.message import Message


class FileAttachment(Base, TimestampMixin):
    """A file attached to a message."""

    __tablename__ = "file_attachments"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign key to message
    message_id: Mapped[int] = mapped_column(
        ForeignKey("messages.id", ondelete="CASCADE"), index=True
    )

    # Telegram-specific file identifiers
    platform_file_id: Mapped[str] = mapped_column(String(255), index=True)
    platform_file_unique_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    access_hash: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # File metadata
    filename: Mapped[str | None] = mapped_column(String(255), nullable=True)
    mime_type: Mapped[str | None] = mapped_column(String(100), nullable=True)
    size: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # Archive detection
    is_archive_candidate: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    archive_type: Mapped[str | None] = mapped_column(String(50), nullable=True)  # zip, 7z, rar, etc.

    # Part detection (for split archives)
    detected_part_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    detected_base_name: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # Relationships
    message: Mapped["Message"] = relationship("Message", back_populates="attachments")
    download_artifact: Mapped[Optional["DownloadArtifact"]] = relationship(
        "DownloadArtifact", back_populates="attachment", uselist=False
    )

    def __repr__(self) -> str:
        return f"<FileAttachment(id={self.id}, filename={self.filename!r}, size={self.size})>"
