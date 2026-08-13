"""DownloadArtifact model - represents a downloaded file."""

from typing import TYPE_CHECKING, Optional

from sqlalchemy import BigInteger, Enum, ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from telecrime.models.base import Base, TimestampMixin
from telecrime.states import DownloadStatus

if TYPE_CHECKING:
    from telecrime.models.archive_group import ArchiveGroupPart
    from telecrime.models.attachment import FileAttachment


class DownloadArtifact(Base, TimestampMixin):
    """A downloaded file on the local filesystem."""

    __tablename__ = "download_artifacts"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign key to attachment
    attachment_id: Mapped[int] = mapped_column(
        ForeignKey("file_attachments.id", ondelete="CASCADE"), unique=True, index=True
    )

    # Local file info
    local_path: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    temp_path: Mapped[str | None] = mapped_column(String(1024), nullable=True)

    # Verification
    verified_size: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    content_hash: Mapped[str | None] = mapped_column(String(64), index=True, nullable=True)  # SHA256

    # Download state
    status: Mapped[DownloadStatus] = mapped_column(
        Enum(DownloadStatus), default=DownloadStatus.PENDING, index=True
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    retry_count: Mapped[int] = mapped_column(default=0)

    # Cleanup tracking
    is_deleted: Mapped[bool] = mapped_column(default=False)

    # Relationships
    attachment: Mapped["FileAttachment"] = relationship(
        "FileAttachment", back_populates="download_artifact"
    )
    group_part: Mapped[Optional["ArchiveGroupPart"]] = relationship(
        "ArchiveGroupPart", back_populates="artifact", uselist=False
    )

    def __repr__(self) -> str:
        return f"<DownloadArtifact(id={self.id}, status={self.status}, path={self.local_path!r})>"
