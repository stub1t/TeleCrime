"""ArchiveGroup and ArchiveGroupPart models - for multi-part archive handling."""

from typing import TYPE_CHECKING

from sqlalchemy import Enum, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from telecrime.models.base import Base, TimestampMixin
from telecrime.states import GroupStatus

if TYPE_CHECKING:
    from telecrime.models.artifact import DownloadArtifact
    from telecrime.models.extraction import ExtractionJob


class ArchiveGroup(Base, TimestampMixin):
    """A group of related archive parts (or a single-part archive)."""

    __tablename__ = "archive_groups"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Group identification
    fingerprint: Mapped[str] = mapped_column(String(64), unique=True, index=True)  # Hash of part hashes
    base_name: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # Part tracking
    expected_part_count: Mapped[int] = mapped_column(Integer, default=1)
    detected_part_count: Mapped[int] = mapped_column(Integer, default=0)

    # Group state
    status: Mapped[GroupStatus] = mapped_column(
        Enum(GroupStatus), default=GroupStatus.INCOMPLETE, index=True
    )
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Denormalized count set at finalize time; avoids expensive COUNT JOIN in the web UI.
    credential_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")

    # Relationships
    parts: Mapped[list["ArchiveGroupPart"]] = relationship(
        "ArchiveGroupPart", back_populates="group", cascade="all, delete-orphan"
    )
    extraction_jobs: Mapped[list["ExtractionJob"]] = relationship(
        "ExtractionJob", back_populates="group", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return (
            f"<ArchiveGroup(id={self.id}, status={self.status}, "
            f"parts={self.detected_part_count}/{self.expected_part_count})>"
        )


class ArchiveGroupPart(Base, TimestampMixin):
    """A single part within an archive group."""

    __tablename__ = "archive_group_parts"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign keys
    group_id: Mapped[int] = mapped_column(
        ForeignKey("archive_groups.id", ondelete="CASCADE"), index=True
    )
    artifact_id: Mapped[int] = mapped_column(
        ForeignKey("download_artifacts.id", ondelete="CASCADE"), unique=True, index=True
    )

    # Part info
    part_index: Mapped[int] = mapped_column(Integer)  # 0-indexed
    role: Mapped[str] = mapped_column(String(50), default="part")  # main, part, volume

    # Relationships
    group: Mapped["ArchiveGroup"] = relationship("ArchiveGroup", back_populates="parts")
    artifact: Mapped["DownloadArtifact"] = relationship(
        "DownloadArtifact", back_populates="group_part"
    )

    def __repr__(self) -> str:
        return f"<ArchiveGroupPart(id={self.id}, group={self.group_id}, index={self.part_index})>"
