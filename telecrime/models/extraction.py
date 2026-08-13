"""ExtractionJob and ExtractedOutput models - archive extraction tracking."""

from typing import TYPE_CHECKING, Optional

from sqlalchemy import BigInteger, Enum, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from telecrime.models.base import Base, TimestampMixin
from telecrime.states import ExtractionStatus

if TYPE_CHECKING:
    from telecrime.models.archive_group import ArchiveGroup
    from telecrime.models.credential import ParsedCredential
    from telecrime.models.password import PasswordCandidate
    from telecrime.models.system_info import SystemInfoRecord


class ExtractionJob(Base, TimestampMixin):
    """An extraction attempt for an archive group."""

    __tablename__ = "extraction_jobs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign key to archive group
    group_id: Mapped[int] = mapped_column(
        ForeignKey("archive_groups.id", ondelete="CASCADE"), index=True
    )

    # Extraction state
    status: Mapped[ExtractionStatus] = mapped_column(
        Enum(ExtractionStatus), default=ExtractionStatus.PENDING, index=True
    )

    # Password used (if any)
    used_password_id: Mapped[int | None] = mapped_column(
        ForeignKey("password_candidates.id", ondelete="SET NULL"), nullable=True
    )
    password_attempts: Mapped[int] = mapped_column(Integer, default=0)

    # Extractor info
    extractor_name: Mapped[str] = mapped_column(String(50), default="7z")
    extractor_version: Mapped[str | None] = mapped_column(String(50), nullable=True)

    # Attempt tracking
    attempts_count: Mapped[int] = mapped_column(Integer, default=0)
    last_error_code: Mapped[str | None] = mapped_column(String(50), nullable=True)
    last_error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Target extraction settings
    target_extensions: Mapped[str | None] = mapped_column(String(255), nullable=True)  # Comma-separated

    # Relationships
    group: Mapped["ArchiveGroup"] = relationship("ArchiveGroup", back_populates="extraction_jobs")
    used_password: Mapped[Optional["PasswordCandidate"]] = relationship("PasswordCandidate")
    outputs: Mapped[list["ExtractedOutput"]] = relationship(
        "ExtractedOutput", back_populates="job", cascade="all, delete-orphan"
    )
    parsed_credentials: Mapped[list["ParsedCredential"]] = relationship(
        "ParsedCredential", back_populates="extraction_job", cascade="all, delete-orphan"
    )
    system_info: Mapped[Optional["SystemInfoRecord"]] = relationship(
        "SystemInfoRecord",
        back_populates="extraction_job",
        uselist=False,
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return f"<ExtractionJob(id={self.id}, group={self.group_id}, status={self.status})>"


class ExtractedOutput(Base, TimestampMixin):
    """A file extracted from an archive."""

    __tablename__ = "extracted_outputs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign key to extraction job
    job_id: Mapped[int] = mapped_column(
        ForeignKey("extraction_jobs.id", ondelete="CASCADE"), index=True
    )

    # Output file info
    output_path: Mapped[str] = mapped_column(String(1024))
    output_filename: Mapped[str] = mapped_column(String(255))
    output_type: Mapped[str | None] = mapped_column(String(50), nullable=True)  # Extension
    output_size: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    output_hash: Mapped[str] = mapped_column(String(64), index=True)  # SHA256

    # Provenance - which message/conversation this came from
    source_conversation_id: Mapped[int] = mapped_column(
        ForeignKey("conversations.id", ondelete="SET NULL"), nullable=True
    )
    source_message_id: Mapped[int] = mapped_column(
        ForeignKey("messages.id", ondelete="SET NULL"), nullable=True
    )

    # Relationships
    job: Mapped["ExtractionJob"] = relationship("ExtractionJob", back_populates="outputs")

    def __repr__(self) -> str:
        return f"<ExtractedOutput(id={self.id}, filename={self.output_filename!r})>"
