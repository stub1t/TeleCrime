"""SystemInfoRecord model — machine metadata parsed from SystemInfo.txt."""

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from telecrime.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from telecrime.models.extraction import ExtractionJob


class SystemInfoRecord(Base, TimestampMixin):
    """Machine metadata extracted from a stealer log's SystemInfo.txt file.

    One record per ExtractionJob (unique constraint on extraction_job_id).
    """

    __tablename__ = "system_info"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # One SystemInfo record per extraction job
    extraction_job_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("extraction_jobs.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )

    # Machine fields from SystemInfo.txt
    hostname: Mapped[str | None] = mapped_column(String(255), nullable=True)
    username: Mapped[str | None] = mapped_column(String(255), nullable=True)  # OS username
    ip_address: Mapped[str | None] = mapped_column(String(50), nullable=True)
    country: Mapped[str | None] = mapped_column(String(100), nullable=True, index=True)
    hwid: Mapped[str | None] = mapped_column(String(255), nullable=True)
    os: Mapped[str | None] = mapped_column(String(255), nullable=True)
    cpu: Mapped[str | None] = mapped_column(String(255), nullable=True)
    gpu: Mapped[str | None] = mapped_column(String(255), nullable=True)
    ram: Mapped[str | None] = mapped_column(String(50), nullable=True)
    timezone: Mapped[str | None] = mapped_column(String(100), nullable=True)
    language: Mapped[str | None] = mapped_column(String(50), nullable=True)
    screen_size: Mapped[str | None] = mapped_column(String(50), nullable=True)
    log_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Self-identification from SystemInfo.txt (highest confidence stealer detection)
    stealer_name: Mapped[str | None] = mapped_column(String(100), nullable=True, index=True)

    # Relationship
    extraction_job: Mapped["ExtractionJob"] = relationship(
        "ExtractionJob", back_populates="system_info"
    )

    def __repr__(self) -> str:
        return (
            f"<SystemInfoRecord(id={self.id}, job_id={self.extraction_job_id}, "
            f"country={self.country!r}, stealer={self.stealer_name!r})>"
        )
