"""Persisted pipeline run summaries."""

from datetime import datetime

from sqlalchemy import DateTime, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from telecrime.models.base import Base, TimestampMixin


class PipelineRun(Base, TimestampMixin):
    """Summary of one pipeline execution."""

    __tablename__ = "pipeline_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    mode: Mapped[str] = mapped_column(String(20), index=True)
    status: Mapped[str] = mapped_column(String(20), index=True, default="running")
    dry_run: Mapped[int] = mapped_column(Integer, default=0)

    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)

    stages_completed_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    stages_failed_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    errors_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    conversations_processed: Mapped[int] = mapped_column(Integer, default=0)
    messages_processed: Mapped[int] = mapped_column(Integer, default=0)
    files_discovered: Mapped[int] = mapped_column(Integer, default=0)
    files_downloaded: Mapped[int] = mapped_column(Integer, default=0)
    archives_extracted: Mapped[int] = mapped_column(Integer, default=0)
    credentials_parsed: Mapped[int] = mapped_column(Integer, default=0)
    duplicates_skipped: Mapped[int] = mapped_column(Integer, default=0)

    def __repr__(self) -> str:
        return f"<PipelineRun(id={self.id}, mode={self.mode}, status={self.status})>"
