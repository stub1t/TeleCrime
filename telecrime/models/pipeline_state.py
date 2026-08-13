"""Pipeline state checkpoints for incremental processing."""

from sqlalchemy import Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from telecrime.models.base import Base, TimestampMixin


class PipelineState(Base, TimestampMixin):
    """Store small persisted checkpoints for pipeline/runtime tasks."""

    __tablename__ = "pipeline_state"

    key: Mapped[str] = mapped_column(String(100), primary_key=True)
    value_int: Mapped[int | None] = mapped_column(Integer, nullable=True)
    value_text: Mapped[str | None] = mapped_column(Text, nullable=True)

    def __repr__(self) -> str:
        return f"<PipelineState(key={self.key!r})>"
