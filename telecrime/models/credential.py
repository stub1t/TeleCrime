"""Parsed credential model for stealer log data."""

import hashlib
from functools import lru_cache
from urllib.parse import urlparse

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    func,
)
from sqlalchemy.orm import relationship

from telecrime.models.base import Base


class ParsedCredential(Base):
    """Credential parsed from stealer logs."""

    __tablename__ = "parsed_credentials"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # The credential data
    url = Column(String(1024), nullable=False)
    domain = Column(String(255), nullable=True)
    username = Column(String(255), nullable=False)
    password = Column(String(255), nullable=False)

    # Deduplication hash (SHA256 of domain-or-url|username|password)
    credential_hash = Column(String(64), unique=True, index=True, nullable=True)
    soft_credential_hash = Column(String(64), nullable=True)

    # Email domain if username is an email
    email_domain = Column(String(255), nullable=True)

    # Application info (browser, etc.)
    # NOTE: index declared for schema completeness, but migration
    # e1f2a3b4c5d6 dropped ix_parsed_credentials_application on purpose
    # (low selectivity, mostly NULL). Fresh create_all builds it; the
    # migration-managed DB does not have it. Do not "fix" by adding a
    # migration that resurrects a 353M-row index.
    application = Column(String(100), nullable=True, index=True)
    profile = Column(String(100), nullable=True)

    # Source tracking
    # NOTE: index declared for schema completeness, but the live DB dropped the
    # single-column ix_parsed_credentials_extraction_job_id during the 2026-09
    # recovery rebuild; ix_parsed_credentials_job_file (extraction_job_id,
    # source_file) covers extraction_job_id-prefix lookups. That compound index
    # comes from migration e1f2a3b4c5d6 and is the "already parsed this file?"
    # path — it is intentionally not declared here, so do NOT let autogenerate
    # drop it as unknown.
    extraction_job_id = Column(
        Integer,
        ForeignKey("extraction_jobs.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    source_file = Column(String(512), nullable=True)
    # NOTE: index declared for schema completeness. d4e5f6a7b8c9 created
    # ix_parsed_credentials_source_archive (btree) and no migration drops it;
    # the 2026-09 recovery rebuild left it out. u1v2w3x4y5z6 dropped only the
    # source_archive trigram GIN. Not worth a 353M-row rebuild unless a query
    # needs the btree.
    source_archive = Column(String(512), nullable=True, index=True)

    # Original message/conversation tracking
    # NOTE: index intentionally NOT declared — migration x4y5z6a7b8c9 dropped
    # ix_parsed_credentials_source_conversation_id (zero scans, 1.5GB), and
    # web/app.py._ensure_stats_indexes explicitly refuses to recreate it.
    # Re-adding it here would resurrect it on the next create_all/autogenerate.
    # The FK is ON DELETE SET NULL, but no production code path deletes
    # conversations, so the FK-trigger scan is latent.
    source_conversation_id = Column(
        Integer,
        ForeignKey("conversations.id", ondelete="SET NULL"),
        nullable=True,
    )
    # FK is ON DELETE SET NULL and the index is required for the FK trigger
    # (deleting a message issues UPDATE ... WHERE source_message_id = ?; with
    # 353M rows an unindexed column means one full scan per deleted message).
    # migration d4e5f6a7b8c9 created ix_parsed_credentials_source_message_id
    # and no migration drops it, but the 2026-09 recovery rebuild lost it;
    # migration b3c4d5e6f7a8 recreates it CONCURRENTLY.
    source_message_id = Column(
        Integer,
        ForeignKey("messages.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    # Stealer type if detected
    # NOTE: index declared for schema completeness, but migration
    # e1f2a3b4c5d6 dropped ix_parsed_credentials_stealer_type on purpose
    # (6 distinct values). Fresh create_all builds it; the migration-managed
    # DB does not have it.
    stealer_type = Column(String(50), nullable=True, index=True)

    # Timestamps
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    # Relationships
    # Both are lazy many-to-one; touching them while iterating credential rows
    # is an N+1. No hot path does today (CLI export joins explicitly); if one
    # ever needs them, use joinedload/selectinload at the query site.
    extraction_job = relationship("ExtractionJob", back_populates="parsed_credentials")
    source_conversation = relationship("Conversation")
    source_message = relationship("Message")

    @staticmethod
    def compute_hash(domain: str, username: str, password: str) -> str:
        """Compute SHA256 hash of domain|username|password for deduplication."""
        raw = f"{domain}|{username}|{password}"
        return hashlib.sha256(raw.encode("utf-8", errors="replace")).hexdigest()

    @staticmethod
    @lru_cache(maxsize=65536)
    def _normalize_soft_domain(domain_or_url: str) -> str:
        """Normalize a domain or URL host for analytics-side grouping."""
        value = (domain_or_url or "").strip()
        if not value:
            return ""
        try:
            parsed = urlparse(value)
            host = parsed.netloc or parsed.path.split("/")[0]
        except ValueError:
            return value.split("/")[0][:255]
        host = host.lower().strip()
        if host.startswith("www."):
            host = host[4:]
        return host

    @staticmethod
    def compute_soft_hash(domain_or_url: str, username: str, password: str) -> str:
        """Compute a softer grouping hash for search/analytics use.

        Canonical write-time dedup remains `credential_hash`.
        """
        normalized_domain = ParsedCredential._normalize_soft_domain(domain_or_url)
        normalized_user = (username or "").strip().casefold()
        normalized_pass = (password or "").strip()
        raw = f"{normalized_domain}|{normalized_user}|{normalized_pass}"
        return hashlib.sha256(raw.encode("utf-8", errors="replace")).hexdigest()

    def __repr__(self) -> str:
        username = self.username[:20] if self.username else None
        return f"<ParsedCredential(id={self.id}, domain={self.domain}, username={username})>"
