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
    application = Column(String(100), nullable=True, index=True)
    profile = Column(String(100), nullable=True)

    # Source tracking
    extraction_job_id = Column(
        Integer,
        ForeignKey("extraction_jobs.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    source_file = Column(String(512), nullable=True)
    source_archive = Column(String(512), nullable=True, index=True)

    # Original message/conversation tracking
    # NOTE: index intentionally NOT declared — migration x4y5z6a7b8c9 dropped
    # ix_parsed_credentials_source_conversation_id (zero scans, 1.5GB).
    # Re-adding it here would resurrect it on the next create_all/autogenerate.
    source_conversation_id = Column(
        Integer,
        ForeignKey("conversations.id", ondelete="SET NULL"),
        nullable=True,
    )
    source_message_id = Column(
        Integer,
        ForeignKey("messages.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    # Stealer type if detected
    stealer_type = Column(String(50), nullable=True, index=True)

    # Timestamps
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    # Relationships
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
