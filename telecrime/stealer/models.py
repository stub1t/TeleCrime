"""Data models for stealer log parsing."""

from dataclasses import dataclass
from datetime import datetime


@dataclass
class Credential:
    """A single credential extracted from stealer logs."""

    url: str
    username: str
    password: str
    application: str | None = None
    profile: str | None = None

    # Derived fields
    domain: str | None = None
    email_domain: str | None = None

    # Source tracking
    source_file: str | None = None
    line_number: int | None = None

    def __post_init__(self) -> None:
        """Extract domain from URL and email."""
        if self.url and not self.domain:
            # All credentials reaching this point have http/https URLs
            # (enforced by _normalize_url before Credential creation).
            # A simple prefix-strip + split("/")[0] is ~15x faster than
            # urllib.parse.urlparse for the typical "https://host/path" case.
            url = self.url
            if url.startswith("https://"):
                self.domain = url[8:].split("/")[0]
            elif url.startswith("http://"):
                self.domain = url[7:].split("/")[0]
            else:
                # Fallback for any edge-case scheme (preserves old behaviour):
                # strip "scheme://" if present, then take everything up to the
                # first "/" — matching urlparse's netloc-or-path semantics.
                rest = url[url.find("//") + 2 :] if "//" in url else url
                self.domain = rest.split("/")[0]

        if self.username and "@" in self.username and not self.email_domain:
            try:
                self.email_domain = self.username.split("@")[1].lower()
            except Exception:
                pass

@dataclass
class SystemInfo:
    """System information extracted from stealer logs."""

    hostname: str | None = None
    username: str | None = None
    ip_address: str | None = None
    country: str | None = None
    hwid: str | None = None
    os: str | None = None
    cpu: str | None = None
    gpu: str | None = None
    ram: str | None = None
    timezone: str | None = None
    language: str | None = None
    screen_size: str | None = None
    log_date: datetime | None = None
    # Stealer self-identification from SystemInfo.txt (highest-confidence detection)
    stealer_name: str | None = None
