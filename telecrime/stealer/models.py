"""Data models for stealer log parsing."""

from dataclasses import dataclass, field
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

    def to_dict(self) -> dict[str, object]:
        """Convert to dictionary."""
        return {
            "url": self.url,
            "username": self.username,
            "password": self.password,
            "domain": self.domain,
            "email_domain": self.email_domain,
            "application": self.application,
            "profile": self.profile,
            "source_file": self.source_file,
            "line_number": self.line_number,
        }


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

    def to_dict(self) -> dict[str, object]:
        """Convert to dictionary."""
        return {
            "hostname": self.hostname,
            "username": self.username,
            "ip_address": self.ip_address,
            "country": self.country,
            "hwid": self.hwid,
            "os": self.os,
            "cpu": self.cpu,
            "gpu": self.gpu,
            "ram": self.ram,
            "timezone": self.timezone,
            "language": self.language,
            "screen_size": self.screen_size,
            "log_date": self.log_date.isoformat() if self.log_date else None,
            "stealer_name": self.stealer_name,
        }


@dataclass
class StealerLog:
    """Parsed stealer log containing credentials and system info."""

    credentials: list[Credential] = field(default_factory=list)
    system_info: SystemInfo | None = None
    stealer_name: str | None = None
    source_archive: str | None = None
    parse_errors: list[str] = field(default_factory=list)

    @property
    def credential_count(self) -> int:
        return len(self.credentials)

    @property
    def unique_domains(self) -> set[str]:
        return {c.domain for c in self.credentials if c.domain}

    def to_dict(self) -> dict[str, object]:
        """Convert to dictionary."""
        return {
            "stealer_name": self.stealer_name,
            "source_archive": self.source_archive,
            "credential_count": self.credential_count,
            "unique_domains": list(self.unique_domains),
            "credentials": [c.to_dict() for c in self.credentials],
            "system_info": self.system_info.to_dict() if self.system_info else None,
            "parse_errors": self.parse_errors,
        }
