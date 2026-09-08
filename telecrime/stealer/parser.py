"""Parser for stealer log credential files."""

import logging
import re
from collections.abc import Generator, Iterator
from pathlib import Path
from typing import TextIO

from telecrime.stealer.models import Credential, SystemInfo

logger = logging.getLogger(__name__)

# Field name variations for each credential component
URL_FIELDS = ["url", "host", "hostname", "link", "site"]
USERNAME_FIELDS = ["username", "login", "user", "email", "usr"]
PASSWORD_FIELDS = ["password", "pass", "pwd"]
APPLICATION_FIELDS = ["soft", "software", "browser", "application", "app"]
PROFILE_FIELDS = ["profile", "path"]

# Line patterns for per-line detection (used in streaming)
_COLON_LINE_RE = re.compile(
    r"^(?P<url>https?://[^\s:]+(?::\d{1,5})?):(?P<username>[^:]+):(?P<password>.+)$"
)
_PIPE_LINE_RE = re.compile(
    r"^(?P<url>https?://[^\s|]+)\s*\|\s*(?P<username>[^|]+)\s*\|\s*(?P<password>.+)$"
)
_SEMICOLON_LINE_RE = re.compile(
    r"^(?P<url>https?://[^\s;]+);(?P<username>[^;]+);(?P<password>.+)$"
)
_LABELED_LINE_RE = re.compile(r"^([A-Za-z_]+)\s*[:=]\s*(.*)$")
_BRACKET_LINE_RE = re.compile(
    r'^\["?(?P<app>[^"=\]]+)"?\s*=\s*"?(?P<profile>[^"\]]+)"?\]'
)
_SEPARATOR_RE = re.compile(r"^(?:---+|===+|_{3,})\s*$")
# Marketplace boilerplate must be preceded by a separator (pipe/┃) or the
# field start — a plain password containing "buy" or "t.me" is legit and must
# NOT be truncated. Verified victims: "I want to buy stuff" → "I want".
_PROMO_MARKERS_RE = re.compile(
    r"(?:^|\s*(?:┃|\|)\s*)(?:https?://)?t\.me/[^\s]+.*$"
    r"|\s*(?:┃|\|)\s*(?:you\s+can\s+buy|to\s+buy|dm\s+@)[^\r\n]*$",
    re.IGNORECASE,
)
# Fast fail-fast trigger scan (C-level, no allocation) replacing the
# unconditional .lower() copy on every field. Intentionally broad — it only
# gates whether the (strict) marker regex runs; it must not filter itself.
_PROMO_TRIGGER_RE = re.compile(r"t\.me|buy|dm\s+@", re.IGNORECASE)
_BRACKET_PROMO_RE = re.compile(r"\s*\[.*?(?:to\s+buy|buy|dm)\b.*$", re.IGNORECASE)

# Exact-match sentinel values that indicate placeholder/garbage rows, not real credentials.
# Only exact case-insensitive matches are rejected — partial matches risk discarding real data.
_GARBAGE_USERNAMES = frozenset({
    "null", "undefined", "user", "username", "example", "sample", "test",
})
_GARBAGE_PASSWORDS = frozenset({
    "null", "undefined", "password", "pass",
})


def _is_garbage_credential(username: str, password: str) -> bool:
    """Return True if username or password is an obvious sentinel/placeholder value."""
    # Both-empty rows carry no credential at all (url+password-only rows are
    # kept — they are real captures from labeled blocks without a Login line).
    if not username and not password:
        return True
    # All garbage sentinel values are ≤15 chars — skip the allocation for long values.
    # HTML/block-char artifacts can appear at any length so they are always checked.
    if len(username) <= 15 and username.lower() in _GARBAGE_USERNAMES:
        return True
    if len(password) <= 15 and password.lower() in _GARBAGE_PASSWORDS:
        return True
    if "<br>" in username or "<br>" in password or "██" in username or "██" in password:
        return True
    return False


def _clean_credential_field(value: str | None, *, username: bool = False) -> str:
    """Remove obvious marketplace boilerplate accidentally captured in credential fields."""
    if not value:
        return ""
    cleaned = value.replace("\x00", "").strip()
    # Promo markers are rare in real credentials. Check for cheap trigger strings
    # before invoking the regex engine — saves ~12-15x on the common clean case.
    if username and "[" in cleaned:
        cleaned = _BRACKET_PROMO_RE.sub("", cleaned).strip()
    # Single case-insensitive C-level scan; no lowercased copy allocated.
    if _PROMO_TRIGGER_RE.search(cleaned):
        cleaned = _PROMO_MARKERS_RE.sub("", cleaned).strip()
    return cleaned


def _make_credential(
    *,
    url: str,
    username: str | None,
    password: str | None,
    source_file: str | None,
    application: str | None = None,
    profile: str | None = None,
) -> Credential:
    # Combo (colon/pipe/semicolon) paths go through this constructor — the
    # labeled-block path already normalizes in _credential_from_fields. A
    # normalized None can't happen here (the combo regexes enforce http(s)).
    _url = _normalize_url(url.strip())
    return Credential(
        url=_url or url.strip(),
        username=_clean_credential_field(username, username=True),
        password=_clean_credential_field(password),
        application=_clean_credential_field(application) or None,
        profile=_clean_credential_field(profile) or None,
        source_file=source_file,
    )


def truncate_field(value: str | None, limit: int) -> str | None:
    """Strip NUL bytes and truncate a credential field to its column limit.

    PostgreSQL rejects NUL bytes in TEXT/VARCHAR values; stealer logs
    occasionally contain them in corrupted rows. The NUL check is short-circuited
    because the overwhelming majority of credential strings contain no NULs and
    `str.replace` allocates a new string regardless of whether anything matched.
    """
    if not value:
        return None
    if "\x00" in value:
        value = value.replace("\x00", "")
    return value[:limit]


def _detect_encoding(hint: str) -> list[str]:
    """Return encoding list to try, with hint first and deduped.

    ORDER IS CRITICAL: any broken-UTF-8 single-byte file decodes as utf-16
    (even-length byte pairs), so utf-16 must come LAST — otherwise latin-1
    files silently decode as utf-16 mojibake and yield zero credentials.
    UTF-16 files with a BOM are handled before this chain runs.
    """
    chain = [hint, "utf-8", "latin-1", "cp1252", "utf-16-le", "utf-16"]
    seen: set[str] = set()
    result = []
    for enc in chain:
        if enc not in seen:
            seen.add(enc)
            result.append(enc)
    return result


def _is_binary_file(file_path: Path) -> bool:
    """Return True if the file appears to be binary (contains null bytes in first 8 KB)."""
    try:
        with open(file_path, "rb") as f:
            chunk = f.read(8192)
        return b"\x00\x00\x00" in chunk  # 3+ consecutive nulls → binary (not UTF-16)
    except OSError:
        return False


def _normalize_url(url: str | None) -> str | None:
    """Strip control characters and validate URL scheme; return None for non-HTTP values."""
    if not url:
        return None
    url = url.strip().strip("\x00\r\n")
    # Reject Windows paths (HOST: C:\...) and other non-HTTP values
    if not url.startswith(("http://", "https://")):
        return None
    # Credentials embedded in the URL ("https://user:pass@example.com") must
    # not leak into the domain column or the dedup hashes.
    scheme, rest = url.split("://", 1)
    if "@" in rest:
        rest = rest.rsplit("@", 1)[1]
        url = f"{scheme}://{rest}"
    return url


def _open_file(file_path: Path, encoding: str) -> TextIO:
    """Open file with given encoding, replacing errors."""
    return open(file_path, encoding=encoding, errors="replace")


def _parse_block_lines(block_lines: list[str]) -> tuple[dict[str, str], bool] | None:
    """Parse a list of lines as a labeled block.

    Returns (fields, has_labeled_field) or None if empty.
    has_labeled_field indicates whether at least one labeled field was found.
    """
    fields: dict[str, str] = {}
    has_labeled = False
    pending_empty_field: str | None = None  # field name whose value was empty

    for line in block_lines:
        line_s = line.strip()
        if not line_s or line_s.startswith("#"):
            pending_empty_field = None
            continue

        # Bracket header
        bm = _BRACKET_LINE_RE.match(line_s)
        if bm:
            fields["application"] = bm.group("app").strip()
            fields["profile"] = bm.group("profile").strip()
            has_labeled = True
            pending_empty_field = None
            continue

        # Labeled field (allow empty value so we can grab continuation)
        lm = _LABELED_LINE_RE.match(line_s)
        if lm:
            field_name = lm.group(1).lower().strip()
            field_value = lm.group(2).strip()
            fields[field_name] = field_value
            has_labeled = True
            # Track fields with empty values — next non-labeled line may be the value
            pending_empty_field = field_name if not field_value else None
            continue

        # Continuation line: a non-labeled line immediately after a field with empty value
        if pending_empty_field:
            fields[pending_empty_field] = line_s
            pending_empty_field = None
            continue

        pending_empty_field = None

    if not fields:
        return None
    return fields, has_labeled


def _credential_from_fields(
    fields: dict[str, str], source_file: str | None
) -> Credential | None:
    """Build a Credential from parsed fields dict, or None if insufficient."""
    url = _normalize_url(_get_field(fields, URL_FIELDS))
    username = _get_field(fields, USERNAME_FIELDS)
    password = _get_field(fields, PASSWORD_FIELDS)

    if url and password:
        return _make_credential(
            url=url,
            username=username or "",
            password=password,
            application=_get_field(fields, APPLICATION_FIELDS),
            profile=_get_field(fields, PROFILE_FIELDS),
            source_file=source_file,
        )
    return None


def _iter_credentials_from_lines(
    lines: Iterator[str],
    source_file: str | None,
) -> Generator[Credential, None, None]:
    """Stream credentials from an iterator of lines.

    Handles labeled-block, bracket-header, colon-separated, and pipe-separated
    formats. Formats can be mixed within the same file.
    """
    block_lines: list[str] = []

    def flush_block() -> Generator[Credential, None, None]:
        """Yield credential from accumulated block_lines if valid."""
        if not block_lines:
            return
        result = _parse_block_lines(block_lines)
        if result is not None:
            fields, has_labeled = result
            if has_labeled:
                cred = _credential_from_fields(fields, source_file)
                if cred is not None:
                    yield cred
        block_lines.clear()

    for raw_line in lines:
        line = raw_line.rstrip("\n").rstrip("\r")
        stripped = line.strip()

        # Blank line or separator = block boundary
        if not stripped or _SEPARATOR_RE.match(stripped):
            yield from flush_block()
            continue

        # Combo-format lines (colon/pipe/semicolon-separated) always start with
        # a URL scheme.  Labeled-block lines start with field names like "URL:",
        # "Host:", "Login:".  Using startswith as a fast discriminator avoids
        # running all three combo regexes on every labeled line — which is the
        # common case in stealer-log files where each labeled block has 4-8 lines.
        if stripped.startswith(("http://", "https://")):
            # Check for inline colon-separated credential
            cm = _COLON_LINE_RE.match(stripped)
            if cm:
                yield from flush_block()
                yield _make_credential(
                    url=cm.group("url").strip(),
                    username=cm.group("username").strip(),
                    password=cm.group("password").strip(),
                    source_file=source_file,
                )
                continue

            # Check for pipe-separated credential
            pm = _PIPE_LINE_RE.match(stripped)
            if pm:
                yield from flush_block()
                yield _make_credential(
                    url=pm.group("url").strip(),
                    username=pm.group("username").strip(),
                    password=pm.group("password").strip(),
                    source_file=source_file,
                )
                continue

            # Check for semicolon-separated credential (ULP/combo list format)
            sm = _SEMICOLON_LINE_RE.match(stripped)
            if sm:
                yield from flush_block()
                yield _make_credential(
                    url=sm.group("url").strip(),
                    username=sm.group("username").strip(),
                    password=sm.group("password").strip(),
                    source_file=source_file,
                )
                continue

        # Accumulate into current block (labeled / bracket header, or a URL
        # line that didn't match any combo pattern)
        block_lines.append(line)

    # Flush final block
    yield from flush_block()


def _iter_credentials_from_file(
    file_path: Path, encoding: str
) -> Generator[Credential, None, None]:
    """Open file with encoding fallback and stream credentials line by line."""
    fh = _open_credential_file(file_path, encoding)
    if fh is None:
        return
    try:
        yield from _iter_credentials_from_lines(fh, str(file_path))
    finally:
        fh.close()


def _open_credential_file(file_path: Path, encoding: str) -> TextIO | None:
    """Open a credential file using the encoding fallback chain.

    Returns an open text handle (in the first encoding that decodes), or None
    if the file is binary or undecodable. Caller is responsible for closing.
    """
    if _is_binary_file(file_path):
        logger.debug("Skipping binary file: %s", file_path)
        return None

    # Explicit BOM handling: a UTF-16 BOM picks the encoding without the
    # fallback chain (which must never guess utf-16 for non-BOM files).
    try:
        with open(file_path, "rb") as raw:
            head = raw.read(4)
    except OSError:
        head = b""
    if head.startswith(b"\xff\xfe"):
        return _open_file(file_path, "utf-16")
    if head.startswith(b"\xfe\xff"):
        return _open_file(file_path, "utf-16-be")

    enc_chain = _detect_encoding(encoding)
    for enc in enc_chain:
        # Decode probe must be STRICT: opening with errors="replace" never
        # raises, so the fallback chain was dead code and non-UTF8 files were
        # silently mojibake'd instead of falling back to cp1252/latin-1.
        try:
            with open(file_path, encoding=enc, errors="strict") as probe:
                probe.readline()
        except (UnicodeDecodeError, UnicodeError):
            continue
        except Exception as exc:
            logger.error("Error probing %s with encoding %s: %s", file_path, enc, exc)
            return None
        fh = _open_file(file_path, enc)
        return fh

    logger.warning("Could not decode file: %s", file_path)
    return None


def iter_credentials_file(
    file_path: Path, encoding: str = "utf-8"
) -> Generator[Credential, None, None]:
    """Stream credentials from a file one at a time (memory-efficient).

    Unlike parse_credentials_file, this yields credentials as they are parsed
    without building a full list in memory.  Deduplication is NOT performed —
    callers are responsible for handling duplicates.

    Args:
        file_path: Path to the credentials file
        encoding: Preferred encoding (default utf-8); fallback chain is tried
    """
    for cred in _iter_credentials_from_file(file_path, encoding):
        if not _is_garbage_credential(cred.username, cred.password):
            yield cred


def parse_credential_lines(
    lines: Iterator[str],
    source_file: str | None = None,
) -> Generator[Credential, None, None]:
    """Parse credentials from an iterator of lines, filtering garbage.

    Used by the parallel parse path: a worker parses a chunk of lines with the
    same block/combo logic as a whole file, so chunks can be processed in
    separate processes and the results merged in order.

    Args:
        lines: Iterator of raw lines (without trailing newlines).
        source_file: Source file name/path for tracking.
    """
    for cred in _iter_credentials_from_lines(lines, source_file):
        if not _is_garbage_credential(cred.username, cred.password):
            yield cred


def _get_field(fields: dict[str, str], possible_names: list[str]) -> str | None:
    """Get a field value by trying multiple possible field names."""
    for name in possible_names:
        if fields.get(name):
            return fields[name]
    return None


def _extract_sysinfo_fields(text: str) -> dict[str, str]:
    """Extract key:value pairs from SystemInfo.txt, handling multi-word field names."""
    fields: dict[str, str] = {}
    # Split on first ':' or '=' per line; normalize key to lowercase with spaces collapsed
    _line_re = re.compile(r"^(.+?)\s*[:=]\s*(.+)$")
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = _line_re.match(line)
        if m:
            key = m.group(1).lower().strip()
            fields[key] = m.group(2).strip()
    return fields


def parse_system_info(text: str) -> SystemInfo:
    """Parse system information from a SystemInfo.txt file.

    Args:
        text: Raw text content

    Returns:
        SystemInfo object with extracted fields
    """
    info = SystemInfo()
    fields = _extract_sysinfo_fields(text)

    # Map common field names (including multi-word variants) to SystemInfo attributes.
    # Keys are lowercase field name prefixes — matched by startswith for flexibility.
    field_mapping: dict[str, list[str]] = {
        "hostname": [
            "hostname", "computername", "computer name", "computer",
            "pcname", "pc name", "pc", "machine",
        ],
        "username": ["username", "user name", "user", "account"],
        "ip_address": [
            "ip", "ip address", "ipaddress", "ip_address",
            "publicip", "public ip", "public_ip", "external ip",
        ],
        "country": ["country", "location", "geo"],
        "hwid": [
            "hwid", "hardware id", "hardwareid", "hardware_id",
            "machine id", "machineid",
        ],
        "os": [
            "os", "operating system", "operatingsystem", "os version",
            "windows version", "windows",
        ],
        "cpu": ["cpu", "processor"],
        "gpu": ["gpu", "graphics", "videocard", "video card"],
        "ram": ["ram", "memory"],
        "timezone": ["timezone", "time zone", "time_zone", "tz"],
        "language": ["language", "lang", "locale"],
        "screen_size": ["screen", "screensize", "screen size", "resolution", "display"],
        # Stealer self-identification (many stealers embed their name in SystemInfo)
        "stealer_name": ["stealer", "malware", "build id", "build", "log type", "client"],
    }

    for attr, possible_names in field_mapping.items():
        value = _get_field(fields, possible_names)
        if value:
            setattr(info, attr, value)

    return info
