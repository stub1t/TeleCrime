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

# Compiled patterns for credential block detection
# Format 1: "Soft: Chrome\nHost: url\nLogin: user\nPassword: pass"
LABELED_BLOCK_PATTERN = re.compile(
    r"(?:^|\n\n|\n(?=[A-Za-z]+\s*:))"  # Block start
    r"(?P<block>(?:[A-Za-z_]+\s*:\s*[^\n]*\n?)+)",  # Labeled lines
    re.MULTILINE
)

# Format 2: ["Chrome" = "Default"]\nHostname: url\nUsername: user
BRACKET_HEADER_PATTERN = re.compile(
    r'\["?(?P<app>[^"=\]]+)"?\s*=\s*"?(?P<profile>[^"\]]+)"?\]',
    re.MULTILINE
)

# Format 3: URL:user:password (colon-separated)
# The URL may carry a :port — "https://host:8080:user:pass" must split as
# url="https://host:8080", user="user", pass="pass". URLs with a port AND a
# path ("https://host:8080/x:u:p") fail the match entirely (fall through to
# other formats) rather than being mangled into bogus triples.
COLON_SEPARATED_PATTERN = re.compile(
    r"^(?P<url>https?://[^\s:]+(?::\d{1,5})?):(?P<username>[^:]+):(?P<password>.+)$",
    re.MULTILINE
)

# Format 4: url | user | password (pipe-separated)
PIPE_SEPARATED_PATTERN = re.compile(
    r"^(?P<url>https?://[^\s|]+)\s*\|\s*(?P<username>[^|]+)\s*\|\s*(?P<password>.+)$",
    re.MULTILINE
)

# Format 5: url;user;password (semicolon-separated, common in ULP/combo lists)
SEMICOLON_SEPARATED_PATTERN = re.compile(
    r"^(?P<url>https?://[^\s;]+);(?P<username>[^;]+);(?P<password>.+)$",
    re.MULTILINE
)

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
_PROMO_MARKERS_RE = re.compile(
    r"\s*(?:┃|\|)?\s*(?:https?://)?t\.me/[^\s]+.*$"
    r"|\s*(?:you\s+can\s+buy|to\s+buy|dm\s+@)[^\r\n]*$",
    re.IGNORECASE,
)
# Fast fail-fast trigger scan (C-level, no allocation) replacing the
# unconditional .lower() copy on every field.
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
    return Credential(
        url=url.strip(),
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


def _detect_encoding(file_path: Path, hint: str) -> list[str]:
    """Return encoding list to try, with hint first and deduped."""
    # utf-16-le handles UTF-16 LE files without BOM (common in some stealers)
    chain = [hint, "utf-8", "utf-16", "utf-16-le", "latin-1", "cp1252"]
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

    enc_chain = _detect_encoding(file_path, encoding)
    for enc in enc_chain:
        try:
            fh = _open_file(file_path, enc)
            # Decode probe: read first line to confirm this encoding works.
            fh.readline()
            fh.seek(0)
            return fh
        except (UnicodeDecodeError, UnicodeError):
            try:
                fh.close()
            except Exception:
                pass
            continue
        except Exception as exc:
            logger.error("Error reading %s with encoding %s: %s", file_path, enc, exc)
            try:
                fh.close()
            except Exception:
                pass
            return None

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


def parse_credentials_file(file_path: Path, encoding: str = "utf-8") -> list[Credential]:
    """Parse credentials from a file using a streaming line-by-line approach.

    Internally iterates the file without loading it all into memory.
    Returns the same list[Credential] API as before.

    Args:
        file_path: Path to the credentials file
        encoding: Preferred encoding (default utf-8); fallback chain is tried

    Returns:
        List of parsed Credential objects (deduplicated)
    """
    try:
        seen: set[tuple[str, str, str]] = set()
        unique_creds: list[Credential] = []

        for cred in _iter_credentials_from_file(file_path, encoding):
            if _is_garbage_credential(cred.username, cred.password):
                continue
            key = (cred.url, cred.username, cred.password)
            if key not in seen:
                seen.add(key)
                unique_creds.append(cred)

        return unique_creds

    except Exception as e:
        logger.error("Error parsing credentials file %s: %s", file_path, e)
        return []


def parse_credentials_text(
    text: str,
    source_file: str | None = None,
) -> list[Credential]:
    """Parse credentials from text content.

    Supports multiple formats:
    - Labeled blocks (Soft:/Host:/Login:/Password:)
    - Bracket headers (["Chrome" = "Default"])
    - Colon-separated (url:user:pass)
    - Pipe-separated (url | user | pass)

    Args:
        text: Raw text content
        source_file: Optional source file name for tracking

    Returns:
        List of parsed Credential objects
    """
    credentials = []

    # Try labeled block format first (most common)
    labeled_creds = _parse_labeled_blocks(text, source_file)
    if labeled_creds:
        credentials.extend(labeled_creds)

    # Try colon-separated format
    colon_creds = _parse_colon_separated(text, source_file)
    if colon_creds:
        credentials.extend(colon_creds)

    # Try pipe-separated format
    pipe_creds = _parse_pipe_separated(text, source_file)
    if pipe_creds:
        credentials.extend(pipe_creds)

    # Try semicolon-separated format (ULP/combo lists)
    semi_creds = _parse_semicolon_separated(text, source_file)
    if semi_creds:
        credentials.extend(semi_creds)

    # Deduplicate by (url, username, password)
    seen: set[tuple[str, str, str]] = set()
    unique_creds: list[Credential] = []
    for cred in credentials:
        key = (cred.url, cred.username, cred.password)
        if key not in seen:
            seen.add(key)
            unique_creds.append(cred)

    return unique_creds


def _parse_labeled_blocks(text: str, source_file: str | None = None) -> list[Credential]:
    """Parse labeled block format credentials.

    Example formats:
        Soft: Google Chrome [Default]
        Host: https://example.com
        Login: user@example.com
        Password: secret123

        ---

        URL: https://other.com
        Username: otheruser
        Password: pass456
    """
    credentials = []

    # Split into blocks (separated by blank lines or separators)
    blocks = re.split(r"\n\s*(?:---+|===+|_{3,}|\n)\s*\n|\n{2,}", text)

    for block_idx, block in enumerate(blocks):
        if not block.strip():
            continue

        fields = _extract_fields(block)

        url = _normalize_url(_get_field(fields, URL_FIELDS))
        username = _get_field(fields, USERNAME_FIELDS)
        password = _get_field(fields, PASSWORD_FIELDS)

        # Must have at least URL and password to be valid
        if url and password:
            cred = _make_credential(
                url=url,
                username=username or "",
                password=password,
                application=_get_field(fields, APPLICATION_FIELDS),
                profile=_get_field(fields, PROFILE_FIELDS),
                source_file=source_file,
            )
            credentials.append(cred)

    return credentials


def _extract_fields(block: str) -> dict[str, str]:
    """Extract field:value pairs from a block of text."""
    fields = {}

    # Check for bracket header first
    bracket_match = BRACKET_HEADER_PATTERN.search(block)
    if bracket_match:
        fields["application"] = bracket_match.group("app").strip()
        fields["profile"] = bracket_match.group("profile").strip()

    # Extract labeled fields; allow empty values for continuation-line handling
    pending_empty_field: str | None = None
    for line in block.split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            pending_empty_field = None
            continue

        match = _LABELED_LINE_RE.match(line)
        if match:
            field_name = match.group(1).lower().strip()
            field_value = match.group(2).strip()
            fields[field_name] = field_value
            pending_empty_field = field_name if not field_value else None
        elif pending_empty_field:
            # Continuation: value is on the next line after an empty-valued field
            fields[pending_empty_field] = line
            pending_empty_field = None
        else:
            pending_empty_field = None

    return fields


def _get_field(fields: dict[str, str], possible_names: list[str]) -> str | None:
    """Get a field value by trying multiple possible field names."""
    for name in possible_names:
        if name in fields and fields[name]:
            return fields[name]
    return None


def _parse_colon_separated(text: str, source_file: str | None = None) -> list[Credential]:
    """Parse colon-separated format: url:username:password"""
    credentials = []

    for match in COLON_SEPARATED_PATTERN.finditer(text):
        cred = _make_credential(
            url=match.group("url").strip(),
            username=match.group("username").strip(),
            password=match.group("password").strip(),
            source_file=source_file,
        )
        credentials.append(cred)

    return credentials


def _parse_pipe_separated(text: str, source_file: str | None = None) -> list[Credential]:
    """Parse pipe-separated format: url | username | password"""
    credentials = []

    for match in PIPE_SEPARATED_PATTERN.finditer(text):
        cred = _make_credential(
            url=match.group("url").strip(),
            username=match.group("username").strip(),
            password=match.group("password").strip(),
            source_file=source_file,
        )
        credentials.append(cred)

    return credentials


def _parse_semicolon_separated(text: str, source_file: str | None = None) -> list[Credential]:
    """Parse semicolon-separated format: url;username;password (ULP/combo lists)"""
    credentials = []

    for match in SEMICOLON_SEPARATED_PATTERN.finditer(text):
        cred = _make_credential(
            url=match.group("url").strip(),
            username=match.group("username").strip(),
            password=match.group("password").strip(),
            source_file=source_file,
        )
        credentials.append(cred)

    return credentials


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
