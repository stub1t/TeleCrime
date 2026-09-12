"""Parser for stealer log credential files."""

import itertools
import logging
import re
from collections.abc import Generator, Iterator
from pathlib import Path
from typing import TextIO
from urllib.parse import urlsplit, urlunsplit

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
# Space-separated combo rows (real ULP dumps): "https://host/login user:pass"
# and "https://host/login user pass". The URL group stops at whitespace (plus
# an optional numeric port), so the scheme can never be swallowed by the user
# field; user and password are whitespace-free tokens for the space/space form
# to keep prose ("https://host please reset") from parsing as a credential.
_SPACE_COLON_LINE_RE = re.compile(
    r"^(?P<url>https?://[^\s:]+(?::\d{1,5})?)[^\S\n]+"
    r"(?P<username>[^\s:]+):(?P<password>.+)$"
)
_SPACE_SPACE_LINE_RE = re.compile(
    r"^(?P<url>https?://[^\s:]+(?::\d{1,5})?)[^\S\n]+"
    r"(?P<username>[^\s:]+)[^\S\n]+(?P<password>\S+)$"
)
# Combo fast path: one C-level finditer over whole text buffers instead of up
# to five anchored regex matches per line. Branch order (colon, pipe,
# semicolon, space-colon, space-space) mirrors the per-line quintet above and
# alternation is ordered, so this matches exactly the lines/fields the slow
# path would. Group names are prefixed per branch because a group name may not
# repeat across alternatives; the branch-marker groups (colon/pipe/semi/
# spcolon/spaces) identify which branch matched. The per-line char classes are
# newline-hardened ([^\S\n]* instead of \s*, [^:\n] etc.): greedy classes like
# [^:]+ would otherwise swallow the "\n" between buffer lines and match across
# them, which the per-line slow path (whose input is a single stripped line)
# can never do. Lockstep with the per-line quintet is asserted by the
# differential test.
_COMBO_FAST_RE = re.compile(
    r"^(?P<colon>https?://[^\s:]+(?::\d{1,5})?):"
    r"(?P<colon_user>[^:\n]+):(?P<colon_pass>.+)$"
    r"|^(?P<pipe>https?://[^\s|]+)[^\S\n]*\|[^\S\n]*"
    r"(?P<pipe_user>[^|\n]+)[^\S\n]*\|[^\S\n]*(?P<pipe_pass>.+)$"
    r"|^(?P<semi>https?://[^\s;]+);"
    r"(?P<semi_user>[^;\n]+);(?P<semi_pass>.+)$"
    r"|^(?P<spcolon>https?://[^\s:]+(?::\d{1,5})?)[^\S\n]+"
    r"(?P<spcolon_user>[^\s:\n]+):(?P<spcolon_pass>.+)$"
    r"|^(?P<spaces>https?://[^\s:]+(?::\d{1,5})?)[^\S\n]+"
    r"(?P<spaces_user>[^\s:\n]+)[^\S\n]+(?P<spaces_pass>\S+)$",
    re.MULTILINE,
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
# Password fields legitimately ARE Telegram links ("t.me/mypass"): the
# start-of-string alternative of _PROMO_MARKERS_RE wiped them to "". Only
# strip a t.me promo when it follows a separator for passwords.
_PROMO_MARKERS_PASSWORD_RE = re.compile(
    r"(?:\s*(?:┃|\|)\s*)(?:https?://)?t\.me/[^\s]+.*$"
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

# Combo fast-path tuning. The classifier probes only the first lines of a file
# so the decision is O(1) per file; a misclassified late-labeled-block file
# would silently drop its labeled credentials, so the probe is deliberately
# conservative (any labeled/bracket header in the window vetoes the fast path).
_COMBO_CLASSIFY_LINES = 200
_COMBO_SCAN_BYTES = 8 * 1024 * 1024
# Decision is per file (cached so parallel chunks of one file share it);
# the cache is bounded because long-running workers see many files.
_COMBO_CACHE_MAX = 256
_COMBO_CLASS_CACHE: dict[str, bool] = {}


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


def _clean_credential_field(
    value: str | None, *, username: bool = False, password: bool = False
) -> str:
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
        marker_re = _PROMO_MARKERS_PASSWORD_RE if password else _PROMO_MARKERS_RE
        cleaned = marker_re.sub("", cleaned).strip()
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
        password=_clean_credential_field(password, password=True),
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


def _detect_bomless_utf16(head: bytes) -> str | None:
    """Detect BOM-less UTF-16 by its alternating-NUL byte pattern.

    ASCII-dominant UTF-16 stores a NUL in every second byte — at odd offsets
    for little-endian, at even offsets for big-endian. Single-byte encodings
    never show that alternation, so this cannot misclassify latin-1/cp1252
    data (which used to decode as NUL-riddled text and yield zero credentials).
    """
    sample = head[:4096]
    pairs = len(sample[1::2])
    if pairs < 2:
        return None
    even_nuls = sample[0::2].count(0)
    odd_nuls = sample[1::2].count(0)
    if odd_nuls >= pairs * 0.8 and even_nuls <= pairs * 0.2:
        return "utf-16-le"
    if even_nuls >= pairs * 0.8 and odd_nuls <= pairs * 0.2:
        return "utf-16-be"
    return None


def _normalize_url(url: str | None) -> str | None:
    """Strip control characters and validate URL scheme; return None for non-HTTP values."""
    if not url:
        return None
    url = url.strip().strip("\x00\r\n")
    # Reject Windows paths (HOST: C:\...) and other non-HTTP values
    if not url.startswith(("http://", "https://")):
        return None
    # Credentials embedded in the URL ("https://user:pass@example.com") must
    # not leak into the domain column or the dedup hashes — but an "@" in the
    # path/query ("/reset?email=a@b.com") is data, not userinfo. urlsplit
    # isolates the netloc so only its "@" is stripped (the old rsplit-on-rest
    # rewrote the host to the query's domain). urlsplit is pure-Python and
    # measurably expensive, so skip it when the authority holds no "@".
    scheme_end = url.find("://") + 3
    authority_end = len(url)
    for sep in "/?#":
        idx = url.find(sep, scheme_end)
        if idx != -1 and idx < authority_end:
            authority_end = idx
    if "@" not in url[scheme_end:authority_end]:
        return url
    try:
        parts = urlsplit(url)
    except ValueError:
        return url
    if "@" in parts.netloc:
        netloc = parts.netloc.rsplit("@", 1)[1]
        url = urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
    return url


def _open_file(file_path: Path, encoding: str) -> TextIO:
    """Open file with given encoding, replacing errors."""
    return open(file_path, encoding=encoding, errors="replace")


def _parse_block_lines(block_lines: list[str]) -> list[tuple[dict[str, str], bool]]:
    """Parse lines into one or more labeled blocks.

    Stealer logs normally separate records with blank lines, but some dumps
    put them back-to-back. A labeled line that re-declares an already-populated
    field starts a new record, as does a ``[...]`` header after a complete
    URL+password block; without that, N consecutive records collapsed into the
    last one (each ``fields[field_name]`` overwrote the previous record).

    Returns a list of (fields, has_labeled_field) per block found.
    """
    blocks: list[tuple[dict[str, str], bool]] = []
    fields: dict[str, str] = {}
    has_labeled = False
    pending_empty_field: str | None = None  # field name whose value was empty

    def flush() -> None:
        nonlocal fields, has_labeled, pending_empty_field
        if fields:
            blocks.append((fields, has_labeled))
        fields = {}
        has_labeled = False
        pending_empty_field = None

    for line in block_lines:
        line_s = line.strip()
        if not line_s or line_s.startswith("#"):
            pending_empty_field = None
            continue

        # Bracket header. After a complete record it starts a new one.
        bm = _BRACKET_LINE_RE.match(line_s)
        if bm:
            if _get_field(fields, URL_FIELDS) and _get_field(fields, PASSWORD_FIELDS):
                flush()
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
            # Same field twice = the previous back-to-back record ended here.
            if fields.get(field_name):
                flush()
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

    flush()
    return blocks


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


def _iter_credentials_from_lines_slow(
    lines: Iterator[str],
    source_file: str | None,
) -> Generator[Credential, None, None]:
    """Stream credentials from an iterator of lines.

    Handles labeled-block, bracket-header, colon-separated, and pipe-separated
    formats. Formats can be mixed within the same file.
    """
    block_lines: list[str] = []

    def flush_block() -> Generator[Credential, None, None]:
        """Yield credentials from accumulated block_lines if valid."""
        if not block_lines:
            return
        for fields, has_labeled in _parse_block_lines(block_lines):
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

        # Combo-format lines (colon/pipe/semicolon/space-separated) always
        # start with a URL scheme.  Labeled-block lines start with field names
        # like "URL:", "Host:", "Login:".  Using startswith as a fast
        # discriminator avoids running all combo regexes on every labeled line —
        # which is the common case in stealer-log files where each labeled
        # block has 4-8 lines.
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

            # Check for space-separated "URL user:pass" credential
            scm = _SPACE_COLON_LINE_RE.match(stripped)
            if scm:
                yield from flush_block()
                yield _make_credential(
                    url=scm.group("url").strip(),
                    username=scm.group("username").strip(),
                    password=scm.group("password").strip(),
                    source_file=source_file,
                )
                continue

            # Check for space-separated "URL user pass" credential
            ssm = _SPACE_SPACE_LINE_RE.match(stripped)
            if ssm:
                yield from flush_block()
                yield _make_credential(
                    url=ssm.group("url").strip(),
                    username=ssm.group("username").strip(),
                    password=ssm.group("password").strip(),
                    source_file=source_file,
                )
                continue

        # Accumulate into current block (labeled / bracket header, or a URL
        # line that didn't match any combo pattern)
        block_lines.append(line)

    # Flush final block
    yield from flush_block()


def _classify_combo_head(head: list[str]) -> bool:
    """Return True when the first lines look like a combo-only file.

    Combo/ULP files are line-independent ``url<sep>user<sep>pass`` rows (colon,
    pipe, semicolon or whitespace). Any labeled/bracket header in the probe
    window may use block semantics (a credential can then span several lines),
    which the fast path cannot reproduce — veto it. Otherwise ≥90% of the
    non-blank lines must match a single-line combo pattern.
    """
    examined = 0
    matched = 0
    for raw in head:
        line = raw.strip()
        if not line:
            continue
        examined += 1
        # Combo rows like "https://a.com:u:p" also fit _LABELED_LINE_RE (key
        # "https"), so the combo check must run first, mirroring per-line order.
        if (
            _COLON_LINE_RE.match(line)
            or _PIPE_LINE_RE.match(line)
            or _SEMICOLON_LINE_RE.match(line)
            or _SPACE_COLON_LINE_RE.match(line)
            or _SPACE_SPACE_LINE_RE.match(line)
        ):
            matched += 1
            continue
        lm = _LABELED_LINE_RE.match(line)
        if lm is not None:
            # A bare "https://x.com" junk row reads as key "https", value
            # "//x.com" — a URL-scheme artifact, not a block header: it can
            # never yield a credential in block context, so it must not veto.
            if not lm.group(2).lstrip().startswith("//"):
                return False
        elif _BRACKET_LINE_RE.match(line):
            return False
    # At least 90% of the examined lines must be single-line combo rows.
    return examined > 0 and matched * 10 >= examined * 9


def _combo_credential(
    m: re.Match[str], source_file: str | None
) -> Credential:
    """Build a Credential from one _COMBO_FAST_RE match (any branch)."""
    make = _make_credential
    if m.group("colon") is not None:
        return make(
            url=m.group("colon"),
            username=m.group("colon_user"),
            password=m.group("colon_pass"),
            source_file=source_file,
        )
    if m.group("pipe") is not None:
        return make(
            url=m.group("pipe"),
            username=m.group("pipe_user"),
            password=m.group("pipe_pass"),
            source_file=source_file,
        )
    if m.group("semi") is not None:
        return make(
            url=m.group("semi"),
            username=m.group("semi_user"),
            password=m.group("semi_pass"),
            source_file=source_file,
        )
    if m.group("spcolon") is not None:
        return make(
            url=m.group("spcolon"),
            username=m.group("spcolon_user"),
            password=m.group("spcolon_pass"),
            source_file=source_file,
        )
    return make(
        url=m.group("spaces"),
        username=m.group("spaces_user"),
        password=m.group("spaces_pass"),
        source_file=source_file,
    )


def _iter_combo_lines(
    lines: Iterator[str],
    source_file: str | None,
) -> Generator[Credential, None, None]:
    """Combo fast path with a labeled-block fallback (hybrid).

    Combo lines are line-independent, so each ~8 MB buffer is scanned with one
    C-level finditer instead of up to five regex matches per line. Lines the
    scan does not match are fed through the same labeled-block state machine as
    the slow path, so a file classified combo-only from its head but containing
    ``Host/Login/Password`` blocks further down can no longer drop them.
    """
    buf: list[str] = []
    buf_size = 0
    block_lines: list[str] = []

    def flush_block() -> Generator[Credential, None, None]:
        if not block_lines:
            return
        for fields, has_labeled in _parse_block_lines(block_lines):
            if has_labeled:
                cred = _credential_from_fields(fields, source_file)
                if cred is not None:
                    yield cred
        block_lines.clear()

    def handle_gap_line(line: str) -> Generator[Credential, None, None]:
        stripped = line.strip()
        if not stripped or _SEPARATOR_RE.match(stripped):
            yield from flush_block()
        else:
            block_lines.append(line)

    def scan_buffer(text: str) -> Generator[Credential, None, None]:
        pos = 0
        consumed = 0
        for m in _COMBO_FAST_RE.finditer(text):
            if m.start() > pos:
                # Newlines in the gap are one full line each (matches are
                # anchored at line starts), so this counts the non-combo lines
                # to hand to the labeled parser without splitting the buffer.
                gap_lines = text.count("\n", pos, m.start())
                for i in range(consumed, consumed + gap_lines):
                    yield from handle_gap_line(buf[i])
                consumed += gap_lines
            if block_lines:
                yield from flush_block()
            yield _combo_credential(m, source_file)
            consumed += 1
            pos = m.end() + 1
        for i in range(consumed, len(buf)):
            yield from handle_gap_line(buf[i])

    for raw in lines:
        line = raw.rstrip("\n").rstrip("\r").strip()
        buf.append(line)
        buf_size += len(line) + 1
        if buf_size >= _COMBO_SCAN_BYTES:
            yield from scan_buffer("\n".join(buf))
            buf = []
            buf_size = 0
    if buf:
        yield from scan_buffer("\n".join(buf))
    # A trailing labeled block still owns block_lines here.
    yield from flush_block()


def _iter_credentials_from_lines(
    lines: Iterator[str],
    source_file: str | None,
    combo_decision: bool | None = None,
) -> Generator[Credential, None, None]:
    """Stream credentials from an iterator of lines.

    Strategy is picked once per file: combo-only files take the whole-buffer
    fast path, everything else the general path below. `combo_decision` lets
    the parallel path pass a classification derived from the true file head,
    so a worker parsing a mid-file chunk can never re-classify from that
    chunk's head (a combo-dominant chunk of a labeled file would otherwise
    take the fast path and drop its labeled credentials). When it is None the
    decision comes from the per-file cache or, on a miss, from the first
    _COMBO_CLASSIFY_LINES lines of `lines` — never more.
    """
    it = iter(lines)
    head: list[str] = []
    decision = combo_decision
    if decision is None:
        if source_file is not None:
            decision = _COMBO_CLASS_CACHE.get(source_file)
        if decision is None:
            for _ in range(_COMBO_CLASSIFY_LINES):
                try:
                    head.append(next(it))
                except StopIteration:
                    break
            decision = _classify_combo_head(head)
            if source_file is not None:
                if len(_COMBO_CLASS_CACHE) >= _COMBO_CACHE_MAX:
                    _COMBO_CLASS_CACHE.clear()
                _COMBO_CLASS_CACHE[source_file] = decision
    rest = itertools.chain(head, it)
    if decision:
        yield from _iter_combo_lines(rest, source_file)
    else:
        yield from _iter_credentials_from_lines_slow(rest, source_file)


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
    # BOM-less UTF-16 is detected by its alternating-NUL pattern before the
    # chain: utf-8 accepts NUL bytes, so the chain used to "succeed" on it and
    # hand the parser NUL-riddled lines (zero credentials).
    try:
        with open(file_path, "rb") as raw:
            head = raw.read(4096)
    except OSError:
        head = b""
    if head.startswith(b"\xff\xfe"):
        return _open_file(file_path, "utf-16")
    if head.startswith(b"\xfe\xff"):
        return _open_file(file_path, "utf-16-be")
    bomless_utf16 = _detect_bomless_utf16(head)
    if bomless_utf16 is not None:
        return _open_file(file_path, bomless_utf16)

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
    combo_decision: bool | None = None,
) -> Generator[Credential, None, None]:
    """Parse credentials from an iterator of lines, filtering garbage.

    Used by the parallel parse path: a worker parses a chunk of lines with the
    same block/combo logic as a whole file, so chunks can be processed in
    separate processes and the results merged in order.

    Args:
        lines: Iterator of raw lines (without trailing newlines).
        source_file: Source file name/path for tracking.
        combo_decision: Optional pre-computed combo classification for the
            source file (from its true head). Chunk workers must pass this so
            a chunk's own head cannot misclassify the file.
    """
    for cred in _iter_credentials_from_lines(
        lines, source_file, combo_decision=combo_decision
    ):
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
