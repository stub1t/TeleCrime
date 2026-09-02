"""Password extraction from message context."""

import os
import re
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from telecrime.models import Conversation, Message, PasswordCandidate
from telecrime.states import PasswordScope

_DEFAULT_DATA_DIR = Path(__file__).parent.parent.parent / "data"

# Cache: (path, mtime) -> list[str]. Avoids re-reading the file on every archive.
_password_file_cache: dict[Path, tuple[float, list[str]]] = {}


def load_password_file(path: Path | None = None) -> list[str]:
    """Load passwords from a text file, caching by mtime."""
    if path is None:
        data_dir = Path(os.environ.get("TELECRIME_DATA_DIR", str(_DEFAULT_DATA_DIR)))
        path = data_dir / "passwords.txt"
    if not path.exists():
        return []

    try:
        mtime = path.stat().st_mtime
    except OSError:
        return []

    cached = _password_file_cache.get(path)
    if cached is not None and cached[0] == mtime:
        return cached[1]

    passwords = []
    with open(path, encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and len(line) >= 3:
                passwords.append(line)
    _password_file_cache[path] = (mtime, passwords)
    return passwords


# Common password markers in messages
# Note: Emoji prefixes (🔑, 🔐, 🔒) are common in Telegram messages
PASSWORD_MARKERS = [
    r"[🔑🔐🔒]?\s*(?:password|pass|pwd|pw)\s*[:\-=]\s*",
    r"(?:password|pass|pwd|pw)\s+(?:is|=)\s+",
    r"unlock\s*[:\-=]\s*",
    r"key\s*[:\-=]\s*",
    r"[🔑🔐🔒]\s*[:\-=]?\s*",  # Just emoji followed by password
]

# Patterns to extract password values
PASSWORD_VALUE_PATTERNS = [
    r"[\"']([^\"']+)[\"']",  # Quoted
    r"`([^`]+)`",  # Backtick quoted
    r"\*\*([^*]+)\*\*",  # Bold markdown
    r"(\S+)",  # Unquoted word
]

# Inline password patterns for filenames and short texts (pass=foo, pwd-foo)
INLINE_PASSWORD_PATTERN = re.compile(
    r"(?i)(?:^|[^a-z0-9])(?:password|pass|pwd|pw|key|unlock)"
    r"[^a-z0-9:=_\-]*[:=_\-]*([^\s,;]+)"
)

MAX_FAILED_ATTEMPTS = 3


def extract_inline_passwords(text: str) -> list[tuple[str, float]]:
    """Extract inline password patterns from filenames or short strings."""
    if not text:
        return []

    results: list[tuple[str, float]] = []
    seen: set[str] = set()
    for match in INLINE_PASSWORD_PATTERN.finditer(text):
        pwd = normalize_password(match.group(1))
        pwd = pwd.strip(" \t\r\n,.;")
        # Strip common archive extensions if present
        for ext in [".zip", ".rar", ".7z", ".tar", ".gz", ".tgz", ".bz2", ".xz"]:
            if pwd.lower().endswith(ext):
                pwd = pwd[: -len(ext)]
                break
        if _is_valid_password(pwd) and pwd not in seen:
            seen.add(pwd)
            results.append((pwd, 0.85))

    return results


async def extract_passwords_from_context(
    session: Session,
    message: Message,
    nearby_count: int = 10,
    archive_name: str | None = None,
    attachment_filename: str | None = None,
) -> list[PasswordCandidate]:
    """Extract password candidates from message and nearby context.

    Sources (in priority order):
    1. Channel username (t.me link) - most common password pattern
    2. Message caption/text with password markers
    3. Archive filename/base name inline patterns
    4. Nearby messages
    5. Previously successful passwords
    6. Password file entries matching channel/archive name

    Args:
        session: Database session
        message: The message containing the archive
        config: Application config
        nearby_count: Number of nearby messages to check

    Returns:
        List of new PasswordCandidate objects (not yet committed)
    """
    candidates: list[PasswordCandidate] = []
    seen_values: set[str] = set()

    # 0. Get channel username as highest priority password
    conversation = session.execute(
        select(Conversation).where(Conversation.id == message.conversation_id)
    ).scalar_one_or_none()

    if conversation and conversation.username:
        # Channel username variations are very common passwords
        channel_passwords = [
            conversation.username,
            conversation.username.lower(),
            conversation.username.upper(),
            f"@{conversation.username}",
            f"@{conversation.username.lower()}",
            f"t.me/{conversation.username}",
            f"t.me/{conversation.username.lower()}",
            f"https://t.me/{conversation.username}",
            f"https://t.me/{conversation.username.lower()}",
        ]
        for pwd in channel_passwords:
            normalized = normalize_password(pwd).strip(" \t\r\n,.;")
            if normalized in seen_values:
                continue
            if not _is_valid_password(normalized):
                continue
            seen_values.add(normalized)
            candidate = PasswordCandidate(
                value=normalized,
                scope=PasswordScope.MESSAGE,
                source_message_id=message.id,
                conversation_id=message.conversation_id,
                extraction_method="channel_username",
                context_text=f"Channel: {conversation.username}",
                confidence=0.95,  # Very high confidence
            )
            session.add(candidate)
            candidates.append(candidate)

    # 1. Extract from message caption/text
    message_passwords = extract_passwords_from_text(
        message.caption or message.text or ""
    )

    for pwd, confidence in message_passwords:
        normalized = normalize_password(pwd).strip(" \t\r\n,.;")
        if normalized in seen_values:
            continue
        if not _is_valid_password(normalized):
            continue
        seen_values.add(normalized)
        candidate = PasswordCandidate(
            value=normalized,
            scope=PasswordScope.MESSAGE,
            source_message_id=message.id,
            conversation_id=message.conversation_id,
            extraction_method="caption",
            context_text=(message.caption or message.text or "")[:200],
            confidence=confidence,
        )
        session.add(candidate)
        candidates.append(candidate)

    # 2. Extract from archive filename/base name
    if attachment_filename:
        normalized_filename = re.sub(r"[._\-\[\]\(\)]+", " ", attachment_filename)
        for pwd, confidence in extract_inline_passwords(normalized_filename):
            normalized = normalize_password(pwd).strip(" \t\r\n,.;")
            if normalized in seen_values:
                continue
            if not _is_valid_password(normalized):
                continue
            seen_values.add(normalized)
            candidate = PasswordCandidate(
                value=normalized,
                scope=PasswordScope.MESSAGE,
                source_message_id=message.id,
                conversation_id=message.conversation_id,
                extraction_method="filename",
                context_text=attachment_filename[:200],
                confidence=min(0.9, confidence),
            )
            session.add(candidate)
            candidates.append(candidate)

    if archive_name:
        normalized_archive = re.sub(r"[._\-\[\]\(\)]+", " ", archive_name)
        for pwd, confidence in extract_inline_passwords(normalized_archive):
            normalized = normalize_password(pwd).strip(" \t\r\n,.;")
            if normalized in seen_values:
                continue
            if not _is_valid_password(normalized):
                continue
            seen_values.add(normalized)
            candidate = PasswordCandidate(
                value=normalized,
                scope=PasswordScope.MESSAGE,
                source_message_id=message.id,
                conversation_id=message.conversation_id,
                extraction_method="archive_name",
                context_text=archive_name[:200],
                confidence=min(0.85, confidence),
            )
            session.add(candidate)
            candidates.append(candidate)

    # 3. Extract from nearby messages
    nearby = session.execute(
        select(Message)
        .where(
            Message.conversation_id == message.conversation_id,
            Message.platform_id != message.platform_id,
        )
        .order_by(
            # Get messages close to this one by platform_id
            func.abs(Message.platform_id - message.platform_id)
        )
        .limit(nearby_count * 2)  # Get both before and after
    ).scalars().all()

    for nearby_msg in nearby:
        text = nearby_msg.caption or nearby_msg.text or ""
        nearby_passwords = extract_passwords_from_text(text)

        for pwd, confidence in nearby_passwords:
            normalized = normalize_password(pwd).strip(" \t\r\n,.;")
            if normalized in seen_values:
                continue
            if not _is_valid_password(normalized):
                continue
            seen_values.add(normalized)
            # Lower confidence for nearby messages
            candidate = PasswordCandidate(
                value=normalized,
                scope=PasswordScope.NEARBY,
                source_message_id=nearby_msg.id,
                conversation_id=message.conversation_id,
                extraction_method="nearby",
                context_text=text[:200],
                confidence=confidence * 0.7,
            )
            session.add(candidate)
            candidates.append(candidate)

    # 4. Get previously successful passwords for this conversation
    learned = session.execute(
        select(PasswordCandidate)
        .where(
            PasswordCandidate.conversation_id == message.conversation_id,
            PasswordCandidate.times_succeeded > 0,
            PasswordCandidate.times_failed < MAX_FAILED_ATTEMPTS,
        )
        .order_by(PasswordCandidate.times_succeeded.desc())
        .limit(5)
    ).scalars().all()

    for learned_pwd in learned:
        normalized = normalize_password(learned_pwd.value).strip(" \t\r\n,.;")
        if normalized in seen_values:
            continue
        if not _is_valid_password(normalized):
            continue
        seen_values.add(normalized)
        # Create a new candidate referencing the learned one
        candidate = PasswordCandidate(
            value=normalized,
            scope=PasswordScope.LEARNED,
            source_message_id=learned_pwd.source_message_id,
            conversation_id=message.conversation_id,
            extraction_method="learned",
            confidence=min(0.95, 0.5 + (learned_pwd.times_succeeded * 0.1)),
        )
        session.add(candidate)
        candidates.append(candidate)

    # 5. Load passwords from password file (lower priority, for fallback)
    # Only add a few from the file to avoid too many attempts
    file_passwords = load_password_file()
    added_from_file = 0
    max_from_file = 20  # Limit file-based passwords per archive

    for pwd in file_passwords:
        if added_from_file >= max_from_file:
            break
        normalized = normalize_password(pwd).strip(" \t\r\n,.;")
        if normalized in seen_values:
            continue
        if not _is_valid_password(normalized):
            continue
        # Avoid weak alpha-only file passwords unless long enough
        if normalized.isalpha() and len(normalized) < 8:
            continue
        seen_values.add(normalized)
        candidate = PasswordCandidate(
            value=normalized,
            scope=PasswordScope.GLOBAL,
            source_message_id=message.id,
            conversation_id=message.conversation_id,
            extraction_method="password_file",
            confidence=0.3,  # Lower confidence for file-based
        )
        session.add(candidate)
        candidates.append(candidate)
        added_from_file += 1

    return candidates


def _strip_telegram_markdown(text: str) -> str:
    """Strip Telegram markdown formatting from text, preserving the content.

    Telegram messages stored via Telethon may contain raw markdown like
    ****text**** (bold/italic overlap), **text** (bold), [text](url) (links).
    Stripping these ensures password patterns like ****@BHFCloud**** are
    correctly extracted as @BHFCloud.
    """
    # Strip [text](url) links — keep both text and url as separate tokens
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1 \2", text)
    # Strip ****text**** and **text** and *text* bold/italic markers
    # Use a loop to handle nested cases (e.g. ****text**** needs two passes)
    for _ in range(3):
        text = re.sub(r"\*{1,4}([^*\n]*?)\*{1,4}", r"\1", text)
    # Strip __text__ italic
    text = re.sub(r"__([^_\n]+)__", r"\1", text)
    return text


def extract_passwords_from_text(text: str) -> list[tuple[str, float]]:
    """Extract potential passwords from text.

    Args:
        text: The text to analyze

    Returns:
        List of (password, confidence) tuples
    """
    if not text:
        return []

    # Strip Telegram markdown formatting so passwords like ****@Channel****
    # are extracted correctly as @Channel rather than with asterisks.
    text = _strip_telegram_markdown(text)

    results: list[tuple[str, float]] = []
    seen_pwds: set[str] = set()
    text_lower = text.lower()

    # Look for explicit password markers
    for marker_pattern in PASSWORD_MARKERS:
        for match in re.finditer(marker_pattern, text_lower):
            # Get the text after the marker
            remaining_text = text[match.end():]
            lines = remaining_text.split("\n")
            rest_of_line = lines[0]

            # Lines to check: current line and next line (for "Password:\n`value`" pattern)
            lines_to_check = [rest_of_line.strip()]
            if len(lines) > 1 and not rest_of_line.strip():
                lines_to_check.append(lines[1].strip())

            found = False
            for line_to_check in lines_to_check:
                if not line_to_check or found:
                    continue
                # Try to extract the password value
                for value_pattern in PASSWORD_VALUE_PATTERNS:
                    value_match = re.match(value_pattern, line_to_check)
                    if value_match:
                        pwd = normalize_password(value_match.group(1))
                        pwd = pwd.strip(" \t\r\n,.;")
                        if _is_valid_password(pwd) and pwd not in seen_pwds:
                            seen_pwds.add(pwd)
                            # Higher confidence for quoted passwords
                            confidence = 0.9 if value_pattern.startswith("[\"']") else 0.8
                            # Reduce confidence for emoji-only marker
                            if marker_pattern == r"[🔑🔐🔒]\s*[:\-=]?\s*":
                                confidence *= 0.75
                            results.append((pwd, confidence))
                            found = True
                            # Also scan remaining tokens on the same line separated by
                            # common delimiters: "pass: @PegasusCloud + @EuropeCloud"
                            segments = re.split(r"\s*[+|/,]\s*", line_to_check)
                            for seg in segments[1:]:
                                seg = seg.strip()
                                if not seg:
                                    continue
                                for vp in PASSWORD_VALUE_PATTERNS:
                                    vm = re.match(vp, seg)
                                    if vm:
                                        extra = normalize_password(vm.group(1)).strip(" \t\r\n,.;")
                                        if _is_valid_password(extra) and extra not in seen_pwds:
                                            seen_pwds.add(extra)
                                            results.append((extra, confidence))
                                        break
                            break

    # Look for common password patterns without markers
    common_patterns = [
        # Telegram-style: standalone token that likely includes digits/symbols
        r"^((?=.*[0-9!@#$%^&*()_+\-=])[A-Za-z0-9!@#$%^&*()_+\-=]{4,30})$",
    ]

    for pattern in common_patterns:
        for match in re.finditer(pattern, text, re.MULTILINE):
            pwd = match.group(1)
            if _is_valid_password(pwd) and pwd not in seen_pwds:
                seen_pwds.add(pwd)
                results.append((pwd, 0.5))

    return results


def _is_valid_password(pwd: str) -> bool:
    """Check if a string looks like a valid password."""
    if not pwd:
        return False

    # Strip whitespace for validation
    pwd = pwd.strip()

    # Too short or too long
    if len(pwd) < 3 or len(pwd) > 100:
        return False

    # Common false positives
    false_positives = {
        "the", "password", "pass", "pwd", "is", "are", "file", "files",
        "download", "link", "here", "click", "open", "extract",
        "mirror", "update", "cloud", "logs", "log", "stealer",
        "zip", "rar", "7z", "part1", "part2", "part01", "part001",
        "password:", "pass:", "pwd:", "key:", "unlock:",  # Marker words with colon
    }
    if pwd.lower() in false_positives:
        return False

    # Allow t.me URLs as passwords (very common in Telegram stealer channels)
    if pwd.startswith("t.me/") or pwd.startswith("https://t.me/") or pwd.startswith("http://t.me/"):
        return True

    # Reject other URLs
    if pwd.startswith("http") or pwd.startswith("www."):
        return False

    return True


def normalize_password(pwd: str) -> str:
    """Normalize password for comparison."""
    # Trim whitespace
    pwd = pwd.strip()

    # Remove surrounding matching quotes/backticks
    if len(pwd) >= 2:
        if (pwd[0] == pwd[-1]) and pwd[0] in "\"'`":
            pwd = pwd[1:-1]

    # Strip any residual leading/trailing backticks (Telegram markdown leakage
    # where only one side was formatted, e.g. "`PegasusCloud" without closing `)
    pwd = pwd.strip("`")

    return pwd
