"""Heuristic-based archive grouping using captions and context."""

import re
from dataclasses import dataclass

from telecrime.models import Message


@dataclass
class CaptionHint:
    """Hint extracted from message caption."""

    part_number: int | None = None
    total_parts: int | None = None
    password_hint: str | None = None
    base_name_hint: str | None = None


def extract_caption_hints(message: Message) -> CaptionHint:
    """Extract grouping hints from message caption/text.

    Args:
        message: The message to analyze

    Returns:
        CaptionHint with extracted information
    """
    text = message.caption or message.text or ""
    hint = CaptionHint()

    # Look for "Part X of Y" patterns
    part_patterns = [
        r"part\s*(\d+)\s*(?:of|/)\s*(\d+)",
        r"(\d+)\s*(?:of|/)\s*(\d+)\s*parts?",
        r"\[(\d+)/(\d+)\]",
        r"#(\d+)\s*(?:of|/)\s*(\d+)",
    ]

    for pattern in part_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            hint.part_number = int(match.group(1))
            hint.total_parts = int(match.group(2))
            break

    # Look for password hints
    password_patterns = [
        # Quoted password: capture everything inside the quotes (may contain spaces).
        r"(?:password|pass|pwd|pw)[:\s]+[\"']([^\"']+)[\"']",
        # Unquoted password: capture up to the next whitespace.
        r"(?:password|pass|pwd|pw)[:\s]+([^\s\"']+)",
        r"[\"']([^\"']+)[\"']\s*(?:is\s+)?(?:the\s+)?(?:password|pass)",
    ]

    for pattern in password_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            hint.password_hint = match.group(1).strip()
            break

    return hint


def _derive_base_name(filename: str) -> str:
    """Derive a base name from filename by removing extensions and part indicators."""
    if not filename:
        return ""

    # Remove common extensions
    extensions = [
        ".rar", ".zip", ".7z", ".tar", ".gz", ".bz2", ".xz",
        ".part", ".001", ".002", ".003", ".r00", ".r01", ".z01",
    ]

    base = filename
    for ext in extensions:
        if base.lower().endswith(ext):
            base = base[:-len(ext)]

    # Remove part numbers
    base = re.sub(r"[._-]?part\d+$", "", base, flags=re.IGNORECASE)
    base = re.sub(r"[._-]?\d+of\d+$", "", base, flags=re.IGNORECASE)
    base = re.sub(r"[._-]?vol(ume)?\d+$", "", base, flags=re.IGNORECASE)

    return base.strip("._- ")
