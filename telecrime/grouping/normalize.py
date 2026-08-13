"""Normalization helpers for archive grouping."""

from __future__ import annotations

import re
import unicodedata

_CONFUSABLE_TRANSLATION = str.maketrans(
    {
        "А": "A",
        "В": "B",
        "С": "C",
        "Е": "E",
        "Н": "H",
        "К": "K",
        "М": "M",
        "О": "O",
        "Р": "P",
        "Т": "T",
        "Х": "X",
        "У": "Y",
        "а": "a",
        "е": "e",
        "о": "o",
        "р": "p",
        "с": "c",
        "у": "y",
        "х": "x",
        "і": "i",
        "ј": "j",
        "к": "k",
        "м": "m",
        "н": "h",
        "т": "t",
        "в": "b",
        "Α": "A",
        "Β": "B",
        "Ε": "E",
        "Ζ": "Z",
        "Η": "H",
        "Ι": "I",
        "Κ": "K",
        "Μ": "M",
        "Ν": "N",
        "Ο": "O",
        "Ρ": "P",
        "Τ": "T",
        "Υ": "Y",
        "Χ": "X",
        "α": "a",
        "β": "b",
        "γ": "y",
        "ι": "i",
        "κ": "k",
        "μ": "m",
        "ν": "v",
        "ο": "o",
        "ρ": "p",
        "τ": "t",
        "υ": "u",
        "χ": "x",
    }
)


def normalize_confusables(text: str) -> str:
    """Fold a conservative set of common Unicode confusables to ASCII lookalikes."""
    return unicodedata.normalize("NFKC", text).translate(_CONFUSABLE_TRANSLATION)


def normalize_group_key(text: str) -> str:
    """Normalize text for grouping/similarity comparisons."""
    if not text:
        return ""
    normalized = normalize_confusables(text).casefold()
    normalized = re.sub(r"[\s._-]+", " ", normalized)
    return normalized.strip()
