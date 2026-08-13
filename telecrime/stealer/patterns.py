"""File patterns and naming conventions for stealer logs."""

import re
from pathlib import Path

# Common credential file names (case-insensitive matching)
CREDENTIAL_FILE_PATTERNS = [
    # Password files
    r"passwords?\.txt$",
    r"all\s*passwords?.*\.txt$",
    r"_?allpasswords_list\.txt$",
    r"credentials?\.txt$",
    r"logins?\.txt$",

    # Combo lists / ULP format
    r"\[?ulp\]?.*\.txt$",
    r"combo.*\.txt$",
    r"lines.*\.txt$",
    r"private.*lines.*\.txt$",

    # Browser-specific
    r"google_\[?chrome\]?.*\.txt$",
    r"microsoft_\[?edge\]?.*\.txt$",
    r"mozilla_\[?firefox\]?.*\.txt$",
    r"opera.*\.txt$",
    r"brave.*\.txt$",
    r"vivaldi.*\.txt$",
    r"chromium.*\.txt$",

    # Autofill
    r"autofills?\.txt$",
    r"important\s*autofills?\.txt$",
]

# System information file patterns
SYSTEM_INFO_PATTERNS = [
    r"system\s*info(rmation)?\.txt$",
    r"user\s*info(rmation)?\.txt$",
    r"pc\s*info(rmation)?\.txt$",
    r"machine\s*info\.txt$",
]

# Cookie file patterns
COOKIE_FILE_PATTERNS = [
    r"cookies?\.txt$",
    r".*_cookies?\.txt$",
]

# Credit card file patterns
CREDIT_CARD_PATTERNS = [
    r"credit\s*cards?\.txt$",
    r"cards?\.txt$",
    r"cc\.txt$",
]

# Crypto wallet patterns
CRYPTO_WALLET_PATTERNS = [
    r"wallets?\.txt$",
    r"exodus.*\.txt$",
    r"atomic.*\.txt$",
    r"metamask.*\.txt$",
    r"electrum.*\.txt$",
]

# Compiled regex for efficiency
_credential_regex = re.compile(
    "|".join(f"({p})" for p in CREDENTIAL_FILE_PATTERNS),
    re.IGNORECASE
)

_system_info_regex = re.compile(
    "|".join(f"({p})" for p in SYSTEM_INFO_PATTERNS),
    re.IGNORECASE
)


def is_credential_file(filename: str) -> bool:
    """Check if filename matches credential file patterns."""
    name = Path(filename).name
    return bool(_credential_regex.search(name))


def is_system_info_file(filename: str) -> bool:
    """Check if filename matches system info patterns."""
    name = Path(filename).name
    return bool(_system_info_regex.search(name))


def find_credential_files(file_list: list[str]) -> list[str]:
    """Find all credential files in a list of paths.

    Args:
        file_list: List of file paths (can be nested like "folder/subfolder/Passwords.txt")

    Returns:
        List of paths matching credential file patterns
    """
    return [f for f in file_list if is_credential_file(f)]


def detect_stealer_type(
    file_list: list[str],
    content_sample: str | None = None,
    sysinfo_stealer: str | None = None,
) -> str | None:
    """Try to detect the stealer type from file structure, content, or sysinfo self-id.

    Detection priority (highest to lowest):
    1. sysinfo_stealer  — stealer self-identifies in SystemInfo.txt (most reliable)
    2. File signatures  — unique filenames only produced by one family
    3. Content keywords — ASCII art / strings in credential files

    Args:
        file_list: List of files in the archive
        content_sample: Optional sample of file content for signature detection
        sysinfo_stealer: Optional stealer name extracted from SystemInfo.txt

    Returns:
        Stealer name (lowercase) if detected, None otherwise
    """
    # Priority 1: SystemInfo self-identification
    # Validate: a real stealer name is short and doesn't look like a Telegram
    # channel description or URL (some logs embed full invite links/captions).
    if sysinfo_stealer:
        candidate = sysinfo_stealer.strip()
        is_plausible = (
            len(candidate) <= 50
            and "http" not in candidate.lower()
            and "t.me" not in candidate.lower()
            and "\n" not in candidate
        )
        if is_plausible:
            return candidate.lower()

    file_names = {Path(f).name.lower() for f in file_list}

    # Priority 2: File signatures (unique filenames per family)
    # RedLine
    if "domaindetects.txt" in file_names or "installedbrowsers.txt" in file_names:
        return "redline"
    # Raccoon
    if "machineinfo.txt" in file_names:
        return "raccoon"
    # Vidar: recognisable by userinfo.txt (distinct from Raccoon's machineinfo.txt)
    if "userinfo.txt" in file_names and "passwords.txt" in file_names:
        return "vidar"
    # Aurora: aurora-branded credential filenames
    if any("aurora" in n for n in file_names):
        return "aurora"
    # Mystic Stealer
    if any("mystic" in n for n in file_names):
        return "mystic"
    # Doenerium
    if any("doenerium" in n or "doen_" in n for n in file_names):
        return "doenerium"
    # Cryptbot: uses "Cryptbot" prefix on filenames
    if any("cryptbot" in n for n in file_names):
        return "cryptbot"
    # CINOSHI
    if any("cinoshi" in n for n in file_names):
        return "cinoshi"
    # Titan Stealer
    if any("titan" in n for n in file_names):
        return "titan"
    # Pandora
    if any("pandora" in n for n in file_names):
        return "pandora"

    # Priority 3: Content keyword signatures
    if content_sample:
        content_lower = content_sample.lower()
        if "redline" in content_lower:
            return "redline"
        if "raccoon" in content_lower:
            return "raccoon"
        if "vidar" in content_lower:
            return "vidar"
        if "lumma" in content_lower or "lummac2" in content_lower:
            return "lumma"
        if "stealc" in content_lower:
            return "stealc"
        if "meta stealer" in content_lower or ("meta" in content_lower and "stealer" in content_lower):
            return "meta"
        if "aurora stealer" in content_lower or "aurora log" in content_lower:
            return "aurora"
        if "mystic stealer" in content_lower or "mysticstealer" in content_lower:
            return "mystic"
        if "doenerium" in content_lower:
            return "doenerium"
        if "cryptbot" in content_lower:
            return "cryptbot"
        if "cinoshi" in content_lower:
            return "cinoshi"
        if "titan stealer" in content_lower:
            return "titan"
        if "pandora stealer" in content_lower:
            return "pandora"

    return None
