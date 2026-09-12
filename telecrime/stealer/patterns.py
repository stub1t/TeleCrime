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

    # Marketplace/stealer-cloud dump names (verified live: 3,881 direct-txt
    # credential files, ~1.1 TB, were rejected before these were added):
    # "@TXTLOG_ALIEN - 712.txt", "@InfernoUrl [URL LOG PASS PRIVATE 133].txt",
    # "@segacloud BIG URL LOGIN PASS (2).txt", "HotmailValid @MASTER_CLOUDS.txt",
    # "Uk_Gov_Service_by@Master_clouds.txt". STRONG tokens match alone;
    # WEAK tokens (log/url/mail/account/valid) only in combination — a bare
    # "log.txt"/"mail.txt"/"url.txt"/"valid.txt" is not a credential dump.
r"(?:^|[_ ])(?:pass|login|logins|combo|dump|dumps|creds?|ulp|txtlog)\w*\.txt$",
    r"(?:^|[_ ])(?:pass|login|logins|combo|dump|dumps|creds?|ulp|txtlog)"
    r"\w*.*(?:^|[_ ])(?:log|url|mail|account|accounts|valid)\b.*\.txt$",
    r"\b(?:log|url|mail|account|accounts|valid)\b.*(?:^|[_ ])(?:pass|login|logins|combo|dump|dumps|creds?|ulp|"
    r"txtlog)\w*\.txt$",
    r"(?:txtlog|url\s*log(?:in)?\s*pass|log\s*in\s*pass|login\s*pass|mail\s*pass).*\.txt$",
    r"\b(?:email|mail|valid)(?:pass|creds?|dump|login|list|account|id|pwd)\w*\.txt$",
    r"passwords?\s*(?:backup|list|dump).*\.txt$",
    # Chromium's credential database export name.
    r"(?:^|[_ ])login\s*data\b.*\.txt$",
    # ^ anchor REQUIRED: without it, "notes@example.com.txt" matches via
    # re.search. Channel dump names start with @.
    r"^@[\w. ()\[\]-]{2,}\.txt$",
    r"^@[\w. #-]+\s*-\s*\d+.*\.txt$",
    r"(?:hotmail|gmail|yahoo|outlook)[\w .@()-]*\.txt$",
    r"\b(?:mansory|raven|segacloud|anubis|wangling|plunder|azul|inferno)[\w .()-]*\.txt$",
    r"\b(?:mix)\b.*\d.*\.txt$",
    r"[a-z0-9]+_com\.txt$",
    r"private.*(?:log|pass|url|dump).*\.txt$",
    r"by@[\w. -]+\.txt$",
]

# System information file patterns
SYSTEM_INFO_PATTERNS = [
    r"system\s*info(rmation)?\.txt$",
    r"user\s*info(rmation)?\.txt$",
    r"pc\s*info(rmation)?\.txt$",
    r"machine\s*info\.txt$",
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

# Download managers, file explorers and dedup extractors rename duplicates
# with a trailing counter or "copy" marker. Stripping it before the gate turns
# "Passwords (1).txt", "Passwords - Copy.txt" etc. into their base name.
_DUPLICATE_SUFFIX_RE = re.compile(
    r"(?:[\s_-]*[\(\[]\s*\d{1,3}\s*[\)\]]"
    r"|[\s_-]+\d{1,3}"
    r"|[\s_-]+copy(?:\s*[\(\[]?\s*\d{1,3}\s*[\)\]]?)?)$",
    re.IGNORECASE,
)


def _strip_duplicate_suffix(name: str) -> str:
    """Remove trailing duplicate/copy suffixes from a filename stem.

    ``Passwords (1).txt`` → ``Passwords.txt``; ``dump - Copy (2).txt`` →
    ``dump.txt``. The extension is preserved as-is.
    """
    stem, dot, ext = name.rpartition(".")
    if not dot:
        return name
    previous = None
    while previous != stem:
        previous = stem
        stem = _DUPLICATE_SUFFIX_RE.sub("", stem)
    return f"{stem}{dot}{ext}"


def is_credential_file(filename: str) -> bool:
    """Check if filename matches credential file patterns."""
    name = Path(filename).name
    if _credential_regex.search(name):
        return True
    return bool(_credential_regex.search(_strip_duplicate_suffix(name)))


def is_system_info_file(filename: str) -> bool:
    """Check if filename matches system info patterns."""
    name = Path(filename).name
    return bool(_system_info_regex.search(name))

def detect_stealer_type(
    file_list: list[str],
    sysinfo_stealer: str | None = None,
) -> str | None:
    """Try to detect the stealer type from file structure or sysinfo self-id.

    Detection priority (highest to lowest):
    1. sysinfo_stealer  — stealer self-identifies in SystemInfo.txt (most reliable)
    2. File signatures  — unique filenames only produced by one family

    Args:
        file_list: List of files in the archive
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

    return None
