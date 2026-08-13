"""Stealer log parsing module."""

from telecrime.stealer.models import Credential, StealerLog
from telecrime.stealer.parser import parse_credentials_file, parse_credentials_text
from telecrime.stealer.patterns import (
    CREDENTIAL_FILE_PATTERNS,
    SYSTEM_INFO_PATTERNS,
    find_credential_files,
)

__all__ = [
    "parse_credentials_file",
    "parse_credentials_text",
    "CREDENTIAL_FILE_PATTERNS",
    "SYSTEM_INFO_PATTERNS",
    "find_credential_files",
    "Credential",
    "StealerLog",
]
