"""Shared helpers for credential deduplication and identity handling."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TypeVar

T = TypeVar("T")


def soft_dedupe_credentials(
    credentials: Iterable[T],
    *,
    limit: int | None = None,
) -> list[T]:
    """Collapse semantically equivalent credentials for search output.

    A credential's identity is its soft (grouped) hash when present, falling
    back to the exact hash, then the row id. Shared by the CLI and web search
    surfaces so both behave identically.
    """
    seen: set[str | int] = set()
    unique: list[T] = []
    for cred in credentials:
        key: str | int | None = (
            getattr(cred, "soft_credential_hash", None)
            or getattr(cred, "credential_hash", None)
            or getattr(cred, "id", None)
        )
        if key is None or key in seen:
            continue
        seen.add(key)
        unique.append(cred)
        if limit is not None and len(unique) >= limit:
            break
    return unique
