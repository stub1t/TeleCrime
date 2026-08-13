"""Retry and backoff utilities."""

from __future__ import annotations

import random


def backoff_delay(
    attempt: int,
    base_seconds: float,
    max_seconds: float,
    jitter: float = 0.1,
) -> float:
    """Compute exponential backoff with jitter.

    Args:
        attempt: Zero-based attempt index.
        base_seconds: Base delay in seconds.
        max_seconds: Maximum delay in seconds.
        jitter: Fractional jitter (0.1 = +/-10%).
    """
    if attempt <= 0:
        delay = base_seconds
    else:
        delay = min(max_seconds, base_seconds * (2 ** attempt))

    if jitter > 0:
        delta = delay * jitter
        delay = random.uniform(max(0.0, delay - delta), delay + delta)

    return delay
