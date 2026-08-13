"""Password candidate ranking."""

from dataclasses import dataclass

from telecrime.models import PasswordCandidate
from telecrime.states import PasswordScope


@dataclass
class RankedPassword:
    """A password with computed rank score."""

    candidate: PasswordCandidate
    score: float
    reason: str


# Scope weights (higher = more likely to be correct)
SCOPE_WEIGHTS: dict[PasswordScope, float] = {
    PasswordScope.MESSAGE: 1.0,      # From the same message - highest priority
    PasswordScope.LEARNED: 0.95,     # Previously worked in this conversation
    PasswordScope.NEARBY: 0.7,       # From nearby messages
    PasswordScope.CONVERSATION: 0.5,  # Conversation-level hints
    PasswordScope.GLOBAL: 0.3,       # Global defaults
}


def rank_passwords(candidates: list[PasswordCandidate]) -> list[RankedPassword]:
    """Rank password candidates by likelihood of success.

    Args:
        candidates: List of PasswordCandidate objects

    Returns:
        List of RankedPassword objects sorted by score (highest first)
    """
    ranked: list[RankedPassword] = []

    for candidate in candidates:
        score = compute_score(candidate)
        reason = get_ranking_reason(candidate)
        ranked.append(RankedPassword(
            candidate=candidate,
            score=score,
            reason=reason,
        ))

    # Sort by score descending
    ranked.sort(key=lambda r: r.score, reverse=True)

    return ranked


def compute_score(candidate: PasswordCandidate) -> float:
    """Compute ranking score for a password candidate.

    Args:
        candidate: The password candidate

    Returns:
        Score between 0 and 1
    """
    # Base score from scope
    scope_weight = SCOPE_WEIGHTS.get(candidate.scope, 0.5)

    # Confidence factor
    confidence_factor = candidate.confidence

    # Success/failure history
    history_factor = 1.0
    total_attempts = candidate.times_succeeded + candidate.times_failed
    if total_attempts > 0:
        success_rate = candidate.times_succeeded / total_attempts
        # Boost successful passwords, penalize failed ones
        history_factor = 0.5 + (success_rate * 0.5)

    # Combine factors
    score = scope_weight * confidence_factor * history_factor

    # Bonus for passwords that have worked before
    if candidate.times_succeeded > 0:
        score = min(1.0, score * 1.2)

    # Penalty for passwords that have failed multiple times
    if candidate.times_failed > 2:
        score *= 0.5

    return min(1.0, max(0.0, score))


def get_ranking_reason(candidate: PasswordCandidate) -> str:
    """Get human-readable reason for password ranking.

    Args:
        candidate: The password candidate

    Returns:
        Explanation string
    """
    reasons = []

    # Scope explanation
    scope_explanations = {
        PasswordScope.MESSAGE: "found in message caption",
        PasswordScope.LEARNED: "worked before in this conversation",
        PasswordScope.NEARBY: "found in nearby message",
        PasswordScope.CONVERSATION: "conversation-level password",
        PasswordScope.GLOBAL: "global default",
    }
    reasons.append(scope_explanations.get(candidate.scope, "unknown source"))

    # Extraction method
    if candidate.extraction_method:
        reasons.append(f"extracted via {candidate.extraction_method}")

    # History
    if candidate.times_succeeded > 0:
        reasons.append(f"succeeded {candidate.times_succeeded}x")
    if candidate.times_failed > 0:
        reasons.append(f"failed {candidate.times_failed}x")

    # Confidence
    if candidate.confidence >= 0.9:
        reasons.append("high confidence")
    elif candidate.confidence >= 0.7:
        reasons.append("medium confidence")
    elif candidate.confidence < 0.5:
        reasons.append("low confidence")

    return "; ".join(reasons)


def deduplicate_candidates(
    candidates: list[PasswordCandidate],
) -> list[PasswordCandidate]:
    """Remove duplicate password values, keeping highest-ranked.

    Args:
        candidates: List of candidates (should already be ranked)

    Returns:
        Deduplicated list
    """
    seen_values: set[str] = set()
    unique: list[PasswordCandidate] = []

    for candidate in candidates:
        normalized = candidate.value.strip().lower()
        if normalized not in seen_values:
            seen_values.add(normalized)
            unique.append(candidate)

    return unique
