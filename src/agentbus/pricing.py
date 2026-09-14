"""Deterministic, metadata-only LLM pricing estimates.

Rates are USD per million tokens.  They are deliberately static: a runner must
not make a network request, or fail a turn, merely to emit observability data.
"""

from __future__ import annotations

from typing import Final


PRICING_SOURCE: Final = "agentbus_pricing_v1"

# input, output, cache-read — USD / 1,000,000 tokens.
# Catalog values are the standard public list prices selected for Phase 3.
MODEL_PRICING: Final[dict[str, tuple[float, float, float]]] = {
    "gpt-5": (1.25, 10.00, 0.125),
    "gpt-4o": (2.50, 10.00, 1.25),
    "gpt-4o-mini": (0.15, 0.60, 0.075),
    "o1": (15.00, 60.00, 7.50),
    "o3-mini": (1.10, 4.40, 0.55),
    "claude-3-7-sonnet": (3.00, 15.00, 0.30),
    "claude-3-5-sonnet": (3.00, 15.00, 0.30),
    "claude-3-5-haiku": (0.80, 4.00, 0.08),
    "gemini-2.5-pro": (1.25, 10.00, 0.125),
    "gemini-2.5-flash": (0.30, 2.50, 0.03),
    "gemini-2.0-flash": (0.10, 0.40, 0.025),
}


def _tokens(value: int) -> int:
    """Coerce a token count safely; negative/invalid values cost nothing."""
    try:
        return max(0, int(value))
    except (TypeError, ValueError, OverflowError):
        return 0


def estimate_llm_cost(
    model: str,
    input_tokens: int,
    output_tokens: int,
    cache_read_tokens: int = 0,
) -> tuple[float, str]:
    """Return a rounded USD estimate and its provenance.

    Unknown models intentionally return zero rather than guessing a price.
    """
    rates = MODEL_PRICING.get(str(model or "").strip().lower())
    if rates is None:
        return 0.0, "unpriced"
    input_rate, output_rate, cache_rate = rates
    cost = (
        _tokens(input_tokens) * input_rate
        + _tokens(output_tokens) * output_rate
        + _tokens(cache_read_tokens) * cache_rate
    ) / 1_000_000
    return round(cost, 12), PRICING_SOURCE
