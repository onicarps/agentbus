from __future__ import annotations

import pytest

from agentbus.pricing import PRICING_SOURCE, estimate_llm_cost


@pytest.mark.parametrize(
    ("model", "input_tokens", "output_tokens", "cache_tokens", "expected"),
    [
        ("gpt-5", 1_000_000, 1_000_000, 1_000_000, 11.375),
        ("GPT-4O-MINI", 1_000_000, 0, 0, 0.15),
        ("claude-3-7-sonnet", 0, 1_000_000, 0, 15.0),
        ("gemini-2.5-flash", 0, 0, 1_000_000, 0.03),
    ],
)
def test_estimate_llm_cost_for_catalog_models(
    model: str, input_tokens: int, output_tokens: int, cache_tokens: int, expected: float
) -> None:
    assert estimate_llm_cost(model, input_tokens, output_tokens, cache_tokens) == (
        expected,
        PRICING_SOURCE,
    )


def test_unknown_or_invalid_usage_is_unpriced_or_zero_bounded() -> None:
    assert estimate_llm_cost("not-a-model", 50, 50) == (0.0, "unpriced")
    assert estimate_llm_cost("gpt-5", -1, -2, -3) == (0.0, PRICING_SOURCE)
