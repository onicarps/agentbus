from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from agentbus.cli import main
from agentbus.pricing import (
    FALLBACK_MODEL_PRICING,
    FALLBACK_PRICING_SOURCE,
    OPENROUTER_PRICING_SOURCE,
    PRICING_SOURCE,
    _generate_model_aliases,
    _parse_rate,
    _tokens,
    estimate_llm_cost,
    get_pricing_status,
    load_pricing_cache,
    resolve_pricing_cache_path,
    sync_openrouter_pricing,
)


@pytest.mark.parametrize(
    ("model", "input_tokens", "output_tokens", "cache_tokens", "expected"),
    [
        ("gpt-5", 1_000_000, 1_000_000, 1_000_000, 11.375),
        ("openai/gpt-5", 1_000_000, 1_000_000, 1_000_000, 11.375),
        ("GPT-4O-MINI", 1_000_000, 0, 0, 0.15),
        ("claude-3-7-sonnet", 0, 1_000_000, 0, 15.0),
        ("claude-3.7-sonnet", 0, 1_000_000, 0, 15.0),
        ("anthropic/claude-3.5-sonnet", 1_000_000, 1_000_000, 1_000_000, 18.30),
        ("gemini-2.5-flash", 0, 0, 1_000_000, 0.03),
        ("google/gemini-2.0-flash", 1_000_000, 1_000_000, 0, 0.50),
        ("deepseek-r1", 1_000_000, 1_000_000, 1_000_000, 2.88),
        ("deepseek/deepseek-r1", 1_000_000, 1_000_000, 0, 2.74),
        ("llama-3.3-70b-instruct", 1_000_000, 1_000_000, 0, 0.42),
        ("qwen-2.5-coder-32b-instruct", 1_000_000, 1_000_000, 0, 0.21),
    ],
)
def test_estimate_llm_cost_for_fallback_catalog_models(
    model: str, input_tokens: int, output_tokens: int, cache_tokens: int, expected: float, tmp_path: Path
) -> None:
    # Using an empty workspace so fallback catalog is guaranteed to be used
    cost, source = estimate_llm_cost(model, input_tokens, output_tokens, cache_tokens, workspace=tmp_path)
    assert cost == expected
    assert source == FALLBACK_PRICING_SOURCE


def test_unknown_or_invalid_usage_is_unpriced_or_zero_bounded(tmp_path: Path) -> None:
    assert estimate_llm_cost("not-a-model", 50, 50, workspace=tmp_path) == (0.0, "unpriced")
    assert estimate_llm_cost("gpt-5", -1, -2, -3, workspace=tmp_path) == (0.0, FALLBACK_PRICING_SOURCE)
    assert estimate_llm_cost("", 100, 100, workspace=tmp_path) == (0.0, "unpriced")


def test_token_coercion() -> None:
    assert _tokens(100) == 100
    assert _tokens(-50) == 0
    assert _tokens("bad") == 0  # type: ignore[arg-type]
    assert _tokens(None) == 0  # type: ignore[arg-type]


def test_parse_rate() -> None:
    assert _parse_rate("0.0000025") == 2.5
    assert _parse_rate(0.00001) == 10.0
    assert _parse_rate("0") == 0.0
    assert _parse_rate("-0.5") == 0.0
    assert _parse_rate("invalid") == 0.0
    assert _parse_rate(None) == 0.0
    assert _parse_rate(float("inf")) == 0.0
    assert _parse_rate(float("nan")) == 0.0


def test_generate_model_aliases() -> None:
    aliases = _generate_model_aliases("Anthropic/Claude-3.5-Sonnet:beta")
    assert "anthropic/claude-3.5-sonnet:beta" in aliases
    assert "claude-3.5-sonnet:beta" in aliases
    assert "claude-3.5-sonnet" in aliases
    assert "claude-3-5-sonnet" in aliases

    aliases_date = _generate_model_aliases("openai/gpt-4o-2024-11-20")
    assert "openai/gpt-4o-2024-11-20" in aliases_date
    assert "gpt-4o-2024-11-20" in aliases_date
    assert "gpt-4o" in aliases_date


def test_sync_openrouter_pricing_and_dynamic_lookup(tmp_path: Path) -> None:
    mock_payload = {
        "data": [
            {
                "id": "mock-vendor/frontier-v1-2026-09-01",
                "pricing": {
                    "prompt": "0.000005",
                    "completion": "0.000020",
                    "input_cache_read": "0.000001",
                },
            },
            {
                "id": "anthropic/claude-3.7-sonnet",
                "pricing": {
                    "prompt": "0.000003",
                    "completion": "0.000015",
                    "input_cache_read": "0.0000003",
                },
            },
        ]
    }

    mock_resp = MagicMock()
    mock_resp.status = 200
    mock_resp.code = 200
    mock_resp.read.return_value = json.dumps(mock_payload).encode("utf-8")
    mock_resp.__enter__.return_value = mock_resp

    with patch("urllib.request.urlopen", return_value=mock_resp):
        res = sync_openrouter_pricing(workspace=tmp_path)
        assert res["source"] == OPENROUTER_PRICING_SOURCE
        assert res["models_count"] >= 2
        assert Path(res["cache_path"]).is_file()

    # Dynamic lookup should find the newly ingested mock model
    cost, source = estimate_llm_cost(
        "frontier-v1",
        input_tokens=1_000_000,
        output_tokens=1_000_000,
        cache_read_tokens=1_000_000,
        workspace=tmp_path,
    )
    assert cost == 26.0  # 5 + 20 + 1
    assert source == OPENROUTER_PRICING_SOURCE

    # Dynamic cache takes priority over fallback
    cost, source = estimate_llm_cost(
        "claude-3.7-sonnet",
        input_tokens=1_000_000,
        output_tokens=1_000_000,
        cache_read_tokens=0,
        workspace=tmp_path,
    )
    assert cost == 18.0
    assert source == OPENROUTER_PRICING_SOURCE


def test_get_pricing_status(tmp_path: Path) -> None:
    # Before cache
    st = get_pricing_status(tmp_path)
    assert st["cache_exists"] is False
    assert st["dynamic_models_count"] == 0
    assert st["primary_source"] == FALLBACK_PRICING_SOURCE

    # Create dummy cache file
    cache_file = resolve_pricing_cache_path(tmp_path)
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(
        json.dumps({
            "version": 1,
            "updated_at": "2026-09-14T08:00:00Z",
            "models": {"test-model": [1.0, 2.0, 0.5]},
        }),
        encoding="utf-8",
    )

    st2 = get_pricing_status(tmp_path)
    assert st2["cache_exists"] is True
    assert st2["dynamic_models_count"] == 1
    assert st2["primary_source"] == OPENROUTER_PRICING_SOURCE


def test_cli_pricing_commands(tmp_path: Path) -> None:
    runner = CliRunner()

    # 1. status
    res = runner.invoke(main, ["pricing", "status", "--workspace", str(tmp_path)])
    assert res.exit_code == 0
    assert "Cache path:" in res.output

    # 2. estimate fallback
    res = runner.invoke(
        main,
        [
            "pricing",
            "estimate",
            "gpt-4o",
            "--input-tokens",
            "1000000",
            "--output-tokens",
            "500000",
            "--workspace",
            str(tmp_path),
            "--output-format",
            "json",
        ],
    )
    assert res.exit_code == 0
    data = json.loads(res.output)
    assert data["estimated_cost_usd"] == 7.5
    assert data["pricing_source"] == FALLBACK_PRICING_SOURCE

    # 3. sync with mock
    mock_payload = {
        "data": [
            {
                "id": "openai/gpt-4o",
                "pricing": {"prompt": "0.0000025", "completion": "0.000010"},
            }
        ]
    }
    mock_resp = MagicMock()
    mock_resp.status = 200
    mock_resp.code = 200
    mock_resp.read.return_value = json.dumps(mock_payload).encode("utf-8")
    mock_resp.__enter__.return_value = mock_resp

    with patch("urllib.request.urlopen", return_value=mock_resp):
        res = runner.invoke(main, ["pricing", "sync", "--workspace", str(tmp_path)])
        assert res.exit_code == 0
        assert "Synced" in res.output
