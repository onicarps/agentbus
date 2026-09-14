"""Dynamic, metadata-only LLM pricing engine and OpenRouter catalog cache.

Supports live OpenRouter model pricing ingestion, persistent disk caching,
resilient static fallback, and smart model alias normalization.
A runner turn must never make a blocking WAN request or fail due to pricing lookup.
"""

from __future__ import annotations

import json
import logging
import os
import re
import tempfile
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Final

logger = logging.getLogger(__name__)

OPENROUTER_PRICING_SOURCE: Final = "openrouter_dynamic_v1"
FALLBACK_PRICING_SOURCE: Final = "agentbus_pricing_v1"
# Backward-compatibility alias
PRICING_SOURCE: Final = FALLBACK_PRICING_SOURCE

OPENROUTER_MODELS_URL: Final = "https://openrouter.ai/api/v1/models"
DEFAULT_CACHE_FILENAME: Final = "pricing_cache.json"

# Static baseline catalog (USD per 1,000,000 tokens: prompt, completion, cache_read)
# Used as guaranteed offline fallback when no dynamic cache exists.
FALLBACK_MODEL_PRICING: Final[dict[str, tuple[float, float, float]]] = {
    # OpenAI
    "gpt-5": (1.25, 10.00, 0.125),
    "openai/gpt-5": (1.25, 10.00, 0.125),
    "gpt-4o": (2.50, 10.00, 1.25),
    "openai/gpt-4o": (2.50, 10.00, 1.25),
    "gpt-4o-mini": (0.15, 0.60, 0.075),
    "openai/gpt-4o-mini": (0.15, 0.60, 0.075),
    "o1": (15.00, 60.00, 7.50),
    "openai/o1": (15.00, 60.00, 7.50),
    "o1-mini": (1.10, 4.40, 0.55),
    "openai/o1-mini": (1.10, 4.40, 0.55),
    "o3-mini": (1.10, 4.40, 0.55),
    "openai/o3-mini": (1.10, 4.40, 0.55),
    # Anthropic
    "claude-3-7-sonnet": (3.00, 15.00, 0.30),
    "claude-3.7-sonnet": (3.00, 15.00, 0.30),
    "anthropic/claude-3.7-sonnet": (3.00, 15.00, 0.30),
    "anthropic/claude-3-7-sonnet": (3.00, 15.00, 0.30),
    "claude-3-5-sonnet": (3.00, 15.00, 0.30),
    "claude-3.5-sonnet": (3.00, 15.00, 0.30),
    "anthropic/claude-3.5-sonnet": (3.00, 15.00, 0.30),
    "anthropic/claude-3-5-sonnet": (3.00, 15.00, 0.30),
    "claude-3-5-haiku": (0.80, 4.00, 0.08),
    "claude-3.5-haiku": (0.80, 4.00, 0.08),
    "anthropic/claude-3.5-haiku": (0.80, 4.00, 0.08),
    "anthropic/claude-3-5-haiku": (0.80, 4.00, 0.08),
    "claude-3-opus": (15.00, 75.00, 1.50),
    "anthropic/claude-3-opus": (15.00, 75.00, 1.50),
    # Google
    "gemini-2.5-pro": (1.25, 10.00, 0.125),
    "google/gemini-2.5-pro": (1.25, 10.00, 0.125),
    "gemini-2.5-flash": (0.30, 2.50, 0.03),
    "google/gemini-2.5-flash": (0.30, 2.50, 0.03),
    "gemini-2.0-flash": (0.10, 0.40, 0.025),
    "google/gemini-2.0-flash": (0.10, 0.40, 0.025),
    "gemini-1.5-pro": (1.25, 5.00, 0.3125),
    "google/gemini-1.5-pro": (1.25, 5.00, 0.3125),
    "gemini-1.5-flash": (0.075, 0.30, 0.01875),
    "google/gemini-1.5-flash": (0.075, 0.30, 0.01875),
    # DeepSeek
    "deepseek-r1": (0.55, 2.19, 0.14),
    "deepseek/deepseek-r1": (0.55, 2.19, 0.14),
    "deepseek-v3": (0.14, 0.28, 0.014),
    "deepseek/deepseek-chat": (0.14, 0.28, 0.014),
    # Meta
    "llama-3.3-70b-instruct": (0.12, 0.30, 0.03),
    "meta-llama/llama-3.3-70b-instruct": (0.12, 0.30, 0.03),
    "llama-3.1-405b-instruct": (0.80, 2.40, 0.20),
    "meta-llama/llama-3.1-405b-instruct": (0.80, 2.40, 0.20),
    # Qwen
    "qwen-2.5-coder-32b-instruct": (0.06, 0.15, 0.015),
    "qwen/qwen-2.5-coder-32b-instruct": (0.06, 0.15, 0.015),
}

# Alias for backward-compatibility
MODEL_PRICING = FALLBACK_MODEL_PRICING

# In-memory pricing cache state: (loaded_path, mtime, models_dict)
_LOADED_CACHE: tuple[str, float, dict[str, tuple[float, float, float]]] | None = None


def resolve_pricing_cache_path(workspace: Path | str | None = None) -> Path:
    """Return the absolute path for the pricing cache JSON file."""
    if workspace:
        return Path(workspace).expanduser().resolve() / ".agentbus" / DEFAULT_CACHE_FILENAME
    env = os.environ.get("AGENTBUS_WORKSPACE")
    if env:
        return Path(env).expanduser().resolve() / ".agentbus" / DEFAULT_CACHE_FILENAME
    cache_home = os.environ.get("XDG_CACHE_HOME")
    if cache_home:
        return Path(cache_home).expanduser().resolve() / "agentbus" / DEFAULT_CACHE_FILENAME
    return Path.home() / ".cache" / "agentbus" / DEFAULT_CACHE_FILENAME


def _parse_rate(val: object) -> float:
    """Safely parse a per-token price string or float into USD / 1,000,000 tokens."""
    if val is None:
        return 0.0
    try:
        f = float(val)  # type: ignore[arg-type]
        if f < 0.0 or not (f == f) or f == float("inf") or f == float("-inf"):
            return 0.0
        # OpenRouter rates are per-token; convert to per 1,000,000 tokens
        return round(f * 1_000_000, 6)
    except (TypeError, ValueError):
        return 0.0


def _generate_model_aliases(model_id: str) -> list[str]:
    """Generate normalized search aliases for a model ID."""
    cleaned = str(model_id or "").strip().lower()
    if not cleaned:
        return []

    aliases = [cleaned]
    # Strip provider prefix: "anthropic/claude-3.5-sonnet" -> "claude-3.5-sonnet"
    if "/" in cleaned:
        _, suffix = cleaned.split("/", 1)
        if suffix and suffix not in aliases:
            aliases.append(suffix)

    # Strip tags/modifiers: ":beta", ":free", "-latest"
    for current in list(aliases):
        if ":" in current:
            base_tag = current.split(":", 1)[0]
            if base_tag and base_tag not in aliases:
                aliases.append(base_tag)
        if current.endswith("-latest"):
            base_lat = current[:-7]
            if base_lat and base_lat not in aliases:
                aliases.append(base_lat)
        # Strip date suffixes: e.g. -2024-11-20 or -20241022 or -0528
        date_stripped = re.sub(r"-(\d{4}-\d{2}-\d{2}|\d{8}|\d{4})$", "", current)
        if date_stripped and date_stripped not in aliases:
            aliases.append(date_stripped)

    # Dot vs Dash variants: "claude-3.5-sonnet" <-> "claude-3-5-sonnet"
    for current in list(aliases):
        if "." in current:
            d_variant = current.replace(".", "-")
            if d_variant not in aliases:
                aliases.append(d_variant)
        # also if version like -3-5- or -2-5-
        v_variant = re.sub(r"-(\d+)-(\d+)-", r"-\1.\2-", current)
        if v_variant != current and v_variant not in aliases:
            aliases.append(v_variant)

    return aliases


def sync_openrouter_pricing(
    workspace: Path | str | None = None,
    endpoint: str = OPENROUTER_MODELS_URL,
    timeout: float = 5.0,
) -> dict:
    """Fetch live model pricing from OpenRouter API and persist to disk cache."""
    global _LOADED_CACHE
    req = urllib.request.Request(
        endpoint,
        headers={"User-Agent": "agentbus/pricing-sync (https://github.com/onicarps/agentbus)"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        status = getattr(resp, "status", None) or getattr(resp, "code", None)
        if status is not None and not isinstance(status, int):
            try:
                status = int(status)
            except (TypeError, ValueError):
                status = 200
        if status not in (None, 200):
            raise RuntimeError(f"OpenRouter API returned HTTP status {status}")
        raw_body = resp.read().decode("utf-8")
        data = json.loads(raw_body)

    models_raw = data.get("data", [])
    if not isinstance(models_raw, list):
        raise ValueError("Invalid OpenRouter response: 'data' array missing")

    indexed_models: dict[str, list[float]] = {}
    for item in models_raw:
        if not isinstance(item, dict):
            continue
        m_id = str(item.get("id") or "").strip()
        if not m_id:
            continue
        p = item.get("pricing")
        if not isinstance(p, dict):
            continue

        prompt_rate = _parse_rate(p.get("prompt"))
        completion_rate = _parse_rate(p.get("completion"))
        cache_rate = _parse_rate(p.get("input_cache_read") or p.get("prompt_cache_read"))

        rates = [prompt_rate, completion_rate, cache_rate]
        aliases = _generate_model_aliases(m_id)
        for alias in aliases:
            # Primary exact match takes precedence over secondary generated aliases
            if alias not in indexed_models or alias == m_id.lower():
                indexed_models[alias] = rates

    cache_path = resolve_pricing_cache_path(workspace)
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    updated_at = datetime.now(timezone.utc).isoformat()
    payload = {
        "version": 1,
        "updated_at": updated_at,
        "source": "openrouter_api",
        "endpoint": endpoint,
        "models_count": len(indexed_models),
        "raw_models_count": len(models_raw),
        "models": indexed_models,
    }

    # Atomic write to cache file
    tmp_fd, tmp_path_str = tempfile.mkstemp(
        dir=str(cache_path.parent),
        prefix=".pricing_cache_",
        suffix=".tmp",
    )
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        os.replace(tmp_path_str, str(cache_path))
    finally:
        if os.path.exists(tmp_path_str):
            try:
                os.unlink(tmp_path_str)
            except OSError:
                pass

    # Update in-memory cache
    try:
        mtime = cache_path.stat().st_mtime
    except OSError:
        mtime = 0.0

    in_memory_dict: dict[str, tuple[float, float, float]] = {
        k: (v[0], v[1], v[2]) for k, v in indexed_models.items() if len(v) >= 3
    }
    _LOADED_CACHE = (str(cache_path), mtime, in_memory_dict)

    return {
        "cache_path": str(cache_path),
        "updated_at": updated_at,
        "models_count": len(indexed_models),
        "raw_models_count": len(models_raw),
        "source": OPENROUTER_PRICING_SOURCE,
    }


def load_pricing_cache(
    workspace: Path | str | None = None,
    force_reload: bool = False,
) -> tuple[dict[str, tuple[float, float, float]], str | None]:
    """Load cached pricing data safely from disk into memory."""
    global _LOADED_CACHE
    cache_path = resolve_pricing_cache_path(workspace)
    if not cache_path.is_file():
        return {}, None

    try:
        mtime = cache_path.stat().st_mtime
        if not force_reload and _LOADED_CACHE is not None:
            c_path, c_mtime, c_data = _LOADED_CACHE
            if c_path == str(cache_path) and c_mtime == mtime:
                return c_data, None

        with open(cache_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        models = data.get("models", {})
        parsed: dict[str, tuple[float, float, float]] = {}
        for k, v in models.items():
            if isinstance(v, (list, tuple)) and len(v) >= 3:
                parsed[str(k).lower()] = (float(v[0]), float(v[1]), float(v[2]))

        _LOADED_CACHE = (str(cache_path), mtime, parsed)
        return parsed, data.get("updated_at")
    except Exception as exc:
        logger.debug("Failed to read pricing cache %s: %s", cache_path, exc)
        return {}, None


def get_pricing_status(workspace: Path | str | None = None) -> dict:
    """Return status information for the pricing catalog and disk cache."""
    cache_path = resolve_pricing_cache_path(workspace)
    models, updated_at = load_pricing_cache(workspace)
    exists = cache_path.is_file()
    size_bytes = cache_path.stat().st_size if exists else 0
    return {
        "cache_path": str(cache_path),
        "cache_exists": exists,
        "cache_size_bytes": size_bytes,
        "updated_at": updated_at,
        "dynamic_models_count": len(models),
        "fallback_models_count": len(FALLBACK_MODEL_PRICING),
        "primary_source": OPENROUTER_PRICING_SOURCE if models else FALLBACK_PRICING_SOURCE,
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
    workspace: Path | str | None = None,
) -> tuple[float, str]:
    """Return a rounded USD estimate and its provenance.

    Checks:
    1. Dynamic OpenRouter disk/memory cache.
    2. Static fallback catalog (`FALLBACK_MODEL_PRICING`).
    3. Returns (0.0, "unpriced") if unknown.
    """
    cleaned = str(model or "").strip().lower()
    if not cleaned:
        return 0.0, "unpriced"

    candidates = _generate_model_aliases(cleaned)

    # 1. Check dynamic cache
    dynamic_models, _ = load_pricing_cache(workspace)
    for cand in candidates:
        rates = dynamic_models.get(cand)
        if rates is not None:
            input_rate, output_rate, cache_rate = rates
            cost = (
                _tokens(input_tokens) * input_rate
                + _tokens(output_tokens) * output_rate
                + _tokens(cache_read_tokens) * cache_rate
            ) / 1_000_000
            return round(cost, 12), OPENROUTER_PRICING_SOURCE

    # 2. Check bundled fallback catalog
    for cand in candidates:
        rates = FALLBACK_MODEL_PRICING.get(cand)
        if rates is not None:
            input_rate, output_rate, cache_rate = rates
            cost = (
                _tokens(input_tokens) * input_rate
                + _tokens(output_tokens) * output_rate
                + _tokens(cache_read_tokens) * cache_rate
            ) / 1_000_000
            return round(cost, 12), FALLBACK_PRICING_SOURCE

    return 0.0, "unpriced"
