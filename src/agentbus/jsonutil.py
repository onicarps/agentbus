"""Strict, dependency-free JSON validation shared by hot and crypto paths."""

from __future__ import annotations

import json
import math
import unicodedata
from typing import Any

from agentbus.errors import IdentityError

MAX_SAFE_INTEGER = (1 << 53) - 1


def _reject_duplicate(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise IdentityError(f"duplicate_json_key: {key}")
        result[key] = value
    return result


def strict_json_loads(raw: str | bytes) -> Any:
    """Decode JSON without lossy or ambiguous inputs."""
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise IdentityError("invalid_utf8") from exc
    try:
        value = json.loads(
            raw,
            object_pairs_hook=_reject_duplicate,
            parse_constant=lambda token: (_ for _ in ()).throw(
                IdentityError(f"non_finite_number: {token}")
            ),
        )
        validate_jcs_value(value)
    except IdentityError:
        raise
    except json.JSONDecodeError as exc:
        raise IdentityError(f"invalid_json: {exc.msg}") from exc
    except (RecursionError, ValueError) as exc:
        raise IdentityError("invalid_json: unsupported_structure") from exc
    return value


def validate_jcs_value(value: Any, *, path: str = "$") -> None:
    """Enforce the common Python/Node/Go AgentID JSON input domain."""
    if value is None or isinstance(value, bool):
        return
    if isinstance(value, str):
        if unicodedata.normalize("NFC", value) != value:
            raise IdentityError(f"non_nfc_string: {path}")
        try:
            value.encode("utf-8", errors="strict")
        except UnicodeEncodeError as exc:
            raise IdentityError(f"invalid_unicode_scalar: {path}") from exc
        return
    if isinstance(value, int):
        if abs(value) > MAX_SAFE_INTEGER:
            raise IdentityError(f"unsafe_integer: {path}")
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise IdentityError(f"non_finite_number: {path}")
        return
    if isinstance(value, list):
        for index, child in enumerate(value):
            validate_jcs_value(child, path=f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, child in value.items():
            if not isinstance(key, str):
                raise IdentityError(f"non_string_key: {path}")
            validate_jcs_value(key, path=f"{path}.<key>")
            validate_jcs_value(child, path=f"{path}.{key}")
        return
    raise IdentityError(f"unsupported_json_type: {path}={type(value).__name__}")
