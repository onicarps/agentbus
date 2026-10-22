"""Deterministic standard-library encoding for AgentBus ledger hashes."""

from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Iterable

GENESIS_HASH = "0" * 64
_CHAIN_SEPARATOR = b"\x1f"

__all__ = [
    "GENESIS_HASH",
    "CanonicalError",
    "canonical_json",
    "canonical_bytes",
    "chain_digest",
    "event_digest",
    "hash_hex",
]


class CanonicalError(ValueError):
    """Raised when a value cannot be represented as canonical JSON."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _check_value(value: Any, path: str) -> None:
    if value is None or value is True or value is False:
        return
    if isinstance(value, str):
        try:
            value.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise CanonicalError(
                "invalid_string", f"{path}: string is not valid UTF-8: {exc}"
            ) from exc
        return
    if isinstance(value, int):
        return
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            raise CanonicalError("invalid_number", f"{path}: {value!r} is not finite")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise CanonicalError(
                    "invalid_key", f"{path}: object key {key!r} is not a string"
                )
            _check_value(item, f"{path}.{key}")
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _check_value(item, f"{path}[{index}]")
        return
    raise CanonicalError("invalid_type", f"{path}: {type(value).__name__} is not JSON")


def _normalize(value: Any) -> Any:
    if isinstance(value, float):
        return 0.0 if value == 0.0 else value
    if isinstance(value, dict):
        return {key: _normalize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize(item) for item in value]
    return value


def canonical_json(value: Any) -> str:
    """Serialize ``value`` to deterministic compact JSON text."""
    _check_value(value, "$")
    return json.dumps(
        _normalize(value),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def canonical_bytes(value: Any) -> bytes:
    return canonical_json(value).encode("utf-8")


def hash_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _validate_prev_hash(prev_hash: str) -> None:
    if not isinstance(prev_hash, str) or len(prev_hash) != 64:
        raise CanonicalError("invalid_prev_hash", "previous hash must be 64 hex chars")
    try:
        int(prev_hash, 16)
    except ValueError as exc:
        raise CanonicalError(
            "invalid_prev_hash", "previous hash must be hexadecimal"
        ) from exc


def chain_digest(prev_hash: str, body_bytes: bytes) -> str:
    """Return SHA-256(prev_hash || 0x1f || canonical body bytes)."""
    _validate_prev_hash(prev_hash)
    if not isinstance(body_bytes, (bytes, bytearray)):
        raise CanonicalError("invalid_body", "body must be bytes")
    return hash_hex(prev_hash.encode("ascii") + _CHAIN_SEPARATOR + bytes(body_bytes))


def event_digest(prev_hash: str, body: Any) -> str:
    return chain_digest(prev_hash, canonical_bytes(body))


def digest_many(prev_hash: str, bodies: Iterable[Any]) -> list[str]:
    digests: list[str] = []
    current = prev_hash
    for body in bodies:
        current = event_digest(current, body)
        digests.append(current)
    return digests
