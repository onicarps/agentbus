"""Portable AgentID dependency and canonical-signature fixture."""

from __future__ import annotations

import base64
import hashlib
import importlib.metadata
import json
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from agentbus.identity import canonical_bytes


def test_agentid_dependency_versions_and_fixture() -> None:
    assert importlib.metadata.version("rfc8785") == "0.1.4"
    assert importlib.metadata.version("cryptography").startswith("50.0.")
    fixture = json.loads(
        (Path(__file__).parent / "fixtures" / "agentid" / "cross_language_v1.json")
        .read_text(encoding="utf-8")
    )
    canonical = canonical_bytes(fixture["envelope"]["signed"])
    assert hashlib.sha256(canonical).hexdigest() == fixture["canonical_sha256"]
    public = Ed25519PublicKey.from_public_bytes(
        base64.urlsafe_b64decode(fixture["public_key"] + "==")
    )
    public.verify(
        base64.urlsafe_b64decode(fixture["envelope"]["signature"] + "=="),
        canonical,
    )
