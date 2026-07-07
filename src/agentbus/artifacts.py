"""Artifact attachment storage — separate blobs from event payloads."""

from __future__ import annotations

from pathlib import Path

MAX_ARTIFACT_BYTES = 1 * 1024 * 1024
ALLOWED_TYPES = frozenset({"git_diff", "file_content", "error_trace"})


class PayloadTooLargeError(Exception):
    """Artifact exceeds size limit — maps to HTTP 413."""

    def __init__(self, message: str, *, code: int = 413) -> None:
        super().__init__(message)
        self.code = code


def _content_bytes(content: str) -> int:
    return len(content.encode("utf-8"))


def validate_artifact(artifact: dict) -> dict:
    art_type = artifact.get("type")
    name = artifact.get("name")
    content = artifact.get("content")
    if art_type not in ALLOWED_TYPES:
        raise ValueError(f"invalid_artifact_type: {art_type}")
    if not name or not isinstance(name, str):
        raise ValueError("invalid_artifact_name")
    if len(name) > 256:
        raise ValueError("invalid_artifact_name: max length 256")
    if not isinstance(content, str):
        raise ValueError("invalid_artifact_content")
    size = _content_bytes(content)
    if size > MAX_ARTIFACT_BYTES:
        raise PayloadTooLargeError(
            f"413 Payload Too Large: artifact '{name}' is {size} bytes (max {MAX_ARTIFACT_BYTES})"
        )
    return {"type": art_type, "name": name, "content": content}


def validate_artifacts(artifacts: list) -> list[dict]:
    if not artifacts:
        return []
    if not isinstance(artifacts, list):
        raise ValueError("invalid_artifacts: expected array")
    if len(artifacts) > 10:
        raise ValueError("invalid_artifacts: max 10 per event")
    return [validate_artifact(a) for a in artifacts]


def artifact_from_file(path: Path, *, art_type: str = "file_content") -> dict:
    content = path.read_text(encoding="utf-8")
    return validate_artifact(
        {"type": art_type, "name": path.name, "content": content}
    )


def extract_artifacts(payload: dict) -> tuple[dict, list[dict]]:
    """Remove artifacts from payload copy; return (stored_payload, artifacts)."""
    stored = dict(payload)
    raw = stored.pop("artifacts", None) or []
    return stored, validate_artifacts(raw)