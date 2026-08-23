from __future__ import annotations

from pathlib import Path

import pytest

from scripts.check_pypi_release import evaluate_release, local_artifacts


def test_release_absent_requires_publish() -> None:
    assert evaluate_release({"pkg.whl": "abc"}, None) is True


def test_exact_release_is_safe_noop() -> None:
    assert evaluate_release({"pkg.whl": "abc"}, {"pkg.whl": "abc"}) is False


def test_partial_matching_release_can_resume() -> None:
    local = {"pkg.whl": "abc", "pkg.tar.gz": "def"}
    assert evaluate_release(local, {"pkg.whl": "abc"}) is True


@pytest.mark.parametrize(
    "remote",
    [
        {"pkg.whl": "different"},
        {"pkg.whl": "abc", "foreign.whl": "def"},
    ],
)
def test_conflicting_release_fails_closed(remote: dict[str, str]) -> None:
    with pytest.raises(ValueError, match="differs from this build"):
        evaluate_release({"pkg.whl": "abc"}, remote)


def test_local_artifacts_hashes_only_release_files(tmp_path: Path) -> None:
    wheel = tmp_path / "pkg.whl"
    wheel.write_bytes(b"wheel")
    (tmp_path / "notes.txt").write_text("ignore", encoding="utf-8")
    artifacts = local_artifacts(tmp_path)
    assert list(artifacts) == ["pkg.whl"]
    assert len(artifacts["pkg.whl"]) == 64
