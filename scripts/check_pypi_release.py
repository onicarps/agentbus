#!/usr/bin/env python3
"""Fail closed when a tag rerun disagrees with files already on PyPI."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import sys
import tomllib
import urllib.error
import urllib.request
from pathlib import Path


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def local_artifacts(dist_dir: Path) -> dict[str, str]:
    files = sorted(
        path for path in dist_dir.iterdir()
        if path.is_file() and (path.suffix == ".whl" or path.name.endswith(".tar.gz"))
    )
    if not files:
        raise ValueError(f"no wheel or sdist artifacts found in {dist_dir}")
    return {path.name: sha256(path) for path in files}


def runtime_version(init_path: Path) -> str:
    """Read a literal ``__version__`` assignment without importing the package."""
    module = ast.parse(init_path.read_text(encoding="utf-8"), filename=str(init_path))
    for statement in module.body:
        if not isinstance(statement, (ast.Assign, ast.AnnAssign)):
            continue
        targets = statement.targets if isinstance(statement, ast.Assign) else [statement.target]
        if not any(isinstance(target, ast.Name) and target.id == "__version__" for target in targets):
            continue
        value = statement.value
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            return value.value
        raise ValueError(f"__version__ must be a literal string in {init_path}")
    raise ValueError(f"no __version__ assignment found in {init_path}")


def fetch_release(project: str, version: str) -> dict[str, str] | None:
    url = f"https://pypi.org/pypi/{project}/{version}/json"
    try:
        with urllib.request.urlopen(url, timeout=15) as response:  # noqa: S310
            payload = json.load(response)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise
    return {
        item["filename"]: item["digests"]["sha256"]
        for item in payload.get("urls", [])
    }


def evaluate_release(local: dict[str, str], remote: dict[str, str] | None) -> bool:
    """Return whether upload is needed; raise when a rerun is not identical."""
    if remote is None:
        return True
    unexpected = sorted(set(remote) - set(local))
    mismatched = sorted(
        name for name in set(local) & set(remote) if local[name] != remote[name]
    )
    if unexpected or mismatched:
        problems = []
        if unexpected:
            problems.append(f"unexpected remote files: {', '.join(unexpected)}")
        if mismatched:
            problems.append(f"hash mismatch: {', '.join(mismatched)}")
        raise ValueError("existing PyPI release differs from this build; " + "; ".join(problems))
    return bool(set(local) - set(remote))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--dist-dir", type=Path, required=True)
    parser.add_argument("--pyproject", type=Path, required=True)
    parser.add_argument("--runtime-init", type=Path, required=True)
    args = parser.parse_args()

    expected_tag = f"v{args.version}"
    if args.tag != expected_tag:
        parser.error(f"tag {args.tag!r} does not match release version {args.version!r}")
    project_version = tomllib.loads(
        args.pyproject.read_text(encoding="utf-8")
    )["project"]["version"]
    if project_version != args.version:
        parser.error(
            f"pyproject version {project_version!r} does not match release version "
            f"{args.version!r}"
        )
    try:
        declared_runtime_version = runtime_version(args.runtime_init)
    except (OSError, SyntaxError, ValueError) as exc:
        parser.error(str(exc))
    if declared_runtime_version != args.version:
        parser.error(
            f"runtime version {declared_runtime_version!r} does not match release "
            f"version {args.version!r}"
        )

    try:
        local = local_artifacts(args.dist_dir)
        needed = evaluate_release(local, fetch_release(args.project, args.version))
    except (OSError, ValueError, urllib.error.URLError) as exc:
        print(f"release preflight failed: {exc}", file=sys.stderr)
        return 1
    print(f"publish-needed={'true' if needed else 'false'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
