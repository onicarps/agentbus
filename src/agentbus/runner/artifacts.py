"""Per-runner artifact paths for headless turns.

Multiple runners may legitimately receive the same event ID, for example when a
broadcast wake is delivered to more than one role.  Their prompts and result
records therefore must never share an event-only directory.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Mapping


_SAFE_RUNNER_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


def runner_artifact_dir(
    workspace: Path,
    options: Mapping[str, Any],
    event_id: int,
    *,
    fallback_runner_id: str,
) -> Path:
    """Return ``runs_dir/<runner_id>/<event_id>`` with a safe namespace.

    Direct adapter users do not have a runner config, so they receive the
    adapter-specific fallback.  The runner loop always injects ``_runner_id``.
    """
    runner_id = str(options.get("_runner_id") or fallback_runner_id).strip()
    if not _SAFE_RUNNER_ID.fullmatch(runner_id):
        raise ValueError("runner artifact namespace must be a simple runner_id")

    runs_dir = Path(str(options.get("runs_dir") or ".agentbus/runs"))
    if not runs_dir.is_absolute():
        runs_dir = workspace / runs_dir
    return runs_dir / runner_id / str(event_id)
