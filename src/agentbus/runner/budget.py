"""Per-causation-chain turn budget for runners."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class ChainBudget:
    def __init__(self, path: Path, max_turns: int) -> None:
        self.path = path
        self.max_turns = max_turns
        self._counts: dict[str, int] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.is_file():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        chains = data.get("chains") if isinstance(data, dict) else None
        if isinstance(chains, dict):
            self._counts = {str(k): int(v) for k, v in chains.items()}

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload: dict[str, Any] = {"chains": self._counts}
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        tmp.replace(self.path)

    def chain_key(self, event_id: int, causation_id: int | None) -> str:
        return str(causation_id if causation_id is not None else event_id)

    def remaining(self, chain_key: str) -> int:
        used = self._counts.get(chain_key, 0)
        return max(0, self.max_turns - used)

    def would_exceed(self, chain_key: str) -> bool:
        return self.remaining(chain_key) <= 0

    def record(self, chain_key: str) -> int:
        self._counts[chain_key] = self._counts.get(chain_key, 0) + 1
        self.save()
        return self._counts[chain_key]
