#!/usr/bin/env python3
"""@bus.topic Pydantic registration — AgentBus v0.7."""

from __future__ import annotations

import tempfile
from pathlib import Path

from agentbus.schema_registry import list_schemas
from agentbus.schemas import set_validation_workspace


def main() -> None:
    pytest = __import__("pytest")
    pytest.importorskip("pydantic")
    from pydantic import BaseModel

    from agentbus.sdk import AgentBus

    with tempfile.TemporaryDirectory(prefix="ab-ex07-") as td:
        ws = Path(td)
        set_validation_workspace(ws)
        bus = AgentBus(ws)

        @bus.topic("ci/build-alert")
        class CIBuildAlert(BaseModel):
            build_id: str
            status: str
            failure_reason: str | None = None

        rows = list_schemas(ws)
        assert any(r["topic_name"] == "ci/build-alert" for r in rows)

        result = bus.publish(
            CIBuildAlert(build_id="ex07-1", status="FAILED", failure_reason="lint"),
            producer_id="demo",
            skip_rbac=True,
        )
        assert result["event_id"] >= 1
        print(f"OK: Pydantic topic registered and published event {result['event_id']}")


if __name__ == "__main__":
    main()