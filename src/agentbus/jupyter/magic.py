"""IPython line magics: ``%agentbus start|stop|status``.

IPython is an **optional** dependency (``okf-agentbus[jupyter]``). This module
must import without IPython installed; Magics are registered only when the
extension is loaded inside IPython.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from agentbus.jupyter.client import AsyncAgentBus

logger = logging.getLogger(__name__)


def load_ipython_extension(ipython: Any) -> None:
    """Register ``%agentbus`` with the given IPython shell."""
    try:
        from IPython.core.magic import Magics, line_magic, magics_class
    except ImportError as exc:  # pragma: no cover - optional extra
        raise ImportError(
            "IPython is required for %agentbus magics. "
            "Install with: pip install 'okf-agentbus[jupyter]'"
        ) from exc

    @magics_class
    class AgentBusMagics(Magics):
        def __init__(self, shell: Any) -> None:
            super().__init__(shell)
            self.bus = AsyncAgentBus()
            self._pending_tasks: list[asyncio.Task[Any]] = []

        @line_magic  # type: ignore[misc]
        def agentbus(self, line: str) -> None:
            args = line.strip().split()
            if not args:
                print("Usage: %agentbus [start|stop|status]")
                return

            cmd = args[0].lower()
            if cmd == "start":
                interval = 1.0
                if len(args) > 1:
                    try:
                        interval = float(args[1])
                    except ValueError:
                        print(f"Invalid interval: {args[1]}")
                        return
                if not self._schedule(self.bus.start_background(interval=interval)):
                    print(
                        "No running asyncio loop — open a notebook / qtconsole "
                        "or await AsyncAgentBus.start_background() yourself."
                    )
                    return
                print(
                    f"AgentBus background polling started (interval={interval}s)."
                )
            elif cmd == "stop":
                if not self._schedule(self.bus.stop()):
                    # stop is short-lived; OK to run synchronously off-loop
                    asyncio.run(self.bus.stop())
                print("AgentBus stopped.")
            elif cmd == "status":
                print(
                    f"running={self.bus.is_running} "
                    f"since_id={self.bus.since_id} "
                    f"workspace={self.bus.workspace} "
                    f"topic={self.bus.topic}"
                )
            else:
                print(f"Unknown command: {cmd}")

        def _schedule(self, coro: Any) -> bool:
            """Schedule *coro* on the running loop. Returns False if no loop."""
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                # Avoid "coroutine was never awaited" if caller discards.
                if hasattr(coro, "close"):
                    coro.close()
                return False
            task = loop.create_task(coro)
            self._pending_tasks = [t for t in self._pending_tasks if not t.done()]
            self._pending_tasks.append(task)
            return True

    ipython.register_magics(AgentBusMagics)
