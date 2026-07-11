"""IPython line magics: ``%agentbus start|stop|status``."""

from __future__ import annotations

import asyncio
import logging

from IPython.core.magic import Magics, line_magic, magics_class

from agentbus.jupyter.client import AsyncAgentBus

logger = logging.getLogger(__name__)


@magics_class
class AgentBusMagics(Magics):
    def __init__(self, shell):
        super().__init__(shell)
        self.bus = AsyncAgentBus()

    @line_magic
    def agentbus(self, line: str):
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
            self._schedule(self.bus.start_background(interval=interval))
            print(f"AgentBus background polling started (interval={interval}s).")
        elif cmd == "stop":
            self._schedule(self.bus.stop())
            print("AgentBus stopped.")
        elif cmd == "status":
            print(
                f"running={self.bus._running} "
                f"since_id={self.bus.since_id} "
                f"workspace={self.bus.workspace} "
                f"topic={self.bus.topic}"
            )
        else:
            print(f"Unknown command: {cmd}")

    def _schedule(self, coro) -> None:
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(coro)
        except RuntimeError:
            # No running loop (rare outside Jupyter) — run to completion.
            asyncio.run(coro)


def load_ipython_extension(ipython) -> None:
    ipython.register_magics(AgentBusMagics)
