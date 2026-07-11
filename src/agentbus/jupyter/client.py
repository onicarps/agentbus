"""Async AgentBus client for Jupyter's event loop (non-blocking poll)."""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Optional injectables for tests / custom transports.
PollFn = Callable[[], Awaitable[list[dict[str, Any]]] | list[dict[str, Any]]]
EventCallback = Callable[[dict[str, Any]], Any]


class AsyncAgentBus:
    """
    Background poller that yields to Jupyter via ``asyncio.sleep``.

    Prefer this over synchronous CLI-style polling loops inside notebook cells.
    """

    def __init__(
        self,
        workspace: str | Path | None = None,
        *,
        topic: str = "okf/handoff",
        since_id: int = 0,
        poll_fn: PollFn | None = None,
        limit: int = 50,
    ) -> None:
        self.workspace = Path(
            workspace
            or os.environ.get("AGENTBUS_WORKSPACE")
            or Path.cwd()
        ).resolve()
        self.topic = topic
        self.since_id = since_id
        self.limit = limit
        self._poll_fn = poll_fn
        self._running = False
        self._task: asyncio.Task[None] | None = None
        self.callbacks: list[EventCallback] = []

    def on_event(self, callback: EventCallback) -> None:
        """Register a callback invoked for each polled event."""
        self.callbacks.append(callback)

    async def poll_async(self) -> list[dict[str, Any]]:
        """Fetch events after ``since_id`` without blocking the event loop forever."""
        if self._poll_fn is not None:
            result = self._poll_fn()
            if asyncio.iscoroutine(result) or isinstance(result, Awaitable):
                events = await result  # type: ignore[misc]
            else:
                events = await asyncio.to_thread(lambda: list(result))  # type: ignore[arg-type]
        else:
            events = await asyncio.to_thread(self._poll_store)

        if not events:
            return []

        for ev in events:
            eid = int(ev.get("event_id") or 0)
            if eid > self.since_id:
                self.since_id = eid
            for cb in self.callbacks:
                try:
                    out = cb(ev)
                    if asyncio.iscoroutine(out):
                        await out
                except Exception:
                    logger.exception("AsyncAgentBus callback failed")
        return events

    def _poll_store(self) -> list[dict[str, Any]]:
        from agentbus.store import EventStore

        store = EventStore(self.workspace)
        try:
            page = store.poll(
                topic=self.topic,
                since_id=self.since_id,
                limit=self.limit,
            )
            return list(page.get("events") or [])
        finally:
            store.close()

    async def _loop(self, interval: float) -> None:
        while self._running:
            try:
                await self.poll_async()
            except Exception as exc:
                logger.error("AgentBus poll error: %s", exc)
            await asyncio.sleep(interval)  # yield to Jupyter

    async def start_background(self, interval: float = 1.0) -> None:
        """Spawn a background poll task on the running event loop."""
        if self._running:
            return
        self._running = True
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(self._loop(interval))

    async def stop(self) -> None:
        """Stop background polling and cancel the task."""
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
