import asyncio

import pytest

from agentbus.jupyter.client import AsyncAgentBus


@pytest.mark.asyncio
async def test_async_bus_initialization():
    bus = AsyncAgentBus()
    assert bus is not None
    assert hasattr(bus, "poll_async")


@pytest.mark.asyncio
async def test_background_polling_yields():
    bus = AsyncAgentBus()
    await bus.start_background(interval=0.05)

    # Prove the event loop is not blocked
    await asyncio.sleep(0.15)
    assert bus._running is True

    await bus.stop()
    assert bus._running is False


@pytest.mark.asyncio
async def test_poll_async_invokes_callbacks_and_advances_cursor():
    calls: list[dict] = []

    async def fake_poll():
        if not hasattr(fake_poll, "done"):
            fake_poll.done = True  # type: ignore[attr-defined]
            return [
                {
                    "event_id": 7,
                    "topic": "okf/handoff",
                    "payload": {"from": "agy", "to": "grok", "summary": "hi"},
                }
            ]
        return []

    bus = AsyncAgentBus(poll_fn=fake_poll, since_id=0)
    bus.on_event(lambda ev: calls.append(ev))

    events = await bus.poll_async()
    assert len(events) == 1
    assert bus.since_id == 7
    assert calls[0]["event_id"] == 7

    # Second poll empty — cursor unchanged
    events2 = await bus.poll_async()
    assert events2 == []
    assert bus.since_id == 7
