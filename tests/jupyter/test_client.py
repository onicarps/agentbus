import asyncio
import logging

import pytest

from agentbus.jupyter.client import AsyncAgentBus


@pytest.mark.asyncio
async def test_async_bus_initialization():
    bus = AsyncAgentBus()
    assert bus is not None
    assert hasattr(bus, "poll_async")
    assert bus.is_running is False


@pytest.mark.asyncio
async def test_background_polling_yields():
    bus = AsyncAgentBus()
    await bus.start_background(interval=0.05)

    # Prove the event loop is not blocked
    await asyncio.sleep(0.15)
    assert bus.is_running is True

    await bus.stop()
    assert bus.is_running is False


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

    events2 = await bus.poll_async()
    assert events2 == []
    assert bus.since_id == 7


@pytest.mark.asyncio
async def test_callback_exception_does_not_crash_poll(caplog):
    calls: list[dict] = []

    def bad_callback(ev):
        raise RuntimeError("boom")

    def good_callback(ev):
        calls.append(ev)

    async def fake_poll():
        return [{"event_id": 1, "topic": "okf/handoff", "payload": {}}]

    bus = AsyncAgentBus(poll_fn=fake_poll, since_id=0)
    bus.on_event(bad_callback)
    bus.on_event(good_callback)

    with caplog.at_level(logging.ERROR):
        events = await bus.poll_async()

    assert len(events) == 1
    assert calls == events
    assert any("callback failed" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_async_callback_is_awaited():
    seen: list[int] = []

    async def async_cb(ev):
        await asyncio.sleep(0)
        seen.append(ev["event_id"])

    async def fake_poll():
        return [{"event_id": 3, "topic": "okf/handoff", "payload": {}}]

    bus = AsyncAgentBus(poll_fn=fake_poll, since_id=0)
    bus.on_event(async_cb)
    await bus.poll_async()
    assert seen == [3]
