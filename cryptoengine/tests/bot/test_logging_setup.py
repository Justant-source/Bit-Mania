import asyncio
import json

import pytest
import structlog

from bot import logging_setup
from bot.logging_setup import attach_notifier, setup_logging
from tests.fakes.notifier import FakeNotifier


@pytest.fixture(autouse=True)
def _reset():
    yield
    attach_notifier(None)


async def _drain():
    for _ in range(5):
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_json_stdout_and_error_goes_to_notifier(capsys):
    setup_logging("INFO")
    fake = FakeNotifier()
    attach_notifier(fake)
    log = structlog.get_logger("t")
    log.info("hello", a=1)
    log.error("order_failed", message="boom detail")
    await _drain()
    lines = [json.loads(l) for l in capsys.readouterr().out.splitlines() if l.strip()]
    assert lines[0]["event"] == "hello" and lines[0]["a"] == 1
    assert fake.of("anomaly") == [{"kind": "order_failed", "message": "boom detail"}]


@pytest.mark.asyncio
async def test_info_and_warning_not_alerted():
    setup_logging("INFO")
    fake = FakeNotifier()
    attach_notifier(fake)
    log = structlog.get_logger("t")
    log.info("a")
    log.warning("b")
    await _drain()
    assert fake.calls == []


@pytest.mark.asyncio
async def test_dedup_per_event_name(monkeypatch):
    setup_logging("INFO")
    fake = FakeNotifier()
    attach_notifier(fake)
    t = [100.0]
    monkeypatch.setattr(logging_setup.time, "monotonic", lambda: t[0])
    log = structlog.get_logger("t")
    log.error("same_event")
    log.error("same_event")
    log.critical("other_event")
    await _drain()
    assert [c["kind"] for c in fake.of("anomaly")] == ["same_event", "other_event"]
    t[0] += 121
    log.error("same_event")
    await _drain()
    assert len(fake.of("anomaly")) == 3


@pytest.mark.asyncio
async def test_no_recursion_when_notifier_logs_error():
    setup_logging("INFO")
    log = structlog.get_logger("notifier")

    class Noisy(FakeNotifier):
        async def anomaly(self, *, kind, message):
            await super().anomaly(kind=kind, message=message)
            log.error("send_failed_inside")  # a different event name, would loop without the guard

    fake = Noisy()
    attach_notifier(fake)
    log.error("first")
    await _drain()
    assert [c["kind"] for c in fake.of("anomaly")] == ["first"]


def test_no_running_loop_does_not_raise():
    setup_logging("INFO")
    attach_notifier(FakeNotifier())
    structlog.get_logger("t").error("sync_error")
