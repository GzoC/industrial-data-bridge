import asyncio
from datetime import timedelta
from uuid import uuid4

import pytest
from pydantic import ValidationError

from bridge.adapters import ModbusAdapter, OpcAdapter, word_float
from bridge.config import Settings
from bridge.events import BY_ID, Event, Sample, now
from bridge.gateway import acquire
from bridge.outbox import Outbox
from bridge.simulators import cell_values, float_words, serve_modbus, serve_opc


@pytest.mark.parametrize("order", ["big", "little"])
def test_modbus_words(order):
    words = float_words(11.125, order)
    assert word_float(list(words), order) == 11.125
    assert word_float(list(words[::-1]), order) != 11.125


def test_contract_rejects_nonfinite_unknown_fields_and_coercion():
    tag = BY_ID["TK101.level"]
    bad = Sample(tag, float("nan"), float("nan"), now()).event(uuid4(), 1)
    assert (bad.quality, bad.value, bad.raw_value) == ("BAD", None, "NaN")
    good = Sample(tag, 1.0, 1.0, now()).event(uuid4(), 1)
    for fields in (
        {"schema_version": 2},
        {"tag_id": "TK101.unknown"},
        {"value": True},
        {"value": float("inf")},
        {"unit": "bar"},
    ):
        with pytest.raises(ValidationError):
            Event.model_validate(good.model_dump() | fields)


def test_timestamp_fallback_and_quality():
    observed = now()
    tag = BY_ID["TK101.level"]
    missing = Sample(tag, 1.0, 1.0, observed).event(uuid4(), 1)
    assert missing.source_timestamp is None
    assert missing.timestamp_origin == "gateway"
    skew = Sample(tag, 1.0, 1.0, observed, observed + timedelta(minutes=1)).event(
        uuid4(), 2
    )
    assert (skew.quality, skew.timestamp_origin, skew.event_time) == (
        "UNCERTAIN",
        "gateway",
        observed,
    )


@pytest.mark.integration
async def test_gateway_pauses_and_records_lost_coverage(tmp_path):
    settings = Settings(
        interval=0.1,
        timeout=0.15,
        capacity=6,
        outbox_path=tmp_path / "gateway.sqlite",
    )
    outbox = Outbox(settings.outbox_path, 6)
    outbox.initialize()
    stop = asyncio.Event()
    task = asyncio.create_task(acquire(settings, outbox, stop))
    try:
        async with asyncio.timeout(8):
            while not outbox.status()["coverage"]:
                await asyncio.sleep(0.1)
        state = outbox.status()
        assert state["pending"] == 6
        assert state["coverage"][0]["skipped_slots"] >= 6
        assert state["coverage"][0]["ended"] is None
    finally:
        stop.set()
        await task


@pytest.mark.integration
@pytest.mark.parametrize("order", ["big", "little"])
async def test_real_protocols(tmp_path, order):
    settings = Settings(controls=tmp_path, interval=0.1, word_order=order)
    (tmp_path / "opcua").write_text("frozen")
    (tmp_path / "modbus").write_text("frozen")
    stop = asyncio.Event()
    opc_ready, modbus_ready = asyncio.Event(), asyncio.Event()
    opc, modbus = OpcAdapter(settings), ModbusAdapter(settings)
    async with asyncio.TaskGroup() as group:
        group.create_task(serve_opc(settings, stop, opc_ready))
        group.create_task(serve_modbus(settings, stop, modbus_ready))
        try:
            await asyncio.wait_for(
                asyncio.gather(opc_ready.wait(), modbus_ready.wait()), 20
            )
            opc_samples, modbus_samples = await asyncio.gather(
                opc.read(), modbus.read()
            )
            session = uuid4()
            events = [
                sample.event(session, i)
                for i, sample in enumerate(
                    opc_samples + modbus_samples,
                    start=1,
                )
            ]
            assert [event.unit for event in events] == [
                "m",
                "°C",
                "L/min",
                "bar",
                "rpm",
                "bool",
            ]
            assert all(event.quality == "GOOD" for event in events)
            assert [event.value for event in events[:5]] == pytest.approx(
                cell_values(0, settings.seed)[:5],
            )
            assert events[-1].value is True
            assert all(event.source_timestamp for event in events[:2])
            assert all(event.source_timestamp is None for event in events[2:])
            (tmp_path / "opcua").write_text("invalid")
            (tmp_path / "modbus").write_text("invalid")
            await asyncio.sleep(0.25)
            bad = await asyncio.gather(opc.read(), modbus.read())
            assert all(
                sample.event(session, 1).quality == "BAD"
                for sample in bad[0] + bad[1][:3]
            )
            (tmp_path / "modbus").write_text("offline")
            await asyncio.sleep(0.25)
            disconnected = await modbus.read()
            assert all(
                sample.failure == "communication_loss" for sample in disconnected
            )
        finally:
            await asyncio.gather(opc.close(), modbus.close())
            stop.set()
