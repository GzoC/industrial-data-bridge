import asyncio
import hashlib
import json
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from pydantic import ValidationError
from pymodbus.datastore import (
    ModbusDeviceContext,
    ModbusSequentialDataBlock,
    ModbusServerContext,
)
from pymodbus.server import ModbusTcpServer

from bridge.config import Settings
from bridge.configured import ConfiguredAdapter, decode
from bridge.events import BY_ID, Event, Sample, now
from bridge.profile import Profile, Variable

EXAMPLE = Path(__file__).resolve().parents[1] / "config/acquisition.example.toml"


@pytest.mark.parametrize(
    ("failure", "reason", "state"),
    [
        ("both", "source_degraded:opcua", "degraded"),
        ("one", "source_degraded:modbus", "degraded"),
        ("consumer", "consumer_unavailable", "degraded"),
        ("gateway", "gateway_heartbeat_expired", "unavailable"),
        ("capacity", "coverage_gap", "degraded"),
        ("postgres", "postgres_unavailable", "unavailable"),
        ("none", None, "healthy"),
    ],
)
def test_operational_expiry_and_recovery(monkeypatch, failure, reason, state):
    from bridge import api

    stamp = now()
    old = stamp - timedelta(minutes=2)
    queue = {
        "gateway": {
            "heartbeat": (old if failure == "gateway" else stamp).isoformat(),
            "mqtt": "online",
            "opcua": "offline" if failure == "both" else "online",
            "modbus": "offline" if failure in ("both", "one") else "online",
            "read:opcua": stamp.isoformat(),
            "read:modbus": stamp.isoformat(),
        },
        "pending": 6 if failure == "capacity" else 1,
        "capacity": 6,
        "coverage": [],
        "oldest_pending": stamp.isoformat(),
    }
    consumer = {
        "heartbeat": old if failure == "consumer" else stamp,
        "subscribed": True,
    }

    class Connection:
        def execute(self, *args):
            return self

        def fetchone(self):
            return consumer

    @contextmanager
    def connection(dsn):
        if failure == "postgres":
            raise psycopg.OperationalError()
        yield Connection()

    monkeypatch.setattr(api, "connect", connection)
    monkeypatch.setattr(api, "backlog", lambda: queue)
    result = api.operational(Settings(database_dsn="fixture"))
    assert result["state"] == state
    assert reason in result["reasons"] if reason else not result["reasons"]
    if failure == "both":
        assert result["reasons"].count("source_degraded:modbus") == 1
    if failure == "gateway":
        assert result["mqtt"] == "unknown"


def test_profile_allowlist_and_secret(monkeypatch):
    profile = Profile.read(str(EXAMPLE))
    raw = profile.model_dump()
    raw["allowed"] = [{"host": "127.0.0.1", "port": 1502}]
    with pytest.raises(ValidationError, match="allowlist"):
        Profile.model_validate(raw)
    monkeypatch.delenv("OT_DATABASE_DSN", raising=False)
    with pytest.raises(ValueError, match="OT_DATABASE_DSN"):
        Settings(profile=profile).dsn()


def test_byte_word_order_and_scale():
    var = Variable(
        id="x",
        asset="a",
        unit="cm",
        kind="int32",
        byte_order="little",
        word_order="little",
        scale=0.5,
        offset=1,
    )
    assert decode([0xFCFF, 0xFFFF], var) == -1


def test_v1_digest_preserved():
    event = Sample(BY_ID["P101.speed"], 123, [123], now()).event(uuid4(), 1)
    legacy = json.dumps(
        event.model_dump(mode="json", exclude={"config_hash", "source_id", "mapping"}),
        sort_keys=True,
        separators=(",", ":"),
    )
    assert (
        Event.model_validate_json(legacy).digest()
        == hashlib.sha256(legacy.encode()).hexdigest()
    )


@pytest.mark.integration
async def test_alternative_modbus_only_map(tmp_path):
    profile = Profile.read(str(EXAMPLE))
    source = profile.sources[0]
    device = ModbusDeviceContext(hr=ModbusSequentialDataBlock(1, [125]))
    server = ModbusTcpServer(
        ModbusServerContext(devices={1: device}, single=False),
        address=("127.0.0.1", 2502),
    )
    await server.serve_forever(background=True)
    adapter = ConfiguredAdapter(source)
    try:
        (sample,) = await asyncio.wait_for(adapter.read(), 5)
        (var,) = source.variables
        event = sample.event(
            uuid4(),
            1,
            schema_version=2,
            site_id=profile.site_id,
            gateway_id=profile.gateway_id,
            simulated=False,
            config_hash=profile.digest,
            source_id=source.id,
            mapping=var,
            tag_id=var.id,
        )
        assert event.value == 12.5
        assert event.unit == "cm"
        from bridge.outbox import Outbox

        queue = Outbox(tmp_path / "pending.sqlite", 6)
        queue.initialize()
        scope = Settings(profile=profile).scope
        queue.bind_scope(scope)
        assert queue.put([event])
        changed = profile.model_copy(update={"assets": {"ESTANQUE_B": "Changed label"}})
        assert changed.digest != event.config_hash
        queue.bind_scope(Settings(profile=changed).scope)
        _, body = queue.due()[0]
        assert Event.model_validate_json(body).digest() == event.digest()
        with pytest.raises(ValueError, match="another site"):
            queue.bind_scope("other/gateway")
    finally:
        await adapter.close()
        await server.shutdown()
