import asyncio
import hashlib
import os
import secrets
import socket
import subprocess
import sys
import time
from contextlib import suppress
from datetime import timedelta
from pathlib import Path
from threading import Event as ThreadEvent
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import NAMESPACE_URL, UUID, uuid5

import paho.mqtt.client as mqtt
import psycopg
import pytest
from pydantic import ValidationError

from bridge import gateway
from bridge.config import ACK_TOPIC, EVENT_TOPIC, Settings
from bridge.events import BY_ID, Sample, now
from bridge.outbox import Outbox
from bridge.simulators import serve_modbus, serve_opc
from bridge.store import connect, initialize, store
from bridge.transport import client_for, start, stop

ROOT = Path(__file__).resolve().parents[1]
TAG = BY_ID["P101.flow"]


@pytest.mark.integration
async def test_configured_pending_map_replays_to_postgres(lab, monkeypatch):
    from bridge.api import assets, history_rows
    from bridge.events import Event
    from bridge.profile import Profile

    monkeypatch.setenv("OT_MQTT_GATEWAY_PASSWORD", lab.settings.password("gateway"))
    monkeypatch.setenv("OT_MQTT_CONSUMER_PASSWORD", lab.settings.password("consumer"))
    monkeypatch.setenv("OT_DATABASE_DSN", lab.settings.dsn())
    raw = Profile.read(str(ROOT / "config/acquisition.example.toml")).model_dump(
        mode="json"
    )
    raw["broker"]["port"] = lab.mqtt_port
    raw["outbox"] = str(lab.outbox.path)
    before = Profile.model_validate(raw)
    settings = Settings(profile=before)
    topic_base = settings.topic("events").rsplit("/", 1)[0]
    lab.terminate("broker")
    path = lab.root / "acl"
    path.write_text(path.read_text().replace("aurora/v1/aurora-gw", topic_base))
    lab.start_broker()
    lab.outbox.bind_scope(settings.scope)
    var = before.sources[0].variables[0]
    event = Event(
        **(
            reading().model_dump()
            | {
                "schema_version": 2,
                "simulated": False,
                "site_id": before.site_id,
                "gateway_id": before.gateway_id,
                "tag_id": var.id,
                "asset_id": var.asset,
                "unit": var.unit,
                "mapping": var,
                "config_hash": before.digest,
                "source_id": before.sources[0].id,
            }
        )
    )
    assert lab.outbox.put([event])
    example = (
        (ROOT / "config/acquisition.example.toml")
        .read_text()
        .replace('unit = "cm"', 'unit = "mm"')
        .replace("port = 1883", f"port = {lab.mqtt_port}")
        .replace(
            'outbox = ".runtime/acquisition.sqlite"',
            f"outbox = '{lab.outbox.path.as_posix()}'",
        )
    )
    path = lab.root / "changed.toml"
    path.write_text(example)
    monkeypatch.setenv("BRIDGE_CONFIG", str(path))
    changed = Settings.load()
    stopping = asyncio.Event()
    from bridge import consumer

    async with asyncio.TaskGroup() as group:
        group.create_task(consumer.run(changed, stopping))
        group.create_task(gateway.Publisher(changed, lab.outbox).run(stopping))
        try:
            await until(lambda: lab.outbox.status()["pending"] == 0)
            stored = assets()["readings"][0]
            assert stored["unit"] == "cm"
            assert stored["config_hash"] == before.digest
            assert stored["simulated"] is False
            assert history_rows(
                var.id, now() - timedelta(minutes=1), now(), None, 10, 0
            )
        finally:
            stopping.set()


def reading(sequence=1, stamp=None):
    return (
        Sample(TAG, 12.0, [16704, 0], stamp or now())
        .event(
            uuid5(NAMESPACE_URL, "aurora-test-session"),
            sequence,
        )
        .model_copy(
            update={"event_id": uuid5(NAMESPACE_URL, f"aurora-test-{sequence}")}
        )
    )


async def until(predicate, seconds=20):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = await asyncio.to_thread(predicate)
        if value:
            return value
        await asyncio.sleep(0.1)
    raise AssertionError("Timed out waiting for a laboratory condition")


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def reachable(port_number):
    try:
        with socket.create_connection(("127.0.0.1", port_number), timeout=0.25):
            return True
    except OSError:
        return False


def executable(directory: Path, name: str) -> str:
    return str(directory / (name + ".exe" if sys.platform == "win32" else name))


class Lab:
    def __init__(self, root, broker, pg_bin, capacity):
        self.root = root
        self.broker = broker
        self.pg_bin = pg_bin
        self.postgres_port = port()
        self.mqtt_port = port()
        self.pgdata = root / "pgdata"
        self.processes = {}
        password, publisher_key, subscriber_key = (
            secrets.token_hex(24) for _ in range(3)
        )
        self.settings = Settings(
            interval=0.2,
            timeout=0.5,
            capacity=capacity,
            broker_port=self.mqtt_port,
            database_dsn=(
                f"postgresql://bridge:{password}@127.0.0.1:{self.postgres_port}/postgres"
            ),
            outbox_path=root / "outbox.sqlite",
            controls=root,
            mqtt_gateway_password=publisher_key,
            mqtt_consumer_password=subscriber_key,
            lab_controls=True,
        )
        self.outbox = Outbox(self.settings.outbox_path, capacity)
        self.outbox.initialize()
        pg_secret = root / "pg-password"
        pg_secret.write_text(password + "\n", encoding="ascii")
        result = subprocess.run(
            [
                executable(pg_bin, "initdb"),
                "-D",
                str(self.pgdata),
                "-U",
                "bridge",
                "--auth=scram-sha-256",
                "--no-locale",
                "-E",
                "UTF8",
                f"--pwfile={pg_secret}",
            ],
            cwd=root,
            capture_output=True,
            timeout=35,
        )
        pg_secret.unlink()
        assert result.returncode == 0, result.stderr.decode(errors="replace")
        # Use the same generator as Compose, and verify the broker can authenticate both roles.
        import runpy

        hashed = runpy.run_path(str(ROOT / "scripts" / "init_demo.py"))[
            "mosquitto_hash"
        ]
        (root / "passwords").write_text(
            f"gateway:{hashed(publisher_key)}\nconsumer:{hashed(subscriber_key)}\n",
            encoding="ascii",
        )
        (root / "acl").write_bytes((ROOT / "broker" / "acl").read_bytes())
        (root / "mosquitto.conf").write_text(
            f"listener {self.mqtt_port} 127.0.0.1\n"
            "allow_anonymous false\npassword_file passwords\nacl_file acl\npersistence false\n",
            encoding="ascii",
        )

    def launch(self, name, command, env=None):
        with (self.root / f"{name}.log").open("ab") as log:
            self.processes[name] = subprocess.Popen(
                command,
                stdout=log,
                stderr=subprocess.STDOUT,
                cwd=self.root,
                env=env,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
            )

    def start_broker(self):
        self.launch("broker", [self.broker, "-c", "mosquitto.conf"])

    def pg(self, operation):
        options = (
            [
                "-l",
                str(self.root / "postgres.log"),
                "-o",
                f"-h 127.0.0.1 -p {self.postgres_port} -c track_commit_timestamp=on"
                + (" -c unix_socket_directories=" if os.name != "nt" else ""),
            ]
            if operation == "start"
            else ["-m", "fast"]
        )
        diagnostic = self.root / "pg_ctl.log"
        with diagnostic.open("ab") as log:
            result = subprocess.run(
                [
                    executable(self.pg_bin, "pg_ctl"),
                    "-D",
                    str(self.pgdata),
                    "-w",
                    "-t",
                    "12",
                    *options,
                    operation,
                ],
                cwd=self.root,
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=18,
            )
        if result.returncode != 0:
            server_log = self.root / "postgres.log"
            pytest.fail(
                diagnostic.read_text(errors="replace")
                + (
                    server_log.read_text(errors="replace")[-4000:]
                    if server_log.exists()
                    else ""
                )
            )

    def start_consumer(self):
        env = os.environ | {
            "BRIDGE_DATABASE_DSN": self.settings.dsn(),
            "BRIDGE_MQTT_CONSUMER_PASSWORD": self.settings.password("consumer"),
            "BRIDGE_BROKER_PORT": str(self.mqtt_port),
            "BRIDGE_LAB_CONTROLS": "true",
            "BRIDGE_CONTROLS": str(self.root),
        }
        self.launch("consumer", [sys.executable, "-m", "bridge", "consumer"], env)

    def start_gateway(self):
        env = os.environ | {
            "BRIDGE_OUTBOX_PATH": str(self.settings.outbox_path),
            "BRIDGE_CAPACITY": str(self.settings.capacity),
            "BRIDGE_MQTT_GATEWAY_PASSWORD": self.settings.password("gateway"),
            "BRIDGE_BROKER_PORT": str(self.mqtt_port),
            "BRIDGE_INTERVAL": str(self.settings.interval),
            "BRIDGE_TIMEOUT": str(self.settings.timeout),
        }
        self.launch("gateway", [sys.executable, "-m", "bridge", "gateway"], env)

    def terminate(self, name):
        process = self.processes.pop(name, None)
        if process and process.poll() is None:
            process.terminate()
            process.wait(timeout=8)

    def ids(self, ids):
        with connect(self.settings.dsn()) as db:
            return {
                str(row["event_id"])
                for row in db.execute(
                    "SELECT event_id FROM events WHERE event_id=ANY(%s)",
                    (ids,),
                )
            }

    def counts(self):
        with connect(self.settings.dsn()) as db:
            return {
                row["kind"]: row["total"]
                for row in db.execute(
                    "SELECT kind,total FROM delivery_counters",
                )
            }


@pytest.fixture
def lab(tmp_path):
    broker = os.environ.get("TEST_MOSQUITTO")
    pg_bin = os.environ.get("TEST_PG_BIN")
    if not broker or not pg_bin:
        if os.environ.get("REQUIRE_INTEGRATION") == "1":
            pytest.fail("Missing TEST_MOSQUITTO or TEST_PG_BIN")
        pytest.skip("Set TEST_MOSQUITTO and TEST_PG_BIN to real local executables")
    instance = Lab(tmp_path, str(Path(broker).resolve()), Path(pg_bin).resolve(), 3000)
    instance.pg("start")
    instance.start_broker()
    try:
        yield instance
    finally:
        instance.terminate("gateway")
        instance.terminate("consumer")
        instance.terminate("broker")
        if reachable(instance.postgres_port):
            instance.pg("stop")


def test_outbox_capacity_persists_and_records_coverage(tmp_path):
    outbox = Outbox(tmp_path / "outbox.sqlite", capacity=6)
    outbox.initialize()
    events = [reading(n) for n in range(1, 7)]
    assert outbox.put(events)
    assert not outbox.has_capacity(6)
    assert not outbox.put([reading(7)])
    outbox.coverage(6)
    outbox.coverage(6)
    reopened = Outbox(outbox.path, 6)
    reopened.initialize()
    assert reopened.status()["coverage"][0]["skipped_slots"] == 12
    assert set(reopened.status()["pending_ids"]) == {
        str(event.event_id) for event in events
    }
    event = events[0]
    reopened.acknowledge(
        str(event.event_id), hashlib.sha256(b"wrong").hexdigest(), "stored"
    )
    assert reopened.status()["pending"] == 6
    reopened.acknowledge(str(event.event_id), event.digest(), "stored")
    assert reopened.status()["pending"] == 5
    reopened.coverage(0)
    assert reopened.status()["coverage"][0]["ended"]


def test_endpoints_limited():
    for key, value in (("opc_host", "10.0.0.4"), ("modbus_host", "plc.test")):
        with pytest.raises(ValidationError):
            Settings(**{key: value})


@pytest.mark.integration
async def test_durable_mqtt_and_postgres(lab):
    await until(lambda: reachable(lab.mqtt_port))
    lab.start_consumer()
    stopping = asyncio.Event()
    publisher = gateway.Publisher(lab.settings, lab.outbox)
    task = asyncio.create_task(publisher.run(stopping))
    try:
        await until(publisher.ready.is_set)
        item = reading()
        assert lab.outbox.put([item])
        await until(lambda: lab.outbox.status()["pending"] == 0)
        assert lab.ids([item.event_id]) == {str(item.event_id)}
        for _ in range(10):
            publisher.client.publish(EVENT_TOPIC, item.wire(), qos=1)
        await until(lambda: lab.counts().get("duplicate", 0) >= 10)
        conflict = item.model_copy(update={"value": 13.0, "raw_value": [16720, 0]})
        publisher.client.publish(EVENT_TOPIC, conflict.wire(), qos=1)
        await until(lambda: lab.counts().get("conflict") == 1)
        publisher.client.publish(EVENT_TOPIC, '{"schema_version":9}', qos=1)
        await until(lambda: lab.counts().get("rejected") == 1)
        assert lab.ids([item.event_id]) == {str(item.event_id)}

        lab.terminate("broker")
        await until(lambda: not publisher.ready.is_set())
        batch_time = now()
        batch = [reading(n, batch_time) for n in range(1000, 3000)]
        assert lab.outbox.put(batch)
        assert lab.outbox.status()["pending"] == 2000
        stopping.set()
        await task
        lab.start_broker()
        stopping = asyncio.Event()
        reopened = Outbox(lab.outbox.path, 3000)
        publisher = gateway.Publisher(lab.settings, reopened)
        recovered_at = time.monotonic()
        task = asyncio.create_task(publisher.run(stopping))
        await until(lambda: reopened.status()["pending"] == 0, seconds=120)
        expected = [event.event_id for event in batch]
        assert lab.ids(expected) == {str(identifier) for identifier in expected}
        print(
            f"Recovery: 2000/2000 event IDs in {time.monotonic() - recovered_at:.2f} s"
        )

        lab.terminate("consumer")
        pending = reading(2)
        assert lab.outbox.put([pending])
        await asyncio.sleep(0.7)
        assert lab.outbox.status()["pending"] == 1
        assert not lab.ids([pending.event_id])
        (lab.root / "consumer-crash-after-commit").touch()
        lab.start_consumer()
        await until(lambda: lab.processes["consumer"].poll() == 42)
        assert lab.ids([pending.event_id]) == {str(pending.event_id)}
        assert lab.outbox.status()["pending"] == 1
        lab.start_consumer()
        await until(lambda: lab.outbox.status()["pending"] == 0)
        assert lab.ids([pending.event_id]) == {str(pending.event_id)}

        await asyncio.to_thread(lab.pg, "stop")
        db_pending = reading(3)
        assert lab.outbox.put([db_pending])
        await asyncio.sleep(0.8)
        assert lab.outbox.status()["pending"] == 1
        await asyncio.to_thread(lab.pg, "start")
        await until(lambda: lab.outbox.status()["pending"] == 0)
        assert lab.ids([db_pending.event_id]) == {str(db_pending.event_id)}

        stopping.set()
        await task
        lab.terminate("broker")
        lab.start_gateway()
        await until(lambda: lab.outbox.status()["pending"] >= 6)
        lab.terminate("gateway")
        persisted = lab.outbox.status()["pending_ids"]
        assert persisted
        lab.start_broker()
        lab.start_gateway()
        await until(
            lambda: len(lab.ids([UUID(value) for value in persisted])) == len(persisted)
        )
    finally:
        stopping.set()
        await task


@pytest.mark.integration
async def test_lot_boundary_and_broker_acl(lab, monkeypatch):
    from bridge.api import assets

    initialize(lab.settings.dsn())
    monkeypatch.setenv("BRIDGE_DATABASE_DSN", lab.settings.dsn())
    stamp = now()
    with connect(lab.settings.dsn()) as db:
        for lot, begin, end in (
            ("A", stamp - timedelta(minutes=1), stamp),
            ("B", stamp, stamp + timedelta(minutes=1)),
        ):
            db.execute(
                """
                INSERT INTO production_orders(order_id,lot_id,asset_id,product_id,valid_from,valid_to)
                VALUES ('order',%s,'P101','demo',%s,%s)
            """,
                (lot, begin, end),
            )
    with (
        pytest.raises(psycopg.errors.ExclusionViolation),
        connect(lab.settings.dsn()) as db,
    ):
        db.execute(
            """
            INSERT INTO production_orders(order_id,lot_id,asset_id,product_id,valid_from,valid_to)
            VALUES ('order','overlap','P101','demo',%s,%s)
        """,
            (stamp - timedelta(seconds=1), stamp + timedelta(seconds=1)),
        )
    late, current = reading(90, stamp - timedelta(microseconds=1)), reading(91, stamp)
    assert store(lab.settings.dsn(), current.wire().encode())
    assert store(lab.settings.dsn(), late.wire().encode())
    with connect(lab.settings.dsn()) as db:
        rows = db.execute(
            "SELECT order_context->>'lot_id' AS lot FROM events ORDER BY event_time"
        )
        assert [row["lot"] for row in rows] == ["A", "B"]
    view = next(row for row in assets()["readings"] if row["tag_id"] == TAG.id)
    assert view["event_id"] == str(current.event_id)
    await until(lambda: reachable(lab.mqtt_port))
    refused = ThreadEvent()
    anonymous = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    anonymous.on_connect = lambda client, userdata, flags, reason, props: (
        refused.set() if reason.is_failure else None
    )
    start(anonymous, lab.settings)
    try:
        assert await asyncio.to_thread(refused.wait, 5)
    finally:
        stop(anonymous)
    subscribed = ThreadEvent()
    observed = ThreadEvent()
    receiver = client_for(lab.settings, "gateway")
    receiver.on_connect = lambda client, *args: client.subscribe(ACK_TOPIC, qos=1)
    receiver.on_subscribe = lambda *args: subscribed.set()
    receiver.on_message = lambda *args: observed.set()
    sender = client_for(lab.settings, "consumer")
    connected = ThreadEvent()
    sender.on_connect = lambda client, userdata, flags, reason, props: connected.set()
    start(receiver, lab.settings)
    start(sender, lab.settings)
    try:
        assert await asyncio.to_thread(subscribed.wait, 5)
        assert await asyncio.to_thread(connected.wait, 5)
        publish = receiver.publish(ACK_TOPIC, "blocked", qos=1)
        await asyncio.to_thread(publish.wait_for_publish, 2)
        assert not await asyncio.to_thread(observed.wait, 0.5)
        sender.publish(ACK_TOPIC, "authorized", qos=1)
        assert await asyncio.to_thread(observed.wait, 5)
    finally:
        stop(receiver)
        stop(sender)


@pytest.mark.integration
async def test_clean_live_demo(lab):
    from json import load

    interval = 0.6
    settings = lab.settings.model_copy(update={"interval": interval})
    stopping = asyncio.Event()
    opc_ready, modbus_ready = asyncio.Event(), asyncio.Event()
    api_port = port()
    base = f"http://127.0.0.1:{api_port}"
    env = os.environ | {
        "BRIDGE_DATABASE_DSN": settings.dsn(),
        "BRIDGE_OUTBOX_PATH": str(settings.outbox_path),
        "BRIDGE_CAPACITY": str(settings.capacity),
        "BRIDGE_INTERVAL": str(interval),
        "BRIDGE_LAB_CONTROLS": "true",
        "BRIDGE_CONTROLS": str(settings.controls),
    }
    lab.start_consumer()
    lab.launch(
        "api",
        [
            sys.executable,
            "-m",
            "uvicorn",
            "bridge.api:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(api_port),
            "--no-access-log",
        ],
        env,
    )

    def get(path, method="GET", body=None):
        with suppress(OSError, HTTPError, URLError):
            req = Request(
                base + path,
                method=method,
                data=body,
                headers={"Content-Type": "application/json"} if body else {},
            )
            with urlopen(req, timeout=1) as response:
                return load(response)
        return None

    try:
        async with asyncio.TaskGroup() as group:
            group.create_task(serve_opc(settings, stopping, opc_ready))
            group.create_task(serve_modbus(settings, stopping, modbus_ready))
            try:
                await asyncio.wait_for(
                    asyncio.gather(opc_ready.wait(), modbus_ready.wait()), 20
                )
                group.create_task(gateway.run(settings, stopping))
                await until(lambda: get("/api/health"))
                assert await until(lambda: get("/api/lab/seed-orders", "POST"))
                await until(lambda: lab.counts().get("stored", 0) >= 6)
                data = await until(lambda: get("/api/assets"))
                assert len(data["readings"]) == 6
                assert all(row["quality"] == "GOOD" for row in data["readings"])
                sample_usage = None
                if os.environ.get("TEST_RESOURCE_USAGE"):
                    import psutil

                    postgres_pid = int(
                        (lab.pgdata / "postmaster.pid").read_text().splitlines()[0]
                    )
                    postgres = psutil.Process(postgres_pid)
                    processes = [
                        psutil.Process(),
                        postgres,
                        *postgres.children(),
                        *(psutil.Process(p.pid) for p in lab.processes.values()),
                    ]

                    def sample_usage():
                        return (
                            sum(sum(p.cpu_times()[:2]) for p in processes),
                            sum(p.memory_info().rss for p in processes) / (1024**2),
                        )

                    start_cpu, start_rss = sample_usage()
                started = time.monotonic()
                await asyncio.sleep(12)
                if sample_usage:
                    end_cpu, end_rss = sample_usage()
                    print(
                        f"Approximate resources: {100 * (end_cpu - start_cpu) / (time.monotonic() - started):.1f}% "
                        f"of one logical CPU, {max(start_rss, end_rss):.1f} MiB summed RSS"
                    )
                with connect(settings.dsn()) as db:
                    summary = db.execute("""
                        SELECT count(*) AS n,
                            (count(*)-1)/extract(epoch FROM max(observed_at)-min(observed_at)) AS hz,
                            percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(
                                epoch FROM pg_xact_commit_timestamp(xmin)-observed_at)) AS p50,
                            percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(
                                epoch FROM pg_xact_commit_timestamp(xmin)-observed_at)) AS p95
                        FROM events WHERE (body->>'quality')='GOOD'
                    """).fetchone()
                print(
                    f"Live sample: {summary['n']} events, {summary['hz']:.2f} events/s, "
                    f"commit p50 {summary['p50']:.3f} s, p95 {summary['p95']:.3f} s"
                )
                if screenshot := os.environ.get("TEST_SCREENSHOT"):
                    chrome = Path(os.environ["TEST_CHROME"])
                    await asyncio.to_thread(
                        subprocess.run,
                        [
                            str(chrome),
                            "--headless",
                            "--disable-gpu",
                            "--no-first-run",
                            f"--user-data-dir={lab.root / 'browser'}",
                            "--window-size=1280,1300",
                            "--virtual-time-budget=3500",
                            f"--screenshot={screenshot}",
                            base,
                        ],
                        check=True,
                        capture_output=True,
                        timeout=30,
                        creationflags=subprocess.CREATE_NO_WINDOW
                        if os.name == "nt"
                        else 0,
                    )
                assert get("/api/lab/consumer", "PUT", b'{"paused":true}') == {
                    "paused": True
                }
                backlog = await until(
                    lambda: (
                        pending
                        if (pending := get("/api/backlog")) and pending["pending"] >= 12
                        else None
                    )
                )
                expected = backlog["pending_ids"]
                assert get("/api/lab/consumer", "PUT", b'{"paused":false}') == {
                    "paused": False
                }
                await until(
                    lambda: all(get(f"/api/events/{event_id}") for event_id in expected)
                )
                query = urlencode(
                    {
                        "tag": "TK101.level",
                        "lot": "LOT-A",
                        "start": (now() - timedelta(minutes=2)).isoformat(),
                        "end": now().isoformat(),
                    }
                )
                history = get(f"/api/history?{query}")
                assert history["events"]
                with urlopen(f"{base}/api/history.csv?{query}", timeout=3) as response:
                    body = response.read().decode()
                    assert "event_time_utc" in body and "LOT-A" in body
                assert get(
                    "/api/lab/simulator/opcua", "PUT", b'{"mode":"offline"}'
                ) == {
                    "mode": "offline",
                }
                await until(
                    lambda: (
                        readings
                        if (readings := get("/api/assets"))
                        and any(
                            row["source_protocol"] == "opcua"
                            and row["quality"] == "BAD"
                            for row in readings["readings"]
                        )
                        else None
                    )
                )
                assert get("/api/lab/simulator/opcua", "PUT", b'{"mode":"normal"}') == {
                    "mode": "normal",
                }
                await until(
                    lambda: (
                        readings
                        if (readings := get("/api/assets"))
                        and all(
                            row["quality"] == "GOOD" for row in readings["readings"]
                        )
                        else None
                    )
                )
            finally:
                stopping.set()
        assert lab.outbox.status()["gateway"]["mqtt"] == "offline"
        assert lab.outbox.status()["gateway"]["opcua"] == "stopped"
        assert lab.outbox.status()["gateway"]["modbus"] == "stopped"
        assert lab.outbox.status()["pending"] <= settings.capacity
    finally:
        lab.terminate("api")
