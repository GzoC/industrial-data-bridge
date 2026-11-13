import asyncio
import logging
import sqlite3
from threading import Event as ThreadEvent
from uuid import uuid4

import paho.mqtt.client as mqtt
from pydantic import ValidationError

from bridge.adapters import ModbusAdapter, OpcAdapter
from bridge.config import Settings
from bridge.configured import ConfiguredAdapter
from bridge.events import TAGS, now
from bridge.outbox import Outbox
from bridge.simulators import wait_tick
from bridge.transport import Ack, client_for, start, status_payload, stop

log = logging.getLogger(__name__)


class Publisher:
    def __init__(self, settings: Settings, outbox: Outbox):
        self.settings = settings
        self.outbox = outbox
        self.ready = ThreadEvent()
        self.client = client_for(settings, "gateway")
        self.client.on_connect = self.connected
        self.client.on_disconnect = self.disconnected
        self.client.on_subscribe = self.subscribed
        self.client.on_message = self.received

    def connected(self, client, userdata, flags, reason, properties):
        if reason.is_failure:
            log.warning("Gateway broker connection refused: %s", reason)
            return
        client.subscribe(self.settings.topic("acks"), qos=1)

    def subscribed(self, client, userdata, mid, reasons, properties):
        if not reasons or any(reason.is_failure for reason in reasons):
            log.error("Gateway ACK subscription refused")
            return
        client.publish(
            self.settings.topic("status"), status_payload("online"), qos=1, retain=True
        )
        self.ready.set()

    def disconnected(self, client, userdata, flags, reason, properties):
        self.ready.clear()

    def received(self, client, userdata, message):
        if (
            message.topic != self.settings.topic("acks")
            or message.retain
            or len(message.payload) > 1024
        ):
            return
        try:
            ack = Ack.model_validate_json(message.payload)
            self.outbox.acknowledge(str(ack.event_id), ack.digest, ack.outcome)
        except (ValidationError, sqlite3.Error) as exc:
            log.warning("ACK not applied: %s", type(exc).__name__)

    async def run(self, stopping: asyncio.Event) -> None:
        start(self.client, self.settings)
        try:
            while not stopping.is_set():
                self.outbox.set_status(
                    "mqtt", "online" if self.ready.is_set() else "offline"
                )
                if self.ready.is_set():
                    for _, body in self.outbox.due():
                        info = self.client.publish(
                            self.settings.topic("events"), body, qos=1, retain=False
                        )
                        if info.rc != mqtt.MQTT_ERR_SUCCESS:
                            break
                await wait_tick(stopping, 0.2)
        finally:
            if self.ready.is_set():
                info = self.client.publish(
                    self.settings.topic("status"),
                    status_payload("offline"),
                    qos=1,
                    retain=True,
                )
                try:
                    await asyncio.to_thread(info.wait_for_publish, 1.0)
                except (RuntimeError, ValueError):
                    pass
            await asyncio.to_thread(stop, self.client)
            self.outbox.set_status("mqtt", "offline")


async def acquire(settings: Settings, outbox: Outbox, stopping: asyncio.Event) -> None:
    opc, modbus = OpcAdapter(settings), ModbusAdapter(settings)
    session = uuid4()
    sequence = 0
    loop = asyncio.get_running_loop()
    try:
        while not stopping.is_set():
            started = loop.time()
            outbox.set_status("heartbeat", now().isoformat())
            if outbox.has_capacity(len(TAGS)):
                readings = await asyncio.gather(opc.read(), modbus.read())
                batch = []
                for protocol, samples in zip(
                    ("opcua", "modbus"), readings, strict=True
                ):
                    outbox.set_status(
                        protocol,
                        "offline"
                        if all(item.failure for item in samples)
                        else "online",
                    )
                    if any(not item.failure for item in samples):
                        outbox.set_status(f"read:{protocol}", now().isoformat())
                    for sample in samples:
                        sequence += 1
                        batch.append(sample.event(session, sequence))
                outbox.coverage(0 if outbox.put(batch) else len(TAGS))
            else:
                outbox.coverage(len(TAGS))
            await wait_tick(
                stopping, max(0, settings.interval - (loop.time() - started))
            )
    finally:
        await asyncio.gather(opc.close(), modbus.close())
        outbox.set_status("opcua", "stopped")
        outbox.set_status("modbus", "stopped")


async def run(settings: Settings, stopping: asyncio.Event) -> None:
    outbox = Outbox(settings.outbox_path, settings.capacity)
    outbox.initialize()
    outbox.bind_scope(settings.scope)
    publisher = Publisher(settings, outbox)
    async with asyncio.TaskGroup() as group:
        group.create_task(publisher.run(stopping))
        worker = acquire_configured if settings.acquisition else acquire
        group.create_task(worker(settings, outbox, stopping))


async def acquire_configured(
    settings: Settings, outbox: Outbox, stopping: asyncio.Event
):
    profile = settings.acquisition
    digest = profile.digest
    session = uuid4()
    sequence = 0
    loop = asyncio.get_running_loop()
    adapters = [ConfiguredAdapter(source) for source in profile.sources]
    due = dict.fromkeys((adapter.source.id for adapter in adapters), 0.0)
    try:
        while not stopping.is_set():
            outbox.set_status("heartbeat", now().isoformat())
            started = loop.time()
            active = [
                adapter for adapter in adapters if due[adapter.source.id] <= started
            ]
            if not active:
                await wait_tick(stopping, min(0.5, min(due.values()) - started))
                continue
            slots = sum(len(adapter.tags) for adapter in active)
            for adapter in active:
                due[adapter.source.id] = started + adapter.source.interval
            if not outbox.has_capacity(slots):
                outbox.coverage(slots)
                continue
            batch = []
            results = await asyncio.gather(*(adapter.read() for adapter in active))
            for adapter, samples in zip(active, results, strict=True):
                source = adapter.source
                quality = [
                    sample.event(
                        session,
                        sequence + index,
                        schema_version=2,
                        site_id=profile.site_id,
                        gateway_id=profile.gateway_id,
                        simulated=False,
                        config_hash=digest,
                        source_id=source.id,
                        mapping=var,
                        tag_id=var.id,
                    )
                    for index, (sample, var) in enumerate(
                        zip(samples, source.variables, strict=True), 1
                    )
                ]
                sequence += len(quality)
                outbox.set_status(
                    source.id,
                    "online"
                    if all(e.quality == "GOOD" for e in quality)
                    else "degraded",
                )
                if any(not sample.failure for sample in samples):
                    outbox.set_status(f"read:{source.id}", now().isoformat())
                batch.extend(quality)
            outbox.coverage(0 if outbox.put(batch) else slots)
    finally:
        await asyncio.gather(*(adapter.close() for adapter in adapters))
        for adapter in adapters:
            outbox.set_status(adapter.source.id, "stopped")
