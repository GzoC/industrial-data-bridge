import asyncio
import logging
import sqlite3
from threading import Event as ThreadEvent
from uuid import uuid4

import paho.mqtt.client as mqtt
from pydantic import ValidationError

from bridge.adapters import ModbusAdapter, OpcAdapter
from bridge.config import ACK_TOPIC, EVENT_TOPIC, STATUS_TOPIC, Settings
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
        client.subscribe(ACK_TOPIC, qos=1)

    def subscribed(self, client, userdata, mid, reasons, properties):
        if not reasons or any(reason.is_failure for reason in reasons):
            log.error("Gateway ACK subscription refused")
            return
        client.publish(STATUS_TOPIC, status_payload("online"), qos=1, retain=True)
        self.ready.set()

    def disconnected(self, client, userdata, flags, reason, properties):
        self.ready.clear()

    def received(self, client, userdata, message):
        if message.topic != ACK_TOPIC or message.retain or len(message.payload) > 1024:
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
                            EVENT_TOPIC, body, qos=1, retain=False
                        )
                        if info.rc != mqtt.MQTT_ERR_SUCCESS:
                            break
                await wait_tick(stopping, 0.2)
        finally:
            if self.ready.is_set():
                info = self.client.publish(
                    STATUS_TOPIC,
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
    publisher = Publisher(settings, outbox)
    async with asyncio.TaskGroup() as group:
        group.create_task(publisher.run(stopping))
        group.create_task(acquire(settings, outbox, stopping))
