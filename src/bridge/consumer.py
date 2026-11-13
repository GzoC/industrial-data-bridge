import asyncio
import logging
import os

import psycopg

from bridge.config import ACK_TOPIC, EVENT_TOPIC, Settings
from bridge.simulators import wait_tick
from bridge.store import connect, initialize, store_on_connection
from bridge.transport import Ack, client_for, start, stop

log = logging.getLogger(__name__)


class Consumer:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.dsn = settings.dsn()
        self.db = None
        self.client = client_for(settings, "consumer")
        self.client.on_connect = self.connected
        self.client.on_subscribe = self.subscribed
        self.client.on_message = self.received

    def connected(self, client, userdata, flags, reason, properties):
        if reason.is_failure:
            log.warning("Consumer broker connection refused: %s", reason)
            return
        client.subscribe(EVENT_TOPIC, qos=1)

    def subscribed(self, client, userdata, mid, reasons, properties):
        if not reasons or any(reason.is_failure for reason in reasons):
            log.error("Consumer event subscription refused")

    def received(self, client, userdata, message):
        if (
            message.topic != EVENT_TOPIC
            or message.retain
            or len(message.payload) > 16384
        ):
            client.ack(message.mid, message.qos)
            return
        if (
            self.settings.lab_controls
            and (self.settings.controls / "consumer-paused").exists()
        ):
            client.ack(message.mid, message.qos)
            return
        try:
            if self.db is None:
                self.db = connect(self.dsn)
            result = store_on_connection(self.db, message.payload)
        except psycopg.Error as exc:
            self.close_db()
            log.warning("PostgreSQL write failed: %s", type(exc).__name__)
            # The gateway holds the durable copy and retries until an application ACK arrives.
            client.ack(message.mid, message.qos)
            return
        if result:
            event, outcome = result
            crash_flag = self.settings.controls / "consumer-crash-after-commit"
            if (
                outcome == "stored"
                and self.settings.lab_controls
                and crash_flag.exists()
            ):
                crash_flag.unlink(missing_ok=True)
                os._exit(42)
            ack = Ack(event_id=event.event_id, digest=event.digest(), outcome=outcome)
            client.publish(ACK_TOPIC, ack.model_dump_json(), qos=1, retain=False)
        client.ack(message.mid, message.qos)

    def close_db(self):
        if self.db is not None:
            self.db.close()
            self.db = None


async def run(settings: Settings, stopping: asyncio.Event) -> None:
    consumer = Consumer(settings)
    while not stopping.is_set():
        try:
            await asyncio.to_thread(initialize, consumer.dsn)
            break
        except psycopg.Error as exc:
            log.warning("PostgreSQL initialization pending: %s", type(exc).__name__)
            await wait_tick(stopping, 2)
    if stopping.is_set():
        return
    start(consumer.client, settings)
    try:
        await stopping.wait()
    finally:
        await asyncio.to_thread(stop, consumer.client)
        consumer.close_db()
