import asyncio
import hashlib
import json
import logging
import os
from threading import Event
from uuid import UUID

import psycopg

from bridge.config import GATEWAY_ID, SITE_ID, Settings
from bridge.simulators import wait_tick
from bridge.store import connect, initialize, store_on_connection
from bridge.transport import Ack, client_for, start, stop

log = logging.getLogger(__name__)


class Consumer:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.dsn = settings.dsn()
        self.db = None
        self.ready = Event()
        profile = settings.acquisition
        self.identity = (
            (profile.site_id, profile.gateway_id) if profile else (SITE_ID, GATEWAY_ID)
        )
        self.client = client_for(settings, "consumer")
        self.client.on_connect = self.connected
        self.client.on_subscribe = self.subscribed
        self.client.on_disconnect = lambda *args: self.ready.clear()
        self.client.on_message = self.received

    def connected(self, client, userdata, flags, reason, properties):
        if reason.is_failure:
            log.warning("Consumer broker connection refused: %s", reason)
            return
        client.subscribe(self.settings.topic("events"), qos=1)

    def subscribed(self, client, userdata, mid, reasons, properties):
        if not reasons or any(reason.is_failure for reason in reasons):
            log.error("Consumer event subscription refused")
        else:
            self.ready.set()

    def received(self, client, userdata, message):
        if (
            message.topic != self.settings.topic("events")
            or message.retain
            or len(message.payload) > 16384
        ):
            client.ack(message.mid, message.qos)
            return
        if (
            not self.settings.acquisition
            and self.settings.lab_controls
            and (self.settings.controls / "consumer-paused").exists()
        ):
            client.ack(message.mid, message.qos)
            return
        try:
            if self.db is None:
                self.db = connect(self.dsn)
            result = store_on_connection(self.db, message.payload, self.identity)
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
                and not self.settings.acquisition
                and self.settings.lab_controls
                and crash_flag.exists()
            ):
                crash_flag.unlink(missing_ok=True)
                os._exit(42)
            ack = Ack(event_id=event.event_id, digest=event.digest(), outcome=outcome)
        else:
            try:
                identifier = UUID(json.loads(message.payload)["event_id"])
                ack = Ack(
                    event_id=identifier,
                    digest=hashlib.sha256(message.payload).hexdigest(),
                    outcome="rejected",
                )
            except (KeyError, TypeError, ValueError):
                ack = None
        if ack:
            client.publish(
                self.settings.topic("acks"), ack.model_dump_json(), qos=1, retain=False
            )
        client.ack(message.mid, message.qos)

    def close_db(self):
        if self.db is not None:
            self.db.close()
            self.db = None

    def heartbeat(self, subscribed: bool):
        with connect(self.dsn) as db:
            db.execute(
                """
                INSERT INTO consumer_status VALUES (%s,now(),%s)
                ON CONFLICT(scope) DO UPDATE SET heartbeat=excluded.heartbeat,
                    subscribed=excluded.subscribed
            """,
                (self.settings.scope, subscribed),
            )


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
        while not stopping.is_set():
            try:
                paused = (
                    settings.lab_controls
                    and (settings.controls / "consumer-paused").exists()
                )
                await asyncio.to_thread(
                    consumer.heartbeat, consumer.ready.is_set() and not paused
                )
            except psycopg.Error:
                log.debug("Consumer heartbeat unavailable")
            await wait_tick(stopping, 2)
    finally:
        await asyncio.to_thread(stop, consumer.client)
        consumer.close_db()
        try:
            await asyncio.to_thread(consumer.heartbeat, False)
        except psycopg.Error:
            pass
