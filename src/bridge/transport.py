import json
import logging
import ssl
from typing import Literal
from uuid import UUID

import paho.mqtt.client as mqtt
from pydantic import BaseModel, ConfigDict, Field

from bridge.config import Settings

log = logging.getLogger(__name__)


class Ack(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    event_id: UUID
    digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    outcome: Literal["stored", "conflict", "rejected"]


def status_payload(state: str) -> str:
    return json.dumps({"schema_version": 1, "state": state}, separators=(",", ":"))


def client_for(settings: Settings, role: Literal["gateway", "consumer"]) -> mqtt.Client:
    broker = settings.acquisition.broker if settings.acquisition else None
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"{settings.scope.replace('/', '.')}.{role}"
        if broker
        else f"aurora-{role}",
        clean_session=True,
        protocol=mqtt.MQTTv311,
        manual_ack=role == "consumer",
    )
    client.username_pw_set(
        getattr(broker, f"{role}_user") if broker else role, settings.password(role)
    )
    if broker and broker.ca:
        client.tls_set_context(ssl.create_default_context(cafile=str(broker.ca)))
    client.max_inflight_messages_set(100)
    client.max_queued_messages_set(200)
    client.reconnect_delay_set(1, 10)
    if role == "gateway":
        client.will_set(
            settings.topic("status"), status_payload("offline"), qos=1, retain=True
        )
    return client


def start(client: mqtt.Client, settings: Settings) -> None:
    broker = settings.acquisition.broker if settings.acquisition else None
    client.connect_async(
        broker.host if broker else settings.broker_host,
        broker.port if broker else settings.broker_port,
        keepalive=15,
    )
    client.loop_start()


def stop(client: mqtt.Client) -> None:
    client.disconnect()
    client.loop_stop()
