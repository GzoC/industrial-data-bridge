import json
import logging
from typing import Literal
from uuid import UUID

import paho.mqtt.client as mqtt
from pydantic import BaseModel, ConfigDict, Field

from bridge.config import STATUS_TOPIC, Settings

log = logging.getLogger(__name__)


class Ack(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    event_id: UUID
    digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    outcome: Literal["stored", "conflict"]


def status_payload(state: str) -> str:
    return json.dumps({"schema_version": 1, "state": state}, separators=(",", ":"))


def client_for(settings: Settings, role: Literal["gateway", "consumer"]) -> mqtt.Client:
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"aurora-{role}",
        clean_session=True,
        protocol=mqtt.MQTTv311,
        manual_ack=role == "consumer",
    )
    client.username_pw_set(role, settings.password(role))
    client.max_inflight_messages_set(100)
    client.max_queued_messages_set(200)
    client.reconnect_delay_set(1, 10)
    if role == "gateway":
        client.will_set(STATUS_TOPIC, status_payload("offline"), qos=1, retain=True)
    return client


def start(client: mqtt.Client, settings: Settings) -> None:
    client.connect_async(settings.broker_host, settings.broker_port, keepalive=15)
    client.loop_start()


def stop(client: mqtt.Client) -> None:
    client.disconnect()
    client.loop_stop()
