import os
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, SecretStr

OPC_PORT = 4840
MODBUS_PORT = 1502
OPC_PATH = "/aurora/"
OPC_NAMESPACE = "urn:aurora:sim:tk101"
GATEWAY_ID = "aurora-gw"
SITE_ID = "aurora-demo"
EVENT_TOPIC = f"aurora/v1/{GATEWAY_ID}/events"
ACK_TOPIC = f"aurora/v1/{GATEWAY_ID}/acks"
STATUS_TOPIC = f"aurora/v1/{GATEWAY_ID}/status"

LabHost = Literal["127.0.0.1", "opcua", "modbus"]
Fault = Literal["normal", "invalid", "frozen", "offline"]


class Settings(BaseModel):
    opc_host: Literal["127.0.0.1", "opcua"] = "127.0.0.1"
    modbus_host: Literal["127.0.0.1", "modbus"] = "127.0.0.1"
    broker_host: Literal["127.0.0.1", "broker"] = "127.0.0.1"
    broker_port: int = Field(default=1883, ge=1, le=65535)
    device_id: int = Field(default=1, ge=1, le=247)
    word_order: Literal["big", "little"] = "big"
    interval: float = Field(default=1.0, ge=0.1, le=60)
    timeout: float = Field(default=2.0, gt=0, le=10)
    capacity: int = Field(default=10_000, ge=6, le=100_000)
    seed: int = 101
    outbox_path: Path = Path(".runtime/gateway.sqlite")
    controls: Path = Path(".runtime/controls")
    database_dsn: SecretStr | None = None
    mqtt_gateway_password: SecretStr | None = None
    mqtt_consumer_password: SecretStr | None = None
    lab_controls: bool = False

    @classmethod
    def load(cls) -> "Settings":
        env = {
            field: os.environ[f"BRIDGE_{field.upper()}"]
            for field in cls.model_fields
            if f"BRIDGE_{field.upper()}" in os.environ
        }
        return cls.model_validate(env)

    def opc_endpoint(self) -> str:
        return f"opc.tcp://{self.opc_host}:{OPC_PORT}{OPC_PATH}"

    def dsn(self) -> str:
        if not self.database_dsn:
            raise ValueError("BRIDGE_DATABASE_DSN must be set")
        return self.database_dsn.get_secret_value()

    def password(self, role: Literal["gateway", "consumer"]) -> str:
        password = (
            self.mqtt_gateway_password
            if role == "gateway"
            else self.mqtt_consumer_password
        )
        if not password:
            raise ValueError(f"BRIDGE_MQTT_{role.upper()}_PASSWORD must be set")
        return password.get_secret_value()
