import os
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, SecretStr

from bridge.profile import Profile, secret

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
    profile: Profile | None = None
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

    model_config = {"hide_input_in_errors": True}

    @classmethod
    def load(cls) -> "Settings":
        env = {
            field: os.environ[f"BRIDGE_{field.upper()}"]
            for field in cls.model_fields
            if field != "profile" and f"BRIDGE_{field.upper()}" in os.environ
        }
        if path := os.environ.get("BRIDGE_CONFIG"):
            profile = Profile.read(path)
            env["profile"] = profile
            if profile.profile == "acquisition":
                env.update(
                    lab_controls=False,
                    capacity=profile.capacity,
                    outbox_path=profile.outbox,
                )
        return cls.model_validate(env)

    @property
    def acquisition(self):
        return (
            self.profile
            if self.profile and self.profile.profile == "acquisition"
            else None
        )

    @property
    def scope(self):
        return (
            f"{self.acquisition.site_id}/{self.acquisition.gateway_id}"
            if self.acquisition
            else "aurora/v1/aurora-gw"
        )

    def topic(self, suffix: str):
        return (
            f"bridge/v2/{self.scope}/{suffix}"
            if self.acquisition
            else f"{self.scope}/{suffix}"
        )

    def opc_endpoint(self) -> str:
        return f"opc.tcp://{self.opc_host}:{OPC_PORT}{OPC_PATH}"

    def dsn(self) -> str:
        if self.acquisition:
            return secret(self.acquisition.database_env)
        if not self.database_dsn:
            raise ValueError("BRIDGE_DATABASE_DSN must be set")
        return self.database_dsn.get_secret_value()

    def password(self, role: Literal["gateway", "consumer"]) -> str:
        if self.acquisition:
            return secret(getattr(self.acquisition.broker, f"{role}_password_env"))
        password = (
            self.mqtt_gateway_password
            if role == "gateway"
            else self.mqtt_consumer_password
        )
        if not password:
            raise ValueError(f"BRIDGE_MQTT_{role.upper()}_PASSWORD must be set")
        return password.get_secret_value()
