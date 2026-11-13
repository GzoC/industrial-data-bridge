import hashlib
import os
import tomllib
from pathlib import Path
from typing import Annotated, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

Identifier = Annotated[str, Field(pattern=r"^[a-zA-Z0-9_-]{1,48}$")]
EnvName = Annotated[str, Field(pattern=r"^[A-Z][A-Z0-9_]*$")]
Port = Annotated[int, Field(ge=1, le=65535)]
Host = Annotated[str, Field(pattern=r"^[A-Za-z0-9.:-]{1,253}$")]
Kind = Literal["bool", "int16", "uint16", "int32", "uint32", "float32", "float64"]


class Config(BaseModel):
    model_config = ConfigDict(
        extra="forbid", allow_inf_nan=False, hide_input_in_errors=True
    )


class Destination(Config):
    host: Host
    port: Port


class Variable(Config):
    id: Identifier
    asset: Identifier
    unit: str = Field(min_length=1, max_length=32)
    kind: Kind
    node: (
        Annotated[int, Field(ge=0, le=4294967295)]
        | Annotated[str, Field(min_length=1, max_length=256)]
        | None
    ) = None
    table: Literal["holding", "input", "coil", "discrete"] | None = None
    address: int | None = Field(default=None, ge=0, le=65535)
    byte_order: Literal["big", "little"] = "big"
    word_order: Literal["big", "little"] = "big"
    scale: float = 1
    offset: float = 0

    @property
    def words(self):
        return 2 if self.kind in ("float32", "uint32", "int32") else 1


class OpcSecurity(Config):
    client_certificate: Path
    client_key: Path
    server_certificate: Path
    application_uri: str
    username_env: EnvName | None = None
    password_env: EnvName | None = None

    @model_validator(mode="after")
    def validate_files(self):
        if any(
            not p.is_file()
            for p in (self.client_certificate, self.client_key, self.server_certificate)
        ):
            raise ValueError("OPC UA certificate or private-key file is missing")
        if bool(self.username_env) != bool(self.password_env):
            raise ValueError("OPC UA username and password references must be paired")
        return self


class Source(Config):
    id: Identifier
    protocol: Literal["opcua", "modbus"]
    endpoint: str | None = None
    namespace: str | None = Field(default=None, min_length=1, max_length=256)
    security: OpcSecurity | None = None
    host: Host | None = None
    port: Port | None = None
    device_id: int = Field(default=1, ge=1, le=247)
    interval: float = Field(default=1, ge=0.2, le=300)
    timeout: float = Field(default=2, ge=0.1, le=15)
    variables: list[Variable] = Field(min_length=1, max_length=64)

    def destination(self) -> tuple[str, int]:
        if self.protocol == "opcua":
            url = urlsplit(self.endpoint or "")
            if (
                url.scheme != "opc.tcp"
                or not url.hostname
                or not url.port
                or url.username is not None
                or url.query
                or url.fragment
            ):
                raise ValueError(
                    "OPC UA requires opc.tcp URL with explicit host and port"
                )
            return url.hostname, url.port
        if not self.host or not self.port:
            raise ValueError("Modbus requires host and port")
        return self.host.lower(), self.port

    @model_validator(mode="after")
    def validate_map(self):
        self.destination()
        opc = self.protocol == "opcua"
        if opc and (not self.namespace or self.host or self.port):
            raise ValueError("OPC UA requires namespace and full endpoint")
        if not opc and (self.namespace or self.endpoint or self.security):
            raise ValueError("OPC UA settings are invalid for Modbus")
        for var in self.variables:
            if var.kind == "bool" and (var.scale != 1 or var.offset != 0):
                raise ValueError("boolean scaling is unsupported")
            if opc and (var.node is None or var.table or var.address is not None):
                raise ValueError("OPC UA variable requires a numeric or textual node")
            if not opc and (
                var.node is not None
                or var.table is None
                or var.address is None
                or (var.kind == "bool") != (var.table in ("coil", "discrete"))
                or var.kind == "float64"
                or var.address + var.words > 65536
            ):
                raise ValueError("invalid Modbus table, type, or zero-based address")
        return self


class Broker(Destination):
    ca: Path | None = None
    gateway_user: Identifier = "gateway"
    consumer_user: Identifier = "consumer"
    gateway_password_env: EnvName
    consumer_password_env: EnvName

    @model_validator(mode="after")
    def check_roles(self):
        if self.gateway_user == self.consumer_user:
            raise ValueError("MQTT roles require distinct users")
        if self.ca is not None and not self.ca.is_file():
            raise ValueError("MQTT CA file is missing")
        return self


class Profile(Config):
    profile: Literal["acquisition", "demo"]
    site_id: Identifier = "aurora-demo"
    gateway_id: Identifier = "aurora-gw"
    assets: dict[Identifier, str] = Field(default_factory=dict)
    allowed: list[Destination] = Field(default_factory=list)
    sources: list[Source] = Field(default_factory=list, max_length=16)
    broker: Broker | None = None
    database_env: EnvName | None = None
    outbox: Path | None = None
    capacity: int = Field(default=10000, ge=6, le=100000)
    heartbeat_max_age: float = Field(default=10, ge=3, le=600)
    pending_max_age: float = Field(default=30, ge=3, le=3600)
    history_days: int = Field(default=90, ge=1)
    diagnostic_days: int = Field(default=30, ge=1)

    @model_validator(mode="after")
    def check_inventory(self):
        if self.profile == "demo":
            if (
                self.sources
                or self.assets
                or self.allowed
                or self.broker
                or self.database_env
                or self.outbox
            ):
                raise ValueError("demo uses the built-in inventory and BRIDGE settings")
            return self
        if not (
            self.assets
            and self.sources
            and self.allowed
            and self.broker
            and self.database_env
            and self.outbox
        ):
            raise ValueError(
                "acquisition requires assets, sources, allowed destinations, broker, database and outbox"
            )
        if len({s.id for s in self.sources}) != len(self.sources):
            raise ValueError("duplicate source ID")
        if any(
            s.id in {"scope", "heartbeat", "mqtt", "last_ack"} for s in self.sources
        ):
            raise ValueError("source ID conflicts with a reserved status field")
        allowed = {(entry.host.lower(), entry.port) for entry in self.allowed}
        if any(s.destination() not in allowed for s in self.sources):
            raise ValueError("source destination is outside the explicit allowlist")
        variables = [v for s in self.sources for v in s.variables]
        if len(variables) > self.capacity or len({v.id for v in variables}) != len(
            variables
        ):
            raise ValueError("variable IDs must be unique and fit in the outbox")
        if any(v.asset not in self.assets for v in variables):
            raise ValueError("variable references an unknown asset")
        return self

    @property
    def digest(self):
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()

    @classmethod
    def read(cls, path: str):
        try:
            with Path(path).open("rb") as handle:
                return cls.model_validate(tomllib.load(handle))
        except ValidationError as exc:
            raise ValueError(
                "; ".join(
                    f"{'.'.join(map(str, issue['loc'])) or 'profile'}: {issue['msg']}"
                    for issue in exc.errors(include_input=False)
                )
            ) from None
        except (OSError, tomllib.TOMLDecodeError):
            raise ValueError("Cannot read profile file or invalid TOML") from None


def secret(name: str) -> str:
    if not os.environ.get(name):
        raise ValueError(f"Missing environment variable {name}")
    return os.environ[name]
