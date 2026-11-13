import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from math import isfinite
from typing import Annotated, Literal
from uuid import UUID, uuid4

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictFloat,
    StrictInt,
    model_validator,
)

from bridge.config import GATEWAY_ID, OPC_NAMESPACE, SITE_ID
from bridge.profile import Identifier, Variable

Protocol = Literal["opcua", "modbus"]
Quality = Literal["GOOD", "UNCERTAIN", "BAD"]
Scalar = StrictBool | StrictInt | StrictFloat
Raw = Scalar | list[StrictInt] | Literal["NaN", "Infinity", "-Infinity"] | None
UTCDateTime = Annotated[
    AwareDatetime, AfterValidator(lambda value: value.astimezone(UTC))
]


@dataclass(frozen=True)
class Tag:
    asset: str
    name: str
    protocol: Protocol
    ref: str
    unit: str
    kind: type
    limits: tuple[float, float] | None = None

    @property
    def id(self) -> str:
        return f"{self.asset}.{self.name}"

    def accepts(self, value: object) -> bool:
        if type(value) is not self.kind:
            return False
        if isinstance(value, float) and not isfinite(value):
            return False
        return self.limits is None or self.limits[0] <= value <= self.limits[1]


TAGS = (
    Tag(
        "TK101",
        "level",
        "opcua",
        f"nsu={OPC_NAMESPACE};s=TK101.level",
        "m",
        float,
        (0, 2),
    ),
    Tag(
        "TK101",
        "temperature",
        "opcua",
        f"nsu={OPC_NAMESPACE};s=TK101.temperature",
        "°C",
        float,
        (15, 40),
    ),
    Tag("P101", "flow", "modbus", "holding:0-1", "L/min", float, (0, 20)),
    Tag("P101", "pressure", "modbus", "holding:2-3", "bar", float, (0, 6)),
    Tag("P101", "speed", "modbus", "holding:4", "rpm", int, (0, 3000)),
    Tag("P101", "running", "modbus", "coil:0", "bool", bool),
)
BY_ID = {tag.id: tag for tag in TAGS}


def now() -> datetime:
    return datetime.now(UTC)


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False, frozen=True)

    event_id: UUID
    schema_version: Literal[1, 2]
    gateway_id: Identifier
    session_id: UUID
    sequence: Annotated[StrictInt, Field(gt=0, le=2**63 - 1)]
    site_id: Identifier
    asset_id: str
    tag_id: str
    source_protocol: Protocol
    source_ref: str
    source_timestamp: UTCDateTime | None
    observed_at: UTCDateTime
    received_at: UTCDateTime | None
    timestamp_origin: Literal["source", "gateway"]
    raw_value: Raw
    value: Scalar | None
    unit: str
    quality: Quality
    quality_reasons: tuple[str, ...]
    source_status: Annotated[StrictInt, Field(ge=0, le=2**32 - 1)] | None = None
    simulated: StrictBool
    config_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    source_id: Identifier | None = None
    mapping: Variable | None = None

    @model_validator(mode="after")
    def check_contract(self) -> "Event":
        if self.schema_version == 2:
            mapping = self.mapping
            if (
                mapping is None
                or not self.config_hash
                or not self.source_id
                or self.simulated
                or (mapping.id, mapping.asset, mapping.unit)
                != (self.tag_id, self.asset_id, self.unit)
            ):
                raise ValueError("invalid version 2 mapping or provenance")
            expected = (
                bool
                if mapping.kind == "bool"
                else (
                    float
                    if mapping.kind.startswith("float")
                    or (mapping.scale, mapping.offset) != (1, 0)
                    else int
                )
            )
            tag = Tag(
                self.asset_id,
                self.tag_id,
                self.source_protocol,
                self.source_ref,
                self.unit,
                expected,
            )
        else:
            tag = BY_ID.get(self.tag_id)
            if (
                not self.simulated
                or self.site_id != SITE_ID
                or self.gateway_id != GATEWAY_ID
                or any(
                    v is not None
                    for v in (self.config_hash, self.source_id, self.mapping)
                )
            ):
                raise ValueError("invalid legacy provenance or identity")
        if not tag or (
            self.asset_id,
            self.source_protocol,
            self.source_ref,
            self.unit,
        ) != (tag.asset, tag.protocol, tag.ref, tag.unit):
            raise ValueError("tag metadata does not match the laboratory map")
        if self.source_protocol == "modbus" and (
            self.source_timestamp is not None or self.source_status is not None
        ):
            raise ValueError("Modbus has no native timestamp or OPC UA StatusCode")
        if self.timestamp_origin == "source" and (
            self.source_timestamp is None
            or abs(self.observed_at - self.source_timestamp) > timedelta(seconds=10)
            or self.quality == "BAD"
        ):
            raise ValueError("untrusted source timestamp")
        if self.quality == "BAD":
            if self.value is not None or not self.quality_reasons:
                raise ValueError("BAD readings require null and a reason")
        elif not tag.accepts(self.value):
            raise ValueError("value type or engineering range is invalid")
        if self.quality == "UNCERTAIN" and not self.quality_reasons:
            raise ValueError("UNCERTAIN readings require a reason")
        if self.source_status is not None:
            severity = self.source_status >> 30
            if (severity >= 2 and self.quality != "BAD") or (
                severity == 1 and self.quality == "GOOD"
            ):
                raise ValueError("normalized quality cannot improve OPC UA quality")
        return self

    @property
    def event_time(self) -> datetime:
        if self.timestamp_origin == "source" and self.source_timestamp is not None:
            return self.source_timestamp
        return self.observed_at

    def wire(self) -> str:
        exclude = (
            {"config_hash", "source_id", "mapping"}
            if self.schema_version == 1
            else set()
        )
        return json.dumps(
            self.model_dump(mode="json", exclude=exclude),
            sort_keys=True,
            separators=(",", ":"),
        )

    def digest(self) -> str:
        return hashlib.sha256(self.wire().encode()).hexdigest()


@dataclass(frozen=True)
class Sample:
    tag: Tag
    value: Scalar | None
    raw: Raw
    observed_at: datetime
    source_timestamp: datetime | None = None
    status: int | None = None
    failure: str | None = None

    def event(self, session: UUID, sequence: int, **metadata) -> Event:
        reasons = []
        if self.failure:
            reasons.append(self.failure)
        severity = (self.status or 0) >> 30
        if severity >= 2:
            reasons.append("opc_bad")
        if not reasons and not self.tag.accepts(self.value):
            reasons.append("invalid_value")
        quality: Quality = "BAD" if reasons else "GOOD"
        origin = "gateway"
        if quality != "BAD":
            if severity == 1:
                quality = "UNCERTAIN"
                reasons.append("opc_uncertain")
            if self.source_timestamp is not None:
                if abs(self.observed_at - self.source_timestamp) <= timedelta(
                    seconds=10
                ):
                    origin = "source"
                else:
                    quality = "UNCERTAIN"
                    reasons.append("clock_skew")
        raw = self.raw
        if isinstance(raw, float) and not isfinite(raw):
            raw = "NaN" if raw != raw else ("Infinity" if raw > 0 else "-Infinity")
        fields = dict(
            event_id=uuid4(),
            schema_version=1,
            gateway_id=GATEWAY_ID,
            session_id=session,
            sequence=sequence,
            site_id=SITE_ID,
            asset_id=self.tag.asset,
            tag_id=self.tag.id,
            source_protocol=self.tag.protocol,
            source_ref=self.tag.ref,
            source_timestamp=self.source_timestamp,
            source_status=self.status,
            observed_at=self.observed_at,
            received_at=None,
            timestamp_origin=origin,
            raw_value=raw,
            value=None if quality == "BAD" else self.value,
            unit=self.tag.unit,
            quality=quality,
            quality_reasons=tuple(reasons),
            simulated=True,
        )
        return Event(**(fields | metadata))
