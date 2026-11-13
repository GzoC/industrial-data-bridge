import asyncio
import struct
from datetime import UTC, datetime

from asyncua import Client, ua
from asyncua.crypto import security_policies, uacrypto
from asyncua.ua.uaerrors import UaError
from pymodbus.client import AsyncModbusTcpClient
from pymodbus.exceptions import ModbusException

from bridge.events import Sample, Tag, now
from bridge.profile import Source, Variable, secret

READERS = {
    "holding": "read_holding_registers",
    "input": "read_input_registers",
    "coil": "read_coils",
    "discrete": "read_discrete_inputs",
}
FORMATS = {"int16": "h", "uint16": "H", "int32": "i", "uint32": "I", "float32": "f"}


def convert(value, var: Variable):
    if (var.scale, var.offset) == (1, 0):
        return value
    return value * var.scale + var.offset


def decode(words: list[int], var: Variable):
    ordered = words if var.word_order == "big" else reversed(words)
    packed = b"".join(word.to_bytes(2, var.byte_order) for word in ordered)
    return convert(struct.unpack(">" + FORMATS[var.kind], packed)[0], var)


class ConfiguredAdapter:
    def __init__(self, source: Source):
        self.source = source
        self.client = None
        self.nodes = []
        self.tags = []
        for var in source.variables:
            if source.protocol == "opcua":
                node_type = "i" if isinstance(var.node, int) else "s"
                ref = f"{source.endpoint}#nsu={source.namespace};{node_type}={var.node}"
            else:
                ref = f"{source.host}:{source.port}/{source.device_id}/{var.table}:{var.address}"
            kind = (
                bool
                if var.kind == "bool"
                else float
                if var.kind.startswith("float") or (var.scale, var.offset) != (1, 0)
                else int
            )
            self.tags.append(
                Tag(var.asset, var.id, source.protocol, ref, var.unit, kind)
            )

    async def connect(self):
        source = self.source
        if source.protocol == "modbus":
            self.client = AsyncModbusTcpClient(
                source.host,
                port=source.port,
                timeout=source.timeout,
                retries=0,
                reconnect_delay=0,
            )
            if not await self.client.connect():
                raise OSError("Modbus connection failed")
            return
        client = self.client = Client(
            source.endpoint, timeout=source.timeout, auto_reconnect=False
        )
        if security := source.security:
            client.application_uri = security.application_uri
            pinned = await uacrypto.load_certificate(security.server_certificate)
            if (
                not pinned.not_valid_before_utc
                <= datetime.now(UTC)
                <= pinned.not_valid_after_utc
            ):
                raise ValueError(
                    "Pinned OPC UA server certificate is outside its validity period"
                )
            await client.set_security(
                security_policies.SecurityPolicyBasic256Sha256,
                security.client_certificate,
                security.client_key,
                server_certificate=str(security.server_certificate),
            )
            if security.username_env:
                client.set_user(secret(security.username_env))
                client.set_password(secret(security.password_env))
        await client.connect()
        namespace = await client.get_namespace_index(source.namespace)
        self.nodes = [
            client.get_node(ua.NodeId(var.node, namespace)) for var in source.variables
        ]

    async def read(self) -> list[Sample]:
        try:
            async with asyncio.timeout(self.source.timeout):
                if self.client is None:
                    await self.connect()
                if self.source.protocol == "opcua":
                    return await self.read_opc()
                return await self.read_modbus()
        except (OSError, UaError, ModbusException, ValueError):
            await self.close()
            stamp = now()
            return [
                Sample(tag, None, None, stamp, failure="source_unavailable")
                for tag in self.tags
            ]

    async def read_opc(self):
        readings = await self.client.read_attributes(self.nodes, ua.AttributeIds.Value)
        stamp = now()
        samples = []
        for tag, var, item in zip(
            self.tags, self.source.variables, readings, strict=True
        ):
            raw = item.Value.Value if item.Value else None
            expected = (
                bool
                if var.kind == "bool"
                else float
                if var.kind.startswith("float")
                else int
            )
            supported = type(raw) is expected
            samples.append(
                Sample(
                    tag,
                    convert(raw, var) if supported else None,
                    raw if type(raw) in (bool, float, int) else None,
                    stamp,
                    item.SourceTimestamp,
                    item.StatusCode.value if item.StatusCode else None,
                    None if supported else "invalid_value_type",
                )
            )
        return samples

    async def read_modbus(self):
        samples = []
        for tag, var in zip(self.tags, self.source.variables, strict=True):
            response = await getattr(self.client, READERS[var.table])(
                var.address,
                count=var.words,
                device_id=self.source.device_id,
            )
            stamp = now()
            if response.isError():
                samples.append(
                    Sample(tag, None, None, stamp, failure="modbus_exception")
                )
            elif var.kind == "bool":
                value = response.bits[0]
                samples.append(Sample(tag, value, value, stamp))
            elif len(response.registers) == var.words:
                samples.append(
                    Sample(
                        tag, decode(response.registers, var), response.registers, stamp
                    )
                )
            else:
                samples.append(
                    Sample(tag, None, None, stamp, failure="invalid_register_response")
                )
        return samples

    async def close(self):
        if self.client is not None:
            try:
                if self.source.protocol == "opcua":
                    async with asyncio.timeout(self.source.timeout):
                        await self.client.disconnect()
                else:
                    self.client.close()
            except (OSError, UaError):
                pass
            finally:
                self.client = None
