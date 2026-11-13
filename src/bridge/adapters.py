import asyncio
import logging
import struct

from asyncua import Client, ua
from asyncua.ua.uaerrors import UaError
from pymodbus.client import AsyncModbusTcpClient
from pymodbus.exceptions import ModbusException

from bridge.config import MODBUS_PORT, OPC_NAMESPACE, Settings
from bridge.events import TAGS, Protocol, Sample, now

log = logging.getLogger(__name__)
OPC_TAGS = tuple(tag for tag in TAGS if tag.protocol == "opcua")
MODBUS_TAGS = tuple(tag for tag in TAGS if tag.protocol == "modbus")


def word_float(words: list[int], order: str) -> float:
    first, second = words
    if order == "little":
        first, second = second, first
    return struct.unpack(">f", struct.pack(">HH", first, second))[0]


def missing(protocol: Protocol, reason: str) -> list[Sample]:
    observed = now()
    return [
        Sample(tag, None, None, observed, failure=reason)
        for tag in TAGS
        if tag.protocol == protocol
    ]


class OpcAdapter:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client: Client | None = None
        self.nodes = []

    async def read(self) -> list[Sample]:
        try:
            async with asyncio.timeout(self.settings.timeout):
                if self.client is None:
                    self.client = Client(
                        self.settings.opc_endpoint(), timeout=self.settings.timeout
                    )
                    await self.client.connect()
                    ns = await self.client.get_namespace_index(OPC_NAMESPACE)
                    self.nodes = [
                        self.client.get_node(ua.NodeId(tag.id, ns)) for tag in OPC_TAGS
                    ]
                result = await self.client.read_attributes(
                    self.nodes, ua.AttributeIds.Value
                )
            observed = now()
            return [
                Sample(
                    tag,
                    item.Value.Value if item.Value else None,
                    item.Value.Value if item.Value else None,
                    observed,
                    item.SourceTimestamp,
                    item.StatusCode.value if item.StatusCode else None,
                )
                for tag, item in zip(OPC_TAGS, result, strict=True)
            ]
        except (OSError, UaError, ValueError) as exc:
            log.debug("OPC UA read failed: %s", type(exc).__name__)
            await self.close()
            return missing("opcua", "communication_loss")

    async def close(self) -> None:
        if self.client is not None:
            try:
                async with asyncio.timeout(self.settings.timeout):
                    await self.client.disconnect()
            except (OSError, UaError):
                pass
            finally:
                self.client = None
                self.nodes = []


class ModbusAdapter:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = AsyncModbusTcpClient(
            settings.modbus_host,
            port=MODBUS_PORT,
            timeout=settings.timeout,
            retries=0,
            reconnect_delay=0,
        )

    async def read(self) -> list[Sample]:
        try:
            async with asyncio.timeout(self.settings.timeout):
                if not self.client.connected and not await self.client.connect():
                    return missing("modbus", "communication_loss")
                holding = await self.client.read_holding_registers(
                    0, count=5, device_id=self.settings.device_id
                )
                holding_at = now()
                coil = await self.client.read_coils(
                    0, count=1, device_id=self.settings.device_id
                )
                coil_at = now()
            samples = []
            if holding.isError() or len(holding.registers) != 5:
                samples.extend(
                    Sample(
                        tag, None, None, holding_at, failure="invalid_register_response"
                    )
                    for tag in MODBUS_TAGS[:3]
                )
            else:
                words = holding.registers
                samples.extend(
                    Sample(
                        tag,
                        word_float(pair, self.settings.word_order),
                        pair,
                        holding_at,
                    )
                    for tag, pair in zip(
                        MODBUS_TAGS[:2], (words[:2], words[2:4]), strict=True
                    )
                )
                samples.append(Sample(MODBUS_TAGS[2], words[4], [words[4]], holding_at))
            if coil.isError() or not coil.bits:
                samples.append(
                    Sample(
                        MODBUS_TAGS[3],
                        None,
                        None,
                        coil_at,
                        failure="invalid_coil_response",
                    )
                )
            else:
                samples.append(
                    Sample(MODBUS_TAGS[3], coil.bits[0], coil.bits[0], coil_at)
                )
            return samples
        except (OSError, ModbusException) as exc:
            log.debug("Modbus read failed: %s", type(exc).__name__)
            await self.close()
            return missing("modbus", "communication_loss")

    async def close(self) -> None:
        self.client.close()
