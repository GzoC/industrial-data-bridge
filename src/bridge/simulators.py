import asyncio
import math
import random
import struct
from pathlib import Path

from asyncua import Server, ua
from pymodbus.constants import ExcCodes
from pymodbus.datastore import (
    ModbusDeviceContext,
    ModbusSequentialDataBlock,
    ModbusServerContext,
)
from pymodbus.server import ModbusTcpServer

from bridge.config import (
    MODBUS_PORT,
    OPC_NAMESPACE,
    OPC_PATH,
    OPC_PORT,
    Fault,
    Settings,
)
from bridge.events import now


def read_fault(directory: Path, name: str) -> Fault:
    path = directory / name
    try:
        value = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return "normal"
    if value in ("normal", "invalid", "frozen", "offline"):
        return value
    raise ValueError(f"Invalid laboratory fault file: {name}")


def cell_values(step: int, seed: int) -> tuple[float, float, float, float, int, bool]:
    phase = (step % 120) * math.tau / 120
    running = step % 120 < 90
    noise = random.Random(seed + step).uniform(-0.1, 0.1)
    return (
        1 + 0.65 * math.sin(phase),
        27 + 5 * math.sin(phase / 2) + noise,
        12 + 4 * math.sin(phase) + noise if running else 0.0,
        3 + math.sin(phase) if running else 0.0,
        round(1500 + 200 * math.sin(phase)) if running else 0,
        running,
    )


def float_words(value: float, order: str) -> tuple[int, int]:
    first, second = struct.unpack(">HH", struct.pack(">f", value))
    return (first, second) if order == "big" else (second, first)


async def wait_tick(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), seconds)
    except TimeoutError:
        pass


async def serve_opc(
    settings: Settings,
    stop: asyncio.Event,
    ready: asyncio.Event | None = None,
    *,
    bind: str = "127.0.0.1",
) -> None:
    server = Server()
    await server.init()
    server.set_endpoint(f"opc.tcp://{bind}:{OPC_PORT}{OPC_PATH}")
    server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
    ns = await server.register_namespace(OPC_NAMESPACE)
    tank = await server.nodes.objects.add_object(ua.NodeId("TK101", ns), "TK101")
    nodes = [
        await tank.add_variable(ua.NodeId(f"TK101.{tag}", ns), tag, initial)
        for tag, initial in zip(
            ("level", "temperature"), cell_values(0, settings.seed)[:2]
        )
    ]
    online = False
    step = 0
    last_values = cell_values(0, settings.seed)[:2]
    try:
        while not stop.is_set():
            fault = read_fault(settings.controls, "opcua")
            if fault == "offline":
                if online:
                    await server.stop()
                    online = False
                await wait_tick(stop, settings.interval)
                continue
            if fault != "frozen":
                last_values = cell_values(step, settings.seed)[:2]
                step += 1
            for node, value in zip(nodes, last_values):
                status = (
                    ua.StatusCodes.BadOutOfRange
                    if fault == "invalid"
                    else ua.StatusCodes.Good
                )
                await node.write_value(
                    ua.DataValue(
                        ua.Variant(value, ua.VariantType.Double),
                        StatusCode=ua.StatusCode(status),
                        SourceTimestamp=now(),
                    )
                )
            if not online:
                await server.start()
                online = True
                if ready:
                    ready.set()
            await wait_tick(stop, settings.interval)
    finally:
        if online:
            await server.stop()


class ReadOnlyDevice(ModbusDeviceContext):
    def setValues(self, func_code, address, values):
        return ExcCodes.ILLEGAL_FUNCTION

    def update(self, words: list[int], running: bool) -> None:
        super().setValues(3, 0, words)
        super().setValues(1, 0, [running])


async def serve_modbus(
    settings: Settings,
    stop: asyncio.Event,
    ready: asyncio.Event | None = None,
    *,
    bind: str = "127.0.0.1",
) -> None:
    device = ReadOnlyDevice(
        co=ModbusSequentialDataBlock(1, [False]),
        hr=ModbusSequentialDataBlock(1, [0] * 5),
        di=ModbusSequentialDataBlock(1, [False]),
        ir=ModbusSequentialDataBlock(1, [0]),
    )
    context = ModbusServerContext(devices={settings.device_id: device}, single=False)
    server = None
    step = 0
    last_values = cell_values(0, settings.seed)[2:]
    try:
        while not stop.is_set():
            fault = read_fault(settings.controls, "modbus")
            if fault == "offline":
                if server:
                    await server.shutdown()
                    server = None
                await wait_tick(stop, settings.interval)
                continue
            if fault != "frozen":
                last_values = cell_values(step, settings.seed)[2:]
                step += 1
            flow, pressure, speed, running = last_values
            if fault == "invalid":
                flow, pressure, speed = float("nan"), 99.0, 65535
            device.update(
                [
                    *float_words(flow, settings.word_order),
                    *float_words(pressure, settings.word_order),
                    speed,
                ],
                running,
            )
            if not server:
                server = ModbusTcpServer(context, address=(bind, MODBUS_PORT))
                await server.serve_forever(background=True)
                if ready:
                    ready.set()
            await wait_tick(stop, settings.interval)
    finally:
        if server:
            await server.shutdown()
