import argparse
import asyncio
import logging
import signal
from functools import partial

from bridge import consumer, gateway
from bridge.config import Settings
from bridge.simulators import serve_modbus, serve_opc


async def serve(role: str, container: bool) -> None:
    settings = Settings.load()
    stopping = asyncio.Event()
    loop = asyncio.get_running_loop()

    def request_stop(signum, frame):
        loop.call_soon_threadsafe(stopping.set)

    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, request_stop)
    bind = "0.0.0.0" if container else "127.0.0.1"
    handler = {
        "opcua": partial(serve_opc, bind=bind),
        "modbus": partial(serve_modbus, bind=bind),
        "gateway": gateway.run,
        "consumer": consumer.run,
    }[role]
    await handler(settings, stopping)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("role", choices=("opcua", "modbus", "gateway", "consumer"))
    parser.add_argument("--container", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s"
    )
    asyncio.run(serve(args.role, args.container))


if __name__ == "__main__":
    main()
