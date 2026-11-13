import argparse
import asyncio
import json
import logging
import os
import signal
from functools import partial

from bridge import consumer, gateway
from bridge.config import Settings
from bridge.configured import ConfiguredAdapter
from bridge.profile import secret
from bridge.simulators import serve_modbus, serve_opc


async def serve(role: str, container: bool) -> None:
    settings = Settings.load()
    if settings.acquisition and role in ("opcua", "modbus"):
        raise ValueError("Simulators are disabled in the acquisition profile")
    if settings.acquisition and role == "gateway":
        check_source_secrets(settings)
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


def check_source_secrets(settings):
    for source in settings.acquisition.sources:
        if source.security and source.security.username_env:
            secret(source.security.username_env)
            secret(source.security.password_env)


async def probe(settings, source_id):
    if not settings.acquisition:
        raise ValueError("probe requires an acquisition profile")
    check_source_secrets(settings)
    source = next(
        (item for item in settings.acquisition.sources if item.id == source_id), None
    )
    if source is None:
        raise ValueError("Unknown configured source")
    adapter = ConfiguredAdapter(source)
    try:
        samples = await adapter.read()
        results = [
            {
                "variable": var.id,
                "value": sample.value if sample.failure is None else None,
                "unit": var.unit,
                "reason": sample.failure,
                "status": sample.status,
            }
            for var, sample in zip(source.variables, samples, strict=True)
        ]
        print(json.dumps(results, ensure_ascii=False, default=str))
        return any(
            sample.failure or (sample.status or 0) >> 30 >= 2 for sample in samples
        )
    finally:
        await adapter.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "role",
        choices=(
            "opcua",
            "modbus",
            "gateway",
            "consumer",
            "validate",
            "diagnose",
            "probe",
        ),
    )
    parser.add_argument("--config")
    parser.add_argument("--source")
    parser.add_argument("--container", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s"
    )
    if args.config:
        os.environ["BRIDGE_CONFIG"] = args.config
    try:
        if args.role in ("validate", "diagnose", "probe"):
            settings = Settings.load()
            if args.role == "probe":
                raise SystemExit(int(asyncio.run(probe(settings, args.source))))
            if args.role == "diagnose":
                settings.dsn()
                settings.password("gateway")
                settings.password("consumer")
                if settings.acquisition:
                    check_source_secrets(settings)
                from importlib.metadata import version

                for package in (
                    "asyncua",
                    "pymodbus",
                    "paho-mqtt",
                    "psycopg",
                    "fastapi",
                ):
                    print(f"{package} {version(package)}")
            print(
                "Acquisition profile valid"
                if settings.acquisition
                else "Demo profile valid"
            )
        else:
            asyncio.run(serve(args.role, args.container))
    except ValueError as exc:
        parser.exit(2, f"{exc}\n")


if __name__ == "__main__":
    main()
