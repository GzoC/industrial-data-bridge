import base64
import hashlib
import secrets
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def mosquitto_hash(password: str) -> str:
    # Mosquitto 2.0 password file format 7. Store the salted hash in the broker.
    salt = secrets.token_bytes(12)
    iterations = 210_000
    digest = hashlib.pbkdf2_hmac("sha512", password.encode(), salt, iterations)
    return "$7$" + "$".join(
        (
            str(iterations),
            base64.b64encode(salt).decode(),
            base64.b64encode(digest).decode(),
        )
    )


def main() -> None:
    env = ROOT / ".env"
    broker = ROOT / ".runtime" / "broker"
    passwords = broker / "passwords"
    if env.exists() or passwords.exists():
        raise SystemExit(
            "Demo credentials already exist. Reuse the current .env and password file."
        )
    broker.mkdir(parents=True, exist_ok=True)
    postgres, gateway, consumer = (secrets.token_hex(24) for _ in range(3))
    env_text = (
        f"DEMO_POSTGRES_PASSWORD={postgres}\n"
        f"BRIDGE_MQTT_GATEWAY_PASSWORD={gateway}\n"
        f"BRIDGE_MQTT_CONSUMER_PASSWORD={consumer}\n"
    )
    with env.open("x", encoding="utf-8") as handle:
        handle.write(env_text)
    try:
        with passwords.open("x", encoding="utf-8") as handle:
            handle.write(
                f"gateway:{mosquitto_hash(gateway)}\nconsumer:{mosquitto_hash(consumer)}\n",
            )
    except OSError:
        env.unlink()
        raise
    env.chmod(0o600)
    print("Created local .env and .runtime/broker/passwords.")


if __name__ == "__main__":
    main()
