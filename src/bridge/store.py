import hashlib
from importlib.resources import files
from typing import Literal

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import ValidationError

from bridge.events import Event, now

Outcome = Literal["stored", "conflict"]


def connect(dsn: str):
    return psycopg.connect(
        dsn,
        connect_timeout=3,
        options="-c statement_timeout=5000 -c timezone=UTC",
        row_factory=dict_row,
    )


def initialize(dsn: str) -> None:
    with connect(dsn) as db:
        db.execute("SELECT pg_advisory_xact_lock(12701, 101)")
        db.execute(files("bridge").joinpath("schema.sql").read_text(encoding="utf-8"))


def count(db, kind: str) -> None:
    db.execute(
        """
        INSERT INTO delivery_counters VALUES (%s,1)
        ON CONFLICT(kind) DO UPDATE SET total=delivery_counters.total+1
    """,
        (kind,),
    )


def error(db, event_id, digest: str, kind: str, detail: str) -> None:
    db.execute(
        """
        INSERT INTO delivery_errors(event_id,digest,kind,detail)
        VALUES (%s,%s,%s,%s) ON CONFLICT(digest,kind) DO NOTHING
    """,
        (event_id, digest, kind, detail),
    )
    count(db, kind)


def store(dsn: str, payload: bytes) -> tuple[Event, Outcome] | None:
    with connect(dsn) as db:
        return store_on_connection(db, payload)


def store_on_connection(db, payload: bytes) -> tuple[Event, Outcome] | None:
    received = now()
    try:
        event = Event.model_validate_json(payload)
        if event.received_at is not None:
            raise ValueError("received_at must be null on the wire")
    except (ValidationError, ValueError) as exc:
        detail = (
            str(exc)
            if not isinstance(exc, ValidationError)
            else ", ".join(
                f"{'.'.join(map(str, item['loc']))}:{item['type']}"
                for item in exc.errors(include_input=False)[:5]
            )
        )
        with db.transaction():
            error(db, None, hashlib.sha256(payload).hexdigest(), "rejected", detail)
        return None

    digest = event.digest()
    with db.transaction():
        context = db.execute(
            """
            SELECT to_jsonb(o) - 'id' AS body FROM production_orders o
            WHERE asset_id=%s AND valid_from<=%s AND valid_to>%s
        """,
            (event.asset_id, event.event_time, event.event_time),
        ).fetchone()
        rule = event.timestamp_origin
        if context is None:
            rule += "_no_order"
        inserted = db.execute(
            """
            INSERT INTO events(event_id,digest,body,asset_id,tag_id,event_time,observed_at,
                               received_at,order_context,context_rule)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(event_id) DO NOTHING RETURNING event_id
        """,
            (
                event.event_id,
                digest,
                Jsonb(event.model_dump(mode="json")),
                event.asset_id,
                event.tag_id,
                event.event_time,
                event.observed_at,
                received,
                Jsonb(context["body"]) if context else None,
                rule,
            ),
        ).fetchone()
        if inserted:
            count(db, "stored")
        else:
            original = db.execute(
                "SELECT digest FROM events WHERE event_id=%s",
                (event.event_id,),
            ).fetchone()
            if original["digest"] != digest:
                error(
                    db,
                    event.event_id,
                    digest,
                    "conflict",
                    "event_id reused with new content",
                )
                return event, "conflict"
            count(db, "duplicate")
    return event, "stored"
