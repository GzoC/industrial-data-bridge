import csv
import io
import sqlite3
from datetime import datetime, timedelta
from importlib.resources import files
from pathlib import Path
from typing import Annotated, Literal
from uuid import UUID

import psycopg
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from pydantic import BaseModel, Field, model_validator

from bridge.config import Fault, Settings
from bridge.events import BY_ID, TAGS, UTCDateTime, now
from bridge.outbox import Outbox
from bridge.store import connect

app = FastAPI(title="Puente OT/IT Aurora")
Asset = Literal["P101", "TK101"]
Code = Annotated[str, Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9._-]+$")]


@app.exception_handler(psycopg.Error)
async def database_unavailable(request: Request, exc: psycopg.Error):
    return JSONResponse({"detail": "PostgreSQL no está disponible"}, status_code=503)


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return files("bridge").joinpath("dashboard.html").read_text(encoding="utf-8")


@app.get("/api/health")
def health():
    settings = Settings.load()
    with connect(settings.dsn()) as db:
        db.execute("SELECT 1 FROM events LIMIT 1")
    state = backlog()
    heartbeat = state["gateway"].get("heartbeat")
    fresh = heartbeat and now() - datetime.fromisoformat(heartbeat) < timedelta(
        seconds=max(5, settings.interval * 3, settings.timeout * 3)
    )
    connected = state["gateway"].get("mqtt") == "online"
    if not fresh or not connected:
        return JSONResponse(
            {"database": "online", "gateway": "degraded", "mqtt": connected},
            status_code=503,
        )
    return {"database": "online", "gateway": "online", "mqtt": "online"}


def fields(row):
    return row["body"] | {
        "event_time": row["event_time"],
        "received_at": row["received_at"],
        "order": row["order_context"],
        "context_rule": row["context_rule"],
    }


@app.get("/api/assets")
def assets():
    settings = Settings.load()
    with connect(settings.dsn()) as db:
        rows = db.execute("""
            SELECT DISTINCT ON (tag_id) * FROM events
            ORDER BY tag_id,event_time DESC,observed_at DESC,event_id DESC
        """).fetchall()
    by_tag = {row["tag_id"]: row for row in rows}
    stamp = now()
    result = []
    for tag in TAGS:
        row = by_tag.get(tag.id)
        if row:
            age = max(0.0, (stamp - row["event_time"]).total_seconds())
            result.append(
                fields(row)
                | {
                    "age_seconds": age,
                    "stale": age > max(3 * settings.interval, settings.timeout * 2),
                }
            )
        else:
            result.append(
                {
                    "tag_id": tag.id,
                    "asset_id": tag.asset,
                    "unit": tag.unit,
                    "value": None,
                    "quality": "SIN DATOS",
                    "source_protocol": tag.protocol,
                    "stale": True,
                }
            )
    return {
        "updated_at": stamp,
        "frequency_hz": 1 / settings.interval,
        "readings": result,
    }


def history_rows(tag: str, start, end, lot: str | None, limit: int, offset: int):
    if tag not in BY_ID:
        raise HTTPException(404, "Variable desconocida")
    if not timedelta(0) < end - start <= timedelta(days=7):
        raise HTTPException(
            422, "El intervalo debe ser mayor que cero y de hasta siete días"
        )
    with connect(Settings.load().dsn()) as db:
        return db.execute(
            """
            SELECT * FROM events WHERE tag_id=%s AND event_time>=%s AND event_time<%s
            AND (%s::text IS NULL OR order_context->>'lot_id'=%s)
            ORDER BY event_time,observed_at,event_id LIMIT %s OFFSET %s
        """,
            (tag, start, end, lot, lot, limit, offset),
        ).fetchall()


@app.get("/api/history")
def history(
    tag: str,
    start: UTCDateTime,
    end: UTCDateTime,
    lot: str | None = None,
    limit: Annotated[int, Query(ge=1, le=5000)] = 500,
    offset: Annotated[int, Query(ge=0, le=100_000)] = 0,
):
    rows = history_rows(tag, start, end, lot, limit + 1, offset)
    return {
        "events": [fields(row) for row in rows[:limit]],
        "next_offset": offset + limit if len(rows) > limit else None,
    }


@app.get("/api/history.csv")
def export_csv(tag: str, start: UTCDateTime, end: UTCDateTime, lot: str | None = None):
    rows = history_rows(tag, start, end, lot, 5001, 0)
    if len(rows) > 5000:
        raise HTTPException(
            422, "Reduce el intervalo para exportar hasta 5000 registros"
        )
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(
        (
            "event_id",
            "tag",
            "event_time_utc",
            "observed_at_utc",
            "received_at_utc",
            "value",
            "unit",
            "quality",
            "lot",
        )
    )
    for row in rows:
        writer.writerow(
            (
                row["event_id"],
                tag,
                row["event_time"].isoformat(),
                row["observed_at"].isoformat(),
                row["received_at"].isoformat(),
                row["body"]["value"],
                row["body"]["unit"],
                row["body"]["quality"],
                (row["order_context"] or {}).get("lot_id", ""),
            )
        )
    return Response(
        buffer.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="aurora.csv"'},
    )


@app.get("/api/backlog")
def backlog():
    settings = Settings.load()
    if settings.outbox_path.is_file():
        try:
            return Outbox(settings.outbox_path, settings.capacity).status()
        except sqlite3.Error as exc:
            raise HTTPException(503, "Outbox no disponible") from exc
    raise HTTPException(503, "Outbox aún no inicializada")


@app.get("/api/stats")
def stats():
    with connect(Settings.load().dsn()) as db:
        counters = db.execute("SELECT kind,total FROM delivery_counters").fetchall()
        latency = db.execute("""
            SELECT count(*) AS samples, count(*)/60.0 AS events_per_second,
                percentile_cont(0.50) WITHIN GROUP (
                    ORDER BY extract(epoch FROM write_at-observed_at)) AS p50_seconds,
                percentile_cont(0.95) WITHIN GROUP (
                    ORDER BY extract(epoch FROM write_at-observed_at)) AS p95_seconds
            FROM events WHERE received_at >= now() - interval '60 seconds'
        """).fetchone()
    return {
        "counters": {row["kind"]: row["total"] for row in counters},
        "last_minute": latency,
    }


@app.get("/api/errors")
def errors(limit: Annotated[int, Query(ge=1, le=100)] = 20):
    with connect(Settings.load().dsn()) as db:
        return db.execute(
            """
            SELECT event_id,kind,detail,received_at FROM delivery_errors ORDER BY id DESC LIMIT %s
        """,
            (limit,),
        ).fetchall()


@app.get("/api/events/{event_id}")
def event(event_id: UUID):
    with connect(Settings.load().dsn()) as db:
        row = db.execute(
            "SELECT * FROM events WHERE event_id=%s", (event_id,)
        ).fetchone()
    if row is None:
        raise HTTPException(404, "Evento pendiente o desconocido")
    return fields(row)


class Order(BaseModel):
    order_id: Code
    lot_id: Code
    asset_id: Asset
    product_id: Code
    valid_from: UTCDateTime
    valid_to: UTCDateTime

    @model_validator(mode="after")
    def valid_range(self):
        if self.valid_from >= self.valid_to:
            raise ValueError("valid_from must precede valid_to")
        return self


@app.get("/api/orders")
def orders(asset: Asset):
    with connect(Settings.load().dsn()) as db:
        return db.execute(
            """
            SELECT * FROM production_orders WHERE asset_id=%s ORDER BY valid_from DESC LIMIT 100
        """,
            (asset,),
        ).fetchall()


@app.post("/api/orders", status_code=201)
def create_order(order: Order):
    try:
        with connect(Settings.load().dsn()) as db:
            return db.execute(
                """
                INSERT INTO production_orders(order_id,lot_id,asset_id,product_id,valid_from,valid_to)
                VALUES (%(order_id)s,%(lot_id)s,%(asset_id)s,%(product_id)s,
                        %(valid_from)s,%(valid_to)s) RETURNING *
            """,
                order.model_dump(),
            ).fetchone()
    except psycopg.errors.ExclusionViolation as exc:
        raise HTTPException(
            409, "El intervalo se superpone a una orden del activo"
        ) from exc


class FaultCommand(BaseModel):
    mode: Fault


@app.put("/api/lab/simulator/{name}")
def simulation(name: Literal["opcua", "modbus"], command: FaultCommand):
    controls = lab_controls()
    temporary = controls / f".{name}-{now().timestamp()}"
    temporary.write_text(command.mode, encoding="utf-8")
    temporary.replace(controls / name)
    return {"mode": command.mode}


def lab_controls() -> Path:
    settings = Settings.load()
    if not settings.lab_controls:
        raise HTTPException(404, "Controles de laboratorio desactivados")
    settings.controls.mkdir(parents=True, exist_ok=True)
    return settings.controls


class ConsumerCommand(BaseModel):
    paused: bool


@app.put("/api/lab/consumer")
def pause_consumer(command: ConsumerCommand):
    flag = lab_controls() / "consumer-paused"
    if command.paused:
        flag.touch()
    else:
        flag.unlink(missing_ok=True)
    return command


@app.post("/api/lab/seed-orders")
def seed_orders():
    lab_controls()
    stamp = now().replace(microsecond=0)
    settings = Settings.load()
    with connect(settings.dsn()) as db:
        db.execute("LOCK TABLE production_orders IN SHARE ROW EXCLUSIVE MODE")
        if db.execute("SELECT 1 FROM production_orders LIMIT 1").fetchone():
            raise HTTPException(
                409, "Ya existen órdenes en la base de datos de demostración"
            )
        for asset in ("P101", "TK101"):
            boundary = stamp + timedelta(minutes=2)
            for order_id, lot_id, begin, end in (
                ("ORD-01", "LOT-A", stamp - timedelta(hours=1), boundary),
                ("ORD-02", "LOT-B", boundary, stamp + timedelta(hours=2)),
            ):
                db.execute(
                    """
                    INSERT INTO production_orders(order_id,lot_id,asset_id,product_id,valid_from,valid_to)
                    VALUES (%s,%s,%s,'AGUA-DEMO',%s,%s)
                """,
                    (order_id, lot_id, asset, begin, end),
                )
    return {"boundary_utc": boundary}
