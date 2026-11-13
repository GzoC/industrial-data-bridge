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

from bridge.config import GATEWAY_ID, SITE_ID, Fault, Settings
from bridge.events import TAGS, UTCDateTime, now
from bridge.outbox import Outbox
from bridge.store import connect

app = FastAPI(title="Puente OT/IT Aurora")
Code = Annotated[str, Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9._-]+$")]
SCOPE = "body->>'site_id'=%s AND body->>'gateway_id'=%s"


def identity(settings):
    profile = settings.acquisition
    return (profile.site_id, profile.gateway_id) if profile else (SITE_ID, GATEWAY_ID)


def inventory(settings):
    if profile := settings.acquisition:
        return [
            (var.id, var.asset, var.unit, source.protocol, source.interval)
            for source in profile.sources
            for var in source.variables
        ]
    return [
        (tag.id, tag.asset, tag.unit, tag.protocol, settings.interval) for tag in TAGS
    ]


@app.exception_handler(psycopg.Error)
async def database_unavailable(request: Request, exc: psycopg.Error):
    return JSONResponse({"detail": "PostgreSQL no está disponible"}, status_code=503)


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return files("bridge").joinpath("dashboard.html").read_text(encoding="utf-8")


@app.get("/api/health")
@app.get("/api/ready")
def health():
    settings = Settings.load()
    state = operational(settings)
    return JSONResponse(state, status_code=200 if state["state"] == "healthy" else 503)


@app.get("/api/live")
def live():
    return {"state": "alive"}


@app.get("/api/status")
def operational_endpoint():
    return operational(Settings.load())


def operational(settings):
    profile = settings.profile
    max_age = profile.heartbeat_max_age if profile else 10
    pending_limit = profile.pending_max_age if profile else 30
    stamp = now()

    def age(value):
        if value is None:
            return None
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return max(0, (stamp - value).total_seconds())

    reasons = []
    db_status = "online"
    consumer = None
    try:
        with connect(settings.dsn()) as db:
            consumer = db.execute(
                "SELECT * FROM consumer_status WHERE scope=%s", (settings.scope,)
            ).fetchone()
    except psycopg.Error:
        db_status = "offline"
        reasons.append("postgres_unavailable")
    try:
        queue = backlog()
    except HTTPException:
        queue = {
            "gateway": {},
            "pending": 0,
            "capacity": settings.capacity,
            "coverage": [],
        }
        reasons.append("outbox_unavailable")
    gateway = queue["gateway"]
    gateway_age = age(gateway.get("heartbeat"))
    consumer_age = age(consumer["heartbeat"]) if consumer else None
    fresh = gateway_age is not None and gateway_age <= max_age
    mqtt = gateway.get("mqtt", "unknown") if fresh else "unknown"
    if not fresh:
        reasons.append("gateway_heartbeat_expired")
    if mqtt != "online":
        reasons.append("mqtt_unavailable")
    if not consumer or consumer_age > max_age or not consumer["subscribed"]:
        reasons.append("consumer_unavailable")
    sources = {}
    source_timing = (
        [(s.id, s.interval, s.timeout) for s in settings.acquisition.sources]
        if settings.acquisition
        else [
            (name, settings.interval, settings.timeout) for name in ("opcua", "modbus")
        ]
    )
    for name, interval, timeout in source_timing:
        read_age = age(gateway.get(f"read:{name}"))
        ok = (
            fresh
            and gateway.get(name) == "online"
            and read_age is not None
            and read_age
            <= max(
                max_age,
                3 * interval + timeout,
            )
        )
        sources[name] = {
            "state": "healthy" if ok else "degraded",
            "last_read_age_seconds": read_age,
        }
        if not ok:
            reasons.append(f"source_degraded:{name}")
    pending_age = age(queue.get("oldest_pending"))
    if queue["pending"] >= queue["capacity"] or any(
        not row["ended"] for row in queue["coverage"]
    ):
        reasons.append("coverage_gap")
    if pending_age is not None and pending_age > pending_limit:
        reasons.append("pending_too_old")
    state = (
        "unavailable"
        if not fresh or db_status == "offline"
        else ("degraded" if reasons else "healthy")
    )
    return {
        "state": state,
        "reasons": reasons,
        "postgres": db_status,
        "mqtt": mqtt,
        "gateway_age_seconds": gateway_age,
        "consumer_age_seconds": consumer_age,
        "last_ack_age_seconds": age(gateway.get("last_ack")),
        "oldest_pending_age_seconds": pending_age,
        "pending": queue["pending"],
        "capacity": queue["capacity"],
        "queue_counters": queue.get("counters", {}),
        "coverage": queue["coverage"],
        "sources": sources,
    }


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
        rows = db.execute(
            f"""
            SELECT DISTINCT ON (tag_id) * FROM events
            WHERE {SCOPE}
            ORDER BY tag_id,event_time DESC,observed_at DESC,event_id DESC
        """,
            identity(settings),
        ).fetchall()
    by_tag = {row["tag_id"]: row for row in rows}
    stamp = now()
    result = []
    for tag_id, asset, unit, protocol, interval in inventory(settings):
        row = by_tag.get(tag_id)
        if row:
            age = max(0.0, (stamp - row["event_time"]).total_seconds())
            result.append(
                fields(row)
                | {
                    "age_seconds": age,
                    "stale": age > max(3 * interval, settings.timeout * 2),
                    "configured_hz": 1 / interval,
                }
            )
        else:
            result.append(
                {
                    "tag_id": tag_id,
                    "asset_id": asset,
                    "unit": unit,
                    "value": None,
                    "quality": "SIN DATOS",
                    "source_protocol": protocol,
                    "configured_hz": 1 / interval,
                    "stale": True,
                }
            )
    return {
        "updated_at": stamp,
        "profile": "acquisition" if settings.acquisition else "demo",
        "lab_controls": settings.lab_controls and not settings.acquisition,
        "assets": settings.acquisition.assets
        if settings.acquisition
        else {"TK101": "Tanque TK101", "P101": "Bomba P101"},
        "readings": result,
    }


def history_rows(tag: str, start, end, lot: str | None, limit: int, offset: int):
    if not timedelta(0) < end - start <= timedelta(days=7):
        raise HTTPException(
            422, "El intervalo debe ser mayor que cero y de hasta siete días"
        )
    settings = Settings.load()
    with connect(settings.dsn()) as db:
        return db.execute(
            f"""
            SELECT * FROM events WHERE tag_id=%s AND event_time>=%s AND event_time<%s
            AND {SCOPE}
            AND (%s::text IS NULL OR order_context->>'lot_id'=%s)
            ORDER BY event_time,observed_at,event_id LIMIT %s OFFSET %s
        """,
            (tag, start, end, *identity(settings), lot, lot, limit, offset),
        ).fetchall()


@app.get("/api/history")
def history(
    tag: str,
    start: UTCDateTime,
    end: UTCDateTime,
    lot: str | None = None,
    limit: Annotated[int, Query(ge=1, le=5000)] = 500,
    offset: Annotated[int, Query(ge=0)] = 0,
):
    rows = history_rows(tag, start, end, lot, limit + 1, offset)
    return {
        "events": [fields(row) for row in rows[:limit]],
        "next_offset": offset + limit if len(rows) > limit else None,
    }


@app.get("/api/history.csv")
def export_csv(
    tag: str,
    start: UTCDateTime,
    end: UTCDateTime,
    lot: str | None = None,
    offset: Annotated[int, Query(ge=0)] = 0,
):
    rows = history_rows(tag, start, end, lot, 5001, offset)
    next_offset = {}
    if len(rows) > 5000:
        next_offset["X-Next-Offset"] = str(offset + 5000)
        rows = rows[:5000]
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
        headers={
            "Content-Disposition": 'attachment; filename="bridge.csv"',
            **next_offset,
        },
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
    settings = Settings.load()
    with connect(settings.dsn()) as db:
        if settings.acquisition:
            counters = db.execute(
                "SELECT kind,total FROM scoped_delivery_counters WHERE site_id=%s AND gateway_id=%s",
                identity(settings),
            ).fetchall()
        else:
            counters = db.execute("SELECT kind,total FROM delivery_counters").fetchall()
        latency = db.execute(
            f"""
            SELECT count(*) AS samples, count(*)/60.0 AS events_per_second,
                percentile_cont(0.50) WITHIN GROUP (
                    ORDER BY extract(epoch FROM write_at-observed_at)) AS p50_seconds,
                percentile_cont(0.95) WITHIN GROUP (
                    ORDER BY extract(epoch FROM write_at-observed_at)) AS p95_seconds
            FROM events WHERE {SCOPE} AND received_at >= now() - interval '60 seconds'
        """,
            identity(settings),
        ).fetchone()
    return {
        "counters": {row["kind"]: row["total"] for row in counters},
        "last_minute": latency,
    }


@app.get("/api/retention/preview")
def retention_preview():
    settings = Settings.load()
    history = settings.profile.history_days if settings.profile else 90
    diagnostics = settings.profile.diagnostic_days if settings.profile else 30
    history_before = now() - timedelta(days=history)
    diagnostics_before = now() - timedelta(days=diagnostics)
    with connect(settings.dsn()) as db:
        events = db.execute(
            f"SELECT count(*) AS n FROM events WHERE {SCOPE} AND event_time<%s",
            (*identity(settings), history_before),
        ).fetchone()["n"]
        errors = db.execute(
            "SELECT count(*) AS n FROM delivery_errors WHERE received_at<%s",
            (diagnostics_before,),
        ).fetchone()["n"]
    return {
        "history_before": history_before,
        "event_candidates": events,
        "diagnostics_before": diagnostics_before,
        "database_diagnostic_candidates": errors,
        "automatic_cleanup": False,
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
    settings = Settings.load()
    with connect(settings.dsn()) as db:
        row = db.execute(
            f"SELECT * FROM events WHERE event_id=%s AND {SCOPE}",
            (event_id, *identity(settings)),
        ).fetchone()
    if row is None:
        raise HTTPException(404, "Evento pendiente o desconocido")
    return fields(row)


class Order(BaseModel):
    order_id: Code
    lot_id: Code
    asset_id: Code
    product_id: Code
    valid_from: UTCDateTime
    valid_to: UTCDateTime

    @model_validator(mode="after")
    def valid_range(self):
        if self.valid_from >= self.valid_to:
            raise ValueError("valid_from must precede valid_to")
        return self


@app.get("/api/orders")
def orders(asset: Code):
    settings = Settings.load()
    with connect(settings.dsn()) as db:
        return db.execute(
            """
            SELECT * FROM production_orders WHERE asset_id=%s AND site_id=%s ORDER BY valid_from DESC LIMIT 100
        """,
            (asset, identity(settings)[0]),
        ).fetchall()


@app.post("/api/orders", status_code=201)
def create_order(order: Order):
    settings = Settings.load()
    if order.asset_id not in {asset for _, asset, *_ in inventory(settings)}:
        raise HTTPException(422, "Activo desconocido")
    try:
        with connect(settings.dsn()) as db:
            return db.execute(
                """
                INSERT INTO production_orders(order_id,lot_id,asset_id,product_id,valid_from,valid_to,site_id)
                VALUES (%(order_id)s,%(lot_id)s,%(asset_id)s,%(product_id)s,
                        %(valid_from)s,%(valid_to)s,%(site_id)s) RETURNING *
            """,
                order.model_dump() | {"site_id": identity(settings)[0]},
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
    if settings.acquisition or not settings.lab_controls:
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
        if db.execute(
            "SELECT 1 FROM production_orders WHERE site_id=%s LIMIT 1", (SITE_ID,)
        ).fetchone():
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
