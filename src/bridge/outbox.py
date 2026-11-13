import random
import sqlite3
import time
from contextlib import closing
from pathlib import Path

from bridge.events import Event, now


class Outbox:
    def __init__(self, path: Path, capacity: int):
        self.path = path
        self.capacity = capacity

    def connect(self):
        return sqlite3.connect(self.path, timeout=2)

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self.connect()) as db:
            db.executescript("""
                PRAGMA journal_mode = WAL;
                CREATE TABLE IF NOT EXISTS pending (
                    event_id TEXT PRIMARY KEY, digest TEXT NOT NULL, body TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0, next_at REAL NOT NULL DEFAULT 0,
                    terminal TEXT
                );
                CREATE INDEX IF NOT EXISTS pending_due ON pending(next_at);
                CREATE TABLE IF NOT EXISTS counters (
                    name TEXT PRIMARY KEY, total INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS coverage (
                    id INTEGER PRIMARY KEY, started TEXT NOT NULL, ended TEXT,
                    skipped_slots INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS status (
                    key TEXT PRIMARY KEY, value TEXT NOT NULL
                );
            """)

    def has_capacity(self, batch: int) -> bool:
        with closing(self.connect()) as db:
            pending = db.execute(
                "SELECT count(*) FROM pending WHERE terminal IS NULL"
            ).fetchone()[0]
            return pending + batch <= self.capacity

    def bind_scope(self, scope: str):
        with closing(self.connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT value FROM status WHERE key='scope'").fetchone()
            if row and row[0] != scope:
                raise ValueError(
                    "Outbox belongs to another site/gateway; drain it with its original profile"
                )
            if (
                not row
                and scope != "aurora/v1/aurora-gw"
                and db.execute("SELECT 1 FROM pending LIMIT 1").fetchone()
            ):
                raise ValueError(
                    "Unbound legacy outbox contains events; use the original demo profile"
                )
            db.execute("INSERT OR IGNORE INTO status VALUES ('scope',?)", (scope,))

    def put(self, events: list[Event]) -> bool:
        rows = [(str(event.event_id), event.digest(), event.wire()) for event in events]
        with closing(self.connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            if (
                db.execute(
                    "SELECT count(*) FROM pending WHERE terminal IS NULL"
                ).fetchone()[0]
                + len(rows)
                > self.capacity
            ):
                return False
            db.executemany(
                "INSERT INTO pending(event_id,digest,body) VALUES (?,?,?)", rows
            )
            db.execute(
                """
                INSERT INTO counters VALUES ('enqueued',?)
                ON CONFLICT(name) DO UPDATE SET total=total+excluded.total
            """,
                (len(rows),),
            )
            return True

    def due(self, limit: int = 12) -> list[tuple[str, str]]:
        with closing(self.connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            rows = db.execute(
                """
                SELECT event_id, body, attempts FROM pending
                WHERE terminal IS NULL AND next_at <= ? ORDER BY next_at, rowid LIMIT ?
            """,
                (time.time(), limit),
            ).fetchall()
            for event_id, _, attempts in rows:
                delay = min(16.0, 2.0 * 2 ** min(attempts, 3)) * random.uniform(
                    0.8, 1.2
                )
                db.execute(
                    "UPDATE pending SET attempts=attempts+1,next_at=? WHERE event_id=?",
                    (time.time() + delay, event_id),
                )
            return [(event_id, body) for event_id, body, _ in rows]

    def acknowledge(self, event_id: str, digest: str, outcome: str) -> None:
        with closing(self.connect()) as db, db:
            if outcome == "stored":
                changed = db.execute(
                    "DELETE FROM pending WHERE event_id=? AND digest=? AND terminal IS NULL",
                    (event_id, digest),
                ).rowcount
            else:
                # Keep a bounded record of terminal failures without blocking further acquisition.
                changed = db.execute(
                    """
                    UPDATE pending SET terminal=? WHERE event_id=? AND digest=? AND terminal IS NULL
                """,
                    (outcome, event_id, digest),
                ).rowcount
                db.execute("""
                    DELETE FROM pending WHERE terminal IS NOT NULL AND rowid NOT IN (
                        SELECT rowid FROM pending WHERE terminal IS NOT NULL
                        ORDER BY rowid DESC LIMIT 1000
                    )
                """)
            if changed:
                db.execute(
                    "INSERT OR REPLACE INTO status VALUES ('last_ack',?)",
                    (now().isoformat(),),
                )
                db.execute(
                    """
                    INSERT INTO counters VALUES (?,1)
                    ON CONFLICT(name) DO UPDATE SET total=total+1
                """,
                    (outcome,),
                )

    def coverage(self, skipped: int) -> None:
        with closing(self.connect()) as db, db:
            stamp = now().isoformat()
            if skipped:
                if not db.execute(
                    """
                    UPDATE coverage SET skipped_slots=skipped_slots+? WHERE ended IS NULL
                """,
                    (skipped,),
                ).rowcount:
                    db.execute(
                        "INSERT INTO coverage(started,skipped_slots) VALUES (?,?)",
                        (
                            stamp,
                            skipped,
                        ),
                    )
            else:
                db.execute("UPDATE coverage SET ended=? WHERE ended IS NULL", (stamp,))

    def set_status(self, key: str, value: str) -> None:
        with closing(self.connect()) as db, db:
            db.execute("INSERT OR REPLACE INTO status VALUES (?,?)", (key, value))

    def status(self) -> dict:
        with closing(self.connect()) as db:
            return {
                "capacity": self.capacity,
                "oldest_pending": db.execute(
                    "SELECT min(json_extract(body,'$.observed_at')) FROM pending WHERE terminal IS NULL"
                ).fetchone()[0],
                "pending": db.execute(
                    "SELECT count(*) FROM pending WHERE terminal IS NULL"
                ).fetchone()[0],
                "pending_ids": [
                    row[0]
                    for row in db.execute(
                        "SELECT event_id FROM pending WHERE terminal IS NULL ORDER BY rowid LIMIT 100"
                    )
                ],
                "counters": dict(db.execute("SELECT name,total FROM counters")),
                "coverage": [
                    dict(zip(("started", "ended", "skipped_slots"), row, strict=True))
                    for row in db.execute(
                        "SELECT started,ended,skipped_slots FROM coverage ORDER BY id DESC LIMIT 10"
                    )
                ],
                "gateway": dict(db.execute("SELECT key,value FROM status")),
            }
