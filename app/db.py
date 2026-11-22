"""SQLite storage for the drain coordinator.

The database is the single source of truth.  In particular, an eviction
reservation is *derived* from ``eviction_tasks`` rows that are still in the
``dispatched`` state, so a process restart cannot lose the fact that a pod has
already been handed to a node agent.
"""

from __future__ import annotations

import os
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "drain.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS workloads (
    name         TEXT PRIMARY KEY,
    ready_floor  INTEGER NOT NULL CHECK (ready_floor >= 0),
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS nodes (
    name        TEXT PRIMARY KEY,
    draining    INTEGER NOT NULL DEFAULT 0 CHECK (draining IN (0, 1)),
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pods (
    uid         TEXT PRIMARY KEY,
    node        TEXT NOT NULL REFERENCES nodes(name),
    workload    TEXT NOT NULL REFERENCES workloads(name),
    ready       INTEGER NOT NULL CHECK (ready IN (0, 1)),
    state       TEXT NOT NULL CHECK (state IN ('active', 'evicted', 'removed')),
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    retired_at  TEXT
);
CREATE INDEX IF NOT EXISTS idx_pods_workload_state_ready
    ON pods(workload, state, ready);
CREATE INDEX IF NOT EXISTS idx_pods_node_state
    ON pods(node, state);

CREATE TABLE IF NOT EXISTS eviction_tasks (
    id              TEXT PRIMARY KEY,
    pod_uid         TEXT NOT NULL REFERENCES pods(uid),
    node            TEXT NOT NULL,
    workload        TEXT NOT NULL REFERENCES workloads(name),
    status          TEXT NOT NULL CHECK (status IN ('dispatched', 'succeeded', 'aborted')),
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    resolved_at     TEXT,
    result_note     TEXT
);
-- One pod can be handed to at most one unresolved eviction at a time.
CREATE UNIQUE INDEX IF NOT EXISTS uq_open_task_per_pod
    ON eviction_tasks(pod_uid) WHERE status = 'dispatched';
CREATE INDEX IF NOT EXISTS idx_tasks_status_workload
    ON eviction_tasks(status, workload);
CREATE INDEX IF NOT EXISTS idx_tasks_pod_created
    ON eviction_tasks(pod_uid, created_at);

CREATE TABLE IF NOT EXISTS report_events (
    report_id     TEXT PRIMARY KEY,
    kind          TEXT NOT NULL,
    target        TEXT NOT NULL,
    response_json TEXT NOT NULL,
    received_at   TEXT NOT NULL
);
"""


def db_path_from_env() -> Path:
    return Path(os.environ.get("DRAIN_DB_PATH", str(DEFAULT_DB_PATH)))


def busy_timeout_ms() -> int:
    return int(os.environ.get("DRAIN_BUSY_TIMEOUT_MS", "15000"))


def connect(path: str | Path) -> sqlite3.Connection:
    """Open a connection suitable for explicit BEGIN IMMEDIATE transactions."""
    conn = sqlite3.connect(
        str(path),
        timeout=busy_timeout_ms() / 1000.0,
        isolation_level=None,  # autocommit; transactions are explicit
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {busy_timeout_ms()}")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA synchronous = FULL")
    return conn


def init_db(path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Several uvicorn workers may run this at once on a fresh volume.  Setting
    # WAL mode can return SQLITE_BUSY even with a busy handler, so retry.
    last: sqlite3.OperationalError | None = None
    for _ in range(50):
        conn = connect(path)
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            # executescript() has its own COMMIT handling; the schema is
            # idempotent and safe to run on every start.
            conn.executescript(SCHEMA)
            return
        except sqlite3.OperationalError as exc:  # pragma: no cover - timing
            last = exc
            time.sleep(0.05)
        finally:
            conn.close()
    if last is not None:  # pragma: no cover - timing
        raise last


@contextmanager
def transaction(conn: sqlite3.Connection, *, immediate: bool = False) -> Iterator[sqlite3.Connection]:
    """Run a block in a SQLite transaction, rolling back on any exception.

    ``BEGIN IMMEDIATE`` takes the write lock up front, which is what makes
    concurrent claims serialize: a second claim cannot read the ready counts
    until the first claim has committed its reservations.
    """
    conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield conn
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    else:
        conn.execute("COMMIT")
