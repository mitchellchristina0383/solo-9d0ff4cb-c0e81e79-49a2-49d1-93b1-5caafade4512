"""Domain logic for the drain coordinator.

Design notes
------------

* **Reservations are derived, never stored as a subtractive counter.**  A pod
  that has been handed to a node agent is still ``state='active'`` but has an
  open (``dispatched``) eviction task.  The effective ready count for a
  workload is therefore::

      active pods with ready=1 and NO open eviction task

  Dispatched pods consequently count as unavailable even if they keep
  reporting ready, and the fact is recomputed from SQLite after a restart
  instead of living in process memory.

* **Claims serialize through ``BEGIN IMMEDIATE``.**  A claim reads the
  effective counts and inserts its reservations inside one write transaction,
  so two concurrent claims cannot both spend the same ready replica.

* **A task is bound to a specific pod uid.**  Pod uids are immutable, and a
  retired uid can never be re-registered, so a late report for an old task can
  never affect a replacement pod or a newer task.  Reporting on an
  already-settled task is a no-op acknowledged as such.

* **Reports are idempotent by optional ``report_id``** and stale reports are
  acknowledged without mutating state.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable

from . import db
from .models import (
    ApprovedEviction,
    ClaimResponse,
    EvictionResultIn,
    EvictionTaskOut,
    NodeOut,
    PodOut,
    PodUpsert,
    StatusOut,
    WaitingPod,
    WorkloadOut,
)

OPEN_TASK_REASON = "eviction_in_progress"
FLOOR_REASON = "ready_floor_limit"
LIMIT_REASON = "limit_reached"

ACTIVE = "active"
EVICTED = "evicted"
REMOVED = "removed"
DISPATCHED = "dispatched"


class Conflict(Exception):
    """Request conflicts with current state (HTTP 409)."""


class NotFound(Exception):
    """Referenced entity does not exist (HTTP 404)."""


def now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _connect() -> sqlite3.Connection:
    return db.connect(db.db_path_from_env())


# ---------------------------------------------------------------------------
# Row helpers


def _open_task_id(conn: sqlite3.Connection, pod_uid: str) -> str | None:
    row = conn.execute(
        "SELECT id FROM eviction_tasks WHERE pod_uid = ? AND status = 'dispatched'",
        (pod_uid,),
    ).fetchone()
    return row["id"] if row else None


def _pod_out(conn: sqlite3.Connection, row: sqlite3.Row) -> PodOut:
    open_id = _open_task_id(conn, row["uid"])
    effective = (
        row["state"] == ACTIVE and bool(row["ready"]) and open_id is None
    )
    return PodOut(
        uid=row["uid"],
        node=row["node"],
        workload=row["workload"],
        ready=bool(row["ready"]),
        state=row["state"],
        effective_ready=effective,
        open_task_id=open_id,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        retired_at=row["retired_at"],
    )


def _task_out(row: sqlite3.Row) -> EvictionTaskOut:
    return EvictionTaskOut(
        task_id=row["id"],
        pod_uid=row["pod_uid"],
        node=row["node"],
        workload=row["workload"],
        status=row["status"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        resolved_at=row["resolved_at"],
        result_note=row["result_note"],
    )


def _workload_stats(conn: sqlite3.Connection, names: Iterable[str]) -> list[WorkloadOut]:
    names = sorted(set(names))
    out: list[WorkloadOut] = []
    for name in names:
        wl = conn.execute(
            "SELECT * FROM workloads WHERE name = ?", (name,)
        ).fetchone()
        if wl is None:
            continue
        active_total = conn.execute(
            "SELECT COUNT(*) AS n FROM pods WHERE workload = ? AND state = 'active'",
            (name,),
        ).fetchone()["n"]
        active_ready = conn.execute(
            "SELECT COUNT(*) AS n FROM pods"
            " WHERE workload = ? AND state = 'active' AND ready = 1",
            (name,),
        ).fetchone()["n"]
        effective_ready = conn.execute(
            "SELECT COUNT(*) AS n FROM pods p"
            " WHERE p.workload = ? AND p.state = 'active' AND p.ready = 1"
            "   AND NOT EXISTS ("
            "     SELECT 1 FROM eviction_tasks t"
            "     WHERE t.pod_uid = p.uid AND t.status = 'dispatched'"
            "   )",
            (name,),
        ).fetchone()["n"]
        reserved = conn.execute(
            "SELECT COUNT(*) AS n FROM eviction_tasks"
            " WHERE workload = ? AND status = 'dispatched'",
            (name,),
        ).fetchone()["n"]
        evicted = conn.execute(
            "SELECT COUNT(*) AS n FROM pods WHERE workload = ? AND state = 'evicted'",
            (name,),
        ).fetchone()["n"]
        removed = conn.execute(
            "SELECT COUNT(*) AS n FROM pods WHERE workload = ? AND state = 'removed'",
            (name,),
        ).fetchone()["n"]
        out.append(
            WorkloadOut(
                name=name,
                ready_floor=wl["ready_floor"],
                active_total=active_total,
                active_ready=active_ready,
                reserved=reserved,
                effective_ready=effective_ready,
                evicted=evicted,
                removed=removed,
                created_at=wl["created_at"],
                updated_at=wl["updated_at"],
            )
        )
    return out


def _ensure_node(conn: sqlite3.Connection, name: str, *, draining: bool = False) -> sqlite3.Row:
    ts = now_iso()
    row = conn.execute("SELECT * FROM nodes WHERE name = ?", (name,)).fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO nodes(name, draining, created_at, updated_at)"
            " VALUES (?, ?, ?, ?)",
            (name, 1 if draining else 0, ts, ts),
        )
    elif draining and not row["draining"]:
        conn.execute(
            "UPDATE nodes SET draining = 1, updated_at = ? WHERE name = ?",
            (ts, name),
        )
    return conn.execute("SELECT * FROM nodes WHERE name = ?", (name,)).fetchone()


# ---------------------------------------------------------------------------
# Idempotency


def _replay_report(
    conn: sqlite3.Connection, report_id: str, kind: str, target: str
) -> dict[str, Any] | None:
    """Return the stored *decision* for a replayed report.

    Only the decision fields (``applied`` / ``note`` / ``ignored_reason``) are
    reused.  Entity state is deliberately left out: the caller refreshes it
    from the database so a replay never reports stale counters.
    """
    row = conn.execute(
        "SELECT * FROM report_events WHERE report_id = ?", (report_id,)
    ).fetchone()
    if row is None:
        return None
    if row["kind"] != kind or row["target"] != target:
        raise Conflict(
            f"report_id {report_id!r} was already used for {row['kind']}:{row['target']}"
        )
    stored = json.loads(row["response_json"])
    return {
        "applied": stored["applied"],
        "duplicate": True,
        "note": stored.get("note"),
        "ignored_reason": stored.get("ignored_reason"),
    }


def _store_report(
    conn: sqlite3.Connection,
    report_id: str | None,
    kind: str,
    target: str,
    payload: dict[str, Any],
) -> None:
    if report_id is None:
        return
    conn.execute(
        "INSERT INTO report_events(report_id, kind, target, response_json, received_at)"
        " VALUES (?, ?, ?, ?, ?)",
        (report_id, kind, target, json.dumps(payload), now_iso()),
    )


# ---------------------------------------------------------------------------
# Workloads / nodes


def upsert_workload(name: str, ready_floor: int) -> WorkloadOut:
    ts = now_iso()
    conn = _connect()
    try:
        with db.transaction(conn, immediate=True):
            if conn.execute("SELECT 1 FROM workloads WHERE name = ?", (name,)).fetchone():
                conn.execute(
                    "UPDATE workloads SET ready_floor = ?, updated_at = ? WHERE name = ?",
                    (ready_floor, ts, name),
                )
            else:
                conn.execute(
                    "INSERT INTO workloads(name, ready_floor, created_at, updated_at)"
                    " VALUES (?, ?, ?, ?)",
                    (name, ready_floor, ts, ts),
                )
            return _workload_stats(conn, [name])[0]
    finally:
        conn.close()


def get_workload(name: str) -> WorkloadOut:
    conn = _connect()
    try:
        stats = _workload_stats(conn, [name])
        if not stats:
            raise NotFound(f"workload {name!r} not found")
        return stats[0]
    finally:
        conn.close()


def list_workloads() -> list[WorkloadOut]:
    conn = _connect()
    try:
        names = [
            r["name"]
            for r in conn.execute("SELECT name FROM workloads ORDER BY name").fetchall()
        ]
        return _workload_stats(conn, names)
    finally:
        conn.close()


def upsert_node(name: str, draining: bool | None = None) -> NodeOut:
    conn = _connect()
    try:
        with db.transaction(conn, immediate=True):
            row = _ensure_node(conn, name, draining=bool(draining))
            if draining is not None and bool(row["draining"]) != draining:
                ts = now_iso()
                conn.execute(
                    "UPDATE nodes SET draining = ?, updated_at = ? WHERE name = ?",
                    (1 if draining else 0, ts, name),
                )
            row = conn.execute("SELECT * FROM nodes WHERE name = ?", (name,)).fetchone()
            count = conn.execute(
                "SELECT COUNT(*) AS n FROM pods WHERE node = ? AND state = 'active'",
                (name,),
            ).fetchone()["n"]
            return NodeOut(
                name=row["name"],
                draining=bool(row["draining"]),
                pod_count=count,
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
    finally:
        conn.close()


def list_nodes() -> list[NodeOut]:
    conn = _connect()
    try:
        out = []
        for row in conn.execute("SELECT * FROM nodes ORDER BY name"):
            count = conn.execute(
                "SELECT COUNT(*) AS n FROM pods WHERE node = ? AND state = 'active'",
                (row["name"],),
            ).fetchone()["n"]
            out.append(
                NodeOut(
                    name=row["name"],
                    draining=bool(row["draining"]),
                    pod_count=count,
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
            )
        return out
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Pods


def upsert_pod(body: PodUpsert) -> PodOut:
    conn = _connect()
    try:
        with db.transaction(conn, immediate=True):
            workload = conn.execute(
                "SELECT * FROM workloads WHERE name = ?", (body.workload,)
            ).fetchone()
            if workload is None:
                raise NotFound(
                    f"workload {body.workload!r} is not registered; register its ready_floor first"
                )
            _ensure_node(conn, body.node)
            ts = now_iso()
            existing = conn.execute(
                "SELECT * FROM pods WHERE uid = ?", (body.uid,)
            ).fetchone()
            if existing is None:
                conn.execute(
                    "INSERT INTO pods(uid, node, workload, ready, state,"
                    " created_at, updated_at)"
                    " VALUES (?, ?, ?, ?, 'active', ?, ?)",
                    (body.uid, body.node, body.workload, int(body.ready), ts, ts),
                )
            elif existing["state"] == ACTIVE:
                if existing["workload"] != body.workload:
                    raise Conflict(
                        f"pod {body.uid!r} already belongs to workload {existing['workload']!r}"
                    )
                if existing["node"] != body.node:
                    # Uids are immutable and a pod cannot change node without
                    # being recreated; a "move" is a new pod with a new uid.
                    raise Conflict(
                        f"pod {body.uid!r} is registered on node"
                        f" {existing['node']!r}, not {body.node!r}"
                    )
                # Same uid means the same pod incarnation.  If an eviction is
                # outstanding it stays outstanding and the pod stays counted
                # as unavailable until it is settled.
                conn.execute(
                    "UPDATE pods SET ready = ?, updated_at = ? WHERE uid = ?",
                    (int(body.ready), ts, body.uid),
                )
            else:
                # A pod uid is immutable in Kubernetes.  A uid that this
                # coordinator has already retired is terminal; resurrecting it
                # via a routine inventory re-registration would make a removed
                # pod count again.  Replacements must use a new uid.
                raise Conflict(
                    f"pod {body.uid!r} is {existing['state']};"
                    " register the replacement with a new uid"
                )
            row = conn.execute("SELECT * FROM pods WHERE uid = ?", (body.uid,)).fetchone()
            return _pod_out(conn, row)
    finally:
        conn.close()


def get_pod(uid: str) -> PodOut:
    conn = _connect()
    try:
        row = conn.execute("SELECT * FROM pods WHERE uid = ?", (uid,)).fetchone()
        if row is None:
            raise NotFound(f"pod {uid!r} not found")
        return _pod_out(conn, row)
    finally:
        conn.close()


def list_pods(
    *, node: str | None = None, workload: str | None = None, state: str | None = None
) -> list[PodOut]:
    conn = _connect()
    try:
        sql = "SELECT * FROM pods"
        clauses: list[str] = []
        params: list[Any] = []
        for column, value in (("node", node), ("workload", workload), ("state", state)):
            if value is not None:
                clauses.append(f"{column} = ?")
                params.append(value)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY workload, created_at, uid"
        return [_pod_out(conn, row) for row in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


def remove_pod(uid: str) -> PodOut:
    """Explicitly retire a pod (e.g. it was deleted out of band)."""
    conn = _connect()
    try:
        with db.transaction(conn, immediate=True):
            row = conn.execute("SELECT * FROM pods WHERE uid = ?", (uid,)).fetchone()
            if row is None:
                raise NotFound(f"pod {uid!r} not found")
            if row["state"] != REMOVED:
                ts = now_iso()
                conn.execute(
                    "UPDATE eviction_tasks SET status = 'aborted', updated_at = ?,"
                    " resolved_at = ?, result_note = 'pod_removed'"
                    " WHERE pod_uid = ? AND status = 'dispatched'",
                    (ts, ts, uid),
                )
                conn.execute(
                    "UPDATE pods SET state = 'removed', ready = 0, updated_at = ?,"
                    " retired_at = ? WHERE uid = ?",
                    (ts, ts, uid),
                )
            row = conn.execute("SELECT * FROM pods WHERE uid = ?", (uid,)).fetchone()
            return _pod_out(conn, row)
    finally:
        conn.close()


def report_readiness(uid: str, ready: bool, report_id: str | None) -> dict[str, Any]:
    conn = _connect()
    try:
        with db.transaction(conn, immediate=True):
            row = conn.execute("SELECT * FROM pods WHERE uid = ?", (uid,)).fetchone()
            if row is None:
                raise NotFound(f"pod {uid!r} not found")
            if report_id is not None:
                replay = _replay_report(conn, report_id, "readiness", uid)
                if replay is not None:
                    # Current pod state, stored decision.
                    replay["pod"] = _pod_out(conn, row).model_dump()
                    return replay
            if row["state"] != ACTIVE:
                # A removed or already-evicted pod must never be brought back
                # into the ready count by a stale report.
                payload = {
                    "pod": _pod_out(conn, row).model_dump(),
                    "applied": False,
                    "duplicate": False,
                    "note": "pod_not_active",
                }
                _store_report(conn, report_id, "readiness", uid, payload)
                return payload
            conn.execute(
                "UPDATE pods SET ready = ?, updated_at = ? WHERE uid = ?",
                (int(ready), now_iso(), uid),
            )
            row = conn.execute("SELECT * FROM pods WHERE uid = ?", (uid,)).fetchone()
            payload = {
                "pod": _pod_out(conn, row).model_dump(),
                "applied": True,
                "duplicate": False,
                "note": None,
            }
            _store_report(conn, report_id, "readiness", uid, payload)
            return payload
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Claims


def claim(node: str, limit: int) -> ClaimResponse:
    conn = _connect()
    try:
        with db.transaction(conn, immediate=True):
            _ensure_node(conn, node, draining=True)
            candidates = conn.execute(
                "SELECT * FROM pods WHERE node = ? AND state = 'active'"
                " ORDER BY workload, ready ASC, created_at, uid",
                (node,),
            ).fetchall()

            effective: dict[str, int] = {}
            floors: dict[str, int] = {}
            for row in candidates:
                wl = row["workload"]
                if wl not in effective:
                    stats = conn.execute(
                        "SELECT ready_floor FROM workloads WHERE name = ?", (wl,)
                    ).fetchone()
                    floors[wl] = stats["ready_floor"] if stats else 0
                    effective[wl] = conn.execute(
                        "SELECT COUNT(*) AS n FROM pods p"
                        " WHERE p.workload = ? AND p.state = 'active' AND p.ready = 1"
                        "   AND NOT EXISTS ("
                        "     SELECT 1 FROM eviction_tasks t"
                        "     WHERE t.pod_uid = p.uid AND t.status = 'dispatched'"
                        "   )",
                        (wl,),
                    ).fetchone()["n"]

            ts = now_iso()
            open_by_pod = {
                r["pod_uid"]: r["id"]
                for r in conn.execute(
                    "SELECT id, pod_uid FROM eviction_tasks WHERE status = 'dispatched'"
                ).fetchall()
            }
            approved: list[ApprovedEviction] = []
            waiting: list[WaitingPod] = []
            for row in candidates:
                open_id = open_by_pod.get(row["uid"])
                if open_id is not None:
                    waiting.append(
                        WaitingPod(
                            pod_uid=row["uid"],
                            workload=row["workload"],
                            reason=OPEN_TASK_REASON,
                            detail={"task_id": open_id},
                            open_task_id=open_id,
                        )
                    )
                    continue
                if len(approved) >= limit:
                    waiting.append(
                        WaitingPod(
                            pod_uid=row["uid"],
                            workload=row["workload"],
                            reason=LIMIT_REASON,
                            detail={"limit": limit},
                        )
                    )
                    continue
                if row["ready"]:
                    # Spending one ready replica must still leave the floor.
                    if effective[row["workload"]] - 1 < floors[row["workload"]]:
                        waiting.append(
                            WaitingPod(
                                pod_uid=row["uid"],
                                workload=row["workload"],
                                reason=FLOOR_REASON,
                                detail={
                                    "effective_ready": effective[row["workload"]],
                                    "ready_floor": floors[row["workload"]],
                                },
                            )
                        )
                        continue
                    effective[row["workload"]] -= 1
                # A not-ready pod consumes no ready budget: evicting it cannot
                # push the workload below its floor.

                task_id = str(uuid.uuid4())
                conn.execute(
                    "INSERT INTO eviction_tasks(id, pod_uid, node, workload,"
                    " status, created_at, updated_at)"
                    " VALUES (?, ?, ?, ?, 'dispatched', ?, ?)",
                    (task_id, row["uid"], node, row["workload"], ts, ts),
                )
                approved.append(
                    ApprovedEviction(
                        task_id=task_id,
                        pod_uid=row["uid"],
                        node=node,
                        workload=row["workload"],
                        dispatched_at=ts,
                    )
                )

            stats = _workload_stats(conn, floors.keys())
            return ClaimResponse(
                node=node,
                approved=approved,
                waiting=waiting,
                workloads=stats,
            )
    finally:
        conn.close()


def record_eviction_result(
    task_id: str, body: EvictionResultIn
) -> dict[str, Any]:
    conn = _connect()
    try:
        with db.transaction(conn, immediate=True):
            task = conn.execute(
                "SELECT * FROM eviction_tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if task is None:
                raise NotFound(f"eviction task {task_id!r} not found")
            if body.report_id is not None:
                replay = _replay_report(conn, body.report_id, "eviction", task_id)
                if replay is not None:
                    # Current task and workload state, stored decision.
                    replay["task"] = _task_out(task).model_dump()
                    replay["workloads"] = [
                        w.model_dump()
                        for w in _workload_stats(conn, [task["workload"]])
                    ]
                    return replay

            workloads = _workload_stats(conn, [task["workload"]])
            if task["status"] != DISPATCHED:
                # Old report for an already-settled task: acknowledge, mutate
                # nothing (in particular, never touch a newer task/pod).
                payload = {
                    "task": _task_out(task).model_dump(),
                    "applied": False,
                    "duplicate": False,
                    "ignored_reason": "task_already_resolved",
                    "workloads": [w.model_dump() for w in workloads],
                }
                _store_report(conn, body.report_id, "eviction", task_id, payload)
                return payload

            pod = conn.execute(
                "SELECT * FROM pods WHERE uid = ?", (task["pod_uid"],)
            ).fetchone()
            ts = now_iso()
            new_status = "succeeded" if body.outcome == "succeeded" else "aborted"
            conn.execute(
                "UPDATE eviction_tasks SET status = ?, updated_at = ?,"
                " resolved_at = ?, result_note = ? WHERE id = ?",
                (new_status, ts, ts, body.note, task_id),
            )
            if pod is not None:
                if body.outcome == "succeeded":
                    # The pod is gone.  Only a ready *replacement* (a new
                    # incarnation) can add ready capacity back.
                    if pod["state"] == ACTIVE:
                        conn.execute(
                            "UPDATE pods SET state = 'evicted', ready = 0,"
                            " updated_at = ?, retired_at = ? WHERE uid = ?",
                            (ts, ts, pod["uid"]),
                        )
                    elif pod["state"] != REMOVED:
                        # Already retired; leave the terminal state alone.
                        conn.execute(
                            "UPDATE pods SET updated_at = ? WHERE uid = ?",
                            (ts, pod["uid"]),
                        )
                    # REMOVED pods stay removed: a late success must not
                    # resurrect them.
                else:
                    # Aborted: the pod was never evicted.  It returns to the
                    # count according to its *actual* current readiness
                    # (latest readiness report), which may have changed while
                    # the eviction was outstanding.
                    if pod["state"] == REMOVED:
                        pass  # stays removed
                    # active pod: closing the task is all that is needed;
                    # effective readiness is derived from ready + open tasks.

            task = conn.execute(
                "SELECT * FROM eviction_tasks WHERE id = ?", (task_id,)
            ).fetchone()
            payload = {
                "task": _task_out(task).model_dump(),
                "applied": True,
                "duplicate": False,
                "ignored_reason": None,
                "workloads": [
                    w.model_dump() for w in _workload_stats(conn, [task["workload"]])
                ],
            }
            _store_report(conn, body.report_id, "eviction", task_id, payload)
            return payload
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Introspection


def list_tasks(
    *, status: str | None = None, node: str | None = None, workload: str | None = None
) -> list[EvictionTaskOut]:
    conn = _connect()
    try:
        sql = "SELECT * FROM eviction_tasks"
        clauses: list[str] = []
        params: list[Any] = []
        for column, value in (("status", status), ("node", node), ("workload", workload)):
            if value is not None:
                clauses.append(f"{column} = ?")
                params.append(value)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY created_at, id"
        return [_task_out(r) for r in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


def get_task(task_id: str) -> EvictionTaskOut | None:
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT * FROM eviction_tasks WHERE id = ?", (task_id,)
        ).fetchone()
        return _task_out(row) if row else None
    finally:
        conn.close()


def status() -> StatusOut:
    conn = _connect()
    try:
        names = [
            r["name"]
            for r in conn.execute("SELECT name FROM workloads ORDER BY name").fetchall()
        ]
        open_tasks = conn.execute(
            "SELECT COUNT(*) AS n FROM eviction_tasks WHERE status = 'dispatched'"
        ).fetchone()["n"]
        return StatusOut(
            workloads=_workload_stats(conn, names),
            nodes=list_nodes(),
            open_tasks=open_tasks,
        )
    finally:
        conn.close()


def health() -> dict[str, str]:
    conn = _connect()
    try:
        conn.execute("SELECT 1").fetchone()
        return {"status": "ok", "database": str(db.db_path_from_env())}
    finally:
        conn.close()
