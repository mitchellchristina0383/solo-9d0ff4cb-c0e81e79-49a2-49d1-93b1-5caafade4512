"""FastAPI application for the multi-node drain coordinator.

Interactive API docs are served at ``/docs`` (Swagger UI) and ``/redoc``.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, status

from . import __version__, db, service
from .models import (
    ClaimRequest,
    ClaimResponse,
    EvictionResultIn,
    EvictionResultOut,
    EvictionTaskOut,
    HealthOut,
    InventoryOut,
    InventorySubmit,
    NodeOut,
    PodOut,
    PodUpsert,
    ReadinessAck,
    ReadinessReport,
    StatusOut,
    WorkloadOut,
    WorkloadUpsert,
)

DESCRIPTION = """
Coordinator for draining several Kubernetes-style nodes at once while every
workload keeps enough ready replicas.

**Safety model**

* Each workload has a `ready_floor`. A pod whose eviction has been handed out
  ("dispatched") counts as **unavailable**, even if it still reports ready.
* Claims take a write lock and recompute effective readiness inside the
  transaction, so concurrent claims can never jointly spend more ready
  replicas than `ready_floor` allows.
* Reservations live in SQLite, so they survive a restart: unresolved evictions
  keep counting as unavailable until a node report settles them.
* An eviction that succeeds runs the pod down; only a ready *replacement* pod
  adds capacity back. An aborted eviction returns the original pod to the
  count according to its actual current readiness.
* Reports are fenced by an optional idempotency `report_id` and by task
  identity. A report for an already-settled task is acknowledged but changes
  nothing, and can never touch a newer task or resurrect a removed pod.
* **Node inventory reconciliation** lets a reconnecting agent prove which pods
  still exist. Issuing an inventory freezes the node's active-pod scope; the
  agent then submits the complete uid list it observes. Scope members still
  active but missing are retired with their in-flight evictions settled by the
  manual-removal rule, all in one atomic transaction with the receipt. A later
  inventory supersedes older open ones, replays return the original receipt,
  and conflicting re-submissions get a 409.
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db(db.db_path_from_env())
    yield


app = FastAPI(
    title="Drain Coordinator",
    version=__version__,
    description=DESCRIPTION,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)


def _http(exc: Exception) -> HTTPException:
    if isinstance(exc, service.NotFound):
        return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    if isinstance(exc, service.Conflict):
        return HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
    return HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Meta


@app.get("/", include_in_schema=False)
def root() -> dict[str, str]:
    return {
        "service": "drain-coordinator",
        "version": __version__,
        "docs": "/docs",
    }


@app.get("/healthz", response_model=HealthOut, tags=["meta"])
def healthz() -> HealthOut:
    try:
        return HealthOut(**service.health())
    except Exception as exc:  # pragma: no cover - defensive
        raise HTTPException(status_code=503, detail=str(exc))


@app.get("/status", response_model=StatusOut, tags=["meta"])
def get_status() -> StatusOut:
    return service.status()


# ---------------------------------------------------------------------------
# Workloads


@app.put(
    "/workloads/{name}",
    response_model=WorkloadOut,
    tags=["workloads"],
    summary="Register a workload or change its ready floor",
)
def put_workload(name: str, body: WorkloadUpsert) -> WorkloadOut:
    try:
        return service.upsert_workload(name, body.ready_floor)
    except Exception as exc:
        raise _http(exc)


@app.get("/workloads", response_model=list[WorkloadOut], tags=["workloads"])
def get_workloads() -> list[WorkloadOut]:
    return service.list_workloads()


@app.get("/workloads/{name}", response_model=WorkloadOut, tags=["workloads"])
def get_workload(name: str) -> WorkloadOut:
    try:
        return service.get_workload(name)
    except Exception as exc:
        raise _http(exc)


# ---------------------------------------------------------------------------
# Nodes


@app.put(
    "/nodes/{name}",
    response_model=NodeOut,
    tags=["nodes"],
    summary="Register a node (optionally mark it as draining)",
)
def put_node(name: str, draining: bool | None = Query(default=None)) -> NodeOut:
    try:
        return service.upsert_node(name, draining)
    except Exception as exc:
        raise _http(exc)


@app.get("/nodes", response_model=list[NodeOut], tags=["nodes"])
def get_nodes() -> list[NodeOut]:
    return service.list_nodes()


# ---------------------------------------------------------------------------
# Node inventory reconciliation


@app.post(
    "/nodes/{name}/inventories",
    response_model=InventoryOut,
    tags=["inventories"],
    status_code=status.HTTP_201_CREATED,
    summary="Issue an inventory id for a reconnecting node",
    description=(
        "Freezes the inventory scope: the uids active on the node at issue "
        "time. The node agent then submits the complete uid list it actually "
        "observes via the submit endpoint. Inventory ids are per-node and "
        "monotonic; completing a later inventory supersedes older open ones."
    ),
)
def post_inventory(name: str) -> InventoryOut:
    try:
        return service.issue_inventory(name)
    except Exception as exc:
        raise _http(exc)


@app.post(
    "/nodes/{name}/inventories/{inventory_id}/submit",
    response_model=InventoryOut,
    tags=["inventories"],
    summary="Submit the node's observed uid list for an issued inventory",
    description=(
        "Retires scope members still active but missing from `uids`, settling "
        "their in-flight evictions by the manual-removal rule; pods registered "
        "after issue time are retained. Retirement, task settlement and the "
        "receipt commit atomically. Replaying the same list returns the "
        "original receipt (`duplicate: true`); a different list on a completed "
        "inventory is a 409, and a superseded inventory is rejected."
    ),
)
def post_inventory_submit(name: str, inventory_id: str, body: InventorySubmit) -> InventoryOut:
    try:
        inv = service.get_inventory(inventory_id)
    except service.NotFound:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"inventory {inventory_id!r} not found",
        )
    if inv.node != name:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"inventory {inventory_id!r} does not belong to node {name!r}",
        )
    try:
        return service.submit_inventory(inventory_id, body.uids)
    except Exception as exc:
        raise _http(exc)


@app.get(
    "/nodes/{name}/inventories",
    response_model=list[InventoryOut],
    tags=["inventories"],
    summary="List inventories issued for a node (newest first)",
)
def get_node_inventories(name: str) -> list[InventoryOut]:
    return service.list_inventories(node=name)


@app.get(
    "/inventories/{inventory_id}",
    response_model=InventoryOut,
    tags=["inventories"],
    summary="Fetch one inventory and (once completed) its receipt",
)
def get_inventory(inventory_id: str) -> InventoryOut:
    try:
        return service.get_inventory(inventory_id)
    except Exception as exc:
        raise _http(exc)


# ---------------------------------------------------------------------------
# Pods


@app.put(
    "/pods/{uid}",
    response_model=PodOut,
    tags=["pods"],
    summary="Register or update a pod",
)
def put_pod(uid: str, body: PodUpsert) -> PodOut:
    if body.uid != uid:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="pod uid in path and body must match",
        )
    try:
        return service.upsert_pod(body)
    except Exception as exc:
        raise _http(exc)


@app.get("/pods", response_model=list[PodOut], tags=["pods"])
def get_pods(
    node: str | None = None,
    workload: str | None = None,
    state: str | None = Query(default=None, pattern="^(active|evicted|removed)$"),
) -> list[PodOut]:
    return service.list_pods(node=node, workload=workload, state=state)


@app.get("/pods/{uid}", response_model=PodOut, tags=["pods"])
def get_pod(uid: str) -> PodOut:
    try:
        return service.get_pod(uid)
    except Exception as exc:
        raise _http(exc)


@app.delete(
    "/pods/{uid}",
    response_model=PodOut,
    tags=["pods"],
    summary="Retire a pod that disappeared out of band",
)
def delete_pod(uid: str) -> PodOut:
    try:
        return service.remove_pod(uid)
    except Exception as exc:
        raise _http(exc)


@app.post(
    "/pods/{uid}/readiness",
    response_model=ReadinessAck,
    tags=["pods"],
    summary="Report a pod's readiness",
)
def post_readiness(uid: str, body: ReadinessReport) -> ReadinessAck:
    try:
        return ReadinessAck(
            **service.report_readiness(
                uid,
                ready=body.ready,
                report_id=body.report_id,
            )
        )
    except Exception as exc:
        raise _http(exc)


# ---------------------------------------------------------------------------
# Eviction lifecycle


@app.post(
    "/drain/claims",
    response_model=ClaimResponse,
    tags=["drain"],
    summary="Claim eviction tasks for a node",
    description=(
        "Returns the pods this node is approved to evict plus the pods that "
        "must wait and why. The approval is durable and counts against the "
        "workload's ready floor until an eviction result is reported."
    ),
)
def post_claim(body: ClaimRequest) -> ClaimResponse:
    try:
        return service.claim(body.node, body.limit)
    except Exception as exc:
        raise _http(exc)


@app.post(
    "/drain/tasks/{task_id}/result",
    response_model=EvictionResultOut,
    tags=["drain"],
    summary="Report the outcome of an eviction",
    description=(
        "`succeeded` retires the pod: the workload loses that ready replica "
        "until a ready replacement is registered. `aborted` restores the pod "
        "to the count according to its actual current readiness. Stale or "
        "replayed reports are acknowledged without changing state."
    ),
)
def post_eviction_result(task_id: str, body: EvictionResultIn) -> EvictionResultOut:
    try:
        return EvictionResultOut(**service.record_eviction_result(task_id, body))
    except Exception as exc:
        raise _http(exc)


@app.get(
    "/drain/tasks",
    response_model=list[EvictionTaskOut],
    tags=["drain"],
    summary="List eviction tasks",
)
def get_tasks(
    status_filter: str | None = Query(
        default=None, alias="status", pattern="^(dispatched|succeeded|aborted)$"
    ),
    node: str | None = None,
    workload: str | None = None,
) -> list[EvictionTaskOut]:
    return service.list_tasks(status=status_filter, node=node, workload=workload)


@app.get(
    "/drain/tasks/{task_id}",
    response_model=EvictionTaskOut,
    tags=["drain"],
)
def get_task(task_id: str) -> EvictionTaskOut:
    task = service.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail=f"eviction task {task_id!r} not found")
    return task


def main() -> None:  # pragma: no cover - container entrypoint
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=os.environ.get("DRAIN_HOST", "0.0.0.0"),
        port=int(os.environ.get("DRAIN_PORT", "8000")),
        workers=int(os.environ.get("DRAIN_WORKERS", "2")),
        log_level=os.environ.get("DRAIN_LOG_LEVEL", "info"),
    )


if __name__ == "__main__":  # pragma: no cover
    main()
