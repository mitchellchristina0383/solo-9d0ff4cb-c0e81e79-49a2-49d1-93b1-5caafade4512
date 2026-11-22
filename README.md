# Drain Coordinator

A small FastAPI + SQLite service that lets you drain **several nodes at once**
while every workload keeps a protected number of ready replicas.

A node agent asks the coordinator for eviction work ("claims" tasks). The
coordinator hands out only as many evictions as the workload's ready floor
allows, and returns the rest with the reason they must wait. Every dispatch is
recorded in SQLite before it is returned, so it keeps counting against the floor
across restarts until the node reports the outcome.

Interactive documentation: **http://localhost:8000/docs**

## Quick start (Docker Compose)

```bash
docker compose up --build -d
open http://localhost:8000/docs        # Swagger UI
curl -s localhost:8000/healthz
```

Data lives in the named volume `drain-data`, so `docker compose restart` (or
`down` without `-v`) preserves registrations and unresolved evictions. To
simulate a restart recovery test:

```bash
# claim some tasks first, then:
docker compose restart drain-api
curl -s localhost:8000/workloads/web    # reserved count is unchanged
```

Stop everything:

```bash
docker compose down          # keep data
docker compose down -v       # also drop the database
```

## Concepts

| Entity | Meaning |
| --- | --- |
| **workload** | A named group of pods with a `ready_floor` (minimum ready replicas that must stay up). |
| **node** | A machine being drained. Registering a node marks it draining. |
| **pod** | `uid` (immutable), `node`, `workload`, `ready`. Uses the Kubernetes convention that pod uids never change. |
| **eviction task** | A durable reservation: `dispatched` → `succeeded` \| `aborted`. |

Readiness as the coordinator sees it:

```
effective_ready(workload) = active pods with ready=1 and no open eviction task
```

A dispatched pod is **counted as unavailable even if it still reports ready**.
This is recomputed from the database, so it holds after a crash or restart.

## Workflow

1. **Register** the workload (and its floor) and its pods.
   ```bash
   curl -X PUT localhost:8000/workloads/web -H 'content-type: application/json' \
        -d '{"ready_floor": 2}'
   curl -X PUT localhost:8000/pods/web-0 -H 'content-type: application/json' \
        -d '{"uid":"web-0","node":"node-a","workload":"web","ready":true}'
   ```
2. **Claim** work for a node (also usable from `/docs`):
   ```bash
   curl -X POST localhost:8000/drain/claims -H 'content-type: application/json' \
        -d '{"node":"node-a","limit":10}'
   ```
   Response:
   ```json
   {
     "node": "node-a",
     "approved": [
       {"task_id": "…", "pod_uid": "web-0", "node": "node-a",
        "workload": "web", "dispatched_at": "…"}
     ],
     "waiting": [
       {"pod_uid": "web-2", "workload": "web", "reason": "ready_floor_limit",
        "detail": {"effective_ready": 2, "ready_floor": 2}}
     ],
     "workloads": [ … ]
   }
   ```
   Waiting reasons: `eviction_in_progress`, `ready_floor_limit`, `limit_reached`.
3. **Report the outcome** of each task:
   ```bash
   curl -X POST localhost:8000/drain/tasks/<task_id>/result \
        -H 'content-type: application/json' \
        -d '{"outcome":"succeeded","report_id":"node-a/web-0/1"}'
   ```
   * `succeeded` — the pod is retired. Capacity only comes back when a **ready
     replacement pod with a new uid** is registered.
   * `aborted` — the pod was not evicted. It returns to the count according to
     its **actual current readiness** (which may have changed while the
     eviction was outstanding).
   * Pass `report_id` to make retries idempotent.

## Safety rules enforced

* **Concurrent claims cannot overspend.** A claim runs in a `BEGIN IMMEDIATE`
  SQLite transaction; it recomputes effective readiness and writes its
  reservations before committing, so parallel claims serialize on the write
  lock. The sum of approved evictions for ready pods never takes a workload
  below its floor, across nodes and across processes.
* **Dispatched = unavailable**, even if the pod keeps reporting ready, and
  even after a restart (the reservation is a row, not memory).
* **Success waits for a replacement.** The evicted pod is no longer counted;
  a new pod counts only once it is ready.
* **Abort restores reality.** The original pod is counted per its latest
  readiness report, not per the state at dispatch time.
* **Stale reports are inert.** A report on an already-settled task is
  acknowledged (`applied: false`) and cannot change a newer task, retire a
  replacement, or resurrect a removed pod. Readiness reports on removed or
  evicted pods are ignored.
* **Uids are immutable.** A pod uid cannot move nodes or be re-registered after
  retirement (409); replacements use a new uid.

## API

| Method | Path | Purpose |
| --- | --- | --- |
| `PUT` | `/workloads/{name}` | Register/update a workload floor |
| `GET` | `/workloads`, `/workloads/{name}` | Readiness accounting |
| `PUT` | `/nodes/{name}?draining=` | Register a node |
| `GET` | `/nodes` | List nodes |
| `PUT` | `/pods/{uid}` | Register/update a pod |
| `GET` | `/pods?node=&workload=&state=` | List pods |
| `GET` | `/pods/{uid}`, `DELETE /pods/{uid}` | Inspect / retire a pod |
| `POST` | `/pods/{uid}/readiness` | Report readiness (optional `report_id`) |
| `POST` | `/drain/claims` | Claim evictions for a node |
| `POST` | `/drain/tasks/{id}/result` | Report `succeeded` / `aborted` |
| `GET` | `/drain/tasks?status=&node=&workload=`, `/drain/tasks/{id}` | Inspect tasks |
| `GET` | `/status` | Workloads + nodes + open task count |
| `GET` | `/healthz` | Liveness/DB check |

## Configuration

| Env var | Default | Meaning |
| --- | --- | --- |
| `DRAIN_DB_PATH` | `./data/drain.db` | SQLite file |
| `DRAIN_BUSY_TIMEOUT_MS` | `15000` | SQLite busy timeout |
| `DRAIN_HOST` / `DRAIN_PORT` | `0.0.0.0` / `8000` | Bind address (`python -m app.main`) |
| `DRAIN_WORKERS` | `2` | uvicorn worker processes (`python -m app.main`) |

SQLite runs in WAL mode with `synchronous=FULL`. This is a single-writer design
appropriate for a coordinator; see **Remaining work** for the Postgres path.

## Development / tests

```bash
python -m pip install -r requirements.txt -r requirements-dev.txt
python -m pytest -q
```

See [`SELFTEST.md`](SELFTEST.md) for the verification that was run, including
concurrent dispatch and restart recovery.
