# Self-test record

Environment used for verification: Linux, Python 3.11.2, uvicorn (4 workers for
the concurrency/restart runs, 2 for the container-entrypoint run), SQLite in
WAL mode. Docker is **not installed** in this sandbox, so the Compose stack
itself was not executed here; everything else was run against a real HTTP
server with the pinned dependency versions from `requirements.txt`
(FastAPI 0.115.14, Pydantic 2.11.7, uvicorn 0.34.3).

## 1. Test suite

```
$ python -m pytest -q
23 passed
```

Coverage of the requested behaviours:

| Requirement | Test |
| --- | --- |
| Approved evictions never drop a workload below its ready floor | `test_claim_never_takes_workload_below_floor` |
| Concurrent claims (different nodes) share one budget | `test_concurrent_claims_across_nodes_share_one_budget` |
| Waiting pods are returned with a reason | `test_limit_is_honoured_and_rest_reported_as_waiting` |
| Not-ready pods do not consume ready budget | `test_not_ready_pods_do_not_consume_the_ready_budget` |
| Dispatched pods count unavailable even while reporting ready | `test_dispatched_pod_counts_unavailable_even_if_it_reports_ready` |
| Success needs a ready replacement to restore capacity | `test_floor_blocks_claims_until_a_replacement_is_ready`, `test_success_does_not_count_until_replacement_reports_ready` |
| Abort restores actual readiness (not stale state) | `test_aborted_eviction_restores_actual_readiness`, `test_aborted_eviction_restores_ready_count_when_still_ready` |
| Stale reports cannot touch later tasks | `test_stale_report_on_settled_task_changes_nothing`, `test_late_report_cannot_touch_a_newer_task_for_the_same_pod` |
| Removed pods cannot be resurrected | `test_late_readiness_report_cannot_resurrect_a_removed_pod`, `test_late_success_cannot_resurrect_a_removed_pod`, `test_readiness_report_on_evicted_pod_is_ignored` |
| Idempotent reports | `test_duplicate_result_report_is_idempotent`, `test_readiness_report_idempotency`, `test_replay_reports_current_state_not_a_stale_snapshot` |
| Unresolved evictions survive restart | `test_open_evictions_still_reserve_after_restart` |
| Settled results survive restart | `test_result_already_applied_survives_restart`, `test_abort_after_restart_uses_actual_readiness` |
| Inventory survives restart | `test_inventory_survives_restart` |

## 2. Concurrent dispatch, real server (4 workers)

`scripts/verify_concurrency.py` floods the API from 20 threads across 5 nodes
(50 ready pods, floor 4), then settles every eviction and adds a replacement.

```
$ python scripts/verify_concurrency.py --base http://127.0.0.1:8020 \
      --nodes 5 --pods-per-node 10 --floor 4 --workers 20 --restart-cmd ...

workload web: 50 ready pods, floor=4, capacity=46
approved 46 evictions in parallel
restarting server with unresolved evictions ...

all invariants held
```

Checked: exactly `50 − 4 = 46` evictions approved (no overspend, no duplicates
task/pod), `effective_ready == floor` after dispatch, and — after killing and
restarting all 4 workers mid-flight — `reserved == 46`, no new approvals, a
dispatched pod still reporting ready stays unavailable, all 46 pre-restart task
ids settle, and capacity returns only with a ready replacement.

## 3. Sustained concurrency (stress)

`scripts/stress_claims.py` runs 24 threads for 15 s mixing claims, `succeeded`,
`aborted`, and replacement registration, while a sampler polls the invariant.

```
$ python scripts/stress_claims.py --base http://127.0.0.1:8020 --seconds 15

no invariant violations under sustained concurrency
```

Over the run the sampler never saw `effective_ready < floor`, never saw
`reserved > active_total` (which would mean one pod was handed out twice), and
the final state had `reserved == 0` with all 953 tasks settled. Server log:
0 tracebacks, 0 `database is locked`, 0 HTTP 500.

## 4. End-to-end flow (the `/docs` walkthrough)

`scripts/demo_flow.py` registers a workload + pods, claims from a node,
simulates a node reporting readiness, then `succeeded` / `aborted` / a stale
replay. All responses were as expected (approved list + waiting reasons,
effective readiness moving 4 → 2 → 2 → 3, stale replay `applied:false`).

## 6. Container entrypoint

The image starts the service via `python -m app.main` (2 uvicorn workers,
non-root user, `/data` volume). The same entrypoint was run natively with
`DRAIN_WORKERS=2`:

```
$ DRAIN_WORKERS=2 python -m app.main
{"status":"ok","database":"/tmp/entry.db"}
docs=200
2 x "Application startup complete"
```

This exercises the multi-process start path (including concurrent schema
initialisation) that the container uses, even though the image itself was not
built here.

## 7. Static checks

* `pyflakes app tests scripts` — clean.
* `/docs` and `/openapi.json` render (13 paths); waiting-reason enum documented.

## 8. Review notes / deliberate decisions

* Reservations are **derived** from `dispatched` task rows, never a separate
  counter that could drift or reset on restart.
* Claims and every mutation use `BEGIN IMMEDIATE`, which is what serialises
  competing claims; the ready count is recomputed inside that transaction.
* Pod uids are treated as immutable (Kubernetes semantics): a retired uid
  cannot be re-registered, and a pod cannot move nodes. Replacements use new
  uids. This is what lets a late report be ignored safely.
* `report_id` gives idempotent retries for both readiness and eviction reports.

## 9. Not verified here / remaining work

* **`docker compose up` was not executed** (no Docker daemon in the sandbox).
  The Dockerfile/compose file are written and the YAML parses; the image build
  and healthcheck need a run on a Docker host.
* No authentication/authorisation — the API assumes a trusted internal network.
* SQLite is a single-writer design. Fine for a coordinator at this scale; for
  higher write throughput, move to Postgres and replace `BEGIN IMMEDIATE` with
  `SELECT … FOR UPDATE` / advisory locks.
* No background reconciliation: if a node agent dies after claiming, its tasks
  stay `dispatched` (correctly counted as unavailable) until an operator
  reports the result or a future reaper times them out. A timeout-based reaper
  is the obvious next addition.
* No pagination on list endpoints.
