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
40 passed
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
| Issue freezes the node's active-pod scope (per-node seq) | `test_issue_freezes_scope_of_active_pods_on_node` |
| Missing active scope pods are retired; present ones kept | `test_missing_active_scope_pods_are_retired_and_others_kept` |
| In-flight eviction on a missing pod is settled like manual removal | `test_in_flight_eviction_on_missing_pod_is_settled_like_manual_removal` |
| Pods registered after issue are out of scope and survive | `test_pods_registered_after_issue_are_out_of_scope_and_survive` |
| Submitted but never-registered uids are reported as unknown | `test_submitted_uid_never_registered_is_reported_as_unknown` |
| Scope pod evicted between issue and submit is left terminal | `test_scope_pod_evicted_between_issue_and_submit_is_left_alone` |
| Already-retired submitted uid is known, not unknown | `test_already_removed_submitted_uid_is_known_not_unknown` |
| Empty submission retires every active scope pod | `test_empty_submission_retires_every_active_scope_pod` |
| Same-list replay returns the original receipt; changed list is 409 | `test_replay_same_list_returns_original_receipt`, `test_different_list_on_completed_inventory_conflicts` |
| Later inventory supersedes older open one; earlier completion leaves the new one open | `test_later_inventory_completing_supersedes_older_open_one`, `test_completing_old_before_new_does_not_supersede_the_new_one` |
| Inventory interleaved with claims/readiness/eviction reports | `test_interleaved_claims_readiness_and_inventory` |
| Unknown inventory / wrong node → 404; duplicate uids → 422 | `test_submit_unknown_inventory_is_404_and_wrong_node_is_404`, `test_duplicate_uids_in_submission_are_rejected` |
| Failed write rolls back and the inventory can be retried | `test_failed_write_preserves_state_and_submit_can_retry` |
| Inventory ids, open scope and receipts all survive restart | `test_inventories_and_receipts_survive_restart` |

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

## 5. Node inventory reconciliation (new)

`scripts/demo_inventory.py` drives the full reconnect story against a live
server (`scripts/verify_inventory_interleave.py` drives the racy one):

```
$ python scripts/demo_inventory.py --base http://127.0.0.1:8026
### reconnected agent issues an inventory              -> 201
### agent submits its complete observed uid list       -> 200
retired: ['web-0', 'web-1'] | settled tasks: [2 tasks] | unknown: ['ghost-uid']
### replay the exact same list -> original receipt     -> duplicate=true
### re-submit a different list                         -> 409
### late submit of the older open inventory            -> 409 (superseded)
```

`scripts/verify_inventory_interleave.py` runs 4 worker threads mixing claims
and readiness flaps while an inventory is open, settles half the evictions
before the submit, registers late pods, then submits and checks every
invariant (retired set equals still-active-and-missing scope members; every
settled task belongs to a retired pod and is `aborted/inventory_missing`;
late pods survive; no dangling reservations; replay/conflict semantics;
concurrent racing submits of two inventories). Repeated across seeds
7/41/99/2026/3:

```
$ for s in 7 41 99 2026 3; do python scripts/verify_inventory_interleave.py --seed $s ...; done
all interleaving invariants held   (each run)
```

Real process-restart check (kill and relaunch uvicorn on the same SQLite
file, 2 workers): a completed inventory replayed its exact original receipt
(`duplicate=true`), a changed list returned 409, an open inventory survived
with its frozen scope and then completed correctly, its retired pod stayed
`removed`, its settled task stayed `aborted/inventory_missing`, and the next
issued `seq` continued monotonically at 3.

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
* `/docs` and `/openapi.json` render (16 paths; the 3 inventory paths appear
  under an `inventories` tag); inventory request/response schemas and the
  `open|completed|superseded` enum are documented.
* `docker-compose.yml` parses (PyYAML). No Docker daemon is available in this
  sandbox, so the image build itself was not executed; the container's
  entrypoint `python -m app.main` was run natively with `DRAIN_WORKERS=2`.

## 8. Review notes / deliberate decisions

* Reservations are **derived** from `dispatched` task rows, never a separate
  counter that could drift or reset on restart.
* Claims and every mutation use `BEGIN IMMEDIATE`, which is what serialises
  competing claims; the ready count is recomputed inside that transaction.
* Pod uids are treated as immutable (Kubernetes semantics): a retired uid
  cannot be re-registered, and a pod cannot move nodes. Replacements use new
  uids. This is what lets a late report be ignored safely.
* `report_id` gives idempotent retries for both readiness and eviction reports.
* Inventory scope is a **frozen snapshot** stored as JSON at issue time, not a
  re-derivation at submit time, so registrations and evictions racing the
  agent's scan cannot move the goalposts. The receipt is a row written in the
  same transaction as the retirements, which is what makes an exact replay
  return the original decision and a write failure fully roll back.
* Inventory fencing uses a per-node monotonic `seq`; only `open` inventories
  can be superseded, so completing an older inventory first never invalidates
  a newer one, while the newer one completing fences any stale older submit.

## 9. Not verified here / remaining work

* **`docker compose up` was not executed** (no Docker daemon in the sandbox).
  The Dockerfile/compose file are written and the YAML parses; the image build
  and healthcheck need a run on a Docker host. The identical entrypoint
  (`python -m app.main`, 2 workers, same env vars and volume path) was run
  natively instead, including a kill-and-relaunch restart test.
* No authentication/authorisation — the API assumes a trusted internal network.
* SQLite is a single-writer design. Fine for a coordinator at this scale; for
  higher write throughput, move to Postgres and replace `BEGIN IMMEDIATE` with
  `SELECT … FOR UPDATE` / advisory locks.
* Reconciliation is **agent-driven**, not a background reaper: an inventory
  only retires pods when the reconnecting agent submits one, and if an agent
  never comes back its `dispatched` tasks stay correctly counted as
  unavailable until an operator settles them. A timeout-based reaper for
  abandoned tasks and open inventories is the obvious next addition.
* Open inventories never expire: an agent that issues and never submits
  leaves an `open` row forever (harmless — it holds no reservations and is
  fenced by the next completed inventory). A TTL/cleanup job could prune them.
* No pagination on list endpoints (including `/nodes/{name}/inventories`).
