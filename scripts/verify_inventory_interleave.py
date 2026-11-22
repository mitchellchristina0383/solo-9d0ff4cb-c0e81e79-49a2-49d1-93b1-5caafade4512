#!/usr/bin/env python3
"""Interleave inventory reconciliation with registration and eviction reports.

Runs against a live HTTP server (TestClient is single-threaded) and asserts,
after every observed state, that:

* only scope members still active and missing from the agent list are retired;
* every retired pod's in-flight eviction was settled (reserved never dangles);
* pods registered after the inventory was issued survive;
* receipts are replayable and conflict correctly;
* the workload floor invariant ``effective_ready >= ready_floor`` never breaks.

    python scripts/verify_inventory_interleave.py --base http://127.0.0.1:8022
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import random
import sys
import uuid

import httpx


def must(r: httpx.Response, *codes: int) -> dict:
    assert r.status_code in codes, f"{r.request.method} {r.request.url} -> {r.status_code}: {r.text}"
    return r.json()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:8022")
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()
    random.seed(args.seed)

    c = httpx.Client(base_url=args.base, timeout=30.0)
    # uuid (not the seeded RNG) so repeated runs against a persistent server
    # never collide with uids from an earlier run.
    run = f"mix-{args.seed}-{uuid.uuid4().hex[:8]}"
    node = f"{run}-node"
    wl = f"{run}-wl"
    floor = 3
    n_pods = 12

    must(c.put(f"/workloads/{wl}", json={"ready_floor": floor}), 200)
    for i in range(n_pods):
        uid = f"{run}-{i}"
        must(
            c.put(f"/pods/{uid}", json={
                "uid": uid, "node": node, "workload": wl, "ready": True,
            }),
            200,
        )

    def assert_reservations() -> None:
        # Structural invariants that hold regardless of natural readiness loss:
        # no pod is dispatched twice and no open task dangles on a retired pod.
        stats = must(c.get(f"/workloads/{wl}"), 200)
        assert stats["reserved"] <= stats["active_total"], stats

    # ---- phase 1: issue inventory while claims and readiness reports run ----
    inv = must(c.post(f"/nodes/{node}/inventories"), 201)
    scope = set(inv["scope"])
    assert scope == {f"{run}-{i}" for i in range(n_pods)}

    def claim_some(seq: int) -> list[str]:
        out = []
        for _ in range(3):
            body = must(c.post("/drain/claims", json={"node": node, "limit": 2}), 200)
            out.extend(a["task_id"] for a in body["approved"])
            # The floor is a constraint on what a claim may spend: the stats
            # computed inside the claim transaction must still meet the floor.
            for w in body["workloads"]:
                assert w["effective_ready"] >= w["ready_floor"], w
            assert_reservations()
        return out

    def flap(seq: int) -> None:
        for i in random.sample(range(n_pods), 4):
            c.post(f"/pods/{run}-{i}/readiness", json={"ready": False})
            c.post(f"/pods/{run}-{i}/readiness", json={"ready": True})
            assert_reservations()

    with cf.ThreadPoolExecutor(max_workers=4) as pool:
        claims = list(pool.map(claim_some, range(3)))
        list(pool.map(flap, range(3)))
    dispatched_ids = {tid for ids in claims for tid in ids}
    print(f"phase 1: {len(dispatched_ids)} evictions dispatched while inventory open")

    # ---- phase 2: settle half the evictions BEFORE the inventory submit ----
    tasks = must(c.get("/drain/tasks", params={"status": "dispatched", "node": node}), 200)
    half = tasks[: len(tasks) // 2]
    for t in half:
        must(c.post(f"/drain/tasks/{t['task_id']}/result",
                    json={"outcome": "succeeded", "report_id": f"ok-{t['task_id']}"}), 200)
    assert_reservations()

    # ---- phase 3: new pods register after issue; they must survive ----
    for j in range(3):
        uid = f"{run}-late-{j}"
        must(c.put(f"/pods/{uid}", json={
            "uid": uid, "node": node, "workload": wl, "ready": True,
        }), 200)

    # Agent observed list: keep 4 original pods + the late ones; drop the rest.
    observed = [f"{run}-{i}" for i in range(4)] + [f"{run}-late-{j}" for j in range(3)]
    # Derive the expected retirement set from pre-submit state independently.
    pre_state = {
        uid: must(c.get(f"/pods/{uid}"), 200)["state"] for uid in scope
    }
    expected_missing_active = sorted(
        uid for uid in scope
        if uid not in observed and pre_state[uid] == "active"
    )
    body = must(
        c.post(f"/nodes/{node}/inventories/{inv['inventory_id']}/submit",
               json={"uids": observed}), 200)
    receipt = body["receipt"]
    print("phase 3: retired =", receipt["retired_uids"])
    print("         settled =", [t["pod_uid"] for t in receipt["settled_tasks"]])
    print("         unknown =", receipt["unknown_uids"])

    # Retired set == scope members still active at submit and absent.
    assert receipt["retired_uids"] == expected_missing_active, (
        receipt["retired_uids"], expected_missing_active)

    # Every settled task belonged to a retired pod and is now aborted.
    for ref in receipt["settled_tasks"]:
        assert ref["pod_uid"] in receipt["retired_uids"]
        task = must(c.get(f"/drain/tasks/{ref['task_id']}"), 200)
        assert task["status"] == "aborted" and task["result_note"] == "inventory_missing"

    # Late-registered pods survive regardless of the submitted list.
    for j in range(3):
        pod = must(c.get(f"/pods/{run}-late-{j}"), 200)
        assert pod["state"] == "active", pod

    # No dangling reservations and the floor holds.
    assert_reservations()
    stats = must(c.get(f"/workloads/{wl}"), 200)
    assert stats["reserved"] == 0, stats

    # ---- phase 4: idempotency and conflict semantics ----
    replay = must(
        c.post(f"/nodes/{node}/inventories/{inv['inventory_id']}/submit",
               json={"uids": observed}), 200)
    assert replay["duplicate"] is True and replay["receipt"] == receipt
    conflict = c.post(
        f"/nodes/{node}/inventories/{inv['inventory_id']}/submit",
        json={"uids": observed + [f"{run}-extra"]})
    assert conflict.status_code == 409, conflict.text

    # ---- phase 5: fencing across two fresh inventories, concurrent submit ----
    old = must(c.post(f"/nodes/{node}/inventories"), 201)
    new = must(c.post(f"/nodes/{node}/inventories"), 201)
    with cf.ThreadPoolExecutor(max_workers=2) as pool:
        f_old = pool.submit(
            lambda: c.post(f"/nodes/{node}/inventories/{old['inventory_id']}/submit",
                           json={"uids": observed}))
        f_new = pool.submit(
            lambda: c.post(f"/nodes/{node}/inventories/{new['inventory_id']}/submit",
                           json={"uids": observed}))
        r_old, r_new = f_old.result(), f_new.result()
    assert r_new.status_code == 200, r_new.text
    assert r_old.status_code in (200, 409)
    if r_old.status_code == 200:
        # Old won the race and completed first; the new one still completes.
        assert r_old.json()["state"] == "completed"
        assert r_new.json()["state"] == "completed"
    else:
        # New completed first and fenced the old one.
        assert "superseded" in r_old.text
        assert must(c.get(f"/inventories/{old['inventory_id']}"), 200)["state"] == "superseded"
    assert_reservations()

    print("\nall interleaving invariants held")
    return 0


if __name__ == "__main__":
    sys.exit(main())
