#!/usr/bin/env python3
"""Concurrency + restart integration check against a real uvicorn server.

Run with the server already listening (see ``docs/SELFTEST.md``), e.g.::

    uvicorn app.main:app --workers 4 --port 8010

The script registers a workload, floods the coordinator with parallel claims
from several "nodes" and asserts that the workload never drops below its ready
floor.  It then settles every dispatch, registers ready replacements and
re-checks the floor.  Pass ``--restart-cmd`` to have the script stop the server
mid-flight, restart it, and verify that unresolved evictions still reserve
capacity.

Exit code 0 means every invariant held.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import subprocess
import sys
import threading
import time

import httpx

FAILURES: list[str] = []
LOCK = threading.Lock()


def check(condition: bool, message: str) -> None:
    if not condition:
        with LOCK:
            FAILURES.append(message)
        print(f"  FAIL {message}")


def api(base: str) -> httpx.Client:
    return httpx.Client(base_url=base, timeout=30.0)


def setup(client: httpx.Client, nodes: int, pods_per_node: int, floor: int) -> None:
    r = client.put("/workloads/web", json={"ready_floor": floor})
    r.raise_for_status()
    for n in range(nodes):
        node = f"node-{n}"
        r = client.put(f"/nodes/{node}", params={"draining": "true"})
        r.raise_for_status()
        for p in range(pods_per_node):
            uid = f"{node}-pod-{p}"
            r = client.put(
                f"/pods/{uid}",
                json={"uid": uid, "node": node, "workload": "web", "ready": True},
            )
            r.raise_for_status()


def effective(client: httpx.Client) -> int:
    return client.get("/workloads/web").json()["effective_ready"]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:8010")
    ap.add_argument("--nodes", type=int, default=4)
    ap.add_argument("--pods-per-node", type=int, default=10)
    ap.add_argument("--floor", type=int, default=2)
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--restart-cmd", default=None)
    args = ap.parse_args()

    total_pods = args.nodes * args.pods_per_node
    capacity = total_pods - args.floor
    print(f"workload web: {total_pods} ready pods, floor={args.floor}, capacity={capacity}")

    with api(args.base) as client:
        setup(client, args.nodes, args.pods_per_node, args.floor)
        base_effective = effective(client)
        check(base_effective == total_pods, f"initial effective_ready {base_effective} != {total_pods}")

    approved: list[dict] = []
    stop = threading.Event()

    def claimer(node: str) -> None:
        with api(args.base) as client:
            while not stop.is_set():
                r = client.post("/drain/claims", json={"node": node, "limit": 50})
                if r.status_code != 200:
                    check(False, f"claim failed: {r.status_code} {r.text}")
                    return
                body = r.json()
                if not body["approved"]:
                    return
                with LOCK:
                    approved.extend(body["approved"])
                # The invariant is checked against the API's own accounting.
                eff = body["workloads"][0]["effective_ready"]
                check(
                    eff >= args.floor,
                    f"claim response reported effective_ready={eff} below floor={args.floor}",
                )

    with cf.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [
            pool.submit(claimer, f"node-{n}")
            for n in range(args.nodes)
            for _ in range(args.workers // args.nodes)
        ]
        for f in futures:
            f.result()

    print(f"approved {len(approved)} evictions in parallel")
    check(len(approved) == capacity, f"approved {len(approved)} != capacity {capacity}")
    check(len({a["pod_uid"] for a in approved}) == capacity, "duplicate pod approved twice")
    check(len({a["task_id"] for a in approved}) == capacity, "duplicate task id")
    with api(args.base) as client:
        check(effective(client) == args.floor, "effective_ready not at floor after claims")
        wl = client.get("/workloads/web").json()
        check(wl["reserved"] == capacity, f"reserved {wl['reserved']} != {capacity}")

    # --- restart mid-flight: unresolved evictions must keep reserving --------
    if args.restart_cmd:
        print("restarting server with unresolved evictions ...")
        subprocess.run(args.restart_cmd, shell=True, check=True)
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                with api(args.base) as client:
                    if client.get("/healthz").status_code == 200:
                        break
            except httpx.HTTPError:
                pass
            time.sleep(0.3)
        with api(args.base) as client:
            wl = client.get("/workloads/web").json()
            check(wl["reserved"] == capacity, f"after restart reserved={wl['reserved']}")
            check(wl["effective_ready"] == args.floor, "after restart effective_ready != floor")
            r = client.post("/drain/claims", json={"node": "node-0", "limit": 50})
            check(r.json()["approved"] == [], "approved new evictions while old ones unresolved")
            # A dispatched pod that still reports ready must stay unavailable.
            uid = approved[0]["pod_uid"]
            ack = client.post(f"/pods/{uid}/readiness", json={"ready": True}).json()
            check(ack["pod"]["effective_ready"] is False, "dispatched pod counted as ready")

    # --- settle every eviction, add ready replacements ----------------------
    def settle(task: dict) -> None:
        with api(args.base) as client:
            r = client.post(
                f"/drain/tasks/{task['task_id']}/result",
                json={"outcome": "succeeded", "report_id": f"done-{task['task_id']}"},
            )
            check(r.status_code == 200, f"settle failed: {r.status_code} {r.text}")

    with cf.ThreadPoolExecutor(max_workers=args.workers) as pool:
        list(pool.map(settle, approved))

    with api(args.base) as client:
        wl = client.get("/workloads/web").json()
        check(wl["reserved"] == 0, f"reserved={wl['reserved']} after settling")
        check(wl["evicted"] == capacity, f"evicted={wl['evicted']} != {capacity}")
        check(
            wl["effective_ready"] == args.floor,
            f"effective_ready={wl['effective_ready']} after successful evictions",
        )

        # Replacements are only counted once ready.
        uid = "replacement-0"
        client.put(
            f"/pods/{uid}",
            json={"uid": uid, "node": "node-0", "workload": "web", "ready": False},
        )
        check(effective(client) == args.floor, "not-ready replacement was counted")
        client.post(f"/pods/{uid}/readiness", json={"ready": True})
        check(effective(client) == args.floor + 1, "ready replacement was not counted")

        # Capacity is floor+1 again, so exactly one more eviction is allowed.
        body = client.post("/drain/claims", json={"node": "node-0", "limit": 10}).json()
        check(
            len(body["approved"]) == 1,
            f"expected exactly one approval, got {body['approved']}",
        )
        for a in body["approved"]:
            state = client.get(f"/pods/{a['pod_uid']}").json()["state"]
            check(state == "active", f"evicted pod {a['pod_uid']} handed out again")
        check(effective(client) == args.floor, "floor not respected after replacement claim")

    if FAILURES:
        print(f"\n{len(FAILURES)} invariant violation(s)")
        return 1
    print("\nall invariants held")
    return 0


if __name__ == "__main__":
    sys.exit(main())
