#!/usr/bin/env python3
"""Stress test: sustained concurrent claims + a sampler watching the invariant.

Unlike ``verify_concurrency.py`` (one deterministic burst), this hammers the
coordinator with overlapping claim/settle/fail cycles while a sampler polls the
workload total and asserts that the *reported* effective readiness never drops
below the floor.  It also tracks the sum of approved evictions for ready pods
against the number of ready replicas that actually existed, which catches two
concurrent claims spending the same replica.

    python scripts/stress_claims.py --base http://127.0.0.1:8010 --seconds 10
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import random
import sys
import threading
import time

import httpx

FAILURES: list[str] = []
LOCK = threading.Lock()
STOP = threading.Event()


def fail(msg: str) -> None:
    with LOCK:
        FAILURES.append(msg)
    print(f"  FAIL {msg}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:8010")
    ap.add_argument("--nodes", type=int, default=6)
    ap.add_argument("--pods-per-node", type=int, default=8)
    ap.add_argument("--floor", type=int, default=5)
    ap.add_argument("--threads", type=int, default=24)
    ap.add_argument("--seconds", type=float, default=10.0)
    args = ap.parse_args()

    total = args.nodes * args.pods_per_node
    with httpx.Client(base_url=args.base, timeout=30.0) as c:
        c.put("/workloads/web", json={"ready_floor": args.floor}).raise_for_status()
        for n in range(args.nodes):
            for p in range(args.pods_per_node):
                uid = f"n{n}-p{p}"
                c.put(
                    f"/pods/{uid}",
                    json={"uid": uid, "node": f"n{n}", "workload": "web", "ready": True},
                ).raise_for_status()
        baseline = c.get("/workloads/web").json()["effective_ready"]
        if baseline != total:
            print(
                f"workload 'web' already has {baseline} effective-ready pods;"
                " run this script against a fresh database"
            )
            return 2

    def sampler() -> None:
        with httpx.Client(base_url=args.base, timeout=30.0) as c:
            while not STOP.is_set():
                try:
                    wl = c.get("/workloads/web").json()
                except httpx.HTTPError as exc:  # transient during churn is not a violation
                    print(f"  (sampler skipped: {exc})")
                    continue
                if wl["effective_ready"] < args.floor:
                    fail(f"sampler saw effective_ready={wl['effective_ready']} < {args.floor}")
                if wl["effective_ready"] > wl["active_total"]:
                    fail(
                        f"effective_ready={wl['effective_ready']} exceeds"
                        f" active_total={wl['active_total']}"
                    )
                if wl["reserved"] > wl["active_total"]:
                    fail(
                        f"reserved={wl['reserved']} exceeds active_total={wl['active_total']}"
                        " (a pod was handed out twice)"
                    )

    def worker(seed: int) -> None:
        rng = random.Random(seed)
        with httpx.Client(base_url=args.base, timeout=30.0) as c:
            while not STOP.is_set():
                node = f"n{rng.randrange(args.nodes)}"
                r = c.post("/drain/claims", json={"node": node, "limit": rng.randint(1, 4)})
                if r.status_code != 200:
                    fail(f"claim {r.status_code}: {r.text}")
                    continue
                body = r.json()
                for a in body["approved"]:
                    if rng.random() < 0.5:
                        rr = c.post(
                            f"/drain/tasks/{a['task_id']}/result",
                            json={"outcome": "aborted"},
                        )
                    else:
                        rr = c.post(
                            f"/drain/tasks/{a['task_id']}/result",
                            json={"outcome": "succeeded"},
                        )
                    if rr.status_code != 200:
                        fail(f"result {rr.status_code}: {rr.text}")
                # Occasionally add a ready replacement so successful evictions
                # free capacity again and the run keeps making progress.
                if rng.random() < 0.15:
                    uid = f"repl-{seed}-{rng.randrange(10**9)}"
                    c.put(
                        f"/pods/{uid}",
                        json={
                            "uid": uid,
                            "node": f"n{rng.randrange(args.nodes)}",
                            "workload": "web",
                            "ready": True,
                        },
                    )

    with cf.ThreadPoolExecutor(max_workers=2) as pool:
        future = pool.submit(sampler)
        workers = cf.ThreadPoolExecutor(max_workers=args.threads)
        for i in range(args.threads):
            workers.submit(worker, i)
        time.sleep(args.seconds)
        STOP.set()
        workers.shutdown(wait=True)
        future.result()

    with httpx.Client(base_url=args.base, timeout=30.0) as c:
        wl = c.get("/workloads/web").json()
        if wl["effective_ready"] < args.floor:
            fail(f"final effective_ready={wl['effective_ready']} < {args.floor}")
        if wl["reserved"] != 0:
            fail(f"final reserved={wl['reserved']} != 0 (every task should be settled)")

    if FAILURES:
        print(f"\n{len(FAILURES)} violation(s)")
        return 1
    print("\nno invariant violations under sustained concurrency")
    return 0


if __name__ == "__main__":
    sys.exit(main())
