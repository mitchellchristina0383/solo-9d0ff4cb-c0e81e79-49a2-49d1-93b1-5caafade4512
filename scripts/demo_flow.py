#!/usr/bin/env python3
"""Walk through the drain workflow against a running server, via /docs and curl.

This mirrors the "claim a task from /docs, simulate node reports" acceptance
flow.  It is intentionally sequential and verbose so its output can be
pasted into a review.

    python scripts/demo_flow.py --base http://127.0.0.1:8000
"""

from __future__ import annotations

import argparse
import json
import sys

import httpx


def show(label: str, response: httpx.Response) -> dict:
    body = response.json()
    print(f"\n### {label}  ->  {response.status_code}")
    print(json.dumps(body, indent=2)[:2000])
    return body


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:8000")
    args = ap.parse_args()

    c = httpx.Client(base_url=args.base, timeout=30.0)
    print(f"Interactive docs: {args.base}/docs")

    show("register workload web (floor 2)", c.put("/workloads/web", json={"ready_floor": 2}))
    for i in range(4):
        show(
            f"register pod web-{i} on node-a",
            c.put(
                f"/pods/web-{i}",
                json={"uid": f"web-{i}", "node": "node-a", "workload": "web", "ready": True},
            ),
        )

    claim_body = show("claim from node-a", c.post("/drain/claims", json={"node": "node-a", "limit": 10}))
    approved = claim_body["approved"]
    print("\nApproved:", [a["pod_uid"] for a in approved])
    print("Waiting: ", [(w["pod_uid"], w["reason"]) for w in claim_body["waiting"]])

    if not approved:
        print("nothing approved; aborting demo")
        return 1
    task = approved[0]

    show(
        "evicted pod still reports ready (must stay unavailable)",
        c.post(f"/pods/{task['pod_uid']}/readiness", json={"ready": True}),
    )
    show("workload view", c.get("/workloads/web"))

    show(
        "node reports the eviction succeeded",
        c.post(f"/drain/tasks/{task['task_id']}/result", json={"outcome": "succeeded"}),
    )
    show("workload view after success", c.get("/workloads/web"))

    if len(approved) > 1:
        other = approved[1]
        show(
            "node reports the second eviction was aborted",
            c.post(
                f"/drain/tasks/{other['task_id']}/result",
                json={"outcome": "aborted", "note": "eviction API refused"},
            ),
        )
        show("workload view after abort", c.get("/workloads/web"))

    show(
        "a stale replay of the first report changes nothing",
        c.post(f"/drain/tasks/{task['task_id']}/result", json={"outcome": "aborted"}),
    )
    show("status", c.get("/status"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
