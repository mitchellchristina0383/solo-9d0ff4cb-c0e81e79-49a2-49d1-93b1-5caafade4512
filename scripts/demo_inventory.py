#!/usr/bin/env python3
"""Walk through a node reconnect inventory reconciliation against a server.

Mirrors the acceptance story: a node agent disconnects and reconnects while
some of its pods have disappeared.  It issues an inventory (freezing the
scope), then submits the complete uid list it actually observes.  The service
retires the active scope members missing from the list and settles their
in-flight evictions by the manual-removal rule, all atomically with a receipt.

It also demonstrates the fencing rules: an exact replay returns the original
receipt, a changed list conflicts, and a later inventory supersedes an older
open one.

    python scripts/demo_inventory.py --base http://127.0.0.1:8000
"""

from __future__ import annotations

import argparse
import json
import sys

import httpx


def show(label: str, response: httpx.Response) -> dict:
    print(f"\n### {label}  ->  {response.status_code}")
    print(json.dumps(response.json(), indent=2)[:2400])
    return response.json()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:8000")
    args = ap.parse_args()

    c = httpx.Client(base_url=args.base, timeout=30.0)
    print(f"Interactive docs: {args.base}/docs")

    show("register workload web (floor 1)", c.put("/workloads/web", json={"ready_floor": 1}))
    for i in range(4):
        show(
            f"register pod web-{i} on node-a",
            c.put(
                f"/pods/web-{i}",
                json={"uid": f"web-{i}", "node": "node-a", "workload": "web", "ready": True},
            ),
        )

    # An eviction is already in flight against web-0 when the agent disconnects.
    approved = show("claim from node-a", c.post(
        "/drain/claims", json={"node": "node-a", "limit": 2}
    ))["approved"]
    open_task = approved[0]

    print("\n--- agent disconnects; web-0 and web-1 vanish while it is gone ---")

    inv = show("reconnected agent issues an inventory", c.post("/nodes/node-a/inventories"))
    inventory_id = inv["inventory_id"]
    print("frozen scope:", inv["scope"])

    # Agent actually observes web-2, web-3, plus a uid it never registered.
    observed = ["web-2", "web-3", "ghost-uid"]
    receipt = show(
        "agent submits its complete observed uid list",
        c.post(f"/nodes/node-a/inventories/{inventory_id}/submit", json={"uids": observed}),
    )
    print(
        "retired:", receipt["receipt"]["retired_uids"],
        "| settled tasks:", [t["task_id"] for t in receipt["receipt"]["settled_tasks"]],
        "| unknown:", receipt["receipt"]["unknown_uids"],
    )

    show("workload view after reconciliation", c.get("/workloads/web"))
    show("the in-flight task was aborted like a manual removal",
         c.get(f"/drain/tasks/{open_task['task_id']}"))

    show("replay the exact same list -> original receipt, duplicate=true",
         c.post(f"/nodes/node-a/inventories/{inventory_id}/submit", json={"uids": observed}))
    changed = c.post(
        f"/nodes/node-a/inventories/{inventory_id}/submit",
        json={"uids": observed + ["web-0"]},
    )
    print(f"\n### re-submit a different list -> {changed.status_code} {changed.json()}")

    print("\n--- fencing: a newer inventory supersedes an older open one ---")
    old = c.post("/nodes/node-a/inventories").json()
    new = c.post("/nodes/node-a/inventories").json()
    show("complete the newer inventory",
         c.post(f"/nodes/node-a/inventories/{new['inventory_id']}/submit",
                json={"uids": ["web-2", "web-3"]}))
    stale = c.post(f"/nodes/node-a/inventories/{old['inventory_id']}/submit",
                   json={"uids": []})
    print(f"\n### late submit of the older open inventory -> {stale.status_code} {stale.json()}")
    show("inventories for node-a (newest first)", c.get("/nodes/node-a/inventories"))
    show("status", c.get("/status"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
