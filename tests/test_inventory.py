"""Node inventory reconciliation tests.

A node agent that disconnects and reconnects may keep counting pods that have
disappeared.  An inventory freezes the node's active-pod scope at issue time
and retires scope members still active but missing from the agent's observed
uid list, settling their in-flight evictions by the manual-removal rule.
"""

from __future__ import annotations

from conftest import add_pod, claim, register_workload, settle


def issue(c, node: str) -> dict:
    r = c.post(f"/nodes/{node}/inventories")
    assert r.status_code == 201, r.text
    return r.json()


def submit(c, node: str, inventory_id: str, uids: list[str]):
    return c.post(f"/nodes/{node}/inventories/{inventory_id}/submit", json={"uids": uids})


def test_issue_freezes_scope_of_active_pods_on_node(client):
    register_workload(client, "web", 0)
    add_pod(client, "web-0", "n1", "web")
    add_pod(client, "web-1", "n1", "web")
    add_pod(client, "web-other", "n2", "web")

    inv = issue(client, "n1")
    assert inv["state"] == "open"
    assert inv["scope"] == ["web-0", "web-1"]
    assert inv["submitted_uids"] is None and inv["receipt"] is None

    # Only n1's pods are in scope; n2 gets its own independent inventory.
    other = issue(client, "n2")
    assert other["scope"] == ["web-other"]
    assert other["seq"] == 1 and inv["seq"] == 1  # seq is per node


def test_missing_active_scope_pods_are_retired_and_others_kept(client):
    register_workload(client, "web", 0)
    for uid in ("web-0", "web-1", "web-2"):
        add_pod(client, uid, "n1", "web")

    inv = issue(client, "n1")
    # The agent sees only two of the three pods: web-1 has vanished.
    resp = submit(client, "n1", inv["inventory_id"], ["web-0", "web-2"])
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["state"] == "completed"
    assert body["submitted_uids"] == ["web-0", "web-2"]
    assert body["receipt"]["retired_uids"] == ["web-1"]

    pod = client.get("/pods/web-1").json()
    assert pod["state"] == "removed" and pod["ready"] is False
    for uid in ("web-0", "web-2"):
        assert client.get(f"/pods/{uid}").json()["state"] == "active"


def test_in_flight_eviction_on_missing_pod_is_settled_like_manual_removal(client):
    register_workload(client, "web", 0)
    for uid in ("web-0", "web-1"):
        add_pod(client, uid, "n1", "web")
    task = claim(client, "n1", limit=1)["approved"][0]
    missing = task["pod_uid"]
    surviving = next(uid for uid in ("web-0", "web-1") if uid != missing)

    inv = issue(client, "n1")
    body = submit(client, "n1", inv["inventory_id"], [surviving]).json()

    assert body["receipt"]["retired_uids"] == [missing]
    settled = body["receipt"]["settled_tasks"]
    assert len(settled) == 1
    assert settled[0]["task_id"] == task["task_id"]
    assert settled[0]["pod_uid"] == missing
    assert settled[0]["result_note"] == "inventory_missing"

    task_view = client.get(f"/drain/tasks/{task['task_id']}").json()
    assert task_view["status"] == "aborted"
    assert client.get(f"/pods/{missing}").json()["state"] == "removed"
    assert client.get("/workloads/web").json()["reserved"] == 0


def test_pods_registered_after_issue_are_out_of_scope_and_survive(client):
    register_workload(client, "web", 0)
    add_pod(client, "web-0", "n1", "web")
    inv = issue(client, "n1")

    # A new pod registers while the agent is collecting its uid list.
    add_pod(client, "web-late", "n1", "web")
    body = submit(client, "n1", inv["inventory_id"], ["web-0"]).json()

    assert body["receipt"]["retired_uids"] == []
    assert client.get("/pods/web-late").json()["state"] == "active"
    # The late pod is not in the frozen scope even though it is missing.
    assert "web-late" not in body["receipt"]["unknown_uids"]


def test_submitted_uid_never_registered_is_reported_as_unknown(client):
    register_workload(client, "web", 0)
    add_pod(client, "web-0", "n1", "web")
    inv = issue(client, "n1")
    body = submit(client, "n1", inv["inventory_id"], ["web-0", "phantom"]).json()
    assert body["receipt"]["unknown_uids"] == ["phantom"]
    assert body["receipt"]["retired_uids"] == []


def test_scope_pod_evicted_between_issue_and_submit_is_left_alone(client):
    register_workload(client, "web", 0)
    for uid in ("web-0", "web-1"):
        add_pod(client, uid, "n1", "web")
    task = claim(client, "n1")["approved"][0]
    gone = task["pod_uid"]
    settle(client, task["task_id"], "succeeded")  # pod now 'evicted'

    inv = issue(client, "n1")
    # It was in scope at issue time, but it is already terminal at submit.
    present = [uid for uid in ("web-0", "web-1") if uid != gone]
    body = submit(client, "n1", inv["inventory_id"], present).json()
    assert body["receipt"]["retired_uids"] == []
    assert body["receipt"]["settled_tasks"] == []
    assert client.get(f"/pods/{gone}").json()["state"] == "evicted"


def test_already_removed_submitted_uid_is_known_not_unknown(client):
    register_workload(client, "web", 0)
    add_pod(client, "web-0", "n1", "web")
    client.delete("/pods/web-0")
    inv = issue(client, "n1")  # empty scope: web-0 is no longer active
    body = submit(client, "n1", inv["inventory_id"], ["web-0"]).json()
    assert body["receipt"]["unknown_uids"] == []
    assert body["receipt"]["retired_uids"] == []


def test_empty_submission_retires_every_active_scope_pod(client):
    register_workload(client, "web", 0)
    for uid in ("web-0", "web-1"):
        add_pod(client, uid, "n1", "web")
    inv = issue(client, "n1")
    body = submit(client, "n1", inv["inventory_id"], []).json()
    assert body["receipt"]["retired_uids"] == ["web-0", "web-1"]
    assert all(
        client.get(f"/pods/{uid}").json()["state"] == "removed"
        for uid in ("web-0", "web-1")
    )


def test_replay_same_list_returns_original_receipt(client):
    register_workload(client, "web", 0)
    add_pod(client, "web-0", "n1", "web")
    inv = issue(client, "n1")
    first = submit(client, "n1", inv["inventory_id"], []).json()
    again = submit(client, "n1", inv["inventory_id"], []).json()
    assert again["duplicate"] is True
    assert again["receipt"] == first["receipt"]
    # The pod was retired exactly once.
    assert client.get("/pods/web-0").json()["state"] == "removed"


def test_different_list_on_completed_inventory_conflicts(client):
    register_workload(client, "web", 0)
    add_pod(client, "web-0", "n1", "web")
    inv = issue(client, "n1")
    submit(client, "n1", inv["inventory_id"], [])
    conflict = submit(client, "n1", inv["inventory_id"], ["web-0"])
    assert conflict.status_code == 409
    # The original outcome stands.
    assert client.get("/pods/web-0").json()["state"] == "removed"


def test_later_inventory_completing_supersedes_older_open_one(client):
    register_workload(client, "web", 0)
    for uid in ("web-0", "web-1"):
        add_pod(client, uid, "n1", "web")
    old = issue(client, "n1")
    new = issue(client, "n1")
    assert new["seq"] == old["seq"] + 1

    # Completing the newer inventory fences the older one.
    done = submit(client, "n1", new["inventory_id"], ["web-0", "web-1"])
    assert done.status_code == 200
    late = submit(client, "n1", old["inventory_id"], [])
    assert late.status_code == 409
    assert client.get(f"/inventories/{old['inventory_id']}").json()["state"] == "superseded"
    # Nothing was retired by the rejected late submission.
    assert all(
        client.get(f"/pods/{uid}").json()["state"] == "active"
        for uid in ("web-0", "web-1")
    )


def test_completing_old_before_new_does_not_supersede_the_new_one(client):
    register_workload(client, "web", 0)
    add_pod(client, "web-0", "n1", "web")
    old = issue(client, "n1")
    new = issue(client, "n1")

    first = submit(client, "n1", old["inventory_id"], [])
    assert first.status_code == 200
    # The newer inventory stays open and is still submittable.
    assert client.get(f"/inventories/{new['inventory_id']}").json()["state"] == "open"
    second = submit(client, "n1", new["inventory_id"], [])
    assert second.status_code == 200
    assert client.get(f"/inventories/{old['inventory_id']}").json()["state"] == "completed"


def test_interleaved_claims_readiness_and_inventory(client):
    """Inventory interleaved with registration and eviction reports."""
    register_workload(client, "web", 1)
    for uid in ("web-0", "web-1", "web-2"):
        add_pod(client, uid, "n1", "web")

    inv = issue(client, "n1")
    # After issue: a claim dispatches one eviction and a readiness flap lands.
    approved = claim(client, "n1")["approved"]
    assert len(approved) == 2  # floor 1 of 3 -> 2 may go
    dispatched = {a["pod_uid"] for a in approved}
    client.post("/pods/web-2/readiness", json={"ready": False})

    # One dispatched pod reports success and retires before the inventory submit.
    succeeded_task = next(a for a in approved if a["pod_uid"] != "web-2")
    settle(client, succeeded_task["task_id"], "succeeded")
    succeeded_pod = succeeded_task["pod_uid"]

    # Agent now reports: web-2 exists; the other dispatched pod vanished.
    missing = next(iter(dispatched - {succeeded_pod}))
    observed = ["web-2"]
    body = submit(client, "n1", inv["inventory_id"], observed).json()

    # Only the still-active missing pod is retired by the inventory; the
    # already-succeeded one is terminal and must not appear twice.
    assert body["receipt"]["retired_uids"] == [missing]
    settled_ids = {t["task_id"] for t in body["receipt"]["settled_tasks"]}
    missing_task = next(
        a["task_id"] for a in approved if a["pod_uid"] == missing
    )
    assert settled_ids == {missing_task}
    assert succeeded_task["task_id"] not in settled_ids

    states = {uid: client.get(f"/pods/{uid}").json()["state"] for uid in
              ("web-0", "web-1", "web-2")}
    assert states[missing] == "removed"
    assert states[succeeded_pod] == "evicted"
    assert states["web-2"] == "active"
    assert client.get("/workloads/web").json()["reserved"] == 0


def test_submit_unknown_inventory_is_404_and_wrong_node_is_404(client):
    r = client.post("/nodes/n1/inventories/nope/submit", json={"uids": []})
    assert r.status_code == 404

    register_workload(client, "web", 0)
    add_pod(client, "web-0", "n1", "web")
    inv = issue(client, "n1")
    r = client.post(f"/nodes/n2/inventories/{inv['inventory_id']}/submit", json={"uids": []})
    assert r.status_code == 404


def test_duplicate_uids_in_submission_are_rejected(client):
    register_workload(client, "web", 0)
    add_pod(client, "web-0", "n1", "web")
    inv = issue(client, "n1")
    r = submit(client, "n1", inv["inventory_id"], ["web-0", "web-0"])
    assert r.status_code == 422
    # Rejected validation leaves the inventory open and state untouched.
    assert client.get(f"/inventories/{inv['inventory_id']}").json()["state"] == "open"


def test_failed_write_preserves_state_and_submit_can_retry(
    client, monkeypatch
):
    """A failure inside the atomic transaction rolls back every change."""
    from app import service as service_module

    register_workload(client, "web", 0)
    add_pod(client, "web-0", "n1", "web")
    add_pod(client, "web-1", "n1", "web")
    inv = issue(client, "n1")

    real_retire = service_module._retire_active_pod

    def boom(conn, uid, *, note, ts):
        raise RuntimeError("simulated disk failure")

    monkeypatch.setattr(service_module, "_retire_active_pod", boom)
    r = submit(client, "n1", inv["inventory_id"], [])
    assert r.status_code == 500

    monkeypatch.setattr(service_module, "_retire_active_pod", real_retire)
    # The failed attempt changed nothing; the same inventory can be retried.
    body = submit(client, "n1", inv["inventory_id"], []).json()
    assert body["state"] == "completed"
    assert body["receipt"]["retired_uids"] == ["web-0", "web-1"]


def test_inventories_and_receipts_survive_restart(client, monkeypatch):
    from tests.test_restart import _restart  # reuse the restart helper

    register_workload(client, "web", 0)
    add_pod(client, "web-0", "n1", "web")
    add_pod(client, "web-1", "n1", "web")
    inv = issue(client, "n1")
    submit(client, "n1", inv["inventory_id"], ["web-0"])
    open_inv = issue(client, "n1")
    client.close()

    with _restart(client.db_path.parent, monkeypatch) as fresh:
        done = fresh.get(f"/inventories/{inv['inventory_id']}").json()
        assert done["state"] == "completed"
        assert done["receipt"]["retired_uids"] == ["web-1"]

        # Exact replay after restart returns the persisted original receipt.
        replay = fresh.post(
            f"/nodes/n1/inventories/{inv['inventory_id']}/submit",
            json={"uids": ["web-0"]},
        ).json()
        assert replay["duplicate"] is True
        assert replay["receipt"] == done["receipt"]

        # The still-open inventory survived with its frozen scope.
        pending = fresh.get(f"/inventories/{open_inv['inventory_id']}").json()
        assert pending["state"] == "open"
        assert set(pending["scope"]) == {"web-0"}

        # And it remains functional post-restart.
        finished = fresh.post(
            f"/nodes/n1/inventories/{open_inv['inventory_id']}/submit",
            json={"uids": []},
        ).json()
        assert finished["receipt"]["retired_uids"] == ["web-0"]
