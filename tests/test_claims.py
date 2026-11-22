from __future__ import annotations

from conftest import add_pod, claim, register_workload, settle


def test_claim_never_takes_workload_below_floor(client):
    register_workload(client, "web", 2)
    for i in range(4):
        add_pod(client, f"web-{i}", "n1", "web")

    first = claim(client, "n1")
    approved = {a["pod_uid"] for a in first["approved"]}
    assert len(approved) == 2
    waiting = {w["pod_uid"]: w["reason"] for w in first["waiting"]}
    assert waiting == {"web-2": "ready_floor_limit", "web-3": "ready_floor_limit"}

    # A second claim sees the reservations from the first one.
    second = claim(client, "n1")
    assert second["approved"] == []
    reasons = {w["pod_uid"]: w["reason"] for w in second["waiting"]}
    assert reasons["web-0"] == "eviction_in_progress"
    assert reasons["web-1"] == "eviction_in_progress"
    assert reasons["web-2"] == "ready_floor_limit"
    assert reasons["web-3"] == "ready_floor_limit"
    wl = second["workloads"][0]
    assert wl["effective_ready"] == 2
    assert wl["reserved"] == 2


def test_concurrent_claims_across_nodes_share_one_budget(client):
    """The safety invariant is per worklaod, not per node."""
    register_workload(client, "web", 3)
    for i in range(3):
        add_pod(client, f"web-a{i}", "n1", "web")
        add_pod(client, f"web-b{i}", "n2", "web")

    a = claim(client, "n1", limit=100)
    b = claim(client, "n2", limit=100)
    total = len(a["approved"]) + len(b["approved"])
    assert total == 3, (a, b)
    for resp in (a, b):
        assert resp["workloads"][0]["effective_ready"] == 3


def test_limit_is_honoured_and_rest_reported_as_waiting(client):
    register_workload(client, "web", 0)
    for i in range(5):
        add_pod(client, f"web-{i}", "n1", "web")
    resp = claim(client, "n1", limit=2)
    assert len(resp["approved"]) == 2
    assert [w["reason"] for w in resp["waiting"]] == ["limit_reached"] * 3


def test_not_ready_pods_do_not_consume_the_ready_budget(client):
    register_workload(client, "web", 2)
    add_pod(client, "web-0", "n1", "web", ready=True)
    add_pod(client, "web-1", "n1", "web", ready=True)
    add_pod(client, "web-2", "n1", "web", ready=False)

    resp = claim(client, "n1")
    approved = {a["pod_uid"] for a in resp["approved"]}
    assert approved == {"web-2"}
    assert resp["workloads"][0]["effective_ready"] == 2


def test_dispatched_pod_counts_unavailable_even_if_it_reports_ready(client):
    register_workload(client, "web", 2)
    for i in range(4):
        add_pod(client, f"web-{i}", "n1", "web")
    resp = claim(client, "n1", limit=2)
    assert len(resp["approved"]) == 2

    uid = resp["approved"][0]["pod_uid"]
    ack = client.post(f"/pods/{uid}/readiness", json={"ready": True}).json()
    assert ack["pod"]["ready"] is True
    assert ack["pod"]["effective_ready"] is False
    assert client.get("/workloads/web").json()["effective_ready"] == 2


def test_floor_blocks_claims_until_a_replacement_is_ready(client):
    register_workload(client, "web", 2)
    for i in range(3):
        add_pod(client, f"web-{i}", "n1", "web")
    approved = claim(client, "n1")["approved"]
    assert len(approved) == 1

    settle(client, approved[0]["task_id"], "succeeded")
    wl = client.get("/workloads/web").json()
    assert wl["effective_ready"] == 2
    assert wl["evicted"] == 1

    # No capacity: the remaining two ready pods are exactly the floor.
    assert claim(client, "n1")["approved"] == []

    # A replacement with a fresh uid becomes ready and unlocks one more slot.
    add_pod(client, "web-replacement", "n1", "web", ready=True)
    assert client.get("/workloads/web").json()["effective_ready"] == 3
    assert len(claim(client, "n1")["approved"]) == 1


def test_aborted_eviction_restores_actual_readiness(client):
    register_workload(client, "web", 2)
    for i in range(3):
        add_pod(client, f"web-{i}", "n1", "web")
    approved = claim(client, "n1")["approved"]
    task = approved[0]
    assert client.get("/workloads/web").json()["effective_ready"] == 2

    # While outstanding the pod flaps to not-ready...
    client.post(f"/pods/{task['pod_uid']}/readiness", json={"ready": False})
    result = settle(client, task["task_id"], "aborted")
    assert result["applied"] is True
    # ...so abort restores it as NOT ready, not as ready.
    wl = result["workloads"][0]
    assert wl["effective_ready"] == 2
    assert wl["active_ready"] == 2
    pod = client.get(f"/pods/{task['pod_uid']}").json()
    assert pod["ready"] is False
    assert pod["open_task_id"] is None


def test_aborted_eviction_restores_ready_count_when_still_ready(client):
    register_workload(client, "web", 1)
    add_pod(client, "web-0", "n1", "web")
    add_pod(client, "web-1", "n1", "web")
    task = claim(client, "n1")["approved"][0]
    assert client.get("/workloads/web").json()["effective_ready"] == 1
    result = settle(client, task["task_id"], "aborted")
    assert result["workloads"][0]["effective_ready"] == 2
    assert result["workloads"][0]["reserved"] == 0


def test_success_does_not_count_until_replacement_reports_ready(client):
    register_workload(client, "web", 1)
    add_pod(client, "web-0", "n1", "web")
    add_pod(client, "web-1", "n1", "web")
    task = claim(client, "n1")["approved"][0]
    settle(client, task["task_id"], "succeeded")
    assert client.get("/workloads/web").json()["effective_ready"] == 1

    add_pod(client, "web-new", "n2", "web", ready=False)
    assert client.get("/workloads/web").json()["effective_ready"] == 1
    client.post("/pods/web-new/readiness", json={"ready": True})
    assert client.get("/workloads/web").json()["effective_ready"] == 2


def test_pod_cannot_be_moved_to_another_node(client):
    register_workload(client, "web", 0)
    add_pod(client, "web-0", "n1", "web")
    r = client.put(
        "/pods/web-0",
        json={"uid": "web-0", "node": "n2", "workload": "web", "ready": True},
    )
    assert r.status_code == 409
