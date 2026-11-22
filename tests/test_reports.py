from __future__ import annotations

from conftest import add_pod, claim, register_workload, settle


def test_duplicate_result_report_is_idempotent(client):
    register_workload(client, "web", 1)
    add_pod(client, "web-0", "n1", "web")
    add_pod(client, "web-1", "n1", "web")
    task = claim(client, "n1")["approved"][0]

    first = settle(client, task["task_id"], "aborted", report_id="r-1")
    assert first["applied"] is True and first["duplicate"] is False

    again = settle(client, task["task_id"], "aborted", report_id="r-1")
    assert again["duplicate"] is True
    assert again["applied"] is True  # the stored decision is replayed

    # Same id for a different task is a conflict, not a silent mix-up.
    other = claim(client, "n1")["approved"][0]
    r = client.post(
        f"/drain/tasks/{other['task_id']}/result",
        json={"outcome": "succeeded", "report_id": "r-1"},
    )
    assert r.status_code == 409


def test_stale_report_on_settled_task_changes_nothing(client):
    register_workload(client, "web", 1)
    add_pod(client, "web-0", "n1", "web")
    add_pod(client, "web-1", "n1", "web")
    task = claim(client, "n1")["approved"][0]
    settle(client, task["task_id"], "succeeded")
    snapshot = client.get("/workloads/web").json()

    late = settle(client, task["task_id"], "aborted")
    assert late["applied"] is False
    assert late["ignored_reason"] == "task_already_resolved"
    assert client.get("/workloads/web").json() == snapshot


def test_late_report_cannot_touch_a_newer_task_for_the_same_pod(client):
    register_workload(client, "web", 1)
    add_pod(client, "web-0", "n1", "web")
    add_pod(client, "web-1", "n1", "web")

    old = claim(client, "n1")["approved"][0]
    settle(client, old["task_id"], "aborted")
    new = claim(client, "n1")["approved"][0]
    assert new["pod_uid"] == old["pod_uid"]
    assert new["task_id"] != old["task_id"]

    # The late success for the old task must not retire the pod or the new task.
    late = settle(client, old["task_id"], "succeeded")
    assert late["applied"] is False
    pod = client.get(f"/pods/{new['pod_uid']}").json()
    assert pod["state"] == "active"
    assert pod["open_task_id"] == new["task_id"]

    done = settle(client, new["task_id"], "succeeded")
    assert done["applied"] is True
    assert client.get(f"/pods/{new['pod_uid']}").json()["state"] == "evicted"


def test_late_readiness_report_cannot_resurrect_a_removed_pod(client):
    register_workload(client, "web", 0)
    add_pod(client, "web-0", "n1", "web")
    client.delete("/pods/web-0")
    assert client.get("/workloads/web").json()["effective_ready"] == 0

    ack = client.post("/pods/web-0/readiness", json={"ready": True}).json()
    assert ack["applied"] is False
    assert ack["note"] == "pod_not_active"
    assert client.get("/workloads/web").json()["effective_ready"] == 0

    # Nor can it be re-registered by inventory to count again.
    r = client.put(
        "/pods/web-0",
        json={"uid": "web-0", "node": "n1", "workload": "web", "ready": True},
    )
    assert r.status_code == 409


def test_late_success_cannot_resurrect_a_removed_pod(client):
    register_workload(client, "web", 0)
    add_pod(client, "web-0", "n1", "web")
    task = claim(client, "n1")["approved"][0]
    client.delete("/pods/web-0")
    settle(client, task["task_id"], "succeeded")
    pod = client.get("/pods/web-0").json()
    assert pod["state"] == "removed"
    assert client.get("/workloads/web").json()["effective_ready"] == 0


def test_readiness_report_idempotency(client):
    register_workload(client, "web", 0)
    add_pod(client, "web-0", "n1", "web", ready=True)
    first = client.post(
        "/pods/web-0/readiness", json={"ready": False, "report_id": "ready-1"}
    ).json()
    assert first["applied"] is True and first["duplicate"] is False
    second = client.post(
        "/pods/web-0/readiness", json={"ready": True, "report_id": "ready-1"}
    ).json()
    assert second["duplicate"] is True
    assert client.get("/pods/web-0").json()["ready"] is False


def test_unknown_task_is_404(client):
    r = client.post("/drain/tasks/nope/result", json={"outcome": "succeeded"})
    assert r.status_code == 404


def test_readiness_report_on_evicted_pod_is_ignored(client):
    register_workload(client, "web", 0)
    add_pod(client, "web-0", "n1", "web")
    task = claim(client, "n1")["approved"][0]
    settle(client, task["task_id"], "succeeded")
    ack = client.post("/pods/web-0/readiness", json={"ready": True}).json()
    assert ack["applied"] is False
    assert ack["note"] == "pod_not_active"
    assert client.get("/workloads/web").json()["effective_ready"] == 0


def test_replay_reports_current_state_not_a_stale_snapshot(client):
    register_workload(client, "web", 1)
    add_pod(client, "web-0", "n1", "web")
    add_pod(client, "web-1", "n1", "web")
    task = claim(client, "n1")["approved"][0]
    settle(client, task["task_id"], "aborted", report_id="snap")
    stored = client.get("/workloads/web").json()

    # State moves on after the report was first stored.
    add_pod(client, "web-2", "n1", "web", ready=True)
    replay = settle(client, task["task_id"], "aborted", report_id="snap")
    assert replay["duplicate"] is True
    assert replay["workloads"][0]["effective_ready"] == 3
    assert replay["workloads"][0]["effective_ready"] != stored["effective_ready"]
