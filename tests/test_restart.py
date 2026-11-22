"""Restart / durability tests.

Every request opens its own SQLite connection, so closing the TestClient and
reopening it against the same file is exactly what a process restart does.
"""

from __future__ import annotations

from conftest import add_pod, claim, register_workload, settle


def _restart(tmp_path, monkeypatch):
    import sys

    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    monkeypatch.setenv("DRAIN_DB_PATH", str(tmp_path / "drain.db"))
    from fastapi.testclient import TestClient

    from app.main import app

    return TestClient(app)


def test_open_evictions_still_reserve_after_restart(client, monkeypatch):
    register_workload(client, "web", 2)
    for i in range(4):
        add_pod(client, f"web-{i}", "n1", "web")
    before = claim(client, "n1", limit=2)
    assert len(before["approved"]) == 2
    task = before["approved"][0]
    client.close()

    with _restart(client.db_path.parent, monkeypatch) as fresh:
        wl = fresh.get("/workloads/web").json()
        assert wl["effective_ready"] == 2
        assert wl["reserved"] == 2

        # The dispatched pod still reports ready; it must stay unavailable.
        ack = fresh.post(f"/pods/{task['pod_uid']}/readiness", json={"ready": True}).json()
        assert ack["pod"]["effective_ready"] is False
        assert fresh.get("/workloads/web").json()["effective_ready"] == 2

        # No new evictions can be approved until the old ones are settled.
        after = claim(fresh, "n1")
        assert after["approved"] == []
        reasons = {w["pod_uid"]: w["reason"] for w in after["waiting"]}
        assert reasons[task["pod_uid"]] == "eviction_in_progress"

        # The pre-restart task id still settles correctly.
        done = settle(fresh, task["task_id"], "succeeded")
        assert done["applied"] is True
        assert fresh.get("/workloads/web").json()["effective_ready"] == 2
        assert fresh.get("/workloads/web").json()["evicted"] == 1


def test_result_already_applied_survives_restart(client, monkeypatch):
    register_workload(client, "web", 1)
    add_pod(client, "web-0", "n1", "web")
    add_pod(client, "web-1", "n1", "web")
    task = claim(client, "n1")["approved"][0]
    settle(fresh := client, task["task_id"], "aborted", report_id="persisted")
    assert fresh.get("/workloads/web").json()["effective_ready"] == 2
    client.close()

    with _restart(client.db_path.parent, monkeypatch) as reopened:
        replay = settle(reopened, task["task_id"], "aborted", report_id="persisted")
        assert replay["duplicate"] is True
        # A late *conflicting* report is also inert after restart.
        late = settle(reopened, task["task_id"], "succeeded")
        assert late["applied"] is False
        assert reopened.get("/workloads/web").json()["effective_ready"] == 2


def test_inventory_survives_restart(client, monkeypatch):
    register_workload(client, "web", 3)
    add_pod(client, "web-0", "n1", "web")
    add_pod(client, "web-1", "n1", "web", ready=False)
    client.close()

    with _restart(client.db_path.parent, monkeypatch) as reopened:
        pods = {p["uid"]: p for p in reopened.get("/pods").json()}
        assert pods["web-0"]["ready"] is True
        assert pods["web-1"]["ready"] is False
        assert reopened.get("/workloads/web").json()["ready_floor"] == 3


def test_abort_after_restart_uses_actual_readiness(client, monkeypatch):
    register_workload(client, "web", 1)
    add_pod(client, "web-0", "n1", "web")
    add_pod(client, "web-1", "n1", "web")
    task = claim(client, "n1")["approved"][0]
    # Readiness changes while the eviction is still unresolved, then restart.
    client.post(f"/pods/{task['pod_uid']}/readiness", json={"ready": False})
    client.close()

    with _restart(client.db_path.parent, monkeypatch) as reopened:
        assert reopened.get("/workloads/web").json()["effective_ready"] == 1
        done = settle(reopened, task["task_id"], "aborted")
        assert done["applied"] is True
        # The pod comes back as NOT ready, so the count stays at the floor.
        assert reopened.get("/workloads/web").json()["effective_ready"] == 1
        pod = reopened.get(f"/pods/{task['pod_uid']}").json()
        assert pod["ready"] is False and pod["open_task_id"] is None
