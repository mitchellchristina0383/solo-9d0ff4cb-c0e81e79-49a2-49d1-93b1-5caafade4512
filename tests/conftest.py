"""Shared pytest fixtures: isolated SQLite file + TestClient per test."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_file = tmp_path / "drain.db"
    monkeypatch.setenv("DRAIN_DB_PATH", str(db_file))
    # Re-import so module-level state cannot leak between tests.
    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    from app.main import app  # noqa: WPS433 (import after env setup)

    with TestClient(app) as c:
        c.db_path = db_file  # type: ignore[attr-defined]
        yield c


def register_workload(c, name: str, floor: int) -> None:
    r = c.put(f"/workloads/{name}", json={"ready_floor": floor})
    assert r.status_code == 200, r.text


def add_pod(c, uid: str, node: str, workload: str, ready: bool = True) -> None:
    r = c.put(
        f"/pods/{uid}",
        json={"uid": uid, "node": node, "workload": workload, "ready": ready},
    )
    assert r.status_code == 200, r.text


def claim(c, node: str, limit: int = 100) -> dict:
    r = c.post("/drain/claims", json={"node": node, "limit": limit})
    assert r.status_code == 200, r.text
    return r.json()


def settle(c, task_id: str, outcome: str, **extra) -> dict:
    r = c.post(f"/drain/tasks/{task_id}/result", json={"outcome": outcome, **extra})
    assert r.status_code == 200, r.text
    return r.json()
