"""Request/response schemas for the drain API."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, field_validator


class WorkloadUpsert(BaseModel):
    ready_floor: int = Field(
        ge=0,
        description="Minimum number of ready replicas that must remain while draining.",
    )


class WorkloadOut(BaseModel):
    name: str
    ready_floor: int
    active_total: int
    active_ready: int
    reserved: int
    effective_ready: int
    evicted: int
    removed: int
    created_at: str
    updated_at: str


class PodUpsert(BaseModel):
    uid: str = Field(min_length=1, max_length=253)
    node: str = Field(min_length=1, max_length=253)
    workload: str = Field(min_length=1, max_length=253)
    ready: bool = True


class PodOut(BaseModel):
    uid: str
    node: str
    workload: str
    ready: bool
    state: Literal["active", "evicted", "removed"]
    effective_ready: bool
    open_task_id: str | None = None
    created_at: str
    updated_at: str
    retired_at: str | None = None


class ReadinessReport(BaseModel):
    ready: bool
    report_id: str | None = Field(
        default=None,
        max_length=200,
        description="Optional idempotency key. Replaying the same id is a no-op.",
    )


class ReadinessAck(BaseModel):
    pod: PodOut
    applied: bool
    duplicate: bool = False
    note: str | None = None


class ClaimRequest(BaseModel):
    node: str = Field(min_length=1, max_length=253)
    limit: int = Field(
        default=1,
        ge=1,
        le=1000,
        description="Maximum number of evictions to approve in this claim.",
    )


class ApprovedEviction(BaseModel):
    task_id: str
    pod_uid: str
    node: str
    workload: str
    dispatched_at: str


class WaitingPod(BaseModel):
    pod_uid: str
    workload: str
    reason: Literal[
        "eviction_in_progress",
        "ready_floor_limit",
        "limit_reached",
    ]
    detail: dict[str, object]
    open_task_id: str | None = None


class ClaimResponse(BaseModel):
    node: str
    approved: list[ApprovedEviction]
    waiting: list[WaitingPod]
    workloads: list[WorkloadOut]


class EvictionResultIn(BaseModel):
    outcome: Literal["succeeded", "aborted"]
    report_id: str | None = Field(
        default=None,
        max_length=200,
        description="Optional idempotency key. Replaying the same id is a no-op.",
    )
    note: str | None = Field(default=None, max_length=1000)


class EvictionTaskOut(BaseModel):
    task_id: str
    pod_uid: str
    node: str
    workload: str
    status: Literal["dispatched", "succeeded", "aborted"]
    created_at: str
    updated_at: str
    resolved_at: str | None = None
    result_note: str | None = None


class EvictionResultOut(BaseModel):
    task: EvictionTaskOut
    applied: bool
    duplicate: bool = False
    ignored_reason: str | None = None
    workloads: list[WorkloadOut] = Field(default_factory=list)


class NodeOut(BaseModel):
    name: str
    draining: bool
    pod_count: int
    created_at: str
    updated_at: str


class StatusOut(BaseModel):
    workloads: list[WorkloadOut]
    nodes: list[NodeOut]
    open_tasks: int


class HealthOut(BaseModel):
    status: Literal["ok"]
    database: str


# ---------------------------------------------------------------------------
# Node inventory reconciliation
# ---------------------------------------------------------------------------


class InventorySubmit(BaseModel):
    uids: list[str] = Field(
        description=(
            "Complete list of pod uids the agent currently observes on the "
            "node. Scope members still active here that are missing are "
            "retired; an empty list means the node reports no pods."
        ),
    )

    @field_validator("uids")
    @classmethod
    def _validate_uids(cls, value: list[str]) -> list[str]:
        if any(not isinstance(uid, str) or not (1 <= len(uid) <= 253) for uid in value):
            raise ValueError("every uid must be a non-empty string of at most 253 characters")
        if len(set(value)) != len(value):
            raise ValueError("uids must not contain duplicates")
        return value


class SettledTaskRef(BaseModel):
    task_id: str
    pod_uid: str
    workload: str
    result_note: str | None = None


class InventoryReceipt(BaseModel):
    retired_uids: list[str]
    settled_tasks: list[SettledTaskRef]
    unknown_uids: list[str] = Field(
        description=(
            "Submitted uids the coordinator has never registered. Uids of "
            "already-retired pods are not listed here (they are known uids)."
        ),
    )


class InventoryOut(BaseModel):
    inventory_id: str
    node: str
    seq: int
    state: Literal["open", "completed", "superseded"]
    scope: list[str] = Field(
        description="Uids active on the node when the inventory was issued.",
    )
    submitted_uids: list[str] | None = Field(
        default=None,
        description="The exact uid list submitted; null while the inventory is open.",
    )
    created_at: str
    updated_at: str
    completed_at: str | None = None
    receipt: InventoryReceipt | None = None
    duplicate: bool = Field(
        default=False,
        description="True when this response replays a previously applied submission.",
    )
