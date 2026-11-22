"""Request/response schemas for the drain API."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


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
