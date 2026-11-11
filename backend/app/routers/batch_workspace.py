"""批次水位提交工作区接口。

路由层只做参数搬运，所有状态流转、争用判断、原子边界都在 BatchCommitService 内。
接口分两组：

- /api/batch-workspace/* ：增量采集页的工作区（暂存、投递、恢复、演示控制）；
- /api/storage/{id}/receipts 与 /api/receipts：采集存储页的批次收据。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.schemas import ActionResult
from app.services.batch_commit import batch_service
from app.services.local_sink import FAILURE_MODES

router = APIRouter(prefix="/api/batch-workspace", tags=["批次水位提交"])
receipt_router = APIRouter(tags=["批次收据"])


class StagePayload(BaseModel):
    source: str
    batch_no: str
    old_watermark: str
    new_watermark: str
    records_text: str = Field(default="", description="本次离线记录，每行一条")


class CommitPayload(BaseModel):
    source: str
    batch_no: str
    target_id: int


class FailureModePayload(BaseModel):
    mode: str


@router.get("/snapshot")
def snapshot() -> dict[str, Any]:
    """工作区总览：来源当前水位、批次、收据、替身落盘文件与故障模式。"""
    data = batch_service.snapshot()
    data["failure_modes"] = FAILURE_MODES
    return data


@router.get("/sources")
def list_sources() -> dict[str, Any]:
    """来源及其当前水位，操作员据此核对旧水位是否仍然有效。"""
    return {"items": batch_service.list_sources()}


@router.get("/targets")
def list_targets() -> dict[str, Any]:
    """可投递的存储配置（连接现有采集存储模块的数据）。"""
    return {"items": batch_service.list_targets()}


@router.get("/batches")
def list_batches(source: str | None = None) -> dict[str, Any]:
    """批次列表，含暂存/受阻/已投递状态与失败步骤信息。"""
    return {"items": batch_service.list_batches(source=source)}


@router.post("/stage", response_model=ActionResult)
def stage_batch(payload: StagePayload) -> ActionResult:
    """第一步：暂存批次。只登记内容，不写存储、不动水位。"""
    batch, code, message = batch_service.stage_batch(
        source=payload.source,
        batch_no=payload.batch_no,
        old_watermark=payload.old_watermark,
        new_watermark=payload.new_watermark,
        records_text=payload.records_text,
    )
    if batch is None:
        return ActionResult(ok=False, message=message, entry={"code": code})
    return ActionResult(ok=True, message=message, entry=batch)


@router.post("/commit", response_model=ActionResult)
def commit_batch(payload: CommitPayload) -> ActionResult:
    """第二步：确认投递。写记录成功才推进水位；失败可凭同一批次号再次提交。"""
    batch, receipt, code, message = batch_service.commit_batch(
        source=payload.source,
        batch_no=payload.batch_no,
        target_id=payload.target_id,
    )
    if batch is None:
        return ActionResult(ok=False, message=message, entry={"code": code})
    return ActionResult(
        ok=True,
        message=message,
        entry={"batch": batch, "receipt": receipt, "code": code},
    )


@router.post("/failure-mode", response_model=ActionResult)
def set_failure_mode(payload: FailureModePayload) -> ActionResult:
    """切换本地存储替身的故障模式，演示成功/拒绝/响应丢失/瞬时抖动。"""
    try:
        data = batch_service.set_failure_mode(payload.mode)
    except ValueError as exc:
        return ActionResult(ok=False, message=str(exc), entry={"code": "INVALID_MODE"})
    return ActionResult(ok=True, message=data["message"], entry=data)


@router.post("/reset", response_model=ActionResult)
def reset_workspace() -> ActionResult:
    """清空批次、收据与替身文件并恢复初始水位（仅本地演示用）。"""
    return ActionResult(ok=True, message="工作区已重置", entry=batch_service.reset_workspace())


@router.post("/demo/contention")
def contention_demo() -> dict[str, Any]:
    """并发争用演示：同一旧水位的两个批次并发提交，只允许一个成功。"""
    return batch_service.run_contention_demo()


@router.get("/sink-records")
def sink_records(target_id: int, idempotency_key: str) -> dict[str, Any]:
    """打开替身文件里真实写入的记录，证明成功不是列表数字变化。"""
    return {
        "target_id": target_id,
        "idempotency_key": idempotency_key,
        "items": batch_service.read_sink_records(target_id, idempotency_key),
    }


# ----- 采集存储页使用的收据接口 -----

@receipt_router.get("/api/receipts")
def list_all_receipts(target_id: int | None = None) -> dict[str, Any]:
    """全部批次收据，可按存储目标过滤。"""
    return {"items": batch_service.list_receipts(target_id=target_id)}


@receipt_router.get("/api/receipts/{receipt_id}")
def get_receipt(receipt_id: str) -> dict[str, Any]:
    """打开一张批次收据：当次水位、存储目标、记录数与落盘路径。"""
    receipt = batch_service.get_receipt(receipt_id)
    if receipt is None:
        raise HTTPException(status_code=404, detail=f"收据 {receipt_id} 不存在")
    receipt["records"] = batch_service.read_sink_records(
        receipt["target_id"], receipt["idempotency_key"]
    )
    return receipt


@receipt_router.get("/api/storage/{target_id}/receipts")
def receipts_by_target(target_id: int) -> dict[str, Any]:
    """某一存储配置收到的全部批次收据与累计记录数。"""
    items = batch_service.list_receipts(target_id=target_id)
    return {
        "target_id": target_id,
        "total_batches": len(items),
        "total_records": sum(int(item["record_count"]) for item in items),
        "items": items,
    }
