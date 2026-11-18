"""序号18（数据采样 → 调试追踪）专用接口。

覆盖：分层采样批次、加权特征、双权重版本调试报告、报告下载、阈值核对、
调试会话观察/规则命中/并发重试/事务回滚/故障重开、跨页面待办。

路由固定路径，需在通用 /api/{module} 资源路由之前注册。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.services.debug_trace import DebugTraceService
from app.services.stratified_sample import (
    StratifiedSampleService,
    WEIGHT_VERSION_FROZEN,
    WEIGHT_VERSION_LABELS,
    WEIGHT_VERSION_LATEST,
)
from app.store import store

router = APIRouter(prefix="/api/sampling", tags=["序号18-采样调试"])

sampler = StratifiedSampleService()
tracer = DebugTraceService()


class SampleBatchPayload(BaseModel):
    sampler_name: str
    events: list[dict[str, Any]] = Field(default_factory=list)
    batch_no: int | None = None
    group_key: str | None = None
    ratio: float | None = None
    max_total: int | None = None


class WeightVersionPayload(BaseModel):
    weight_version: str = WEIGHT_VERSION_FROZEN
    lower: float | None = None
    upper: float | None = None


class RecomputePayload(BaseModel):
    updated_strata: dict[str, int] | None = None
    lower: float | None = None
    upper: float | None = None


class ThresholdPayload(BaseModel):
    lower: float
    upper: float
    weight_version: str = WEIGHT_VERSION_FROZEN


class SessionStartPayload(BaseModel):
    session_name: str
    target_operator: str
    batch_no: int


class ObservePayload(BaseModel):
    lower: float | None = None
    upper: float | None = None


class RuleHitPayload(BaseModel):
    lower: float
    upper: float


class EndSessionPayload(BaseModel):
    use_latest_weights: bool = False
    updated_strata: dict[str, int] | None = None


class RetryGroupPayload(BaseModel):
    results: list[dict[str, Any]] = Field(default_factory=list)


class ReopenPayload(BaseModel):
    new_session_name: str | None = None


def _require_ok(result: dict[str, Any]) -> dict[str, Any]:
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error", "处理失败"))
    return result


# ---- 分层采样批次 ----

@router.post("/batches")
def create_batch(payload: SampleBatchPayload) -> dict[str, Any]:
    batch = sampler.record_stratified_batch(
        payload.sampler_name,
        payload.events,
        batch_no=payload.batch_no,
        group_key=payload.group_key,
        ratio=payload.ratio,
        max_total=payload.max_total,
    )
    return {"ok": True, "batch_no": batch["批次号"], "batch": _public_batch(batch)}


@router.get("/batches")
def list_batches() -> dict[str, Any]:
    return {"ok": True, "items": [_public_batch(row) for row in sampler.list_batches()]}


@router.get("/batches/{batch_no}")
def get_batch(batch_no: int) -> dict[str, Any]:
    batch = sampler.get_batch(batch_no)
    if batch is None:
        raise HTTPException(status_code=404, detail=f"采样批次 {batch_no} 不存在")
    return {"ok": True, "batch": _public_batch(batch)}


@router.get("/batches/{batch_no}/features")
def get_features(
    batch_no: int,
    weight_version: str = WEIGHT_VERSION_FROZEN,
    lower: float | None = None,
    upper: float | None = None,
) -> dict[str, Any]:
    return _require_ok(sampler.compute_weighted_features(
        batch_no, weight_version=weight_version, lower=lower, upper=upper,
    ))


# ---- 调试异常报告（双权重版本）----

@router.post("/batches/{batch_no}/debug-report")
def create_debug_report(batch_no: int, payload: WeightVersionPayload) -> dict[str, Any]:
    if payload.weight_version not in WEIGHT_VERSION_LABELS:
        raise HTTPException(status_code=400, detail="未知权重版本")
    return _require_ok(sampler.generate_debug_report(
        batch_no,
        weight_version=payload.weight_version,
        lower=payload.lower,
        upper=payload.upper,
    ))


@router.post("/batches/{batch_no}/recompute-latest")
def recompute_latest(batch_no: int, payload: RecomputePayload) -> dict[str, Any]:
    return _require_ok(sampler.recompute_report_with_latest_weights(
        batch_no,
        updated_strata=payload.updated_strata,
        lower=payload.lower,
        upper=payload.upper,
    ))


@router.get("/reports")
def list_reports(batch_no: int | None = None) -> dict[str, Any]:
    return {"ok": True, "items": sampler.list_reports(batch_no)}


@router.get("/reports/{report_id}")
def get_report(report_id: str) -> dict[str, Any]:
    report = sampler.get_report(report_id)
    if report is None:
        raise HTTPException(status_code=404, detail=f"报告 {report_id} 不存在")
    return {"ok": True, "report": report}


@router.get("/reports/{report_id}/download")
def download_report(report_id: str) -> StreamingResponse:
    result = _require_ok(sampler.download_report_csv(report_id))
    return StreamingResponse(
        iter([result["content"]]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{result["filename"]}"'},
    )


# ---- 阈值核对 ----

@router.post("/batches/{batch_no}/threshold-review")
def threshold_review(batch_no: int, payload: ThresholdPayload) -> dict[str, Any]:
    if payload.weight_version not in WEIGHT_VERSION_LABELS:
        raise HTTPException(status_code=400, detail="未知权重版本")
    return _require_ok(sampler.review_threshold(
        batch_no,
        payload.lower,
        payload.upper,
        weight_version=payload.weight_version,
    ))


@router.get("/threshold-reviews")
def list_threshold_reviews(batch_no: int | None = None) -> dict[str, Any]:
    return {"ok": True, "items": sampler.list_threshold_reviews(batch_no)}


# ---- 调试会话 ----

@router.post("/sessions")
def start_session(payload: SessionStartPayload) -> dict[str, Any]:
    result = tracer.start_session(
        payload.session_name, payload.target_operator, payload.batch_no,
    )
    return _require_ok(result)


@router.post("/sessions/{session_name}/observe")
def observe(session_name: str, payload: ObservePayload) -> dict[str, Any]:
    return _require_ok(tracer.record_observations(
        session_name, lower=payload.lower, upper=payload.upper,
    ))


@router.post("/sessions/{session_name}/rule-hits")
def rule_hits(session_name: str, payload: RuleHitPayload) -> dict[str, Any]:
    return _require_ok(tracer.verify_rule_hits(
        session_name, payload.lower, payload.upper,
    ))


@router.post("/sessions/{session_name}/end")
def end_session(session_name: str, payload: EndSessionPayload) -> dict[str, Any]:
    return _require_ok(tracer.end_session(
        session_name,
        use_latest_weights=payload.use_latest_weights,
        updated_strata=payload.updated_strata,
    ))


@router.post("/sessions/{session_name}/retry-group")
def retry_group(session_name: str, payload: RetryGroupPayload) -> dict[str, Any]:
    return _require_ok(tracer.receive_retry_group(session_name, payload.results))


@router.post("/sessions/{session_name}/rollback")
def rollback(session_name: str) -> dict[str, Any]:
    return _require_ok(tracer.rollback_session(session_name))


@router.post("/sessions/{session_name}/reopen")
def reopen(session_name: str, payload: ReopenPayload) -> dict[str, Any]:
    return _require_ok(tracer.reopen_failed_session(
        session_name, new_session_name=payload.new_session_name,
    ))


@router.get("/sessions/{session_name}/patterns")
def session_patterns(session_name: str) -> dict[str, Any]:
    return _require_ok(tracer.analyze_anomaly_patterns(session_name))


# ---- 跨页面待办 ----

@router.get("/todos")
def list_todos(target_module: str | None = None) -> dict[str, Any]:
    items = store.list_todos(target_module)
    return {
        "ok": True,
        "total": len(items),
        "open": sum(1 for item in items if not item.get("done")),
        "items": items,
    }


@router.post("/todos/{todo_id}/resolve")
def resolve_todo(todo_id: int) -> dict[str, Any]:
    todo = store.resolve_todo(todo_id)
    if todo is None:
        raise HTTPException(status_code=404, detail=f"待办 {todo_id} 不存在")
    return {"ok": True, "todo": todo}


def _public_batch(batch: dict[str, Any]) -> dict[str, Any]:
    """输出批次时剔除大体积样本明细，保留分层口径与报告引用。"""
    return {
        "id": batch.get("id"),
        "批次号": batch.get("批次号"),
        "策略名称": batch.get("策略名称"),
        "分组键": batch.get("分组键"),
        "采样比例": batch.get("采样比例"),
        "样本上限": batch.get("样本上限"),
        "全量事件数": batch.get("全量事件数"),
        "样本事件数": batch.get("样本事件数"),
        "当前权重版本": batch.get("当前权重版本"),
        "status": batch.get("status"),
        "_strata": batch.get("_strata", []),
        "_frozen_strata": batch.get("_frozen_strata", []),
        "_latest_strata": batch.get("_latest_strata", []),
        "_debug_reports": batch.get("_debug_reports", []),
        "_threshold_reviews": batch.get("_threshold_reviews", []),
        "_current_result": batch.get("_current_result"),
        "_sampled_at": batch.get("_sampled_at"),
    }
