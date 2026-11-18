"""数据采样领域服务：管理流式数据的采样策略。

增强能力（序号18）:
  - 采样策略应用、样本特征分析、分层采样、样本保留校验、输出到debug_trace
  - 随机种子按批次独立派生，废弃全局 random.seed(42)：相同批次重试得到
    同一组样本（幂等），不同批次不再反复抽中相同事件；
  - 分层采样委托 stratified_sample 服务，样本特征携带分层总数与入选概率，
    加权特征与调试异常报告回到全量分层口径。
"""
from __future__ import annotations

import copy
import random
import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.stratified_sample import (
    DEFAULT_BASE_SEED,
    StratifiedSampleService,
    WEIGHT_VERSION_FROZEN,
)
from app.store import store


class DataSamplerService(BaseModuleService):
    MODULE = "data_sampler"
    ENTITY = "采样策略"
    LIST_FIELDS = ["策略名称", "采样方式", "采样比例", "分组键", "保留特征", "样本上限", "采样窗口"]
    REQUIRED_FIELDS = ["策略名称", "采样方式", "采样比例"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用策略": "草稿", "停用策略": "已启用"}
    NEGATIVE_ACTIONS = ["停用策略"]

    def __init__(self) -> None:
        self._stratified = StratifiedSampleService()

    def apply_sampling(
        self,
        sampler_name: str,
        data: list[dict[str, Any]],
        *,
        batch_no: int | None = None,
        base_seed: int = DEFAULT_BASE_SEED,
    ) -> dict[str, Any]:
        """对数据执行采样策略：随机采样或分层采样。

        batch_no 相同的重复调用只落一次记录（幂等）；不传时自动分配新批次号，
        每个批次使用独立随机源，避免固定种子导致相同样本反复进入报告。
        """
        sampler = None
        for r in store.rows(self.MODULE):
            if r.get("策略名称") == sampler_name:
                sampler = r
                break
        if sampler is None:
            return {"ok": False, "error": f"采样策略「{sampler_name}」不存在"}

        method = sampler.get("采样方式", "random")
        try:
            ratio = float(sampler.get("采样比例", "0.1"))
        except (ValueError, TypeError):
            ratio = 0.1
        try:
            max_samples = int(sampler.get("样本上限", "100"))
        except (ValueError, TypeError):
            max_samples = 100
        group_key = sampler.get("分组键", "")
        features = [f.strip() for f in sampler.get("保留特征", "").split(",") if f.strip()]

        # 分层采样：走全量分层口径服务，样本携带分层总数与入选概率
        if method == "stratified" and group_key:
            batch = self._stratified.record_stratified_batch(
                sampler_name, data, batch_no=batch_no, group_key=group_key,
                ratio=ratio, max_total=max_samples, base_seed=base_seed,
            )
            sampled = batch.get("_sampled_events", [])
            effective_batch_no = batch["批次号"]
        else:
            effective_batch_no, sampled = self._random_sample(
                sampler_name, data, ratio, max_samples,
                batch_no=batch_no, base_seed=base_seed,
            )

        return {
            "ok": True,
            "sampler": sampler_name,
            "batch_no": effective_batch_no,
            "method": method,
            "total": len(data),
            "sampled": len(sampled),
            "ratio": round(len(sampled) / max(len(data), 1), 4),
        }

    def _random_sample(
        self,
        sampler_name: str,
        data: list[dict[str, Any]],
        ratio: float,
        max_samples: int,
        *,
        batch_no: int | None,
        base_seed: int,
    ) -> tuple[int, list[dict[str, Any]]]:
        """随机采样：批次独立随机源 + 批次级幂等。"""
        if batch_no is not None:
            for row in store.rows(self.MODULE):
                if row.get("_batch_no") == batch_no and row.get("策略名称") == sampler_name:
                    return batch_no, copy.deepcopy(row.get("_sampled_data", []))
        if batch_no is None:
            batch_no = max(
                (int(r.get("_batch_no", 0)) for r in store.rows(self.MODULE)),
                default=0,
            ) + 1

        sample_count = min(max(1, int(len(data) * ratio)), max_samples, max(len(data), 1))
        rng = random.Random(base_seed + batch_no)
        sampled = rng.sample(data, min(sample_count, len(data))) if data else []
        annotated = [copy.deepcopy(event) for event in sampled]
        for event in annotated:
            event.setdefault("_sample_meta", {
                "batch_no": batch_no,
                "inclusion_probability": round(len(annotated) / max(len(data), 1), 6),
            })

        result_entry = {
            "id": max(
                (int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0
            ) + 1,
            "策略名称": sampler_name,
            "采样方式": "random",
            "采样比例": str(ratio),
            "分组键": "",
            "保留特征": "",
            "样本上限": str(max_samples),
            "采样窗口": "60",
            "status": "已启用",
            "pending": True,
            "abnormal": False,
            "_batch_no": batch_no,
            "_sampled_data": annotated,
            "_sample_count": len(annotated),
            "_total_count": len(data),
            "_sampled_at": time.time(),
        }
        store.rows(self.MODULE).append(result_entry)
        return batch_no, annotated

    def compute_sample_features(
        self,
        sampler_name: str,
        sampled_data: list[dict[str, Any]],
        *,
        batch_no: int | None = None,
    ) -> dict[str, Any]:
        """分析采样数据的统计特征。

        分层样本（携带 _sample_meta 且含 stratum_total）时直接回到全量分层
        口径计算加权特征；普通样本仍输出分布、均值、方差等朴素统计。
        """
        if not sampled_data:
            return {"ok": True, "sampler": sampler_name, "features": {}}

        if batch_no is not None and self._stratified.get_batch(batch_no) is not None:
            weighted = self._stratified.compute_weighted_features(
                batch_no, weight_version=WEIGHT_VERSION_FROZEN,
            )
            return weighted

        values = []
        for item in sampled_data:
            payload = item.get("payload", item)
            val = payload.get("value", 0) if isinstance(payload, dict) else 0
            if not isinstance(val, (int, float)):
                try:
                    val = float(val)
                except (ValueError, TypeError):
                    val = 0.0
            values.append(val)

        n = len(values)
        mean = sum(values) / n
        variance = sum((v - mean) ** 2 for v in values) / n
        stddev = variance ** 0.5
        sorted_vals = sorted(values)

        features = {
            "count": n,
            "mean": round(mean, 4),
            "stddev": round(stddev, 4),
            "min": sorted_vals[0] if sorted_vals else 0,
            "max": sorted_vals[-1] if sorted_vals else 0,
            "p50": sorted_vals[n // 2] if n > 0 else 0,
            "p95": sorted_vals[int(n * 0.95)] if n > 1 else (sorted_vals[0] if n > 0 else 0),
        }
        return {"ok": True, "sampler": sampler_name, "features": features}

    def list_threshold_breaches(self, sampler_name: str) -> list[dict[str, Any]]:
        """列出超出样本上限的采样策略。"""
        breaches = []
        for r in store.rows(self.MODULE):
            if r.get("策略名称") == sampler_name:
                break
        else:
            return breaches

        for r in store.rows(self.MODULE):
            if r.get("策略名称") == sampler_name and r.get("_sample_count"):
                try:
                    max_samples = int(r.get("样本上限", "100"))
                except (ValueError, TypeError):
                    max_samples = 100
                if r["_sample_count"] > max_samples:
                    breaches.append({
                        "sampler": r.get("策略名称", ""),
                        "sample_count": r["_sample_count"],
                        "limit": max_samples,
                        "exceed_by": r["_sample_count"] - max_samples,
                    })
        return breaches

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["total_samples"] = sum(
            r.get("_sample_count", 0) for r in rows if r.get("_sample_count")
        )
        return base
