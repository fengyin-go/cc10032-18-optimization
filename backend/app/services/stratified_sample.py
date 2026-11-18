"""分层采样领域服务（序号18）。

修复旧调试报告“直接用样本均值代表全量事件”的口径问题：

- 每个采样批次按分组键分层，样本事件携带分层总数（stratum_total）与
  入选概率（inclusion_probability），并据此得到分层权重（N_h / n_h）；
- 批次种子按批次号独立派生（不再使用全局 random.seed(42)），相同批次号
  重试得到同一组样本，不同批次不再反复抽中相同事件；
- 加权特征（加权均值、加权方差、全量分层口径异常率）回到全量分层口径，
  同时保留朴素样本均值用于对比，头部层级不再被放大；
- 调试报告区分两个权重版本：会话开始冻结权重（frozen）与会话结束按最新
  分层样本重算（latest），同一批次同一版本重复分析只更新同一条报告；
- 报告首次生成时向其他业务页面幂等登记待办，重复重试只记一次。
"""
from __future__ import annotations

import copy
import csv
import io
import random
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 权重版本：会话开始时冻结 / 会话结束时按最新分层样本重算
WEIGHT_VERSION_FROZEN = "session_start_frozen"
WEIGHT_VERSION_LATEST = "session_end_latest"
WEIGHT_VERSION_LABELS = {
    WEIGHT_VERSION_FROZEN: "会话开始冻结权重",
    WEIGHT_VERSION_LATEST: "会话结束最新权重",
}

DEFAULT_BASE_SEED = 20261118


def _event_stratum(event: dict[str, Any], group_key: str) -> str:
    payload = event.get("payload", event)
    if isinstance(payload, dict) and group_key in payload:
        return str(payload[group_key])
    if group_key in event:
        return str(event[group_key])
    return "__default__"


def _event_value(event: dict[str, Any]) -> float:
    payload = event.get("payload", event)
    val = payload.get("value", 0) if isinstance(payload, dict) else 0
    if isinstance(val, bool):
        return float(val)
    if isinstance(val, (int, float)):
        return float(val)
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _event_is_abnormal(event: dict[str, Any], lower: float, upper: float) -> bool:
    """异常判定：显式 anomaly 标记为权威标签；未标记事件再按条件范围判定越界。

    这样阈值核对既能消费人工/上游标记，也能对未标记事件按当前口径补判，
    两部分共同决定全量分层口径异常率。
    """
    payload = event.get("payload", event)
    flagged = (
        isinstance(payload, dict)
        and payload.get("anomaly") in (True, "true", "1", 1)
    ) or event.get("abnormal") is True
    if flagged:
        return True
    value = _event_value(event)
    return value < lower or value > upper


class StratifiedSampleService(BaseModuleService):
    MODULE = "stratified_sample"
    ENTITY = "分层采样批次"
    LIST_FIELDS = ["批次号", "策略名称", "分组键", "采样比例", "全量事件数", "样本事件数", "当前权重版本"]
    REQUIRED_FIELDS = ["策略名称", "分组键"]
    STATUS_ORDER = ["已采样", "已分析", "已重算"]
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS: list[str] = []

    # ------------------------------------------------------------------
    # 分层采样批次：读取源事件 → 分层样本数据（携带分层总数与入选概率）
    # ------------------------------------------------------------------

    def record_stratified_batch(
        self,
        sampler_name: str,
        events: list[dict[str, Any]],
        *,
        batch_no: int | None = None,
        group_key: str | None = None,
        ratio: float | None = None,
        max_total: int | None = None,
        base_seed: int = DEFAULT_BASE_SEED,
    ) -> dict[str, Any]:
        """对一批源事件执行分层采样并落一条批次记录。

        幂等：相同 batch_no 的并发/重试请求只落一次记录，直接返回首次结果。
        """
        if batch_no is None:
            batch_no = self._next_batch_no()
        existing = self.get_batch(batch_no)
        if existing is not None:
            return existing

        sampler = self._find_sampler(sampler_name)
        if group_key is None:
            group_key = str((sampler or {}).get("分组键", "region")) or "region"
        if ratio is None:
            ratio = self._to_float((sampler or {}).get("采样比例"), 0.2)
        if max_total is None:
            max_total = int(self._to_float((sampler or {}).get("样本上限"), 200))
        ratio = min(max(ratio, 0.0), 1.0)

        # 分层（保持首次出现顺序，报告层顺序稳定）
        strata_order: list[str] = []
        strata: dict[str, list[dict[str, Any]]] = {}
        for event in events:
            key = _event_stratum(event, group_key)
            if key not in strata:
                strata[key] = []
                strata_order.append(key)
            strata[key].append(event)

        total_n = len(events)
        # 目标样本量先按比例折算，再受样本上限约束
        target_n = min(int(total_n * ratio), max_total, total_n) if total_n else 0
        target_n = max(target_n, 0)
        allocations = self._allocate_proportional(strata, strata_order, ratio, target_n)

        # 批次号派生独立种子：不再污染全局随机状态，批次之间样本不再雷同
        rng = random.Random(base_seed + batch_no)
        sampled_events: list[dict[str, Any]] = []
        strata_meta: list[dict[str, Any]] = []
        for key in strata_order:
            items = strata[key]
            n_h = allocations[key]
            n_total_h = len(items)
            inclusion = (n_h / n_total_h) if n_total_h else 0.0
            chosen_idx = set(rng.sample(range(n_total_h), n_h)) if n_h else set()
            for idx in sorted(chosen_idx):
                annotated = copy.deepcopy(items[idx])
                annotated["_sample_meta"] = {
                    "batch_no": batch_no,
                    "stratum": key,
                    "stratum_total": n_total_h,
                    "inclusion_probability": round(inclusion, 6),
                    "weight": round(n_total_h / n_h, 6) if n_h else None,
                }
                sampled_events.append(annotated)
            strata_meta.append({
                "stratum": key,
                "stratum_total": n_total_h,
                "sample_count": n_h,
                "inclusion_probability": round(inclusion, 6),
                "weight": round(n_total_h / n_h, 6) if n_h else None,
                "weight_share": round(n_total_h / total_n, 6) if total_n else 0.0,
            })

        batch = {
            "id": max(
                (int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0
            ) + 1,
            "批次号": batch_no,
            "策略名称": sampler_name,
            "分组键": group_key,
            "采样比例": round(ratio, 6),
            "样本上限": max_total,
            "全量事件数": total_n,
            "样本事件数": len(sampled_events),
            "当前权重版本": WEIGHT_VERSION_FROZEN,
            "status": "已采样",
            "pending": True,
            "abnormal": False,
            "_seed": base_seed + batch_no,
            "_strata": strata_meta,
            # 会话开始时冻结的分层口径；latest 版本在重算前与之相同
            "_frozen_strata": copy.deepcopy(strata_meta),
            "_latest_strata": copy.deepcopy(strata_meta),
            "_sampled_events": sampled_events,
            "_debug_reports": [],
            "_threshold_reviews": [],
            "_current_result": None,
            "_sampled_at": time.time(),
        }
        store.rows(self.MODULE).append(batch)
        return batch

    @staticmethod
    def _allocate_proportional(
        strata: dict[str, list[dict[str, Any]]],
        order: list[str],
        ratio: float,
        target_n: int,
    ) -> dict[str, int]:
        """按分层比例做最大余数法分配，约束 0 <= n_h <= N_h。"""
        total_n = sum(len(strata[k]) for k in order)
        if total_n == 0 or target_n == 0:
            return {key: 0 for key in order}
        raw = {key: len(strata[key]) / total_n * target_n for key in order}
        alloc = {key: min(len(strata[key]), int(raw[key])) for key in order}
        leftover = target_n - sum(alloc.values())
        fractions = sorted(
            ((raw[key] - int(raw[key]), key) for key in order), reverse=True,
        )
        while leftover > 0:
            progressed = False
            for _, key in fractions:
                if leftover <= 0:
                    break
                cap = len(strata[key])
                if alloc[key] < cap:
                    alloc[key] += 1
                    leftover -= 1
                    progressed = True
            if not progressed:
                break
        return alloc

    def get_batch(self, batch_no: int) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if row.get("批次号") == batch_no:
                return row
        return None

    def list_batches(self) -> list[dict[str, Any]]:
        return list(store.rows(self.MODULE))

    # ------------------------------------------------------------------
    # 加权特征：回到全量分层口径，样本只通过权重代表全量
    # ------------------------------------------------------------------

    def compute_weighted_features(
        self,
        batch_no: int,
        *,
        weight_version: str = WEIGHT_VERSION_FROZEN,
        lower: float | None = None,
        upper: float | None = None,
    ) -> dict[str, Any]:
        batch = self.get_batch(batch_no)
        if batch is None:
            return {"ok": False, "error": f"采样批次 {batch_no} 不存在"}
        strata_meta = self._weights_for_version(batch, weight_version)
        events = batch.get("_sampled_events", [])
        by_stratum: dict[str, list[dict[str, Any]]] = {}
        for event in events:
            meta = event.get("_sample_meta", {})
            by_stratum.setdefault(str(meta.get("stratum", "__default__")), []).append(event)

        values = [_event_value(event) for event in events]
        naive_mean = round(sum(values) / len(values), 6) if values else 0.0

        # 默认条件范围：加权均值 ± 3 倍加权标准差
        weighted_mean = self._weighted_mean(strata_meta, by_stratum)
        weighted_var = self._weighted_variance(strata_meta, by_stratum, weighted_mean)
        weighted_std = weighted_var ** 0.5
        if lower is None:
            lower = weighted_mean - 3 * weighted_std
        if upper is None:
            upper = weighted_mean + 3 * weighted_std

        strata_summary: list[dict[str, Any]] = []
        estimated_population = 0.0
        estimated_anomalies = 0.0
        for meta in strata_meta:
            key = meta["stratum"]
            items = by_stratum.get(key, [])
            n_h = meta["sample_count"]
            n_total_h = meta["stratum_total"]
            sample_mean = (
                sum(_event_value(item) for item in items) / len(items) if items else 0.0
            )
            abnormal_in_sample = sum(
                1 for item in items if _event_is_abnormal(item, lower, upper)
            )
            sample_anomaly_rate = abnormal_in_sample / n_h if n_h else 0.0
            # 全量分层口径：用样本率估计该层全量异常事件数
            est_anomaly_h = sample_anomaly_rate * n_total_h
            estimated_anomalies += est_anomaly_h
            estimated_population += n_total_h
            strata_summary.append({
                "stratum": key,
                "stratum_total": n_total_h,
                "sample_count": n_h,
                "inclusion_probability": meta["inclusion_probability"],
                "weight": meta["weight"],
                "weight_share": meta["weight_share"],
                "sample_mean": round(sample_mean, 6),
                "sample_anomaly_rate": round(sample_anomaly_rate, 6),
                "estimated_stratum_anomalies": round(est_anomaly_h, 4),
                "weighted_anomaly_rate": round(sample_anomaly_rate, 6),
            })

        weighted_anomaly_rate = (
            estimated_anomalies / estimated_population if estimated_population else 0.0
        )
        return {
            "ok": True,
            "batch_no": batch_no,
            "weight_version": weight_version,
            "weight_version_label": WEIGHT_VERSION_LABELS.get(weight_version, weight_version),
            # 全量总数按当前权重版本的分层合计：latest 重算后跟随最新口径
            "population_total": int(round(estimated_population)) or batch.get("全量事件数", 0),
            "sample_total": len(events),
            "naive_sample_mean": naive_mean,
            "weighted_mean": round(weighted_mean, 6),
            "weighted_stddev": round(weighted_std, 6),
            "weighted_anomaly_rate": round(weighted_anomaly_rate, 6),
            "estimated_population_anomalies": round(estimated_anomalies, 4),
            "condition_range": {"lower": round(lower, 6), "upper": round(upper, 6)},
            "strata": strata_summary,
            "uncovered_strata": [
                meta["stratum"] for meta in strata_meta if meta["sample_count"] == 0
            ],
        }

    @staticmethod
    def _weighted_mean(
        strata_meta: list[dict[str, Any]],
        by_stratum: dict[str, list[dict[str, Any]]],
    ) -> float:
        total_pop = sum(meta["stratum_total"] for meta in strata_meta)
        if total_pop == 0:
            return 0.0
        acc = 0.0
        for meta in strata_meta:
            items = by_stratum.get(meta["stratum"], [])
            if not items:
                continue
            stratum_mean = sum(_event_value(item) for item in items) / len(items)
            acc += stratum_mean * meta["stratum_total"]
        return acc / total_pop

    @staticmethod
    def _weighted_variance(
        strata_meta: list[dict[str, Any]],
        by_stratum: dict[str, list[dict[str, Any]]],
        weighted_mean: float,
    ) -> float:
        total_pop = sum(meta["stratum_total"] for meta in strata_meta)
        if total_pop == 0:
            return 0.0
        acc = 0.0
        for meta in strata_meta:
            items = by_stratum.get(meta["stratum"], [])
            if not items:
                continue
            mean_h = sum(_event_value(item) for item in items) / len(items)
            var_h = sum((_event_value(item) - mean_h) ** 2 for item in items) / len(items)
            share = meta["stratum_total"] / total_pop
            acc += share * (var_h + (mean_h - weighted_mean) ** 2)
        return acc

    def _weights_for_version(
        self, batch: dict[str, Any], weight_version: str,
    ) -> list[dict[str, Any]]:
        """取指定版本的分层权重；latest 读取批次上记录的最新分层口径。"""
        if weight_version == WEIGHT_VERSION_LATEST:
            source = batch.get("_latest_strata", batch.get("_strata", []))
        else:
            source = batch.get("_frozen_strata", batch.get("_strata", []))
        return copy.deepcopy(source)

    # ------------------------------------------------------------------
    # 调试异常报告：两个权重版本，同批次同版本重复分析只更新一条报告
    # ------------------------------------------------------------------

    def generate_debug_report(
        self,
        batch_no: int,
        *,
        weight_version: str = WEIGHT_VERSION_FROZEN,
        lower: float | None = None,
        upper: float | None = None,
    ) -> dict[str, Any]:
        batch = self.get_batch(batch_no)
        if batch is None:
            return {"ok": False, "error": f"采样批次 {batch_no} 不存在"}
        features = self.compute_weighted_features(
            batch_no, weight_version=weight_version, lower=lower, upper=upper,
        )
        report_id = f"R-{batch_no}-{weight_version}"
        report = {
            "report_id": report_id,
            "batch_no": batch_no,
            "sampler": batch.get("策略名称", ""),
            "weight_version": weight_version,
            "weight_version_label": WEIGHT_VERSION_LABELS.get(weight_version, weight_version),
            "features": features,
            "sample_count": batch.get("样本事件数", 0),
            "population_total": batch.get("全量事件数", 0),
            "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "revision": 1,
        }
        self._upsert_report(batch, report)
        batch["_current_result"] = copy.deepcopy(report)
        batch["当前权重版本"] = weight_version
        batch["status"] = "已分析" if weight_version == WEIGHT_VERSION_FROZEN else "已重算"
        self._spawn_followup_todos(batch, report)
        return {"ok": True, "report": report}

    def recompute_report_with_latest_weights(
        self,
        batch_no: int,
        *,
        updated_strata: dict[str, int] | None = None,
        lower: float | None = None,
        upper: float | None = None,
    ) -> dict[str, Any]:
        """会话结束时点分层比例已变化：按最新分层样本重算 latest 版本。

        updated_strata 形如 {层名: 最新全量事件数}；样本数不变，入选概率与
        权重按最新分层总数重算。两种口径各自形成独立报告版本。
        """
        batch = self.get_batch(batch_no)
        if batch is None:
            return {"ok": False, "error": f"采样批次 {batch_no} 不存在"}
        if updated_strata:
            latest = []
            for meta in batch.get("_frozen_strata", []):
                key = meta["stratum"]
                new_total = int(updated_strata.get(key, meta["stratum_total"]))
                n_h = meta["sample_count"]
                inclusion = (n_h / new_total) if new_total else 0.0
                latest.append({
                    "stratum": key,
                    "stratum_total": new_total,
                    "sample_count": n_h,
                    "inclusion_probability": round(inclusion, 6),
                    "weight": round(new_total / n_h, 6) if n_h else None,
                    "weight_share": round(
                        new_total / sum(updated_strata.values()), 6,
                    ) if sum(updated_strata.values()) else 0.0,
                })
            batch["_latest_strata"] = latest
        return self.generate_debug_report(
            batch_no, weight_version=WEIGHT_VERSION_LATEST, lower=lower, upper=upper,
        )

    def _upsert_report(
        self, batch: dict[str, Any], report: dict[str, Any],
    ) -> dict[str, Any]:
        reports = batch.setdefault("_debug_reports", [])
        for idx, existing in enumerate(reports):
            if existing["report_id"] == report["report_id"]:
                # 同批同版本重复分析：只更新这一个报告版本，不新增记录
                report["revision"] = existing.get("revision", 1) + 1
                reports[idx] = report
                return report
        reports.append(report)
        return report

    def list_reports(self, batch_no: int | None = None) -> list[dict[str, Any]]:
        if batch_no is not None:
            batch = self.get_batch(batch_no)
            return list((batch or {}).get("_debug_reports", []))
        result: list[dict[str, Any]] = []
        for row in store.rows(self.MODULE):
            result.extend(row.get("_debug_reports", []))
        return result

    def get_report(self, report_id: str) -> dict[str, Any] | None:
        for report in self.list_reports():
            if report.get("report_id") == report_id:
                return report
        return None

    def download_report_csv(self, report_id: str) -> dict[str, Any]:
        """下载与该权重版本一致的样本报告（每层权重 + 加权异常率 + 样本明细）。"""
        report = self.get_report(report_id)
        if report is None:
            return {"ok": False, "error": f"报告 {report_id} 不存在"}
        batch = self.get_batch(report["batch_no"])
        output = io.StringIO()
        output.write("\ufeff")
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow([
            "报告编号", "批次号", "权重版本", "权重版本说明",
            "全量事件数", "样本事件数", "加权异常率", "生成时间", "修订版本",
        ])
        features = report["features"]
        writer.writerow([
            report["report_id"], report["batch_no"], report["weight_version"],
            report["weight_version_label"], report["population_total"],
            report["sample_count"], features["weighted_anomaly_rate"],
            report["generated_at"], report["revision"],
        ])
        writer.writerow([])
        writer.writerow([
            "分层", "分层总数", "样本数", "入选概率", "分层权重", "全量占比",
            "样本均值", "样本异常率", "估计层内异常数", "加权异常率",
        ])
        for meta in features["strata"]:
            writer.writerow([
                meta["stratum"], meta["stratum_total"], meta["sample_count"],
                meta["inclusion_probability"], meta["weight"], meta["weight_share"],
                meta["sample_mean"], meta["sample_anomaly_rate"],
                meta["estimated_stratum_anomalies"], meta["weighted_anomaly_rate"],
            ])
        writer.writerow([])
        writer.writerow(["样本事件ID", "分层", "分层总数", "入选概率", "分层权重", "value"])
        for event in (batch or {}).get("_sampled_events", []):
            sample_meta = event.get("_sample_meta", {})
            writer.writerow([
                event.get("event_id", ""), sample_meta.get("stratum", ""),
                sample_meta.get("stratum_total", ""),
                sample_meta.get("inclusion_probability", ""),
                sample_meta.get("weight", ""), _event_value(event),
            ])
        return {
            "ok": True,
            "filename": f"stratified_report_{report_id}.csv",
            "content": output.getvalue(),
            "report": report,
        }

    # ------------------------------------------------------------------
    # 阈值核对：当前口径设定条件范围，判定分层样本数据
    # ------------------------------------------------------------------

    def review_threshold(
        self,
        batch_no: int,
        lower: float,
        upper: float,
        *,
        weight_version: str = WEIGHT_VERSION_FROZEN,
    ) -> dict[str, Any]:
        batch = self.get_batch(batch_no)
        if batch is None:
            return {"ok": False, "error": f"采样批次 {batch_no} 不存在"}
        features = self.compute_weighted_features(
            batch_no, weight_version=weight_version, lower=lower, upper=upper,
        )
        hit_strata = [
            {
                "stratum": item["stratum"],
                "sample_anomaly_rate": item["sample_anomaly_rate"],
                "estimated_stratum_anomalies": item["estimated_stratum_anomalies"],
                "weight": item["weight"],
            }
            for item in features["strata"]
            if item["sample_anomaly_rate"] > 0
        ]
        review = {
            "review_id": f"T-{batch_no}-{weight_version}-{len(batch.get('_threshold_reviews', [])) + 1}",
            "batch_no": batch_no,
            "weight_version": weight_version,
            "weight_version_label": WEIGHT_VERSION_LABELS.get(weight_version, weight_version),
            "condition_range": features["condition_range"],
            "weighted_anomaly_rate": features["weighted_anomaly_rate"],
            "hit_strata": hit_strata,
            "passed": features["weighted_anomaly_rate"] == 0,
            "checked_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        batch.setdefault("_threshold_reviews", []).append(review)
        return {"ok": True, "review": review, "features": features}

    def list_threshold_reviews(self, batch_no: int | None = None) -> list[dict[str, Any]]:
        if batch_no is not None:
            batch = self.get_batch(batch_no)
            return list((batch or {}).get("_threshold_reviews", []))
        result: list[dict[str, Any]] = []
        for row in store.rows(self.MODULE):
            result.extend(row.get("_threshold_reviews", []))
        return result

    # ------------------------------------------------------------------
    # 跨页面待办：报告首次生成时幂等登记，重复重试只记一次
    # ------------------------------------------------------------------

    def _spawn_followup_todos(
        self, batch: dict[str, Any], report: dict[str, Any],
    ) -> None:
        batch_no = batch["批次号"]
        rate = report["features"]["weighted_anomaly_rate"]
        if rate <= 0:
            return
        store.add_todo(
            "alert_policy",
            f"stratified:{batch_no}:alert",
            f"分层批次{batch_no}加权异常率 {rate:.2%}，需核对告警策略",
            {"batch_no": batch_no, "report_id": report["report_id"],
             "weight_version": report["weight_version"]},
        )
        store.add_todo(
            "debug_trace",
            f"stratified:{batch_no}:trace",
            f"分层批次{batch_no}调试报告已生成，待调试追踪复核",
            {"batch_no": batch_no, "report_id": report["report_id"]},
        )
        store.add_todo(
            "aggregate_sink",
            f"stratified:{batch_no}:sink",
            f"分层批次{batch_no}全量口径异常估计待聚合落库",
            {"batch_no": batch_no, "report_id": report["report_id"]},
        )

    # ------------------------------------------------------------------
    # 辅助
    # ------------------------------------------------------------------

    def _next_batch_no(self) -> int:
        return max(
            (int(row.get("批次号", 0)) for row in store.rows(self.MODULE)), default=0
        ) + 1

    def _find_sampler(self, sampler_name: str) -> dict[str, Any] | None:
        for row in store.rows("data_sampler"):
            if row.get("策略名称") == sampler_name:
                return row
        return None

    @staticmethod
    def _to_float(value: Any, default: float) -> float:
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["batch_total"] = len(rows)
        base["report_total"] = sum(len(r.get("_debug_reports", [])) for r in rows)
        base["sample_total"] = sum(int(r.get("样本事件数", 0)) for r in rows)
        return base
