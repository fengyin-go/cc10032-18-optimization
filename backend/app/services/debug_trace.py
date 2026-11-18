"""调试追踪领域服务：针对特定算子启动调试会话。

增强能力（序号18 调试追踪侧）:
  - 会话按采样批次读取分层样本数据，用分层权重生成异常观察，并与目标算子
    状态对应；会话开始时冻结当时的权重版本；
  - 异常率回到全量分层口径，报告标明所用权重版本（冻结 / 最新）；
  - 同批样本重复分析只更新同一报告版本，并发重试按当前标准接收整组结果，
    事务失败退回同一口径；故障会话可在原口径上重开。
"""
from __future__ import annotations

import copy
import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.stratified_sample import (
    StratifiedSampleService,
    WEIGHT_VERSION_FROZEN,
    WEIGHT_VERSION_LATEST,
    WEIGHT_VERSION_LABELS,
    _event_is_abnormal,
    _event_value,
)
from app.store import store


class DebugTraceService(BaseModuleService):
    MODULE = "debug_trace"
    ENTITY = "调试会话"
    LIST_FIELDS = ["会话名称", "目标算子", "采样数据", "中间结果", "异常标记", "开始时间", "结束时间"]
    REQUIRED_FIELDS = ["会话名称", "目标算子", "采样数据"]
    STATUS_ORDER = ["运行中", "已完成", "已终止"]
    ACTION_RULES = {"终止会话": "运行中"}
    NEGATIVE_ACTIONS: list[str] = []

    def __init__(self) -> None:
        self._stratified = StratifiedSampleService()

    # ------------------------------------------------------------------
    # 会话生命周期
    # ------------------------------------------------------------------

    def start_session(
        self,
        session_name: str,
        target_operator: str,
        batch_no: int,
    ) -> dict[str, Any]:
        """创建调试会话并冻结会话开始时的分层权重。

        同一会话名重复提交只落一次记录：已存在则直接返回原会话（幂等）。
        """
        existing = self._find_session(session_name)
        if existing is not None:
            return {"ok": True, "session": existing, "idempotent": True}

        batch = self._stratified.get_batch(batch_no)
        if batch is None:
            return {"ok": False, "error": f"采样批次 {batch_no} 不存在，无法启动调试"}

        start_time = time.strftime("%Y-%m-%d %H:%M:%S")
        session = {
            "id": max(
                (int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0
            ) + 1,
            "会话名称": session_name,
            "目标算子": target_operator,
            "采样数据": f"批次{batch_no}",
            "中间结果": str(batch.get("样本事件数", 0)),
            "异常标记": "none",
            "开始时间": start_time,
            "结束时间": "",
            "status": "运行中",
            "pending": True,
            "abnormal": False,
            "_batch_no": batch_no,
            "_weight_version": WEIGHT_VERSION_FROZEN,
            "_frozen_weights": copy.deepcopy(batch.get("_frozen_strata", [])),
            "_observations": [],
            "_rule_hits": [],
            "_report_refs": [],
            "_reopened_from": None,
        }
        store.rows(self.MODULE).append(session)
        return {"ok": True, "session": session, "idempotent": False}

    def record_observations(
        self,
        session_name: str,
        *,
        lower: float | None = None,
        upper: float | None = None,
    ) -> dict[str, Any]:
        """按采样批次读取分层样本，用分层权重生成异常观察。

        每条观察携带分层权重、入选概率与目标算子状态，异常率按全量分层口径
        汇总；同批样本重复调用只更新该会话的观察集合，不新增会话记录。
        """
        session = self._find_session(session_name)
        if session is None:
            return {"ok": False, "error": f"调试会话「{session_name}」不存在"}

        batch_no = session.get("_batch_no")
        batch = self._stratified.get_batch(batch_no)
        if batch is None:
            return {"ok": False, "error": f"采样批次 {batch_no} 已丢失"}

        weight_version = session.get("_weight_version", WEIGHT_VERSION_FROZEN)
        features = self._stratified.compute_weighted_features(
            batch_no, weight_version=weight_version, lower=lower, upper=upper,
        )
        condition = features["condition_range"]

        observations: list[dict[str, Any]] = []
        for event in batch.get("_sampled_events", []):
            meta = event.get("_sample_meta", {})
            is_abnormal = _event_is_abnormal(
                event, condition["lower"], condition["upper"],
            )
            observations.append({
                "event_id": event.get("event_id", ""),
                "stratum": meta.get("stratum"),
                "value": _event_value(event),
                "stratum_total": meta.get("stratum_total"),
                "inclusion_probability": meta.get("inclusion_probability"),
                "weight": meta.get("weight"),
                "is_abnormal": is_abnormal,
                "operator": session.get("目标算子", ""),
                "operator_state": self._operator_state(session.get("目标算子", "")),
            })
        session["_observations"] = observations
        session["异常标记"] = (
            "weighted_anomaly" if features["weighted_anomaly_rate"] > 0 else "none"
        )
        session["abnormal"] = features["weighted_anomaly_rate"] > 0
        return {
            "ok": True,
            "session": session_name,
            "weight_version": weight_version,
            "weight_version_label": WEIGHT_VERSION_LABELS.get(weight_version, weight_version),
            "observations": observations,
            "weighted_anomaly_rate": features["weighted_anomaly_rate"],
            "condition_range": condition,
        }

    def verify_rule_hits(
        self,
        session_name: str,
        lower: float,
        upper: float,
    ) -> dict[str, Any]:
        """详情页规则命中校验：当前口径条件范围内逐条核对观察。"""
        session = self._find_session(session_name)
        if session is None:
            return {"ok": False, "error": f"调试会话「{session_name}」不存在"}
        observations = session.get("_observations") or []
        if not observations:
            recorded = self.record_observations(
                session_name, lower=lower, upper=upper,
            )
            if not recorded.get("ok"):
                return recorded
            observations = recorded["observations"]

        hits = [
            {
                "event_id": item["event_id"],
                "stratum": item["stratum"],
                "value": item["value"],
                "weight": item["weight"],
                "rule": f"value 不在 [{lower}, {upper}]",
            }
            for item in observations
            if item["is_abnormal"]
        ]
        review = {
            "session": session_name,
            "condition_range": {"lower": lower, "upper": upper},
            "hit_count": len(hits),
            "hits": hits,
            "checked_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        session.setdefault("_rule_hits", []).append(review)
        return {"ok": True, **review}

    def end_session(
        self,
        session_name: str,
        *,
        use_latest_weights: bool = False,
        updated_strata: dict[str, int] | None = None,
    ) -> dict[str, Any]:
        """结束会话并生成与权重版本收敛的调试报告。

        use_latest_weights=False（默认）：按会话开始冻结权重出报告；
        use_latest_weights=True：按结束时最新分层样本重算，两种口径各自形成
        不同报告版本并写入分层样本数据的当前结果。
        """
        session = self._find_session(session_name)
        if session is None:
            return {"ok": False, "error": f"调试会话「{session_name}」不存在"}
        if session.get("status") != "运行中":
            return {"ok": False, "error": f"会话状态为「{session.get('status')}」，不可结束"}

        batch_no = session.get("_batch_no")
        if use_latest_weights:
            result = self._stratified.recompute_report_with_latest_weights(
                batch_no, updated_strata=updated_strata,
            )
            session["_weight_version"] = WEIGHT_VERSION_LATEST
        else:
            result = self._stratified.generate_debug_report(
                batch_no, weight_version=WEIGHT_VERSION_FROZEN,
            )
            session["_weight_version"] = WEIGHT_VERSION_FROZEN
        if not result.get("ok"):
            # 事务失败：会话退回原口径，不写入结束状态
            return result

        report = result["report"]
        session["结束时间"] = time.strftime("%Y-%m-%d %H:%M:%S")
        session["status"] = "已完成"
        session["pending"] = False
        session["异常标记"] = (
            "weighted_anomaly"
            if report["features"]["weighted_anomaly_rate"] > 0 else "none"
        )
        refs = session.setdefault("_report_refs", [])
        if report["report_id"] not in refs:
            refs.append(report["report_id"])
        return {"ok": True, "session": session_name, "report": report}

    def receive_retry_group(
        self,
        session_name: str,
        results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """并发重试按当前标准接收整组结果：同组结果只处理一次。

        以结果集的幂等键去重；重复重试只记一次，部分失败则整组不生效。
        """
        session = self._find_session(session_name)
        if session is None:
            return {"ok": False, "error": f"调试会话「{session_name}」不存在"}

        group_key = "|".join(sorted(str(item.get("event_id", "")) for item in results))
        received = session.setdefault("_retry_groups", {})
        if group_key in received:
            return {
                "ok": True, "idempotent": True,
                "accepted": received[group_key]["accepted"],
                "message": "该重试组已接收，重复重试只记一次",
            }
        if any(item.get("error") for item in results):
            # 事务失败退回同一口径：整组拒绝，会话维持原状态
            return {
                "ok": False,
                "error": "重试组包含失败结果，整组退回，会话保持原口径",
                "rejected": len(results),
            }

        accepted = copy.deepcopy(results)
        received[group_key] = {"accepted": len(accepted), "at": time.time()}
        return {"ok": True, "idempotent": False, "accepted": len(accepted)}

    def rollback_session(self, session_name: str) -> dict[str, Any]:
        """事务失败退回同一口径：撤销未完成的观察与状态变更。"""
        session = self._find_session(session_name)
        if session is None:
            return {"ok": False, "error": f"调试会话「{session_name}」不存在"}
        if session.get("status") != "运行中":
            return {"ok": False, "error": "仅运行中会话可回滚"}
        snapshot = {
            "weight_version": session.get("_weight_version"),
            "observations": len(session.get("_observations", [])),
            "rule_hits": len(session.get("_rule_hits", [])),
        }
        session["_observations"] = []
        session["_rule_hits"] = []
        session["异常标记"] = "none"
        session["abnormal"] = False
        session["_weight_version"] = WEIGHT_VERSION_FROZEN
        return {"ok": True, "session": session_name, "rolled_back": snapshot}

    def reopen_failed_session(
        self,
        session_name: str,
        new_session_name: str | None = None,
    ) -> dict[str, Any]:
        """序号18故障重开：在原采样批次与冻结权重口径上重开调试会话。"""
        session = self._find_session(session_name)
        if session is None:
            return {"ok": False, "error": f"调试会话「{session_name}」不存在"}
        if session.get("status") not in ("已终止", "已完成"):
            return {"ok": False, "error": "仅已终止或已完成的故障会话可以重开"}

        batch_no = session.get("_batch_no")
        reopened_name = new_session_name or f"{session_name}_重开"
        started = self.start_session(
            reopened_name, session.get("目标算子", ""), batch_no,
        )
        if not started.get("ok"):
            return started
        new_session = started["session"]
        new_session["_reopened_from"] = session_name
        new_session["_frozen_weights"] = copy.deepcopy(session.get("_frozen_weights", []))
        new_session["_weight_version"] = WEIGHT_VERSION_FROZEN
        return {
            "ok": True,
            "reopened_session": reopened_name,
            "batch_no": batch_no,
            "weight_version": WEIGHT_VERSION_FROZEN,
        }

    # ------------------------------------------------------------------
    # 查询与统计（保留原接口签名，统计口径升级为全量分层异常率）
    # ------------------------------------------------------------------

    def analyze_anomaly_patterns(
        self, session_name: str,
    ) -> dict[str, Any]:
        """分析调试会话中的异常标记模式（加权口径）。"""
        session = self._find_session(session_name)
        if session is None:
            return {"ok": False, "error": f"调试会话「{session_name}」不存在"}

        target_operator = session.get("目标算子", "")
        weight_version = session.get("_weight_version", WEIGHT_VERSION_FROZEN)
        batch_no = session.get("_batch_no")
        weighted_rate = 0.0
        if batch_no is not None:
            features = self._stratified.compute_weighted_features(
                batch_no, weight_version=weight_version,
            )
            if features.get("ok"):
                weighted_rate = features["weighted_anomaly_rate"]

        patterns = {
            "anomaly": session.get("异常标记", "none"),
            "intermediate_count": session.get("中间结果", ""),
            "sampled_data_ref": session.get("采样数据", ""),
            "duration": f"{session.get('开始时间', '')} → {session.get('结束时间', '')}",
            "weight_version": weight_version,
            "weight_version_label": WEIGHT_VERSION_LABELS.get(weight_version, weight_version),
            "weighted_anomaly_rate": weighted_rate,
            "observation_count": len(session.get("_observations", [])),
            "rule_hit_count": len(session.get("_rule_hits", [])),
        }

        linked_states = [
            s for s in store.rows("state_store")
            if target_operator in str(s.get("_operator", ""))
        ]
        patterns["linked_state_entries"] = len(linked_states)

        return {
            "ok": True,
            "session": session_name,
            "target": target_operator,
            "patterns": patterns,
            "status": session.get("status", ""),
        }

    def complete_session_with_analysis(
        self, session_name: str,
    ) -> dict[str, Any]:
        """完成调试会话并生成分析报告（冻结权重口径，保持旧入口可用）。"""
        session = self._find_session(session_name)
        if session is None:
            return {"ok": False, "error": f"调试会话「{session_name}」不存在"}
        if session.get("status") != "运行中":
            return {"ok": False, "error": f"会话状态为「{session.get('status')}」, 不可完成"}

        ended = self.end_session(session_name, use_latest_weights=False)
        if not ended.get("ok"):
            return ended
        report = ended["report"]
        analysis = {
            "target": session.get("目标算子", ""),
            "sampled_data_present": bool(session.get("采样数据")),
            "intermediate_count": session.get("中间结果"),
            "anomaly_detected": report["features"]["weighted_anomaly_rate"] > 0,
            "weighted_anomaly_rate": report["features"]["weighted_anomaly_rate"],
            "weight_version": report["weight_version"],
            "report_id": report["report_id"],
            "completed_at": session.get("结束时间"),
        }
        return {"ok": True, "session": session_name, "analysis": analysis}

    def list_session_statistics(self) -> dict[str, Any]:
        """列出所有调试会话的统计信息（异常率按会话加权口径汇总）。"""
        sessions = [r for r in store.rows(self.MODULE) if r.get("id", 0) < 1000]
        running = sum(1 for s in sessions if s.get("status") == "运行中")
        completed = sum(1 for s in sessions if s.get("status") == "已完成")
        terminated = sum(1 for s in sessions if s.get("status") == "已终止")
        anomalous = sum(1 for s in sessions if s.get("异常标记") != "none")

        weighted_rates: list[float] = []
        for session in sessions:
            batch_no = session.get("_batch_no")
            if batch_no is None:
                continue
            features = self._stratified.compute_weighted_features(
                batch_no,
                weight_version=session.get("_weight_version", WEIGHT_VERSION_FROZEN),
            )
            if features.get("ok"):
                weighted_rates.append(features["weighted_anomaly_rate"])

        return {
            "total": len(sessions),
            "running": running,
            "completed": completed,
            "terminated": terminated,
            "with_anomalies": anomalous,
            "anomaly_rate": anomalous / max(len(sessions), 1),
            "avg_weighted_anomaly_rate": (
                round(sum(weighted_rates) / len(weighted_rates), 6)
                if weighted_rates else 0.0
            ),
        }

    def link_to_sampler(self, session_name: str) -> dict[str, Any]:
        """将调试会话关联到采样批次与采样策略。"""
        session = self._find_session(session_name)
        if session is None:
            return {"ok": False, "error": f"调试会话「{session_name}」不存在"}

        batch_no = session.get("_batch_no")
        linked: list[dict[str, Any]] = []
        if batch_no is not None:
            batch = self._stratified.get_batch(batch_no)
            if batch is not None:
                linked.append({
                    "sampler": batch.get("策略名称", ""),
                    "batch_no": batch_no,
                    "sample_count": batch.get("样本事件数", 0),
                    "weight_version": session.get("_weight_version"),
                })
        session["_linked_samplers"] = linked

        return {
            "ok": True,
            "session": session_name,
            "linked_samplers": len(linked),
            "details": linked,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["running_sessions"] = sum(1 for r in rows if r.get("status") == "运行中")
        base["completed_sessions"] = sum(1 for r in rows if r.get("status") == "已完成")
        return base

    # ------------------------------------------------------------------
    # 辅助
    # ------------------------------------------------------------------

    def _find_session(self, session_name: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if row.get("会话名称") == session_name:
                return row
        return None

    @staticmethod
    def _operator_state(target_operator: str) -> str:
        for row in store.rows("calc_operator"):
            if row.get("算子名称") == target_operator:
                return str(row.get("status", ""))
        return "unknown"
