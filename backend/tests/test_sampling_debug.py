"""序号18（数据采样 → 调试追踪）全量分层口径测试。

覆盖需求点：
  - 分层样本携带分层总数与入选概率，比例分配不失真，头部层级不被放大
  - 批次独立种子：同批次重试幂等、不同批次样本不雷同
  - 加权特征与全量分层口径异常率，朴素均值 vs 加权均值对比
  - 双权重版本报告（会话开始冻结 / 会话结束最新），同版重复分析只更新一条
  - 报告下载与版本一致；阈值核对与规则命中
  - 调试会话生命周期：观察、并发重试整组接收、事务回滚、故障重开
  - 跨页面待办幂等；pipeline 链路6 贯通
"""
from __future__ import annotations

import time

import pytest

from app.pipeline import sample_and_debug_trace
from app.services.debug_trace import DebugTraceService
from app.services.stratified_sample import (
    StratifiedSampleService,
    WEIGHT_VERSION_FROZEN,
    WEIGHT_VERSION_LATEST,
)
from app.store import store


# ---------------------------------------------------------------------------
# 测试数据：构造头重脚轻的分层，验证朴素均值会被头部层级放大
# ---------------------------------------------------------------------------

def _make_stratified_events() -> list[dict]:
    """r_a 层 80 条（小值 10），r_b 层 20 条（大值 100）。

    全量真实均值 = (80*10 + 20*100)/100 = 28；
    等比例采样的朴素样本均值同样接近 28，但人为改变分层比例后可以放大头部。
    """
    events = []
    for i in range(80):
        events.append({
            "event_id": f"a_{i}",
            "payload": {"region": "r_a", "value": 10.0},
            "event_time": time.time() + i,
        })
    for i in range(20):
        events.append({
            "event_id": f"b_{i}",
            "payload": {"region": "r_b", "value": 100.0},
            "event_time": time.time() + 100 + i,
        })
    return events


@pytest.fixture(autouse=True)
def _fresh_store():
    """每个用例前重置内存仓库，避免用例间串数据。"""
    from app import store as store_module
    store_module.store.__init__()
    yield


# ---------------------------------------------------------------------------
# 1. 分层采样：分层总数、入选概率、比例分配
# ---------------------------------------------------------------------------

class TestStratifiedSampling:

    def test_sample_carries_stratum_total_and_inclusion_probability(self):
        service = StratifiedSampleService()
        batch = service.record_stratified_batch(
            "数据采样样例1", _make_stratified_events(),
            group_key="region", ratio=0.5, max_total=100,
        )
        assert batch["全量事件数"] == 100
        assert batch["样本事件数"] == 50
        strata = {item["stratum"]: item for item in batch["_strata"]}
        assert strata["r_a"]["stratum_total"] == 80
        assert strata["r_b"]["stratum_total"] == 20
        assert strata["r_a"]["sample_count"] == 40
        assert strata["r_b"]["sample_count"] == 10
        assert strata["r_a"]["inclusion_probability"] == pytest.approx(0.5)
        assert strata["r_b"]["inclusion_probability"] == pytest.approx(0.5)
        # 权重 = 分层总数 / 层内样本数
        assert strata["r_a"]["weight"] == pytest.approx(2.0)
        assert strata["r_b"]["weight"] == pytest.approx(2.0)
        # 每个样本事件必须携带分层总数和入选概率
        for event in batch["_sampled_events"]:
            meta = event["_sample_meta"]
            assert meta["stratum_total"] in (80, 20)
            assert 0 < meta["inclusion_probability"] <= 1
            assert meta["weight"] is not None

    def test_proportional_allocation_does_not_amplify_head_stratum(self):
        """分层比例不一致时，样本构成仍与全量构成一致（80/20）。"""
        service = StratifiedSampleService()
        batch = service.record_stratified_batch(
            "s1", _make_stratified_events(),
            group_key="region", ratio=0.3, max_total=100,
        )
        strata = {item["stratum"]: item for item in batch["_strata"]}
        share_a = strata["r_a"]["sample_count"] / batch["样本事件数"]
        assert share_a == pytest.approx(0.8, abs=0.05)
        # 全量占比字段回显
        assert strata["r_a"]["weight_share"] == pytest.approx(0.8)

    def test_batch_idempotent_same_batch_no(self):
        """相同批次号并发/重试只落一次记录。"""
        service = StratifiedSampleService()
        events = _make_stratified_events()
        first = service.record_stratified_batch(
            "s1", events, batch_no=99, group_key="region", ratio=0.4,
        )
        second = service.record_stratified_batch(
            "s1", events, batch_no=99, group_key="region", ratio=0.4,
        )
        assert first is second
        assert len([b for b in service.list_batches() if b["批次号"] == 99]) == 1

    def test_independent_seeds_different_batches_not_repeated_samples(self):
        """不同批次使用独立随机源，不再因固定 seed(42) 反复抽中相同事件。"""
        service = StratifiedSampleService()
        events = _make_stratified_events()
        b1 = service.record_stratified_batch(
            "s1", events, batch_no=1, group_key="region", ratio=0.3,
        )
        b2 = service.record_stratified_batch(
            "s1", events, batch_no=2, group_key="region", ratio=0.3,
        )
        ids1 = {e["event_id"] for e in b1["_sampled_events"]}
        ids2 = {e["event_id"] for e in b2["_sampled_events"]}
        assert ids1 != ids2
        # 同批次号重试得到完全相同的样本集合（幂等 + 可复现）
        b1_retry = service.record_stratified_batch(
            "s1", events, batch_no=1, group_key="region", ratio=0.3,
        )
        retry_ids = {e["event_id"] for e in b1_retry["_sampled_events"]}
        assert retry_ids == ids1

    def test_sample_cap_respected_by_allocation(self):
        service = StratifiedSampleService()
        batch = service.record_stratified_batch(
            "s1", _make_stratified_events(),
            group_key="region", ratio=0.9, max_total=30,
        )
        assert batch["样本事件数"] == 30
        strata = {item["stratum"]: item for item in batch["_strata"]}
        assert strata["r_a"]["sample_count"] + strata["r_b"]["sample_count"] == 30


# ---------------------------------------------------------------------------
# 2. 加权特征：全量分层口径，头部层级不被放大
# ---------------------------------------------------------------------------

class TestWeightedFeatures:

    def test_weighted_mean_matches_population_mean(self):
        service = StratifiedSampleService()
        batch = service.record_stratified_batch(
            "s1", _make_stratified_events(),
            group_key="region", ratio=0.4, max_total=100,
        )
        features = service.compute_weighted_features(batch["批次号"])
        # 全量真实均值 28，加权估计应贴近全量口径
        assert features["weighted_mean"] == pytest.approx(28.0, abs=2.0)
        assert features["population_total"] == 100
        assert len(features["strata"]) == 2

    def test_weighted_mean_survives_distorted_latest_proportions(self):
        """会话结束时分层比例变化：latest 口径按新分层总数重算权重。

        模拟 r_b 层从 20 条暴增到 320 条（占比 80%），冻结权重会给出旧口径，
        latest 权重必须把大值层的占比反映到加权均值上。
        """
        service = StratifiedSampleService()
        batch = service.record_stratified_batch(
            "s1", _make_stratified_events(),
            group_key="region", ratio=0.5, max_total=100,
        )
        batch_no = batch["批次号"]
        frozen = service.compute_weighted_features(
            batch_no, weight_version=WEIGHT_VERSION_FROZEN,
        )
        result = service.recompute_report_with_latest_weights(
            batch_no, updated_strata={"r_a": 80, "r_b": 320},
        )
        assert result["ok"]
        latest = result["report"]["features"]
        # 新全量均值 = (80*10 + 320*100)/400 = 82
        assert latest["weighted_mean"] == pytest.approx(82.0, abs=2.0)
        assert latest["population_total"] == 400
        assert frozen["weighted_mean"] != pytest.approx(latest["weighted_mean"], abs=1.0)
        assert latest["weight_version"] == WEIGHT_VERSION_LATEST

    def test_weighted_anomaly_rate_uses_population_scope(self):
        """异常率必须回到全量分层口径，而不是样本里数异常个数。"""
        service = StratifiedSampleService()
        events = _make_stratified_events()
        # 给 r_b 层前 10 条打异常标记（全量异常率 10%）
        for event in events[80:90]:
            event["payload"]["anomaly"] = True
        batch = service.record_stratified_batch(
            "s1", events, group_key="region", ratio=0.5, max_total=100,
        )
        # 条件范围覆盖全部正常数值，仅显式 anomaly 标记的事件计入异常，
        # 验证异常率来自全量分层口径而非样本内直接计数
        features = service.compute_weighted_features(
            batch["批次号"], lower=0, upper=200,
        )
        # r_b 层样本异常率 50%，按全量 20 条估计为 10 条，全量异常率 10%
        assert features["weighted_anomaly_rate"] == pytest.approx(0.1, abs=0.02)
        assert features["estimated_population_anomalies"] == pytest.approx(10, abs=1)


# ---------------------------------------------------------------------------
# 3. 调试报告：双权重版本 + 幂等修订
# ---------------------------------------------------------------------------

class TestDebugReports:

    def test_two_weight_versions_form_distinct_reports(self):
        service = StratifiedSampleService()
        batch = service.record_stratified_batch(
            "s1", _make_stratified_events(),
            group_key="region", ratio=0.5, max_total=100,
        )
        batch_no = batch["批次号"]
        r1 = service.generate_debug_report(batch_no)
        r2 = service.recompute_report_with_latest_weights(
            batch_no, updated_strata={"r_a": 80, "r_b": 320},
        )
        assert r1["ok"] and r2["ok"]
        reports = service.list_reports(batch_no)
        assert len(reports) == 2
        assert {r["weight_version"] for r in reports} == {
            WEIGHT_VERSION_FROZEN, WEIGHT_VERSION_LATEST,
        }
        # 批次当前结果指向最近一次生成的版本
        assert batch["_current_result"]["report_id"] == r2["report"]["report_id"]
        assert batch["当前权重版本"] == WEIGHT_VERSION_LATEST

    def test_repeat_analysis_updates_single_report_version(self):
        """同批同版本重复分析只更新一个报告版本，revision 递增。"""
        service = StratifiedSampleService()
        batch = service.record_stratified_batch(
            "s1", _make_stratified_events(),
            group_key="region", ratio=0.5, max_total=100,
        )
        batch_no = batch["批次号"]
        first = service.generate_debug_report(batch_no)["report"]
        second = service.generate_debug_report(batch_no)["report"]
        third = service.generate_debug_report(batch_no)["report"]
        assert len(service.list_reports(batch_no)) == 1
        assert first["report_id"] == second["report_id"] == third["report_id"]
        assert third["revision"] == 3

    def test_report_download_matches_version(self):
        service = StratifiedSampleService()
        batch = service.record_stratified_batch(
            "s1", _make_stratified_events(),
            group_key="region", ratio=0.5, max_total=100,
        )
        report = service.generate_debug_report(batch["批次号"])["report"]
        download = service.download_report_csv(report["report_id"])
        assert download["ok"]
        csv_text = download["content"]
        assert report["report_id"] in csv_text
        assert report["weight_version_label"] in csv_text
        assert "分层权重" in csv_text and "加权异常率" in csv_text
        # 样本明细行携带权重与入选概率
        assert "入选概率" in csv_text

    def test_followup_todos_idempotent(self):
        service = StratifiedSampleService()
        events = _make_stratified_events()
        for event in events[80:90]:
            event["payload"]["anomaly"] = True
        batch = service.record_stratified_batch(
            "s1", events, group_key="region", ratio=0.5, max_total=100,
        )
        service.generate_debug_report(batch["批次号"], lower=0, upper=50)
        service.generate_debug_report(batch["批次号"], lower=0, upper=50)
        todos = store.list_todos()
        keys = {(t["target_module"], t["biz_key"]) for t in todos}
        assert len(todos) == len(keys)
        assert len(todos) == 3  # alert_policy / debug_trace / aggregate_sink 各一条


# ---------------------------------------------------------------------------
# 4. 阈值核对与规则命中
# ---------------------------------------------------------------------------

class TestThresholdAndRuleHits:

    def test_threshold_review_records_condition_and_hits(self):
        service = StratifiedSampleService()
        batch = service.record_stratified_batch(
            "s1", _make_stratified_events(),
            group_key="region", ratio=0.5, max_total=100,
        )
        result = service.review_threshold(
            batch["批次号"], lower=0, upper=50,
        )
        assert result["ok"]
        review = result["review"]
        assert review["condition_range"] == {"lower": 0.0, "upper": 50.0}
        assert review["weight_version"] == WEIGHT_VERSION_FROZEN
        hit_strata = {item["stratum"] for item in review["hit_strata"]}
        assert "r_b" in hit_strata
        assert review["passed"] is False

    def test_debug_session_rule_hits_in_detail(self):
        sampler_service = StratifiedSampleService()
        tracer = DebugTraceService()
        batch = sampler_service.record_stratified_batch(
            "s1", _make_stratified_events(),
            group_key="region", ratio=0.5, max_total=100,
        )
        tracer.start_session("sess-1", "window_agg", batch["批次号"])
        tracer.record_observations("sess-1", lower=0, upper=50)
        hits = tracer.verify_rule_hits("sess-1", 0, 50)
        assert hits["ok"]
        assert hits["hit_count"] > 0
        assert all(h["rule"] for h in hits["hits"])
        # 观察携带分层权重与目标算子状态
        observations = tracer._find_session("sess-1")["_observations"]
        assert all(o["weight"] is not None for o in observations)
        assert all("operator_state" in o for o in observations)


# ---------------------------------------------------------------------------
# 5. 调试会话：幂等、并发重试、回滚、故障重开
# ---------------------------------------------------------------------------

class TestDebugSessionLifecycle:

    def test_start_session_idempotent_and_freezes_weights(self):
        sampler_service = StratifiedSampleService()
        tracer = DebugTraceService()
        batch = sampler_service.record_stratified_batch(
            "s1", _make_stratified_events(),
            group_key="region", ratio=0.5, max_total=100,
        )
        first = tracer.start_session("sess-x", "window_agg", batch["批次号"])
        second = tracer.start_session("sess-x", "window_agg", batch["批次号"])
        assert first["idempotent"] is False
        assert second["idempotent"] is True
        assert first["session"] is second["session"]
        session = first["session"]
        assert session["_weight_version"] == WEIGHT_VERSION_FROZEN
        assert len(session["_frozen_weights"]) == 2

    def test_end_session_frozen_and_latest_converge_to_labeled_versions(self):
        sampler_service = StratifiedSampleService()
        tracer = DebugTraceService()
        batch = sampler_service.record_stratified_batch(
            "s1", _make_stratified_events(),
            group_key="region", ratio=0.5, max_total=100,
        )
        tracer.start_session("sess-y", "window_agg", batch["批次号"])
        ended_frozen = tracer.end_session("sess-y")
        assert ended_frozen["ok"]
        assert ended_frozen["report"]["weight_version"] == WEIGHT_VERSION_FROZEN
        # 已结束会话不能再次结束
        again = tracer.end_session("sess-y")
        assert again["ok"] is False

        # 故障重开后按结束时最新样本重算
        reopened = tracer.reopen_failed_session("sess-y", "sess-y-v2")
        assert reopened["ok"]
        latest = tracer.end_session(
            "sess-y-v2",
            use_latest_weights=True,
            updated_strata={"r_a": 80, "r_b": 320},
        )
        assert latest["ok"]
        assert latest["report"]["weight_version"] == WEIGHT_VERSION_LATEST

    def test_concurrent_retry_group_accepted_once(self):
        sampler_service = StratifiedSampleService()
        tracer = DebugTraceService()
        batch = sampler_service.record_stratified_batch(
            "s1", _make_stratified_events(),
            group_key="region", ratio=0.5, max_total=100,
        )
        tracer.start_session("sess-r", "window_agg", batch["批次号"])
        group = [{"event_id": "a_1", "value": 10}, {"event_id": "b_2", "value": 100}]
        first = tracer.receive_retry_group("sess-r", group)
        second = tracer.receive_retry_group("sess-r", group)
        assert first["idempotent"] is False
        assert second["idempotent"] is True
        assert second["accepted"] == 2

    def test_retry_group_with_failure_rejected_entirely(self):
        """并发重试按当前标准接收整组结果：含失败则整组退回同一口径。"""
        sampler_service = StratifiedSampleService()
        tracer = DebugTraceService()
        batch = sampler_service.record_stratified_batch(
            "s1", _make_stratified_events(),
            group_key="region", ratio=0.5, max_total=100,
        )
        tracer.start_session("sess-f", "window_agg", batch["批次号"])
        bad_group = [{"event_id": "a_1"}, {"event_id": "b_2", "error": "boom"}]
        result = tracer.receive_retry_group("sess-f", bad_group)
        assert result["ok"] is False
        session = tracer._find_session("sess-f")
        assert "_retry_groups" not in session or not session["_retry_groups"]
        assert session["status"] == "运行中"

    def test_transaction_failure_rolls_back_to_same_scope(self):
        sampler_service = StratifiedSampleService()
        tracer = DebugTraceService()
        batch = sampler_service.record_stratified_batch(
            "s1", _make_stratified_events(),
            group_key="region", ratio=0.5, max_total=100,
        )
        tracer.start_session("sess-b", "window_agg", batch["批次号"])
        tracer.record_observations("sess-b", lower=0, upper=50)
        rollback = tracer.rollback_session("sess-b")
        assert rollback["ok"]
        session = tracer._find_session("sess-b")
        assert session["_observations"] == []
        assert session["异常标记"] == "none"
        assert session["_weight_version"] == WEIGHT_VERSION_FROZEN


# ---------------------------------------------------------------------------
# 6. 数据采样服务与 pipeline 链路
# ---------------------------------------------------------------------------

class TestSamplerServiceAndPipeline:

    def test_data_sampler_apply_returns_batch_no_and_no_global_seed(self):
        import random
        from app.services.data_sampler import DataSamplerService
        # 先在 data_sampler 模块登记一条分层策略
        store.rows("data_sampler").append({
            "id": 100,
            "策略名称": "分层策略A",
            "采样方式": "stratified",
            "采样比例": "0.4",
            "分组键": "region",
            "保留特征": "value",
            "样本上限": "100",
            "采样窗口": "60",
            "status": "草稿",
            "pending": True,
            "abnormal": False,
        })
        service = DataSamplerService()
        random.seed(7)  # 外部随机状态不应被采样实现篡改
        reference = random.Random(7).random()
        result = service.apply_sampling("分层策略A", _make_stratified_events())
        assert result["ok"]
        assert result["batch_no"] >= 1
        # 实现不再调用 random.seed：全局随机流仍停留在 seed(7) 的下一个值
        assert random.random() == reference
        # 重复批次号幂等
        retry = service.apply_sampling(
            "分层策略A", _make_stratified_events(), batch_no=result["batch_no"],
        )
        assert retry["batch_no"] == result["batch_no"]

    def test_pipeline_chain6_sampler_to_debug_trace(self):
        result = sample_and_debug_trace(
            "数据采样样例1", "window_agg",
            _make_stratified_events(), sample_rate=0.4, batch_no=88,
        )
        assert result["sampled_count"] > 0
        assert result["total_events"] == 100
        assert result["weight_version"] == WEIGHT_VERSION_FROZEN
        # 链路重复执行（同批次）保持幂等：会话只落一次
        again = sample_and_debug_trace(
            "数据采样样例1", "window_agg",
            _make_stratified_events(), sample_rate=0.4, batch_no=88,
        )
        assert again["idempotent"] is True
        assert again["batch_no"] == result["batch_no"]

    def test_session_statistics_include_weighted_rate(self):
        sampler_service = StratifiedSampleService()
        tracer = DebugTraceService()
        events = _make_stratified_events()
        for event in events[80:90]:
            event["payload"]["anomaly"] = True
        batch = sampler_service.record_stratified_batch(
            "s1", events, group_key="region", ratio=0.5, max_total=100,
        )
        tracer.start_session("sess-stat", "window_agg", batch["批次号"])
        stats = tracer.list_session_statistics()
        assert "avg_weighted_anomaly_rate" in stats
        assert stats["avg_weighted_anomaly_rate"] == pytest.approx(0.1, abs=0.02)
