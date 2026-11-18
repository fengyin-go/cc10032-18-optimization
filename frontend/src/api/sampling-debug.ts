/**
 * 序号18（数据采样 → 调试追踪）接口封装。
 * 全部走后端 /api/sampling 专用路由，类型与后端 stratified_sample / debug_trace 服务对齐。
 */
import { request } from './client'

export type WeightVersion = 'session_start_frozen' | 'session_end_latest'

export type StratumMeta = {
  stratum: string
  stratum_total: number
  sample_count: number
  inclusion_probability: number
  weight: number | null
  weight_share: number
}

export type WeightedFeatures = {
  ok: boolean
  batch_no: number
  weight_version: WeightVersion
  weight_version_label: string
  population_total: number
  sample_total: number
  naive_sample_mean: number
  weighted_mean: number
  weighted_stddev: number
  weighted_anomaly_rate: number
  estimated_population_anomalies: number
  condition_range: { lower: number; upper: number }
  strata: Array<StratumMeta & {
    sample_mean: number
    sample_anomaly_rate: number
    estimated_stratum_anomalies: number
    weighted_anomaly_rate: number
  }>
  uncovered_strata: string[]
}

export type SampleBatch = {
  id: number
  批次号: number
  策略名称: string
  分组键: string
  采样比例: number
  样本上限: number
  全量事件数: number
  样本事件数: number
  当前权重版本: WeightVersion
  status: string
  _strata: StratumMeta[]
  _frozen_strata: StratumMeta[]
  _latest_strata: StratumMeta[]
  _debug_reports: DebugReport[]
  _threshold_reviews: ThresholdReview[]
  _current_result: DebugReport | null
}

export type DebugReport = {
  report_id: string
  batch_no: number
  sampler: string
  weight_version: WeightVersion
  weight_version_label: string
  features: WeightedFeatures
  sample_count: number
  population_total: number
  generated_at: string
  revision: number
}

export type ThresholdReview = {
  review_id: string
  batch_no: number
  weight_version: WeightVersion
  weight_version_label: string
  condition_range: { lower: number; upper: number }
  weighted_anomaly_rate: number
  hit_strata: Array<{
    stratum: string
    sample_anomaly_rate: number
    estimated_stratum_anomalies: number
    weight: number | null
  }>
  passed: boolean
  checked_at: string
}

export type TodoItem = {
  id: number
  target_module: string
  biz_key: string
  title: string
  detail: Record<string, unknown>
  done: boolean
  created_at: number
}

async function call<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await request(path, init)
  if (!response.ok) {
    let detail = `接口返回 ${response.status}`
    try {
      const body = await response.json()
      detail = body.detail ?? detail
    } catch {
      /* 保留默认错误文案 */
    }
    throw new Error(detail)
  }
  return (await response.json()) as T
}

const jsonPost = <T>(path: string, body: unknown): Promise<T> =>
  call<T>(path, { method: 'POST', body: JSON.stringify(body) })

// ---- 分层采样批次 ----

export function createSampleBatch(payload: {
  sampler_name: string
  events: Record<string, unknown>[]
  batch_no?: number
  group_key?: string
  ratio?: number
  max_total?: number
}) {
  return jsonPost<{ ok: boolean; batch_no: number; batch: SampleBatch }>(
    '/api/sampling/batches',
    payload,
  )
}

export function listSampleBatches() {
  return call<{ ok: boolean; items: SampleBatch[] }>('/api/sampling/batches')
}

export function getBatchFeatures(
  batchNo: number,
  weightVersion: WeightVersion = 'session_start_frozen',
) {
  const query = new URLSearchParams({ weight_version: weightVersion })
  return call<WeightedFeatures>(`/api/sampling/batches/${batchNo}/features?${query}`)
}

// ---- 调试报告 ----

export function createDebugReport(
  batchNo: number,
  payload: { weight_version: WeightVersion; lower?: number; upper?: number },
) {
  return jsonPost<{ ok: boolean; report: DebugReport }>(
    `/api/sampling/batches/${batchNo}/debug-report`,
    payload,
  )
}

export function recomputeLatestReport(
  batchNo: number,
  payload: { updated_strata?: Record<string, number>; lower?: number; upper?: number },
) {
  return jsonPost<{ ok: boolean; report: DebugReport }>(
    `/api/sampling/batches/${batchNo}/recompute-latest`,
    payload,
  )
}

export function listReports(batchNo?: number) {
  const query = batchNo ? `?batch_no=${batchNo}` : ''
  return call<{ ok: boolean; items: DebugReport[] }>(`/api/sampling/reports${query}`)
}

export function downloadReport(reportId: string): Promise<void> {
  return request(`/api/sampling/reports/${reportId}/download`)
    .then(async (response) => {
      if (!response.ok) throw new Error('样本报告下载失败')
      const url = URL.createObjectURL(await response.blob())
      const anchor = document.createElement('a')
      anchor.href = url
      anchor.download = `stratified_report_${reportId}.csv`
      document.body.appendChild(anchor)
      anchor.click()
      document.body.removeChild(anchor)
      URL.revokeObjectURL(url)
    })
}

// ---- 阈值核对 ----

export function submitThresholdReview(
  batchNo: number,
  payload: { lower: number; upper: number; weight_version: WeightVersion },
) {
  return jsonPost<{ ok: boolean; review: ThresholdReview; features: WeightedFeatures }>(
    `/api/sampling/batches/${batchNo}/threshold-review`,
    payload,
  )
}

export function listThresholdReviews(batchNo?: number) {
  const query = batchNo ? `?batch_no=${batchNo}` : ''
  return call<{ ok: boolean; items: ThresholdReview[] }>(
    `/api/sampling/threshold-reviews${query}`,
  )
}

// ---- 调试会话 ----

export function startDebugSession(payload: {
  session_name: string
  target_operator: string
  batch_no: number
}) {
  return jsonPost<{ ok: boolean; idempotent: boolean }>('/api/sampling/sessions', payload)
}

export function observeSession(
  sessionName: string,
  payload: { lower?: number; upper?: number } = {},
) {
  return jsonPost<{
    ok: boolean
    session: string
    weight_version: WeightVersion
    weight_version_label: string
    weighted_anomaly_rate: number
    condition_range: { lower: number; upper: number }
    observations: Array<{
      event_id: string
      stratum: string
      value: number
      stratum_total: number
      inclusion_probability: number
      weight: number | null
      is_abnormal: boolean
      operator: string
      operator_state: string
    }>
  }>(`/api/sampling/sessions/${encodeURIComponent(sessionName)}/observe`, payload)
}

export function verifyRuleHits(
  sessionName: string,
  payload: { lower: number; upper: number },
) {
  return jsonPost<{
    ok: boolean
    session: string
    hit_count: number
    hits: Array<{ event_id: string; stratum: string; value: number; weight: number | null; rule: string }>
  }>(`/api/sampling/sessions/${encodeURIComponent(sessionName)}/rule-hits`, payload)
}

export function endDebugSession(
  sessionName: string,
  payload: {
    use_latest_weights: boolean
    updated_strata?: Record<string, number>
  },
) {
  return jsonPost<{ ok: boolean; report: DebugReport }>(
    `/api/sampling/sessions/${encodeURIComponent(sessionName)}/end`,
    payload,
  )
}

export function submitRetryGroup(
  sessionName: string,
  results: Record<string, unknown>[],
) {
  return jsonPost<{ ok: boolean; idempotent: boolean; accepted?: number; message?: string }>(
    `/api/sampling/sessions/${encodeURIComponent(sessionName)}/retry-group`,
    { results },
  )
}

export function rollbackSession(sessionName: string) {
  return jsonPost<{ ok: boolean; rolled_back: Record<string, unknown> }>(
    `/api/sampling/sessions/${encodeURIComponent(sessionName)}/rollback`,
    {},
  )
}

export function reopenSession(sessionName: string, newSessionName?: string) {
  return jsonPost<{
    ok: boolean
    reopened_session: string
    batch_no: number
    weight_version: WeightVersion
  }>(`/api/sampling/sessions/${encodeURIComponent(sessionName)}/reopen`, {
    new_session_name: newSessionName ?? null,
  })
}

// ---- 跨页面待办 ----

export function listSamplingTodos(targetModule?: string) {
  const query = targetModule ? `?target_module=${encodeURIComponent(targetModule)}` : ''
  return call<{ ok: boolean; total: number; open: number; items: TodoItem[] }>(
    `/api/sampling/todos${query}`,
  )
}

export function resolveSamplingTodo(todoId: number) {
  return jsonPost<{ ok: boolean }>(`/api/sampling/todos/${todoId}/resolve`, {})
}
