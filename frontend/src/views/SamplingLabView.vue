<template>
  <section class="page" data-module="data_sampler">
    <header class="page-head">
      <div>
        <h2>数据采样 · 分层采样实验室（序号18）</h2>
        <p class="page-desc">
          读取源事件按分层生成样本，样本携带分层总数与入选概率；加权特征回到全量分层口径，
          头部层级不再被放大。会话开始冻结权重、会话结束按最新样本重算，两种口径各成报告版本。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" type="button" data-testid="refresh-batches" @click="reload">刷新批次</button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">采样批次</span>
        <strong class="stat-value">{{ batches.length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">报告版本</span>
        <strong class="stat-value">{{ reportCount }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">待办（其他页面）</span>
        <strong class="stat-value">{{ todoOpen }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="generate">
      <label class="filter-item">
        <span>采样策略</span>
        <input v-model="form.samplerName" data-testid="form-sampler" required />
      </label>
      <label class="filter-item">
        <span>分组键</span>
        <input v-model="form.groupKey" data-testid="form-group-key" required />
      </label>
      <label class="filter-item">
        <span>采样比例</span>
        <input v-model.number="form.ratio" type="number" min="0.05" max="1" step="0.05" data-testid="form-ratio" />
      </label>
      <label class="filter-item">
        <span>样本上限</span>
        <input v-model.number="form.maxTotal" type="number" min="10" data-testid="form-max" />
      </label>
      <label class="filter-item">
        <span>固定批次号（幂等重试）</span>
        <input v-model.number="form.batchNo" type="number" min="1" placeholder="留空自动分配" data-testid="form-batch-no" />
      </label>
      <button class="btn primary" type="submit" data-testid="submit-batch" :disabled="loading">
        {{ loading ? '采样中…' : '读取源事件并分层采样' }}
      </button>
    </form>

    <p v-if="message" class="page-foot"><span :class="messageOk ? '' : 'error-text'">{{ message }}</span></p>

    <table class="data-table" data-testid="batch-table">
      <thead>
        <tr>
          <th>批次号</th><th>策略</th><th>分组键</th><th>全量/样本</th>
          <th>分层构成（层: 总数/样本, 入选概率, 权重）</th><th>当前权重版本</th><th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="batch in batches" :key="batch.批次号" :data-testid="`batch-row-${batch.批次号}`">
          <td>{{ batch.批次号 }}</td>
          <td>{{ batch.策略名称 }}</td>
          <td>{{ batch.分组键 }}</td>
          <td>{{ batch.全量事件数 }} / {{ batch.样本事件数 }}</td>
          <td>
            <div v-for="meta in batch._strata" :key="meta.stratum" class="stratum-line">
              {{ meta.stratum }}：{{ meta.stratum_total }}/{{ meta.sample_count }}
              ，入选概率 {{ formatPct(meta.inclusion_probability) }}
              ，权重 {{ meta.weight ?? '—' }}
            </div>
          </td>
          <td>{{ versionLabel(batch.当前权重版本) }}</td>
          <td class="row-actions">
            <button class="link" type="button" data-testid="frozen-report" @click="frozenReport(batch.批次号)">
              冻结权重报告
            </button>
            <button class="link" type="button" data-testid="latest-report" @click="latestDialog(batch)">
              最新权重重算
            </button>
          </td>
        </tr>
        <tr v-if="!batches.length">
          <td colspan="7" class="empty-state">暂无采样批次，先在上方读取源事件生成分层样本</td>
        </tr>
      </tbody>
    </table>

    <div v-if="currentReport" class="report-panel" data-testid="report-panel">
      <header class="page-head">
        <div>
          <h3>调试异常报告 {{ currentReport.report_id }}（修订 v{{ currentReport.revision }}）</h3>
          <p class="page-desc">
            权重版本：<strong>{{ currentReport.weight_version_label }}</strong>
            ，生成时间 {{ currentReport.generated_at }}
          </p>
        </div>
        <div class="page-actions">
          <button class="btn" type="button" data-testid="download-report" @click="download">
            下载与该版本一致的样本报告
          </button>
        </div>
      </header>

      <div class="stat-row">
        <article class="stat-card">
          <span class="stat-label">朴素样本均值（旧口径，头部会被放大）</span>
          <strong class="stat-value">{{ currentReport.features.naive_sample_mean }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">加权均值（全量分层口径）</span>
          <strong class="stat-value">{{ currentReport.features.weighted_mean }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">加权异常率（全量口径）</span>
          <strong class="stat-value">{{ formatPct(currentReport.features.weighted_anomaly_rate) }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">估计全量异常事件</span>
          <strong class="stat-value">{{ currentReport.features.estimated_population_anomalies }}</strong>
        </article>
      </div>

      <table class="data-table">
        <thead>
          <tr>
            <th>分层</th><th>分层总数</th><th>样本数</th><th>入选概率</th>
            <th>分层权重</th><th>全量占比</th><th>样本均值</th><th>样本异常率</th>
            <th>估计层内异常数</th><th>加权异常率</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in currentReport.features.strata" :key="row.stratum">
            <td>{{ row.stratum }}</td>
            <td>{{ row.stratum_total }}</td>
            <td>{{ row.sample_count }}</td>
            <td>{{ formatPct(row.inclusion_probability) }}</td>
            <td>{{ row.weight ?? '—' }}</td>
            <td>{{ formatPct(row.weight_share) }}</td>
            <td>{{ row.sample_mean }}</td>
            <td>{{ formatPct(row.sample_anomaly_rate) }}</td>
            <td>{{ row.estimated_stratum_anomalies }}</td>
            <td>{{ formatPct(row.weighted_anomaly_rate) }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-if="showLatest" class="modal-backdrop" data-testid="latest-modal">
      <form class="modal" @submit.prevent="latestReport">
        <h3>按会话结束时最新分层样本重算</h3>
        <p class="page-desc">
          分层比例在会话期间发生变化时，录入每层最新全量事件数；样本数不变，入选概率与
          分层权重按最新分层总数重算，形成 session_end_latest 报告版本。
        </p>
        <label v-for="meta in latestForm.strata" :key="meta.stratum" class="create-field">
          <span>分层「{{ meta.stratum }}」最新全量事件数（冻结值 {{ meta.stratum_total }}）</span>
          <input v-model.number="meta.latestTotal" type="number" min="1" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showLatest = false">取消</button>
          <button class="btn primary" type="submit" data-testid="confirm-latest">确认重算</button>
        </div>
      </form>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'

import {
  createDebugReport,
  createSampleBatch,
  downloadReport,
  listSampleBatches,
  listSamplingTodos,
  recomputeLatestReport,
  type DebugReport,
  type SampleBatch,
  type WeightVersion,
} from '@/api/sampling-debug'

const batches = ref<SampleBatch[]>([])
const currentReport = ref<DebugReport | null>(null)
const todoOpen = ref(0)
const loading = ref(false)
const message = ref('')
const messageOk = ref(true)
const showLatest = ref(false)
const latestBatchNo = ref<number | null>(null)
const latestForm = reactive<{ strata: Array<{ stratum: string; stratum_total: number; latestTotal: number }> }>({
  strata: [],
})

const form = reactive({
  samplerName: '数据采样样例1',
  groupKey: 'region',
  ratio: 0.4,
  maxTotal: 200,
  batchNo: undefined as number | undefined,
})

const reportCount = computed(() =>
  batches.value.reduce((sum, batch) => sum + batch._debug_reports.length, 0),
)

function formatPct(value: number): string {
  return `${(value * 100).toFixed(2)}%`
}

function versionLabel(version: WeightVersion): string {
  return version === 'session_end_latest' ? '会话结束最新权重' : '会话开始冻结权重'
}

/** 构造头重脚轻的模拟源事件：r_a 80 条小值、r_b 20 条大值（含 5 条异常标记）。 */
function buildDemoEvents(): Record<string, unknown>[] {
  const events: Record<string, unknown>[] = []
  for (let i = 0; i < 80; i += 1) {
    events.push({ event_id: `a_${i}`, payload: { region: 'r_a', value: 10.0 }, event_time: Date.now() + i })
  }
  for (let i = 0; i < 20; i += 1) {
    events.push({
      event_id: `b_${i}`,
      payload: { region: 'r_b', value: 100.0, ...(i < 5 ? { anomaly: true } : {}) },
      event_time: Date.now() + 100 + i,
    })
  }
  return events
}

function notify(text: string, ok = true) {
  message.value = text
  messageOk.value = ok
}

async function generate() {
  loading.value = true
  try {
    const result = await createSampleBatch({
      sampler_name: form.samplerName,
      events: buildDemoEvents(),
      batch_no: form.batchNo,
      group_key: form.groupKey,
      ratio: form.ratio,
      max_total: form.maxTotal,
    })
    notify(`批次 ${result.batch_no} 已生成，样本 ${result.batch.样本事件数} 条，同批次号重复提交只落一次记录`)
    await reload()
  } catch (error) {
    notify(error instanceof Error ? error.message : '分层采样失败', false)
  } finally {
    loading.value = false
  }
}

async function frozenReport(batchNo: number) {
  try {
    const result = await createDebugReport(batchNo, { weight_version: 'session_start_frozen' })
    currentReport.value = result.report
    notify(`冻结权重报告已生成，重复分析只更新报告版本（当前修订 v${result.report.revision}）`)
    await reload()
  } catch (error) {
    notify(error instanceof Error ? error.message : '报告生成失败', false)
  }
}

function latestDialog(batch: SampleBatch) {
  latestBatchNo.value = batch.批次号
  latestForm.strata = batch._frozen_strata.map((meta) => ({
    stratum: meta.stratum,
    stratum_total: meta.stratum_total,
    latestTotal: meta.stratum_total,
  }))
  showLatest.value = true
}

async function latestReport() {
  if (latestBatchNo.value === null) return
  try {
    const updatedStrata = Object.fromEntries(
      latestForm.strata.map((item) => [item.stratum, item.latestTotal]),
    )
    const result = await recomputeLatestReport(latestBatchNo.value, { updated_strata: updatedStrata })
    currentReport.value = result.report
    showLatest.value = false
    notify(`已按最新分层样本重算，报告版本：${result.report.weight_version_label}`)
    await reload()
  } catch (error) {
    notify(error instanceof Error ? error.message : '重算失败', false)
  }
}

async function download() {
  if (!currentReport.value) return
  try {
    await downloadReport(currentReport.value.report_id)
    notify('样本报告下载已开始，内容与当前权重版本一致')
  } catch (error) {
    notify(error instanceof Error ? error.message : '下载失败', false)
  }
}

async function reload() {
  try {
    const [batchPayload, todoPayload] = await Promise.all([
      listSampleBatches(),
      listSamplingTodos(),
    ])
    batches.value = batchPayload.items
    todoOpen.value = todoPayload.open
    if (currentReport.value) {
      const refreshed = batches.value
        .flatMap((batch) => batch._debug_reports)
        .find((report) => report.report_id === currentReport.value?.report_id)
      if (refreshed) currentReport.value = refreshed
    }
  } catch (error) {
    notify(error instanceof Error ? error.message : '批次读取失败', false)
  }
}

onMounted(reload)
</script>
