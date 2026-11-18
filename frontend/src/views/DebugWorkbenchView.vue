<template>
  <section class="page" data-module="debug_trace">
    <header class="page-head">
      <div>
        <h2>调试追踪 · 加权异常观察工作台（序号18 数据流）</h2>
        <p class="page-desc">
          按采样批次读取分层样本数据，用分层权重生成异常观察并与目标算子状态对应；
          会话开始冻结权重，详情页验证规则命中，并发重试整组接收、事务失败退回同一口径。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" type="button" data-testid="refresh-sessions" @click="reload">刷新</button>
      </div>
    </header>

    <form class="filter-bar" @submit.prevent="start">
      <label class="filter-item">
        <span>调试会话名称</span>
        <input v-model="sessionForm.name" data-testid="session-name" required />
      </label>
      <label class="filter-item">
        <span>目标算子</span>
        <input v-model="sessionForm.operator" data-testid="session-operator" required />
      </label>
      <label class="filter-item">
        <span>采样批次号</span>
        <input v-model.number="sessionForm.batchNo" type="number" min="1" data-testid="session-batch" required />
      </label>
      <button class="btn primary" type="submit" data-testid="session-start">启动会话（冻结权重）</button>
    </form>

    <p v-if="message" class="page-foot"><span :class="messageOk ? '' : 'error-text'">{{ message }}</span></p>

    <div v-if="sessions.length" class="session-grid">
      <article
        v-for="session in sessions"
        :key="session.会话名称"
        class="stat-card session-card"
        :data-testid="`session-card-${session.会话名称}`"
      >
        <h3>{{ session.会话名称 }}</h3>
        <p class="page-desc">
          目标算子：{{ session.目标算子 }} ｜ {{ session.采样数据 }} ｜ 状态：{{ session.status }}
        </p>
        <p class="page-desc">
          权重版本：<strong>{{ versionLabel(session._weight_version) }}</strong>
          ｜ 异常标记：{{ session.异常标记 }}
        </p>
        <div class="row-actions">
          <button class="link" type="button" @click="observe(session.会话名称)">生成加权异常观察</button>
          <button class="link" type="button" @click="openRuleHit(session.会话名称)">规则命中校验</button>
          <button class="link" type="button" @click="endFrozen(session.会话名称)">结束（冻结口径）</button>
          <button class="link" type="button" @click="openReopen(session.会话名称)">故障重开</button>
          <button class="link" type="button" @click="rollback(session.会话名称)">事务回滚</button>
        </div>
      </article>
    </div>
    <p v-else class="empty-state">暂无调试会话，请先在采样实验室生成分层批次后启动会话</p>

    <div v-if="observations.length" class="report-panel" data-testid="observation-panel">
      <header class="page-head">
        <div>
          <h3>{{ activeSession }} 的异常观察（{{ observationMeta.label }}）</h3>
          <p class="page-desc">
            全量分层口径加权异常率：
            <strong>{{ formatPct(observationMeta.rate) }}</strong>
            ，条件范围 [{{ observationMeta.range.lower }}, {{ observationMeta.range.upper }}]
          </p>
        </div>
        <div class="page-actions">
          <button class="btn" type="button" data-testid="retry-group" @click="retryGroup">
            模拟并发重试整组提交
          </button>
        </div>
      </header>
      <table class="data-table">
        <thead>
          <tr>
            <th>事件ID</th><th>分层</th><th>数值</th><th>分层总数</th>
            <th>入选概率</th><th>分层权重</th><th>目标算子状态</th><th>异常</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in observations" :key="item.event_id" :data-abnormal="String(item.is_abnormal)">
            <td>{{ item.event_id }}</td>
            <td>{{ item.stratum }}</td>
            <td>{{ item.value }}</td>
            <td>{{ item.stratum_total }}</td>
            <td>{{ formatPct(item.inclusion_probability) }}</td>
            <td>{{ item.weight ?? '—' }}</td>
            <td>{{ item.operator }}（{{ item.operator_state }}）</td>
            <td>{{ item.is_abnormal ? '命中' : '正常' }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-if="ruleHitSession" class="modal-backdrop" data-testid="rule-modal">
      <form class="modal" @submit.prevent="submitRuleHit">
        <h3>详情页规则命中校验 · {{ ruleHitSession }}</h3>
        <label class="create-field">
          <span>条件下限（当前口径）</span>
          <input v-model.number="ruleForm.lower" type="number" data-testid="rule-lower" required />
        </label>
        <label class="create-field">
          <span>条件上限（当前口径）</span>
          <input v-model.number="ruleForm.upper" type="number" data-testid="rule-upper" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="ruleHitSession = ''">取消</button>
          <button class="btn primary" type="submit" data-testid="rule-submit">验证命中</button>
        </div>
        <div v-if="ruleHits.length" class="report-panel">
          <p class="page-desc">命中 {{ ruleHits.length }} 条：</p>
          <ul>
            <li v-for="hit in ruleHits" :key="hit.event_id">
              {{ hit.event_id }}（{{ hit.stratum }}，值 {{ hit.value }}，权重 {{ hit.weight ?? '—' }}）：{{ hit.rule }}
            </li>
          </ul>
        </div>
      </form>
    </div>

    <div v-if="reopenTarget" class="modal-backdrop" data-testid="reopen-modal">
      <form class="modal" @submit.prevent="submitReopen">
        <h3>故障重开 · {{ reopenTarget }}</h3>
        <p class="page-desc">在原采样批次与会话开始冻结权重口径上重开调试会话。</p>
        <label class="create-field">
          <span>新会话名称</span>
          <input v-model="reopenName" data-testid="reopen-name" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="reopenTarget = ''">取消</button>
          <button class="btn primary" type="submit" data-testid="reopen-submit">确认重开</button>
        </div>
      </form>
    </div>
  </section>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'

import {
  endDebugSession,
  observeSession,
  reopenSession,
  rollbackSession,
  startDebugSession,
  submitRetryGroup,
  verifyRuleHits,
} from '@/api/sampling-debug'
import type { WeightVersion } from '@/api/sampling-debug'

type TraceSession = {
  会话名称: string
  目标算子: string
  采样数据: string
  status: string
  异常标记: string
  _weight_version: WeightVersion
}

type Observation = Awaited<ReturnType<typeof observeSession>>['observations'][number]
type RuleHit = Awaited<ReturnType<typeof verifyRuleHits>>['hits'][number]

// 调试会话由后端 /api/debug_trace 通用资源接口列出（含序号18专用会话）
async function fetchSessions(): Promise<TraceSession[]> {
  const response = await fetch('/api/debug_trace?size=200')
  if (!response.ok) throw new Error('调试会话读取失败')
  const body = (await response.json()) as { items: TraceSession[] }
  return body.items
}

const sessions = ref<TraceSession[]>([])
const observations = ref<Observation[]>([])
const activeSession = ref('')
const observationMeta = reactive({
  rate: 0,
  label: '',
  range: { lower: 0, upper: 0 },
})
const message = ref('')
const messageOk = ref(true)

const sessionForm = reactive({ name: '', operator: 'window_agg', batchNo: 1 })
const ruleHitSession = ref('')
const ruleForm = reactive({ lower: 0, upper: 50 })
const ruleHits = ref<RuleHit[]>([])
const reopenTarget = ref('')
const reopenName = ref('')

function formatPct(value: number): string {
  return `${(value * 100).toFixed(2)}%`
}

function versionLabel(version: WeightVersion): string {
  return version === 'session_end_latest' ? '会话结束最新权重' : '会话开始冻结权重'
}

function notify(text: string, ok = true) {
  message.value = text
  messageOk.value = ok
}

async function start() {
  try {
    const result = await startDebugSession({
      session_name: sessionForm.name,
      target_operator: sessionForm.operator,
      batch_no: sessionForm.batchNo,
    })
    notify(result.idempotent ? '同名会话已存在，重复提交只落一次记录（幂等）' : '调试会话已启动并冻结会话开始权重')
    await reload()
  } catch (error) {
    notify(error instanceof Error ? error.message : '会话启动失败', false)
  }
}

async function observe(sessionName: string) {
  try {
    const result = await observeSession(sessionName, { lower: 0, upper: 50 })
    activeSession.value = sessionName
    observations.value = result.observations
    observationMeta.rate = result.weighted_anomaly_rate
    observationMeta.label = result.weight_version_label
    observationMeta.range = result.condition_range
    notify(`已按 ${result.weight_version_label} 生成 ${result.observations.length} 条加权异常观察`)
  } catch (error) {
    notify(error instanceof Error ? error.message : '观察生成失败', false)
  }
}

function openRuleHit(sessionName: string) {
  ruleHitSession.value = sessionName
  ruleHits.value = []
}

async function submitRuleHit() {
  if (!ruleHitSession.value) return
  try {
    const result = await verifyRuleHits(ruleHitSession.value, {
      lower: ruleForm.lower,
      upper: ruleForm.upper,
    })
    ruleHits.value = result.hits
    notify(`规则命中校验完成，命中 ${result.hit_count} 条`)
  } catch (error) {
    notify(error instanceof Error ? error.message : '规则校验失败', false)
  }
}

async function endFrozen(sessionName: string) {
  try {
    const result = await endDebugSession(sessionName, { use_latest_weights: false })
    notify(`会话已结束，报告 ${result.report.report_id} 标明权重版本：${result.report.weight_version_label}`)
    await reload()
  } catch (error) {
    notify(error instanceof Error ? error.message : '结束失败', false)
  }
}

async function retryGroup() {
  if (!activeSession.value) return
  const group = observations.value.slice(0, 5).map((item) => ({ event_id: item.event_id, value: item.value }))
  try {
    const first = await submitRetryGroup(activeSession.value, group)
    const second = await submitRetryGroup(activeSession.value, group)
    notify(
      `整组接收 ${first.accepted ?? 0} 条；重复重试：${second.idempotent ? '只记一次（幂等）' : '重复落库'}`,
    )
  } catch (error) {
    notify(error instanceof Error ? error.message : '重试组提交失败', false)
  }
}

async function rollback(sessionName: string) {
  try {
    await rollbackSession(sessionName)
    notify('事务失败已回滚，会话退回会话开始冻结口径')
    if (activeSession.value === sessionName) observations.value = []
    await reload()
  } catch (error) {
    notify(error instanceof Error ? error.message : '回滚失败', false)
  }
}

function openReopen(sessionName: string) {
  reopenTarget.value = sessionName
  reopenName.value = `${sessionName}_重开`
}

async function submitReopen() {
  if (!reopenTarget.value) return
  try {
    const result = await reopenSession(reopenTarget.value, reopenName.value)
    reopenTarget.value = ''
    notify(`已在批次 ${result.batch_no} 的冻结权重口径上重开：${result.reopened_session}`)
    await reload()
  } catch (error) {
    notify(error instanceof Error ? error.message : '故障重开失败', false)
  }
}

async function reload() {
  try {
    sessions.value = await fetchSessions()
  } catch (error) {
    notify(error instanceof Error ? error.message : '会话读取失败', false)
  }
}

onMounted(reload)
</script>

<style scoped>
.session-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
  gap: 12px;
  margin-bottom: 16px;
}
.session-card h3 {
  margin: 0 0 6px;
  font-size: 15px;
}
.session-card .row-actions {
  flex-wrap: wrap;
  margin-top: 8px;
}
.report-panel {
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 12px;
  margin-top: 16px;
}
.stratum-line {
  font-size: 12px;
  line-height: 1.7;
}
</style>
