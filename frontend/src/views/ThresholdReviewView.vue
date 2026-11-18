<template>
  <section class="page" data-module="threshold-review">
    <header class="page-head">
      <div>
        <h2>阈值核对页（序号18 当前口径）</h2>
        <p class="page-desc">
          调试异常报告在此标明权重版本与全量分层样本数据口径；按当前条件范围判定分层样本数据，
          命中层级与加权异常率均可追溯到具体报告版本。其他业务页面的联动待办在右侧汇总。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" type="button" data-testid="refresh-review" @click="reload">刷新核对结果</button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">报告版本数</span>
        <strong class="stat-value">{{ reports.length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">阈值核对记录</span>
        <strong class="stat-value">{{ reviews.length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">未通过核对</span>
        <strong class="stat-value">{{ failedReviews }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">跨页面待办</span>
        <strong class="stat-value">{{ todos.open }} / {{ todos.total }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="submitReview">
      <label class="filter-item">
        <span>采样批次号</span>
        <input v-model.number="form.batchNo" type="number" min="1" data-testid="review-batch" required />
      </label>
      <label class="filter-item">
        <span>条件下限</span>
        <input v-model.number="form.lower" type="number" data-testid="review-lower" required />
      </label>
      <label class="filter-item">
        <span>条件上限</span>
        <input v-model.number="form.upper" type="number" data-testid="review-upper" required />
      </label>
      <label class="filter-item">
        <span>权重口径</span>
        <select v-model="form.weightVersion" data-testid="review-version">
          <option value="session_start_frozen">会话开始冻结权重</option>
          <option value="session_end_latest">会话结束最新权重</option>
        </select>
      </label>
      <button class="btn primary" type="submit" data-testid="review-submit">按当前口径判定</button>
    </form>

    <p v-if="message" class="page-foot"><span :class="messageOk ? '' : 'error-text'">{{ message }}</span></p>

    <h3>调试异常报告（权重版本 + 全量分层口径）</h3>
    <table class="data-table" data-testid="report-table">
      <thead>
        <tr>
          <th>报告编号</th><th>批次</th><th>权重版本</th><th>全量/样本</th>
          <th>加权均值</th><th>加权异常率</th><th>条件范围</th><th>修订</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="report in reports" :key="report.report_id" :data-testid="`report-row-${report.report_id}`">
          <td>{{ report.report_id }}</td>
          <td>{{ report.batch_no }}</td>
          <td>{{ report.weight_version_label }}</td>
          <td>{{ report.population_total }} / {{ report.sample_count }}</td>
          <td>{{ report.features.weighted_mean }}</td>
          <td>{{ formatPct(report.features.weighted_anomaly_rate) }}</td>
          <td>[{{ report.features.condition_range.lower }}, {{ report.features.condition_range.upper }}]</td>
          <td>v{{ report.revision }}</td>
        </tr>
        <tr v-if="!reports.length">
          <td colspan="8" class="empty-state">暂无报告，请先在采样实验室生成调试异常报告</td>
        </tr>
      </tbody>
    </table>

    <h3>阈值核对记录</h3>
    <table class="data-table" data-testid="review-table">
      <thead>
        <tr>
          <th>核对编号</th><th>批次</th><th>权重版本</th><th>条件范围</th>
          <th>加权异常率</th><th>命中分层</th><th>结论</th><th>核对时间</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="review in reviews" :key="review.review_id" :data-testid="`review-row-${review.review_id}`">
          <td>{{ review.review_id }}</td>
          <td>{{ review.batch_no }}</td>
          <td>{{ review.weight_version_label }}</td>
          <td>[{{ review.condition_range.lower }}, {{ review.condition_range.upper }}]</td>
          <td>{{ formatPct(review.weighted_anomaly_rate) }}</td>
          <td>
            <span v-for="hit in review.hit_strata" :key="hit.stratum" class="stratum-tag">
              {{ hit.stratum }}（{{ formatPct(hit.sample_anomaly_rate) }}）
            </span>
            <span v-if="!review.hit_strata.length">—</span>
          </td>
          <td :class="review.passed ? '' : 'error-text'">{{ review.passed ? '通过' : '不通过' }}</td>
          <td>{{ review.checked_at }}</td>
        </tr>
        <tr v-if="!reviews.length">
          <td colspan="8" class="empty-state">暂无核对记录</td>
        </tr>
      </tbody>
    </table>

    <h3>其他页面联动待办</h3>
    <table class="data-table" data-testid="todo-table">
      <thead>
        <tr><th>目标页面</th><th>待办事项</th><th>关联批次/报告</th><th>状态</th><th>操作</th></tr>
      </thead>
      <tbody>
        <tr v-for="todo in todos.items" :key="todo.id">
          <td>{{ todo.target_module }}</td>
          <td>{{ todo.title }}</td>
          <td>
            批次 {{ todo.detail.batch_no ?? '—' }} ／ {{ todo.detail.report_id ?? '—' }}
          </td>
          <td>{{ todo.done ? '已处理' : '待处理' }}</td>
          <td>
            <button
              v-if="!todo.done"
              class="link"
              type="button"
              :data-testid="`resolve-todo-${todo.id}`"
              @click="resolve(todo.id)"
            >
              标记处理
            </button>
          </td>
        </tr>
        <tr v-if="!todos.items.length">
          <td colspan="5" class="empty-state">暂无联动待办（报告加权异常率为 0 时不会生成）</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'

import {
  listReports,
  listSamplingTodos,
  listThresholdReviews,
  resolveSamplingTodo,
  submitThresholdReview,
  type DebugReport,
  type ThresholdReview,
  type TodoItem,
  type WeightVersion,
} from '@/api/sampling-debug'

const reports = ref<DebugReport[]>([])
const reviews = ref<ThresholdReview[]>([])
const todos = ref<{ total: number; open: number; items: TodoItem[] }>({ total: 0, open: 0, items: [] })
const message = ref('')
const messageOk = ref(true)

const form = reactive({
  batchNo: 1,
  lower: 0,
  upper: 50,
  weightVersion: 'session_start_frozen' as WeightVersion,
})

const failedReviews = computed(() => reviews.value.filter((item) => !item.passed).length)

function formatPct(value: number): string {
  return `${(value * 100).toFixed(2)}%`
}

function notify(text: string, ok = true) {
  message.value = text
  messageOk.value = ok
}

async function submitReview() {
  try {
    const result = await submitThresholdReview(form.batchNo, {
      lower: form.lower,
      upper: form.upper,
      weight_version: form.weightVersion,
    })
    notify(
      `核对完成：加权异常率 ${formatPct(result.review.weighted_anomaly_rate)}，` +
        `${result.review.passed ? '通过' : `命中 ${result.review.hit_strata.length} 个分层`}`,
      result.review.passed,
    )
    await reload()
  } catch (error) {
    notify(error instanceof Error ? error.message : '阈值核对失败', false)
  }
}

async function resolve(todoId: number) {
  try {
    await resolveSamplingTodo(todoId)
    await reload()
  } catch (error) {
    notify(error instanceof Error ? error.message : '待办处理失败', false)
  }
}

async function reload() {
  try {
    const [reportPayload, reviewPayload, todoPayload] = await Promise.all([
      listReports(),
      listThresholdReviews(),
      listSamplingTodos(),
    ])
    reports.value = reportPayload.items
    reviews.value = reviewPayload.items
    todos.value = { total: todoPayload.total, open: todoPayload.open, items: todoPayload.items }
  } catch (error) {
    notify(error instanceof Error ? error.message : '核对数据读取失败', false)
  }
}

onMounted(reload)
</script>

<style scoped>
h3 {
  margin: 18px 0 8px;
  font-size: 15px;
}
.stratum-tag {
  display: inline-block;
  margin-right: 6px;
  padding: 1px 6px;
  border: 1px solid var(--border);
  border-radius: 10px;
  font-size: 12px;
}
</style>
