<template>
  <section class="page" data-module="increment">
    <header class="page-head">
      <div>
        <h2>增量采集管理</h2>
        <p class="page-desc">维护增量游标的输入、执行约束与结果记录；批次水位工作区保证记录先落库、游标后推进。</p>
      </div>
      <div class="page-actions">
        <button class="btn" type="button" @click="exportRows">导出增量采集清单</button>
      </div>
    </header>

    <!-- ============ 批次水位提交工作区 ============ -->
    <div class="workspace">
      <div class="ws-head">
        <h3>批次水位提交工作区</h3>
        <div class="ws-controls">
          <label class="fault-select">
            <span>存储替身故障模式</span>
            <select :value="snapshot.failure_mode" @change="changeFailureMode(($event.target as HTMLSelectElement).value)">
              <option v-for="(label, mode) in snapshot.failure_modes" :key="mode" :value="mode">
                {{ label }}
              </option>
            </select>
          </label>
          <button class="btn" type="button" @click="runContention">演示并发争用</button>
          <button class="btn ghost danger" type="button" @click="resetAll">重置工作区</button>
        </div>
      </div>

      <!-- 来源水位 + 落盘实证 -->
      <div class="ws-panels">
        <article class="ws-panel">
          <h4>来源当前水位（游标只在记录写入成功后推进）</h4>
          <table class="mini-table">
            <thead><tr><th>来源标识</th><th>当前水位</th></tr></thead>
            <tbody>
              <tr v-for="item in snapshot.sources" :key="item.source">
                <td>{{ item.source }}</td>
                <td><code>{{ item.current_watermark }}</code></td>
              </tr>
            </tbody>
          </table>
        </article>
        <article class="ws-panel">
          <h4>本地存储替身实落文件（成功不止是数字变化）</h4>
          <p v-if="!snapshot.sink_files.length" class="muted">尚无批次落盘</p>
          <ul v-else class="sink-list">
            <li v-for="file in snapshot.sink_files" :key="file.idempotency_key">
              <div class="sink-line">
                <strong>存储配置 #{{ file.target_id }}</strong>
                <span class="badge ok">{{ file.record_count }} 条记录</span>
              </div>
              <code class="sink-path">{{ file.path }}</code>
              <button class="link" type="button" @click="openSinkFile(file)">打开落盘记录</button>
            </li>
          </ul>
        </article>
      </div>

      <!-- 第一步：暂存批次 -->
      <article class="ws-panel">
        <h4>第一步 · 暂存批次（只登记内容，不写存储、不动水位）</h4>
        <form class="stage-form" @submit.prevent="onStage">
          <label>
            <span>来源</span>
            <select v-model="form.source" required>
              <option value="" disabled>选择来源</option>
              <option v-for="item in snapshot.sources" :key="item.source" :value="item.source">{{ item.source }}</option>
            </select>
          </label>
          <label>
            <span>批次号</span>
            <input v-model="form.batch_no" placeholder="如 B-20261111-01" required />
          </label>
          <label>
            <span>旧水位</span>
            <input v-model="form.old_watermark" placeholder="批次开始时的来源水位" required />
          </label>
          <label>
            <span>新水位</span>
            <input v-model="form.new_watermark" placeholder="本批处理完成后的目标水位" required />
          </label>
          <label class="span-2">
            <span>本次离线记录（每行一条）</span>
            <textarea v-model="form.records_text" rows="4" placeholder="record-1&#10;record-2" required></textarea>
          </label>
          <div class="span-2 form-actions">
            <button class="btn primary" type="submit">暂存批次</button>
            <button class="btn ghost" type="button" @click="fillCurrentWatermark">填入所选来源当前水位</button>
          </div>
        </form>
      </article>

      <!-- 第二步：批次清单与确认投递 -->
      <article class="ws-panel">
        <h4>第二步 · 确认投递到存储配置（写入受阻可从失败步骤接着提交）</h4>
        <table class="data-table">
          <thead>
            <tr>
              <th>来源 / 批次号</th><th>旧水位 → 新水位</th><th>记录数</th><th>状态</th>
              <th>尝试</th><th>失败步骤说明</th><th>投递操作</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="batch in snapshot.batches" :key="batch.id" :class="{ blocked: batch.status === '写入受阻' }">
              <td>
                <div>{{ batch.source }}</div>
                <code>{{ batch.batch_no }}</code>
              </td>
              <td class="wm-cell">
                <code>{{ batch.old_watermark }}</code>
                <span class="arrow">→</span>
                <code>{{ batch.new_watermark }}</code>
              </td>
              <td>{{ batch.record_count }}</td>
              <td><span class="badge" :class="statusClass(batch.status)">{{ batch.status }}</span></td>
              <td>{{ batch.attempts }}</td>
              <td class="error-cell">{{ batch.last_error ?? '—' }}</td>
              <td>
                <template v-if="batch.status === '已投递'">
                  <span class="muted">已完成</span>
                </template>
                <template v-else>
                  <select :value="batch.target_id ?? ''" @change="onPickTarget(batch, ($event.target as HTMLSelectElement).value)">
                    <option value="" disabled>选择存储配置</option>
                    <option
                      v-for="target in snapshot.targets"
                      :key="target.target_id"
                      :value="target.target_id"
                      :disabled="target.status !== '已启用'"
                    >
                      {{ target.配置编号 }} · {{ target.存储类型 }}（{{ target.status }}）
                    </option>
                  </select>
                  <button class="btn primary small" type="button" @click="onCommit(batch)">
                    {{ batch.status === '写入受阻' ? '从失败步骤接着提交' : '确认投递' }}
                  </button>
                </template>
              </td>
            </tr>
            <tr v-if="!snapshot.batches.length">
              <td colspan="7" class="empty-state">暂无暂存批次，先在上面录入并暂存</td>
            </tr>
          </tbody>
        </table>
      </article>

      <!-- 历史收据 -->
      <article class="ws-panel">
        <h4>批次收据（保留当次水位与存储目标，可打开核对落盘记录）</h4>
        <table class="data-table">
          <thead>
            <tr><th>收据号</th><th>来源 / 批次</th><th>当次水位</th><th>存储目标</th><th>记录数</th><th>投递时间</th><th></th></tr>
          </thead>
          <tbody>
            <tr v-for="receipt in snapshot.receipts" :key="receipt.receipt_id">
              <td><code>{{ receipt.receipt_id }}</code></td>
              <td><div>{{ receipt.source }}</div><code>{{ receipt.batch_no }}</code></td>
              <td class="wm-cell"><code>{{ receipt.old_watermark }}</code><span class="arrow">→</span><code>{{ receipt.new_watermark }}</code></td>
              <td>{{ receipt.target_code }}<div class="muted small-text">{{ receipt.target_address }}</div></td>
              <td>
                <span class="badge ok">{{ receipt.record_count }}</span>
                <span v-if="receipt.reused_file" class="badge reused">复用原文件</span>
              </td>
              <td>{{ receipt.delivered_at }}</td>
              <td><button class="link" type="button" @click="openReceipt(receipt.receipt_id)">打开收据</button></td>
            </tr>
            <tr v-if="!snapshot.receipts.length">
              <td colspan="7" class="empty-state">尚无投递成功的批次收据</td>
            </tr>
          </tbody>
        </table>
      </article>
    </div>

    <!-- ============ 原有增量游标清单 ============ -->
    <h3 class="section-title">增量游标登记</h3>
    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无增量采集数据</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条增量游标记录</span>
      <span v-if="notice" :class="noticeOk ? 'ok-text' : 'error-text'">{{ notice }}</span>
    </footer>

    <!-- 收据弹窗 -->
    <div v-if="receiptDetail" class="modal-mask" @click.self="receiptDetail = null">
      <div class="modal">
        <header class="modal-head">
          <h4>批次收据 {{ receiptDetail.receipt_id }}</h4>
          <button class="btn ghost small" type="button" @click="receiptDetail = null">关闭</button>
        </header>
        <dl class="receipt-grid">
          <dt>来源</dt><dd>{{ receiptDetail.source }}</dd>
          <dt>批次号</dt><dd><code>{{ receiptDetail.batch_no }}</code></dd>
          <dt>当次旧水位</dt><dd><code>{{ receiptDetail.old_watermark }}</code></dd>
          <dt>推进后新水位</dt><dd><code>{{ receiptDetail.new_watermark }}</code></dd>
          <dt>存储目标</dt><dd>{{ receiptDetail.target_code }} · {{ receiptDetail.target_type }}</dd>
          <dt>连接地址</dt><dd>{{ receiptDetail.target_address }}</dd>
          <dt>记录数</dt><dd>{{ receiptDetail.record_count }}</dd>
          <dt>幂等键</dt><dd><code>{{ receiptDetail.idempotency_key }}</code></dd>
          <dt>落盘路径</dt><dd><code>{{ receiptDetail.sink_path }}</code></dd>
          <dt>投递时间</dt><dd>{{ receiptDetail.delivered_at }}</dd>
        </dl>
        <h5>替身文件内的真实记录</h5>
        <table class="mini-table">
          <thead><tr><th>#</th><th>记录内容</th><th>携带水位</th></tr></thead>
          <tbody>
            <tr v-for="record in receiptDetail.records ?? []" :key="record.seq">
              <td>{{ record.seq }}</td>
              <td>{{ record.content }}</td>
              <td><code>{{ record.old_watermark }} → {{ record.new_watermark }}</code></td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- 落盘记录弹窗 -->
    <div v-if="sinkViewer" class="modal-mask" @click.self="sinkViewer = null">
      <div class="modal">
        <header class="modal-head">
          <h4>替身落盘文件 · {{ sinkViewer.path }}</h4>
          <button class="btn ghost small" type="button" @click="sinkViewer = null">关闭</button>
        </header>
        <table class="mini-table">
          <thead><tr><th>#</th><th>来源</th><th>批次</th><th>记录内容</th></tr></thead>
          <tbody>
            <tr v-for="record in sinkViewer.records" :key="record.seq">
              <td>{{ record.seq }}</td><td>{{ record.source }}</td><td>{{ record.batch_no }}</td><td>{{ record.content }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'

import {
  commitBatch,
  fetchReceipt,
  fetchSinkRecords,
  fetchSnapshot,
  resetWorkspace,
  runContentionDemo,
  setFailureMode,
  stageBatch,
  type BatchRecord,
  type Receipt,
  type SinkFile,
  type SinkRecord,
  type WorkspaceSnapshot,
} from '@/api/batchWorkspace'
import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/increment'
const columns = ['游标编号', '来源标识', '增量字段', '游标位置', '批次大小', '采集间隔', '起始水位', '截止水位']
const actions = ['提交校验', '启用游标', '停用游标']
const filterFields = columns.slice(0, 3)

const emptySnapshot = (): WorkspaceSnapshot => ({
  failure_mode: 'normal',
  failure_modes: {},
  sources: [],
  targets: [],
  batches: [],
  receipts: [],
  sink_files: [],
})

const snapshot = ref<WorkspaceSnapshot>(emptySnapshot())
const rows = ref<Row[]>([])
const total = ref(0)
const notice = ref('')
const noticeOk = ref(false)
const filters = ref<Record<string, string>>({})
const receiptDetail = ref<Receipt | null>(null)
const sinkViewer = ref<{ path: string; records: SinkRecord[] } | null>(null)
const targetChoices = reactive<Record<number, number>>({})

const form = reactive({
  source: '',
  batch_no: '',
  old_watermark: '',
  new_watermark: '',
  records_text: '',
})

function flash(message: string, ok = false) {
  notice.value = message
  noticeOk.value = ok
}

function statusClass(status: string) {
  if (status === '已投递') return 'ok'
  if (status === '写入受阻') return 'blocked'
  return 'staged'
}

async function refresh() {
  snapshot.value = await fetchSnapshot()
}

function fillCurrentWatermark() {
  const found = snapshot.value.sources.find((item) => item.source === form.source)
  if (found) form.old_watermark = found.current_watermark
  else flash('请先选择来源', false)
}

async function onStage() {
  const result = await stageBatch({ ...form })
  flash(result.message, result.ok)
  if (result.ok) {
    form.batch_no = ''
    form.new_watermark = ''
    form.records_text = ''
  }
  await refresh()
}

function onPickTarget(batch: BatchRecord, value: string) {
  targetChoices[batch.id] = Number(value)
}

async function onCommit(batch: BatchRecord) {
  const targetId = targetChoices[batch.id] ?? batch.target_id
  if (!targetId) {
    flash('请先选择要投递的存储配置', false)
    return
  }
  const result = await commitBatch({ source: batch.source, batch_no: batch.batch_no, target_id: targetId })
  flash(result.message, result.ok)
  await refresh()
}

async function changeFailureMode(mode: string) {
  const result = await setFailureMode(mode)
  flash(result.message, result.ok)
  await refresh()
}

async function resetAll() {
  const result = await resetWorkspace()
  flash(result.message, true)
  await refresh()
}

async function runContention() {
  const result = await runContentionDemo()
  await refresh()
  flash(
    result.only_one_succeeded
      ? `争用演示完成：批次 ${result.outcomes.map((o) => o.batch_no).join('、')} 中仅 1 个提交成功，另 1 个被水位争用拒绝`
      : '争用演示结果异常，请查看后端返回',
    result.only_one_succeeded,
  )
}

async function openReceipt(receiptId: string) {
  receiptDetail.value = await fetchReceipt(receiptId)
}

async function openSinkFile(file: SinkFile) {
  const payload = await fetchSinkRecords(file.target_id, file.idempotency_key)
  sinkViewer.value = { path: file.path, records: payload.items }
}

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

async function runAction(action: string, row: Row) {
  notice.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ values: { action } }),
    })
    const payload = await response.json()
    if (!payload.ok) throw new Error(payload.message)
    flash(payload.message, true)
    await reload()
  } catch (error) {
    flash(error instanceof Error ? error.message : '增量采集操作失败', false)
  }
}

async function reload() {
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) throw new Error('增量游标列表读取失败')
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    flash(error instanceof Error ? error.message : '增量游标列表读取失败', false)
  }
}

onMounted(async () => {
  await refresh()
  await reload()
})
</script>
