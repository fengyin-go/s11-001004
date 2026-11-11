<template>
  <section class="page" data-module="storage">
    <header class="page-head">
      <div>
        <h2>采集存储管理</h2>
        <p class="page-desc">维护采集存储配置；每个配置实际收到的批次可打开收据，核对当次水位与落盘记录数。</p>
      </div>
      <div class="page-actions">
        <button class="btn" type="button" @click="exportRows">导出采集存储清单</button>
      </div>
    </header>

    <!-- 批次收据汇总：连接增量采集页的投递结果 -->
    <div class="workspace">
      <div class="ws-head">
        <h3>批次收据与记录数</h3>
        <button class="btn ghost" type="button" @click="loadReceipts">刷新收据</button>
      </div>
      <table class="data-table">
        <thead>
          <tr>
            <th>配置编号</th><th>存储类型</th><th>连接地址</th><th>状态</th>
            <th>收到批次数</th><th>累计记录数</th><th>批次收据</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in rows" :key="String(row.id)">
            <td>{{ row['配置编号'] }}</td>
            <td>{{ row['存储类型'] }}</td>
            <td>{{ row['连接地址'] }}</td>
            <td>{{ row.status }}</td>
            <td>
              <span class="badge ok">{{ summaryMap[Number(row.id)]?.total_batches ?? 0 }}</span>
            </td>
            <td>
              <strong>{{ summaryMap[Number(row.id)]?.total_records ?? 0 }}</strong>
            </td>
            <td>
              <button class="link" type="button" @click="openTargetReceipts(Number(row.id))">
                打开批次收据{{ (summaryMap[Number(row.id)]?.total_batches ?? 0) ? `（${summaryMap[Number(row.id)]?.total_batches}）` : '' }}
              </button>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <h3 class="section-title">存储配置登记</h3>
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
          <td :colspan="columns.length + 1" class="empty-state">暂无采集存储数据</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条采集存储记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>

    <!-- 某存储配置的收据列表 -->
    <div v-if="targetReceipts" class="modal-mask" @click.self="targetReceipts = null">
      <div class="modal wide">
        <header class="modal-head">
          <h4>存储配置 #{{ targetReceipts.target_id }} 的批次收据</h4>
          <button class="btn ghost small" type="button" @click="targetReceipts = null">关闭</button>
        </header>
        <p class="muted">
          共 {{ targetReceipts.total_batches }} 个批次、{{ targetReceipts.total_records }} 条记录，
          收据永久保留当次水位与存储目标。
        </p>
        <table class="mini-table">
          <thead>
            <tr><th>收据号</th><th>来源 / 批次</th><th>当次水位</th><th>记录数</th><th>投递时间</th><th></th></tr>
          </thead>
          <tbody>
            <tr v-for="receipt in targetReceipts.items" :key="receipt.receipt_id">
              <td><code>{{ receipt.receipt_id }}</code></td>
              <td><div>{{ receipt.source }}</div><code>{{ receipt.batch_no }}</code></td>
              <td><code>{{ receipt.old_watermark }} → {{ receipt.new_watermark }}</code></td>
              <td>{{ receipt.record_count }}</td>
              <td>{{ receipt.delivered_at }}</td>
              <td><button class="link" type="button" @click="openReceiptDetail(receipt.receipt_id)">打开</button></td>
            </tr>
            <tr v-if="!targetReceipts.items.length">
              <td colspan="6" class="empty-state">该存储配置还没有收到任何批次</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- 收据详情 -->
    <div v-if="receiptDetail" class="modal-mask" @click.self="receiptDetail = null">
      <div class="modal wide">
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
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  fetchReceipt,
  fetchReceiptsByTarget,
  type Receipt,
} from '@/api/batchWorkspace'
import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/storage'
const columns = ['配置编号', '存储类型', '连接地址', '输出路径', '分区格式', '压缩算法', '保留天数', '冷热策略']
const actions = ['提交校验', '启用配置', '停用配置']
const filterFields = columns.slice(0, 3)

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const summaryMap = ref<Record<number, { total_batches: number; total_records: number }>>({})
const targetReceipts = ref<Awaited<ReturnType<typeof fetchReceiptsByTarget>> | null>(null)
const receiptDetail = ref<Receipt | null>(null)

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

async function openTargetReceipts(targetId: number) {
  targetReceipts.value = await fetchReceiptsByTarget(targetId)
}

async function openReceiptDetail(receiptId: string) {
  receiptDetail.value = await fetchReceipt(receiptId)
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ values: { action } }),
    })
    const payload = await response.json()
    if (!payload.ok) throw new Error(payload.message)
    await Promise.all([reload(), loadReceipts()])
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集存储操作失败'
  }
}

async function loadReceipts() {
  const map: Record<number, { total_batches: number; total_records: number }> = {}
  await Promise.all(
    rows.value.map(async (row) => {
      const id = Number(row.id)
      map[id] = await fetchReceiptsByTarget(id)
    }),
  )
  summaryMap.value = map
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) throw new Error('存储配置列表读取失败')
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集存储列表读取失败'
  }
}

onMounted(async () => {
  await reload()
  await loadReceipts()
})
</script>
