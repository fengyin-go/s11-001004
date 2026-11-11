/** 批次水位提交工作区接口封装与类型。 */
import { request } from '@/api/client'

export type BatchStatus = '已暂存' | '已投递' | '写入受阻'

export interface SourceInfo {
  source: string
  current_watermark: string
}

export interface StorageTarget {
  target_id: number
  配置编号: string
  存储类型: string
  连接地址: string
  输出路径: string | null
  status: string
}

export interface BatchRecord {
  id: number
  source: string
  batch_no: string
  old_watermark: string
  new_watermark: string
  records: string[]
  record_count: number
  status: BatchStatus
  target_id: number | null
  attempts: number
  last_error: string | null
  idempotency_key: string | null
  staged_at: string
  updated_at: string
}

export interface Receipt {
  receipt_id: string
  source: string
  batch_no: string
  batch_id: number
  old_watermark: string
  new_watermark: string
  target_id: number
  target_code: string
  target_type: string
  target_address: string
  record_count: number
  idempotency_key: string
  sink_path: string
  reused_file: boolean
  delivered_at: string
  records?: SinkRecord[]
}

export interface SinkRecord {
  source: string
  batch_no: string
  seq: number
  old_watermark: string
  new_watermark: string
  content: string
}

export interface SinkFile {
  target_id: number
  idempotency_key: string
  path: string
  record_count: number
}

export interface WorkspaceSnapshot {
  failure_mode: string
  failure_modes: Record<string, string>
  sources: SourceInfo[]
  targets: StorageTarget[]
  batches: BatchRecord[]
  receipts: Receipt[]
  sink_files: SinkFile[]
}

export interface ActionEnvelope<T = unknown> {
  ok: boolean
  message: string
  entry: T & { code?: string }
}

async function postJson<T>(path: string, body: unknown): Promise<ActionEnvelope<T>> {
  const response = await request(path, { method: 'POST', body: JSON.stringify(body) })
  return (await response.json()) as ActionEnvelope<T>
}

export async function fetchSnapshot(): Promise<WorkspaceSnapshot> {
  const response = await request('/api/batch-workspace/snapshot')
  return (await response.json()) as WorkspaceSnapshot
}

export function stageBatch(payload: {
  source: string
  batch_no: string
  old_watermark: string
  new_watermark: string
  records_text: string
}) {
  return postJson<BatchRecord>('/api/batch-workspace/stage', payload)
}

export function commitBatch(payload: { source: string; batch_no: string; target_id: number }) {
  return postJson<{ batch: BatchRecord; receipt: Receipt | null }>(
    '/api/batch-workspace/commit',
    payload,
  )
}

export function setFailureMode(mode: string) {
  return postJson<{ failure_mode: string }>('/api/batch-workspace/failure-mode', { mode })
}

export function resetWorkspace() {
  return postJson<{ ok: boolean }>('/api/batch-workspace/reset', {})
}

export async function runContentionDemo() {
  const response = await request('/api/batch-workspace/demo/contention', { method: 'POST' })
  return (await response.json()) as {
    source: string
    old_watermark: string
    current_watermark: string
    outcomes: { batch_no: string; code: string; message: string; receipt_id: string | null }[]
    success_count: number
    conflict_count: number
    only_one_succeeded: boolean
  }
}

export async function fetchSinkRecords(targetId: number, idempotencyKey: string) {
  const query = new URLSearchParams({
    target_id: String(targetId),
    idempotency_key: idempotencyKey,
  })
  const response = await request(`/api/batch-workspace/sink-records?${query}`)
  return (await response.json()) as { items: SinkRecord[] }
}

export async function fetchReceiptsByTarget(targetId: number) {
  const response = await request(`/api/storage/${targetId}/receipts`)
  return (await response.json()) as {
    target_id: number
    total_batches: number
    total_records: number
    items: Receipt[]
  }
}

export async function fetchReceipt(receiptId: string) {
  const response = await request(`/api/receipts/${encodeURIComponent(receiptId)}`)
  return (await response.json()) as Receipt
}
