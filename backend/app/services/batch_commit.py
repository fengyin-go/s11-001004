"""批次水位提交工作区：暂存批次、原子投递、水位推进与失败恢复。

原子边界的安排（关键设计）
--------------------------
一次「确认投递」在同一把进程锁内完成下面四步，对外呈现为一个原子事务：

1. 身份核对：以 (来源, 批次号) 找到批次；批次号重用但内容或存储目标改变 -> 拒绝；
2. 水位争用检查（CAS）：来源当前水位必须仍等于批次记录的旧水位，
   两个批次争用同一旧水位时，先提交的把水位推走，后提交的只能失败，绝不重复投递；
3. 记录落库：调用本地存储替身写 JSONL，带批次幂等键，
   网络中断后再次确认复用原批次、原文件，不会再写一份；
4. 推进游标：只有第 3 步确认成功后，才把来源水位推进到新水位并开具收据。

任意一步失败都在锁内返回，已修改的内存状态回滚，水位原样保留。
因此「写入受阻时水位必须留在原处」是结构性保证，而不是约定。
"""
from __future__ import annotations

import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from app.services.local_sink import LocalStorageSink, StorageSinkError
from app.store import store

# 批次状态
STAGED = "已暂存"        # 操作员已暂存，尚未确认投递
DELIVERED = "已投递"     # 记录写入成功且水位已推进，收据已开具
WRITE_FAILED = "写入受阻"  # 投递失败，水位未动，可从失败步骤接着提交

# 来源水位初始种子：演示用，真实系统来自增量游标表
SOURCE_SEED: list[dict[str, str]] = [
    {"source": "orders-db.order_events", "watermark": "2026-11-10T20:00:00Z"},
    {"source": "logs.web.clickstream", "watermark": "offset-102400"},
]


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


class BatchConflictError(RuntimeError):
    """业务冲突（批次号重用且内容/目标变化、水位已被其他批次推进等）。"""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class BatchCommitService:
    def __init__(self, sink: LocalStorageSink) -> None:
        self._sink = sink
        self._lock = threading.RLock()
        # 来源 -> 当前水位
        self._watermarks: dict[str, str] = {
            item["source"]: item["watermark"] for item in SOURCE_SEED
        }
        # (来源, 批次号) -> 批次
        self._batches: dict[tuple[str, str], dict[str, Any]] = {}
        # 收据（投递成功后永久保留，记录当次水位与存储目标）
        self._receipts: list[dict[str, Any]] = []
        # 自增序号
        self._batch_seq = 0

    # ================= 基础查询 =================

    def list_sources(self) -> list[dict[str, Any]]:
        with self._lock:
            return [
                {"source": source, "current_watermark": wm}
                for source, wm in self._watermarks.items()
            ]

    def list_targets(self) -> list[dict[str, Any]]:
        """连接现有采集存储配置，只允许向「已启用」的配置投递。"""
        rows = store.rows("storage")
        return [
            {
                "target_id": int(row["id"]),
                "配置编号": row.get("配置编号"),
                "存储类型": row.get("存储类型"),
                "连接地址": row.get("连接地址"),
                "输出路径": row.get("输出路径"),
                "status": row.get("status"),
            }
            for row in rows
        ]

    def list_batches(self, *, source: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            items = [dict(batch) for batch in self._batches.values()]
        if source:
            items = [item for item in items if item["source"] == source]
        items.sort(key=lambda item: item["id"])
        return items

    def get_batch(self, source: str, batch_no: str) -> dict[str, Any] | None:
        with self._lock:
            batch = self._batches.get((source, batch_no))
            return dict(batch) if batch else None

    def list_receipts(self, *, target_id: int | None = None) -> list[dict[str, Any]]:
        with self._lock:
            items = [dict(item) for item in self._receipts]
        if target_id is not None:
            items = [item for item in items if item["target_id"] == target_id]
        items.sort(key=lambda item: item["receipt_id"], reverse=True)
        return items

    def get_receipt(self, receipt_id: str) -> dict[str, Any] | None:
        with self._lock:
            for receipt in self._receipts:
                if receipt["receipt_id"] == receipt_id:
                    return dict(receipt)
        return None

    def snapshot(self) -> dict[str, Any]:
        """工作区总览：水位、批次、收据与替身落盘情况，一屏看清系统真实状态。"""
        with self._lock:
            return {
                "failure_mode": self._sink.failure_mode,
                "sources": self.list_sources(),
                "targets": self.list_targets(),
                "batches": self.list_batches(),
                "receipts": self.list_receipts(),
                "sink_files": self._sink.inspect(),
            }

    # ================= 第一步：暂存批次 =================

    def stage_batch(
        self,
        *,
        source: str,
        batch_no: str,
        old_watermark: str,
        new_watermark: str,
        records_text: str,
    ) -> tuple[dict[str, Any] | None, str, str]:
        """操作员暂存一个批次。返回 (批次, 错误码, 错误信息)。

        暂存只登记批次内容，不触碰存储、也不动水位；存储目标在确认投递时才锁定。
        """
        source = source.strip()
        batch_no = batch_no.strip()
        old_watermark = old_watermark.strip()
        new_watermark = new_watermark.strip()
        records = self._parse_records(records_text)

        if not source or not batch_no:
            return None, "INVALID", "来源与批次号不能为空"
        if not old_watermark or not new_watermark:
            return None, "INVALID", "旧水位与新水位不能为空"
        if old_watermark == new_watermark:
            return None, "INVALID", "新水位必须高于旧水位，不能提交一个原地不动的批次"
        if not records:
            return None, "INVALID", "本次离线记录至少要有一条非空内容"
        if source not in self._watermarks:
            return None, "UNKNOWN_SOURCE", f"来源「{source}」不在增量游标登记范围内"

        with self._lock:
            key = (source, batch_no)
            existing = self._batches.get(key)
            if existing is not None:
                # 批次号重用：内容完全一致视为重复暂存（幂等返回原批次），
                # 只要有一个字段变了就拒绝，防止同号批次承载不同内容。
                same = (
                    existing["old_watermark"] == old_watermark
                    and existing["new_watermark"] == new_watermark
                    and existing["records"] == records
                )
                if not same:
                    return (
                        None,
                        "BATCH_REUSED",
                        f"批次号 {batch_no} 已被来源 {source} 的另一份内容占用，"
                        "重用批次号但内容改变必须拒绝",
                    )
                return dict(existing), "OK", "批次已存在且内容一致，直接复用原暂存"

            self._batch_seq += 1
            batch = {
                "id": self._batch_seq,
                "source": source,
                "batch_no": batch_no,
                "old_watermark": old_watermark,
                "new_watermark": new_watermark,
                "records": records,
                "record_count": len(records),
                "status": STAGED,
                "target_id": None,
                "attempts": 0,
                "last_error": None,
                "idempotency_key": None,
                "staged_at": _now(),
                "updated_at": _now(),
            }
            self._batches[key] = batch
            return dict(batch), "OK", "批次已暂存，等待确认投递"

    # ================= 第二步：原子确认投递 =================

    def commit_batch(
        self,
        *,
        source: str,
        batch_no: str,
        target_id: int,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str, str]:
        """确认投递一个已暂存批次。

        返回 (批次, 收据, 错误码, 信息)。成功时收据非空；失败时收据为空、水位未动。
        """
        with self._lock:
            key = (source.strip(), batch_no.strip())
            batch = self._batches.get(key)
            if batch is None:
                return None, None, "NOT_FOUND", f"来源 {source} 的批次 {batch_no} 尚未暂存"

            # 已投递：直接把原收据还回去（天然幂等，绝不重复写）。
            if batch["status"] == DELIVERED:
                receipt = self._find_receipt(batch["idempotency_key"])
                return dict(batch), receipt, "ALREADY_DELIVERED", "该批次已投递，复用原收据，未重复写入"

            target = store.find("storage", int(target_id))
            if target is None:
                return None, None, "UNKNOWN_TARGET", f"存储配置 {target_id} 不存在"
            if target.get("status") != "已启用":
                return None, None, "TARGET_DISABLED", (
                    f"存储配置 {target.get('配置编号')} 当前状态为「{target.get('status')}」，"
                    "只能向已启用的配置投递"
                )

            # 批次号重用但目标改变：首次投递时锁定目标，之后不允许改投。
            if batch["target_id"] is not None and batch["target_id"] != target_id:
                return None, None, "TARGET_CHANGED", (
                    f"批次 {batch_no} 已锁定存储配置 {batch['target_id']}，"
                    "批次号重用但存储目标改变必须拒绝"
                )

            # ---- 原子事务开始 ----
            snapshot_wm = self._watermarks[batch["source"]]
            if snapshot_wm != batch["old_watermark"]:
                batch["last_error"] = (
                    f"水位争用：来源当前水位已推进到 {snapshot_wm}，"
                    f"本批次登记的旧水位 {batch['old_watermark']} 已失效"
                )
                batch["updated_at"] = _now()
                return None, None, "WATERMARK_CONFLICT", batch["last_error"]

            idem_key = batch["idempotency_key"] or self._build_idem_key(batch, target_id)
            batch["target_id"] = target_id
            batch["idempotency_key"] = idem_key
            batch["attempts"] += 1
            before_wm = self._watermarks[batch["source"]]

            try:
                sink_result = self._sink.write_batch(
                    target_id=target_id,
                    idempotency_key=idem_key,
                    records=[
                        {
                            "source": batch["source"],
                            "batch_no": batch["batch_no"],
                            "seq": seq,
                            "old_watermark": batch["old_watermark"],
                            "new_watermark": batch["new_watermark"],
                            "content": record,
                        }
                        for seq, record in enumerate(batch["records"], start=1)
                    ],
                )
            except StorageSinkError as exc:
                # 写入受阻：本事务内没有任何已提交改动，水位留在原处。
                # possibly_written 的场景（响应丢失）交给下一次确认通过幂等键查证恢复。
                batch["status"] = WRITE_FAILED
                batch["last_error"] = str(exc)
                batch["updated_at"] = _now()
                assert self._watermarks[batch["source"]] == before_wm  # 结构性自检
                return None, None, "WRITE_BLOCKED", str(exc)

            # ---- 记录已确认落库，此刻才允许推进游标 ----
            self._watermarks[batch["source"]] = batch["new_watermark"]
            batch["status"] = DELIVERED
            batch["last_error"] = None
            batch["updated_at"] = _now()

            receipt = self._find_receipt(idem_key)
            if receipt is None:
                receipt = {
                    "receipt_id": f"RCPT-{uuid.uuid4().hex[:10]}",
                    "source": batch["source"],
                    "batch_no": batch["batch_no"],
                    "batch_id": batch["id"],
                    "old_watermark": batch["old_watermark"],
                    "new_watermark": batch["new_watermark"],
                    "target_id": target_id,
                    "target_code": target.get("配置编号"),
                    "target_type": target.get("存储类型"),
                    "target_address": target.get("连接地址"),
                    "record_count": sink_result.record_count,
                    "idempotency_key": idem_key,
                    "sink_path": sink_result.path,
                    "reused_file": sink_result.reused,
                    "delivered_at": _now(),
                }
                self._receipts.append(receipt)

            return dict(batch), dict(receipt), "OK", (
                "记录已写入存储，水位推进完成"
                if not sink_result.reused
                else "检测到原批次文件已存在，复用幂等记录并推进水位，未重复写入"
            )

    # ================= 演示控制 =================

    def set_failure_mode(self, mode: str) -> dict[str, Any]:
        self._sink.set_failure_mode(mode)
        return {"failure_mode": mode, "message": f"故障模式已切换为：{mode}"}

    def reset_workspace(self) -> dict[str, Any]:
        """清空工作区与替身文件，恢复初始水位（仅本地演示/测试使用）。"""
        with self._lock:
            self._sink.reset()
            self._watermarks = {
                item["source"]: item["watermark"] for item in SOURCE_SEED
            }
            self._batches.clear()
            self._receipts.clear()
            self._batch_seq = 0
        return {"ok": True, "message": "工作区与本地存储替身已重置"}

    def read_sink_records(self, target_id: int, idempotency_key: str) -> list[dict[str, Any]]:
        return self._sink.read_records(target_id, idempotency_key)

    def run_contention_demo(self) -> dict[str, Any]:
        """内置并发争用演示：两个线程用同一旧水位同时提交，只允许一个成功。

        用 Barrier 保证两个请求真正同时进入临界区，结果不受手速影响、可重复。
        """
        import threading as _threading

        source = SOURCE_SEED[0]["source"]
        start_wm = self._watermarks[source]
        target_id = next(
            (int(row["id"]) for row in store.rows("storage") if row.get("status") == "已启用"),
            int(store.rows("storage")[0]["id"]),
        )
        # 演示前先恢复正常模式，避免故障注入干扰争用结论。
        self._sink.set_failure_mode("normal")

        # 暂存两个批次，登记相同的旧水位、不同的新水位。
        self.stage_batch(
            source=source, batch_no="CONTEND-A",
            old_watermark=start_wm, new_watermark=f"{start_wm}#A",
            records_text="争用批次A-记录1\n争用批次A-记录2",
        )
        self.stage_batch(
            source=source, batch_no="CONTEND-B",
            old_watermark=start_wm, new_watermark=f"{start_wm}#B",
            records_text="争用批次B-记录1",
        )

        barrier = _threading.Barrier(2)
        outcomes: list[dict[str, Any]] = []

        def worker(batch_no: str) -> None:
            barrier.wait()  # 两个线程同时开始提交
            _, receipt, code, message = self.commit_batch(
                source=source, batch_no=batch_no, target_id=target_id,
            )
            outcomes.append({
                "batch_no": batch_no,
                "code": code,
                "message": message,
                "receipt_id": receipt["receipt_id"] if receipt else None,
            })

        t1 = _threading.Thread(target=worker, args=("CONTEND-A",))
        t2 = _threading.Thread(target=worker, args=("CONTEND-B",))
        t1.start(); t2.start()
        t1.join(); t2.join()

        winners = [item for item in outcomes if item["code"] == "OK"]
        losers = [item for item in outcomes if item["code"] == "WATERMARK_CONFLICT"]
        return {
            "source": source,
            "old_watermark": start_wm,
            "current_watermark": self._watermarks[source],
            "outcomes": sorted(outcomes, key=lambda item: item["batch_no"]),
            "success_count": len(winners),
            "conflict_count": len(losers),
            "only_one_succeeded": len(winners) == 1 and len(losers) == 1,
        }

    # ================= 内部工具 =================

    @staticmethod
    def _parse_records(records_text: str) -> list[str]:
        return [line.strip() for line in records_text.splitlines() if line.strip()]

    @staticmethod
    def _build_idem_key(batch: dict[str, Any], target_id: int) -> str:
        # 幂等键绑定 来源+批次号+目标：同号改目标在前面已被拒绝，
        # 因此同一键永远对应同一份内容与同一个目标，重放安全。
        safe_source = batch["source"].replace(".", "_").replace("/", "_")
        return f"{safe_source}__{batch['batch_no']}__t{target_id}"

    def _find_receipt(self, idempotency_key: str) -> dict[str, Any] | None:
        for receipt in self._receipts:
            if receipt["idempotency_key"] == idempotency_key:
                return dict(receipt)
        return None


# 单例：存储替身文件放在后端运行目录下的 var/sink，可随时打开核对。
_sink = LocalStorageSink(str(Path(__file__).resolve().parents[2] / "var" / "sink"))
batch_service = BatchCommitService(_sink)
