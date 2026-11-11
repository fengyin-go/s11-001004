"""批次水位提交工作区的端到端场景验证（不依赖网络，直接用 FastAPI TestClient）。

覆盖需求点：
1. 正常成功：记录真实落盘 + 收据 + 水位推进；
2. 写入失败（deny）：水位必须留在原处，批次进入「写入受阻」，可接着提交；
3. 响应丢失（lost_ack）：重连后复用原批次，不重复写第二份；
4. 瞬时抖动（flaky）：第一次失败、第二次成功；
5. 并发争用：两个批次争用同一旧水位，只允许一个成功；
6. 批次号重用但内容/目标改变：拒绝；内容一致：幂等复用；
7. 历史收据保留当次水位与存储目标，且能打开替身文件核对记录数。
"""
from __future__ import annotations

import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402
from app.services.batch_commit import batch_service  # noqa: E402

client = TestClient(app)
PASS = 0
FAIL = 0


def check(name: str, condition: bool, detail: str = "") -> None:
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name} {detail}")


def reset() -> None:
    client.post("/api/batch-workspace/reset")


def stage(source: str, batch_no: str, old: str, new: str, text: str):
    return client.post("/api/batch-workspace/stage", json={
        "source": source, "batch_no": batch_no,
        "old_watermark": old, "new_watermark": new, "records_text": text,
    }).json()


def commit(source: str, batch_no: str, target_id: int):
    return client.post("/api/batch-workspace/commit", json={
        "source": source, "batch_no": batch_no, "target_id": target_id,
    }).json()


def wm(source: str) -> str:
    items = client.get("/api/batch-workspace/sources").json()["items"]
    return next(item["current_watermark"] for item in items if item["source"] == source)


def main() -> int:
    snap = client.get("/api/batch-workspace/snapshot").json()
    source = snap["sources"][0]["source"]
    initial_wm = snap["sources"][0]["current_watermark"]
    # 种子存储配置里 id=3 是「已启用」
    target = next(item for item in snap["targets"] if item["status"] == "已启用")
    target_id = int(target["target_id"])
    disabled_target = next(item for item in snap["targets"] if item["status"] != "已启用")

    print("== 场景 1：正常成功，落盘文件可核对，水位推进 ==")
    reset()
    check("初始水位为种子值", wm(source) == initial_wm, wm(source))
    r = stage(source, "B-1001", initial_wm, f"{initial_wm}|step1", "记录甲\n记录乙\n记录丙")
    check("暂存成功", r["ok"], r)
    r = commit(source, "B-1001", target_id)
    check("投递成功", r["ok"], r)
    receipt = r["entry"]["receipt"]
    check("收据记录数为 3", receipt["record_count"] == 3, receipt)
    check("收据保留当次旧/新水位",
          receipt["old_watermark"] == initial_wm and receipt["new_watermark"] == f"{initial_wm}|step1")
    check("收据保留存储目标", receipt["target_id"] == target_id and receipt["target_code"])
    check("水位已推进到新值", wm(source) == f"{initial_wm}|step1", wm(source))
    path = Path(receipt["sink_path"])
    check("替身文件真实存在", path.exists(), str(path))
    lines = path.read_text(encoding="utf-8").strip().splitlines()
    check("文件内确实有 3 条记录", len(lines) == 3, lines)
    records = client.get("/api/batch-workspace/sink-records",
                         params={"target_id": target_id, "idempotency_key": receipt["idempotency_key"]}).json()
    check("可通过接口打开落盘记录", len(records["items"]) == 3 and records["items"][0]["content"] == "记录甲")

    print("== 场景 2：重复确认同一批次 -> 幂等，复用原收据，不写第二份 ==")
    r2 = commit(source, "B-1001", target_id)
    check("重复投递返回成功（幂等）", r2["ok"], r2)
    check("复用同一张收据", r2["entry"]["receipt"]["receipt_id"] == receipt["receipt_id"])
    check("水位没有被二次推进", wm(source) == f"{initial_wm}|step1")
    check("替身目录里该批次仍只有一个文件",
          len([p for p in path.parent.glob("*.jsonl")]) == 1)

    print("== 场景 3：deny 持续拒绝 -> 水位留在原处，可从失败步骤恢复 ==")
    reset()
    client.post("/api/batch-workspace/failure-mode", json={"mode": "deny"})
    stage(source, "B-2001", initial_wm, f"{initial_wm}|deny", "受阻记录1\n受阻记录2")
    r = commit(source, "B-2001", target_id)
    check("投递被拒绝", not r["ok"] and r["entry"]["code"] == "WRITE_BLOCKED", r)
    check("水位留在原处", wm(source) == initial_wm, wm(source))
    batch = next(b for b in client.get("/api/batch-workspace/batches").json()["items"]
                 if b["batch_no"] == "B-2001")
    check("批次状态为写入受阻", batch["status"] == "写入受阻" and batch["attempts"] == 1)
    check("失败步骤有错误说明", bool(batch["last_error"]))
    sink_files = client.get("/api/batch-workspace/snapshot").json()["sink_files"]
    check("拒绝时没有产生任何落盘文件", sink_files == [], sink_files)
    # 恢复正常后，从失败步骤接着提交（同一批次号）
    client.post("/api/batch-workspace/failure-mode", json={"mode": "normal"})
    r = commit(source, "B-2001", target_id)
    check("恢复后接着提交成功", r["ok"], r)
    check("恢复后水位才推进", wm(source) == f"{initial_wm}|deny", wm(source))
    check("恢复后收据记录数仍为 2", r["entry"]["receipt"]["record_count"] == 2)

    print("== 场景 4：lost_ack 响应丢失 -> 重连复用原文件，不重复写 ==")
    reset()
    client.post("/api/batch-workspace/failure-mode", json={"mode": "lost_ack"})
    stage(source, "B-3001", initial_wm, f"{initial_wm}|lost", "丢失确认的记录1\n丢失确认的记录2")
    r = commit(source, "B-3001", target_id)
    check("首次确认收到网络中断错误", not r["ok"] and r["entry"]["code"] == "WRITE_BLOCKED", r)
    check("中断时水位未推进", wm(source) == initial_wm)
    # 文件其实已经写入
    snap2 = client.get("/api/batch-workspace/snapshot").json()
    check("响应丢失但记录已真实落盘", len(snap2["sink_files"]) == 1, snap2["sink_files"])
    # 网络恢复，再次确认同一批次
    client.post("/api/batch-workspace/failure-mode", json={"mode": "normal"})
    r = commit(source, "B-3001", target_id)
    check("再次确认成功", r["ok"], r)
    check("复用原文件标志为真", r["entry"]["receipt"]["reused_file"] is True)
    check("只有一个落盘文件（没写第二份）",
          len(client.get("/api/batch-workspace/snapshot").json()["sink_files"]) == 1)
    check("水位只推进一次", wm(source) == f"{initial_wm}|lost")

    print("== 场景 5：flaky 瞬时抖动 -> 首次失败、再次提交成功 ==")
    reset()
    client.post("/api/batch-workspace/failure-mode", json={"mode": "flaky"})
    stage(source, "B-4001", initial_wm, f"{initial_wm}|flaky", "抖动记录1")
    r1 = commit(source, "B-4001", target_id)
    check("首次写入失败", not r1["ok"] and r1["entry"]["code"] == "WRITE_BLOCKED")
    check("失败时水位不动", wm(source) == initial_wm)
    r2 = commit(source, "B-4001", target_id)
    check("再次提交成功", r2["ok"], r2)
    check("水位推进", wm(source) == f"{initial_wm}|flaky")
    check("尝试次数累计为 2", r2["entry"]["batch"]["attempts"] == 2)

    print("== 场景 6：并发争用同一旧水位 -> 只允许一个成功 ==")
    result = client.post("/api/batch-workspace/demo/contention").json()
    check("两个线程都跑完", len(result["outcomes"]) == 2, result)
    check("恰好一个成功", result["success_count"] == 1, result)
    check("另一个被水位争用拒绝", result["conflict_count"] == 1, result)
    check("只有胜者的水位生效", result["current_watermark"] != result["old_watermark"])
    # 手工并发再验一次（不依赖内置演示接口），用 Barrier 同时提交两个不同批次
    reset()
    stage(source, "P-A", initial_wm, f"{initial_wm}|pa", "并发A")
    stage(source, "P-B", initial_wm, f"{initial_wm}|pb", "并发B")
    barrier = threading.Barrier(2)
    answers: list[dict] = []

    def worker(no: str) -> None:
        barrier.wait()
        answers.append(commit(source, no, target_id))

    t1 = threading.Thread(target=worker, args=("P-A",))
    t2 = threading.Thread(target=worker, args=("P-B",))
    t1.start(); t2.start(); t1.join(); t2.join()
    ok_count = sum(1 for a in answers if a["ok"])
    conflict = [a for a in answers if not a["ok"] and a["entry"]["code"] == "WATERMARK_CONFLICT"]
    check("手工并发：恰好一个成功", ok_count == 1, answers)
    check("手工并发：败者报 WATERMARK_CONFLICT", len(conflict) == 1)
    check("手工并发：落盘文件只有一份",
          len(client.get("/api/batch-workspace/snapshot").json()["sink_files"]) == 1)

    print("== 场景 7：批次号重用但内容/目标改变必须拒绝 ==")
    reset()
    client.post("/api/batch-workspace/failure-mode", json={"mode": "normal"})
    stage(source, "B-7001", initial_wm, f"{initial_wm}|v1", "原始内容")
    r = stage(source, "B-7001", initial_wm, f"{initial_wm}|v2", "被篡改的内容")
    check("内容改变被拒绝", not r["ok"] and r["entry"]["code"] == "BATCH_REUSED", r)
    r = stage(source, "B-7001", initial_wm, f"{initial_wm}|v1", "原始内容")
    check("同内容重复暂存幂等放行", r["ok"], r)

    # 启用第二个存储配置，保证「目标改变」不会与「未启用」两个原因混淆
    other_target = next(i for i in client.get("/api/batch-workspace/targets").json()["items"]
                        if int(i["target_id"]) != target_id)
    client.post(f"/api/storage/{other_target['target_id']}/actions",
                json={"values": {"action": "启用配置"}})
    disabled_target = next(i for i in client.get("/api/batch-workspace/targets").json()["items"]
                           if i["status"] != "已启用")

    # 用当前水位暂存批次，先在目标 A 上写入失败从而锁定目标，再改投目标 B
    cur_wm = wm(source)
    stage(source, "B-7003", cur_wm, f"{cur_wm}|lock2", "换目标记录")
    client.post("/api/batch-workspace/failure-mode", json={"mode": "deny"})
    r = commit(source, "B-7003", target_id)
    check("首次在目标A受阻并锁定目标", not r["ok"] and r["entry"]["code"] == "WRITE_BLOCKED", r)
    client.post("/api/batch-workspace/failure-mode", json={"mode": "normal"})
    r = commit(source, "B-7003", int(other_target["target_id"]))
    check("存储目标改变被拒绝", not r["ok"] and r["entry"]["code"] == "TARGET_CHANGED", r)
    # 向未启用配置投递也要被拦
    stage(source, "B-7004", cur_wm, f"{cur_wm}|dis", "禁用目标记录")
    r = commit(source, "B-7004", int(disabled_target["target_id"]))
    check("未启用存储目标被拒绝", not r["ok"] and r["entry"]["code"] == "TARGET_DISABLED", r)

    print("== 场景 8：存储页收据与记录数 ==")
    reset()
    stage(source, "B-8001", initial_wm, f"{initial_wm}|r1", "收据记录1\n收据记录2")
    commit(source, "B-8001", target_id)
    source2 = client.get("/api/batch-workspace/sources").json()["items"][1]["source"]
    wm2 = next(i["current_watermark"] for i in client.get("/api/batch-workspace/sources").json()["items"]
               if i["source"] == source2)
    stage(source2, "B-8002", wm2, f"{wm2}|r2", "另一来源的记录1")
    commit(source2, "B-8002", target_id)
    per_target = client.get(f"/api/storage/{target_id}/receipts").json()
    check("按目标汇总批次数为 2", per_target["total_batches"] == 2, per_target)
    check("按目标汇总记录数为 3", per_target["total_records"] == 3, per_target)
    receipt_id = per_target["items"][0]["receipt_id"]
    detail = client.get(f"/api/receipts/{receipt_id}").json()
    check("收据详情可打开且含水位与目标",
          detail["old_watermark"] and detail["target_code"] and len(detail["records"]) >= 1)
    all_receipts = client.get("/api/receipts").json()["items"]
    check("历史收据全部保留", len(all_receipts) == 2)

    print("== 场景 9：输入校验 ==")
    r = stage(source, "B-9001", initial_wm, initial_wm, "新旧水位相同")
    check("新旧水位相同被拒绝", not r["ok"] and r["entry"]["code"] == "INVALID")
    r = stage(source, "B-9002", initial_wm, f"{initial_wm}|x", "  \n  ")
    check("空记录被拒绝", not r["ok"])
    r = stage("not-exist-source", "B-9003", "a", "b", "记录")
    check("未知来源被拒绝", not r["ok"] and r["entry"]["code"] == "UNKNOWN_SOURCE")

    print(f"\n结果：通过 {PASS} 项，失败 {FAIL} 项")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
