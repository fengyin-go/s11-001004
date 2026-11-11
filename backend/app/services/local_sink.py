"""本地可控的存储替身。

真实项目里这一层会换成对象存储 / 数据湖 / 消息队列的客户端；演示环境里我们把
每条离线记录真实落成本地 JSONL 文件（带临时文件 + fsync 的原子替换），从而让
「记录写入成功」是可以打开文件验证的事实，而不只是页面上的数字变化。

同时内置三种故障开关，用来在同一套代码上演示写入失败、响应丢失与瞬时抖动：

- deny      ：始终拒绝写入（模拟目标存储不可用 / 权限不足），不会产生任何文件；
- lost_ack  ：记录其实已经落盘，但客户端收到网络中断错误（模拟写成功但响应丢失）；
- flaky     ：同一批次第一次写失败、之后恢复，用来演示从失败步骤接着提交；
- normal    ：不注入故障。
"""
from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


class StorageSinkError(RuntimeError):
    """存储替身写入失败。

    possibly_written=True 表示「请求方无法确认结果」：记录可能已经落盘
    （典型场景：写入成功但响应在网络中断中丢失），调用方必须先查证再决定是否重写。
    """

    def __init__(self, message: str, *, possibly_written: bool = False) -> None:
        super().__init__(message)
        self.possibly_written = possibly_written


# 故障模式 -> 对外的中文说明
FAILURE_MODES: dict[str, str] = {
    "normal": "正常：写入即成功并返回收据",
    "deny": "拒绝写入：目标存储持续不可用，记录不会落盘",
    "lost_ack": "响应丢失：记录实际已落盘，但返回网络中断错误",
    "flaky": "瞬时抖动：每个批次首次写入失败，再次提交恢复",
}


@dataclass
class SinkWriteResult:
    target_id: int
    path: str
    record_count: int
    idempotency_key: str
    reused: bool  # True 表示命中幂等，没有重复写第二份


@dataclass
class _TargetState:
    root: Path
    # idempotency_key -> 已落盘文件相对路径，保证同一批次只写一份
    written: dict[str, str] = field(default_factory=dict)
    # flaky 模式下已经「失败过一次」的批次，第二次放行
    flaky_seen: set[str] = field(default_factory=set)


class LocalStorageSink:
    """按存储配置隔离目录的本地文件存储替身。"""

    def __init__(self, base_dir: str | os.PathLike[str]) -> None:
        self._base = Path(base_dir)
        self._base.mkdir(parents=True, exist_ok=True)
        self._targets: dict[int, _TargetState] = {}
        self._failure_mode = "normal"
        self._lock = threading.RLock()

    # ----- 故障注入（仅本地演示用） -----

    def set_failure_mode(self, mode: str) -> str:
        if mode not in FAILURE_MODES:
            raise ValueError(f"未知故障模式：{mode}")
        with self._lock:
            self._failure_mode = mode
        return mode

    @property
    def failure_mode(self) -> str:
        return self._failure_mode

    def reset(self) -> None:
        """清空所有已落盘文件与故障痕迹，恢复 normal。"""
        with self._lock:
            self._targets.clear()
            self._failure_mode = "normal"
            for child in self._base.glob("*"):
                if child.is_dir():
                    for path in child.glob("**/*"):
                        if path.is_file():
                            path.unlink()

    def _target(self, target_id: int) -> _TargetState:
        state = self._targets.get(target_id)
        if state is None:
            root = self._base / f"storage-{target_id}"
            root.mkdir(parents=True, exist_ok=True)
            state = _TargetState(root=root)
            self._targets[target_id] = state
        return state

    # ----- 真实写入 -----

    def write_batch(
        self,
        *,
        target_id: int,
        idempotency_key: str,
        records: list[dict[str, Any]],
    ) -> SinkWriteResult:
        """把一个批次的记录原子写入目标存储。

        以 idempotency_key 做幂等：同一批次重复投递直接复用原文件，绝不写第二份。
        """
        with self._lock:
            state = self._target(target_id)

            # 幂等命中：网络中断后重连，原批次直接复用，不再写一份。
            if idempotency_key in state.written:
                rel_path = state.written[idempotency_key]
                count = self._count_lines(state.root / rel_path)
                return SinkWriteResult(
                    target_id=target_id,
                    path=str(state.root / rel_path),
                    record_count=count,
                    idempotency_key=idempotency_key,
                    reused=True,
                )

            mode = self._failure_mode
            if mode == "deny":
                raise StorageSinkError(
                    "存储替身拒绝写入：目标存储当前不可用，记录未持久化",
                    possibly_written=False,
                )
            if mode == "flaky" and idempotency_key not in state.flaky_seen:
                state.flaky_seen.add(idempotency_key)
                raise StorageSinkError(
                    "存储替身瞬时抖动：首次写入失败，请从失败步骤重新提交",
                    possibly_written=False,
                )

            rel_path = f"{idempotency_key}.jsonl"
            final_path = state.root / rel_path
            tmp_path = state.root / f".{idempotency_key}.tmp"
            # 临时文件写完再原子替换，避免半截文件被当成有效批次。
            with tmp_path.open("w", encoding="utf-8") as handle:
                for record in records:
                    handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, final_path)
            state.written[idempotency_key] = rel_path

            if mode == "lost_ack":
                # 数据已经在上面落盘，却模拟响应在回程中断：调用方无法直接确认。
                raise StorageSinkError(
                    "响应丢失：写入请求已发出但连接中断，结果未知，请重新确认",
                    possibly_written=True,
                )

            return SinkWriteResult(
                target_id=target_id,
                path=str(final_path),
                record_count=len(records),
                idempotency_key=idempotency_key,
                reused=False,
            )

    def has_batch(self, target_id: int, idempotency_key: str) -> bool:
        """查证某一批次是否已经真实落盘（用于响应丢失后的恢复判断）。"""
        with self._lock:
            state = self._targets.get(target_id)
            if state is None or idempotency_key not in state.written:
                return False
            return (state.root / state.written[idempotency_key]).exists()

    # ----- 演示核查：证明成功不是数字变化 -----

    def inspect(self) -> list[dict[str, Any]]:
        """列出每个目标存储下落盘的批次文件与记录数，供页面/测试打开核对。"""
        with self._lock:
            result: list[dict[str, Any]] = []
            for target_id, state in sorted(self._targets.items()):
                for key, rel_path in sorted(state.written.items()):
                    path = state.root / rel_path
                    result.append({
                        "target_id": target_id,
                        "idempotency_key": key,
                        "path": str(path),
                        "record_count": self._count_lines(path),
                    })
            return result

    def read_records(self, target_id: int, idempotency_key: str) -> list[dict[str, Any]]:
        with self._lock:
            state = self._targets.get(target_id)
            if state is None or idempotency_key not in state.written:
                return []
            path = state.root / state.written[idempotency_key]
            with path.open("r", encoding="utf-8") as handle:
                return [json.loads(line) for line in handle if line.strip()]

    @staticmethod
    def _count_lines(path: Path) -> int:
        with path.open("r", encoding="utf-8") as handle:
            return sum(1 for line in handle if line.strip())
