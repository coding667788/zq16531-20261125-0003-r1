"""协议常量、规范化哈希、崩溃注入与回执构造。

本模块不含任何数据库代码，纯函数，便于单元测试与参考模型复用。
"""
from __future__ import annotations

import hashlib
import json
import os

# 协调请求状态机（coord.db.requests.state）
RECEIVED = "RECEIVED"          # 已登记，尚未决定
COMMITTED = "COMMITTED"        # 已做出提交决定（提交点，不可逆）
ABORTED = "ABORTED"            # 已做出终止决定（终态）
COMPLETED = "COMPLETED"        # 两边均已落实，回执已固化（终态）

# 钥匙 / 证物袋条目在各自台账中的状态
RESERVED = "RESERVED"          # 半成品：已预留，业务读者不可视为消耗
CONSUMED = "CONSUMED"          # 已提交：钥匙真正消耗
BAG_PREPARED = "PREPARED"      # 半成品：证物袋交接已预备，持有人不变更
HANDED = "HANDED"              # 已提交：交接生效

# 终止原因（业务级终止，全部发生在提交决定之前，可安全重试）
REASON_LOCK_TIMEOUT = "LOCK_TIMEOUT"
REASON_KEY_NOT_AVAILABLE = "KEY_NOT_AVAILABLE"
REASON_BAG_NOT_FOUND = "BAG_NOT_FOUND"
REASON_BAG_HOLDER_MISMATCH = "BAG_HOLDER_MISMATCH"
REASON_BAG_BUSY = "BAG_BUSY"
REASON_RECOVERY_ABORT = "RECOVERY_ABORT"

CRASH_ENV = "DUALRECEIPT_CRASH"
DELAY_ENV = "DUALRECEIPT_DELAY"
LOCK_TIMEOUT_ENV = "DUALRECEIPT_LOCK_TIMEOUT"

# 命名崩溃边界：apply / recover 执行到对应持久化点之后，
# 若环境变量 DUALRECEIPT_CRASH 等于该名称，则 os._exit 硬退出。
CRASH_BOUNDARIES = (
    "CRASH_AFTER_COORD_RECEIVED",
    "CRASH_AFTER_KEYS_PREPARED",
    "CRASH_AFTER_BAGS_PREPARED",
    "CRASH_AFTER_COMMIT_DECISION",
    "CRASH_AFTER_KEYS_COMMITTED",
    "CRASH_AFTER_BAGS_COMMITTED",
    "CRASH_AFTER_ABORT_DECISION",
    "CRASH_AFTER_KEYS_ABORTED",
)

# 命名延迟边界：用于并发测试制造确定性的等待窗口。
DELAY_BOUNDARIES = (
    "DELAY_AFTER_LOCKS_ACQUIRED",
    "DELAY_IN_LOCK_WAIT",
)


def canonical_json(obj) -> str:
    """规范化 JSON：键排序、无多余空白、ASCII 关闭以稳定 UTF-8 字节。"""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False)


def content_hash(request: dict) -> str:
    """请求内容哈希：仅覆盖业务内容，不含 request_id 以外的易变字段。

    同 request_id 重试必须提交完全相同的内容，否则冲突。
    """
    payload = {
        "key_id": request["key_id"],
        "bag_id": request["bag_id"],
        "from_holder": request["from_holder"],
        "to_holder": request["to_holder"],
        "note": request.get("note", ""),
    }
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def maybe_crash(boundary: str) -> None:
    """到达命名崩溃边界：环境变量命中则硬退出（不执行 Python 清理）。

    使用 ``os._exit`` 而非 ``sys.exit``，保证不会跑 finally、不会刷
    解释器缓冲，模拟进程被 kill -9 / 断电。退出码 88 专供测试识别。
    """
    if os.environ.get(CRASH_ENV) == boundary:
        os._exit(88)


def maybe_delay(boundary: str) -> None:
    """命名延迟边界：仅并发测试使用，睡眠秒数由环境变量给出。"""
    if os.environ.get(DELAY_ENV) == boundary:
        import time
        time.sleep(float(os.environ.get("DUALRECEIPT_DELAY_SECONDS", "1.0")))


def lock_timeout_seconds() -> float:
    return float(os.environ.get(LOCK_TIMEOUT_ENV, "15.0"))


def committed_receipt(request_id: str, seq: int, chash: str) -> dict:
    """提交成功回执。seq 为协调日志中完成事件的逻辑时间。"""
    return {
        "receipt": "COMMITTED",
        "request_id": request_id,
        "content_hash": chash,
        "coord_seq": seq,
    }


def aborted_receipt(request_id: str, seq: int, chash: str, reason: str) -> dict:
    """终止回执：业务上两边都没有任何可见效果，可按原内容重试为新请求。"""
    return {
        "receipt": "ABORTED",
        "request_id": request_id,
        "content_hash": chash,
        "coord_seq": seq,
        "reason": reason,
    }


def pending_response(request_id: str, state: str, chash: str) -> dict:
    """决定尚未落实时的明确 pending 答复（不是终态回执）。"""
    return {
        "receipt": "PENDING",
        "request_id": request_id,
        "content_hash": chash,
        "coord_state": state,
        "hint": "决定已记录但尚未在两本账上落实，运行 recover 后重试",
    }


def conflict_response(request_id: str, existing_hash: str, incoming_hash: str) -> dict:
    return {
        "error": "ID_CONTENT_CONFLICT",
        "request_id": request_id,
        "existing_content_hash": existing_hash,
        "incoming_content_hash": incoming_hash,
    }
