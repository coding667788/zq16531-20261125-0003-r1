"""协调器：apply（传输）与 recover（崩溃恢复）。

协议时序（每个 "==" 是一次独立 SQLite 事务、一次 fsync）::

  coord: RECEIVED ===============================
  lock:  资源锁（coord.db 锁表，全序加锁）
  keys:  RESERVE（钥匙 -> RESERVED，半成品）====
  bags:  PREPARE（袋子 -> PREPARED，持有人不变）=
  coord: COMMIT_DECISION =======================  ← 唯一提交点，不可逆
  keys:  COMMIT（钥匙 -> CONSUMED）=============
  bags:  COMMIT（袋子 -> HANDED，换人）=========
  coord: COMPLETED + 终态回执 ==================
  unlock

提交点之前任意崩溃：recover 走全量终止（ABORT），两边都无可见效果；
提交点之后任意崩溃：recover 前滚（COMMIT），两边都落实。
已返回成功的回执在 receipts 表中只读固化，恢复路径绝不改写。
"""
from __future__ import annotations

import json
import sqlite3
import time

from . import protocol as p
from . import store


class ConflictError(Exception):
    """同 request_id 但内容哈希不同。"""


class LockTimeoutError(Exception):
    """资源锁等待超时（调用方将其转成 ABORTED 回执）。"""


REQUIRED_FIELDS = ("request_id", "key_id", "bag_id", "from_holder", "to_holder")


class Coordinator:
    def __init__(self, keys_path, bags_path, coord_path):
        self.kc = store.connect(keys_path)
        self.bc = store.connect(bags_path)
        self.cc = store.connect(coord_path)

    def close(self):
        for c in (self.kc, self.bc, self.cc):
            c.close()

    # ------------------------------------------------------------------ apply
    def apply(self, request: dict) -> dict:
        for f in REQUIRED_FIELDS:
            if f not in request or not isinstance(request[f], str) or not request[f]:
                raise ValueError(f"缺少或非法字段: {f}")
        rid = request["request_id"]
        chash = p.content_hash(request)

        # 1) 登记请求（幂等 / 冲突检查的唯一入口）
        existing = self._register_received(rid, chash, request)
        if existing is not None:
            return self._answer_existing(rid, existing)

        # 2) 跨进程全序资源锁
        try:
            self._acquire_locks(rid, request["key_id"], request["bag_id"])
        except LockTimeoutError:
            return self._abort(rid, chash, request, p.REASON_LOCK_TIMEOUT,
                               keys_reserved=False, bags_prepared=False)
        p.maybe_delay(p.DELAY_BOUNDARIES[0])

        # 3) 两阶段 prepare，任一步业务预检失败 => 提交点之前安全终止
        try:
            keys_reserved = self._keys_prepare(rid, request["key_id"])
        except _PrecheckFailed as e:
            self._release_locks(rid)
            return self._abort(rid, chash, request, e.reason,
                               keys_reserved=False, bags_prepared=False)
        p.maybe_crash(p.CRASH_BOUNDARIES[1])  # CRASH_AFTER_KEYS_PREPARED

        try:
            bags_prepared = self._bags_prepare(rid, request)
        except _PrecheckFailed as e:
            return self._abort(rid, chash, request, e.reason,
                               keys_reserved=True, bags_prepared=False)
        p.maybe_crash(p.CRASH_BOUNDARIES[2])  # CRASH_AFTER_BAGS_PREPARED

        # 4) 提交决定（提交点）
        self._coord_decision(rid, p.COMMITTED)
        p.maybe_crash(p.CRASH_BOUNDARIES[3])  # CRASH_AFTER_COMMIT_DECISION

        # 5) 两边各自落实
        self._keys_commit(rid, request["key_id"])
        p.maybe_crash(p.CRASH_BOUNDARIES[4])  # CRASH_AFTER_KEYS_COMMITTED
        self._bags_commit(rid, request)
        p.maybe_crash(p.CRASH_BOUNDARIES[5])  # CRASH_AFTER_BAGS_COMMITTED

        # 6) 固化回执并解锁
        receipt = p.committed_receipt(rid, self._coord_finish(rid), chash)
        self._store_receipt(rid, receipt)
        self._release_locks(rid)
        return receipt

    # ------------------------------------------------------------- registration
    def _register_received(self, rid: str, chash: str,
                           request: dict) -> sqlite3.Row | None:
        """已存在则返回该行（含 state），新请求则落 RECEIVED 后返回 None。

        RECEIVED 事件保存完整业务内容：崩溃可能发生在两侧库留下任何
        事件之前（CRASH_AFTER_COORD_RECEIVED），恢复时只能从协调日志
        重建请求。完整内容入日志也让 audit 能独立复核 content_hash。
        """
        self.cc.execute("BEGIN IMMEDIATE")
        row = self.cc.execute(
            "SELECT * FROM requests WHERE request_id=?", (rid,)).fetchone()
        if row is not None:
            self.cc.execute("COMMIT")
            if row["content_hash"] != chash:
                raise ConflictError(rid)
            return row
        seq = store.next_seq(self.cc, "coord_events")
        self.cc.execute(
            "INSERT INTO requests(request_id,content_hash,state,reason,updated_seq)"
            " VALUES(?,?,?,NULL,?)", (rid, chash, p.RECEIVED, seq))
        self.cc.execute(
            "INSERT INTO coord_events(seq,event_type,request_id,detail)"
            " VALUES(?, 'RECEIVED', ?, ?)",
            (seq, rid, p.canonical_json(
                {"content_hash": chash, "request": request})))
        self.cc.execute("COMMIT")
        p.maybe_crash(p.CRASH_BOUNDARIES[0])  # CRASH_AFTER_COORD_RECEIVED
        return None

    def _answer_existing(self, rid: str, row: sqlite3.Row) -> dict:
        """同请求重试：终态给同一张回执，未落实给明确 pending。"""
        receipt_row = self.cc.execute(
            "SELECT payload FROM receipts WHERE request_id=?", (rid,)).fetchone()
        if receipt_row is not None:
            return json.loads(receipt_row["payload"])
        return p.pending_response(rid, row["state"], row["content_hash"])

    # -------------------------------------------------------------------- locks
    def _lock_keys(self, key_id: str, bag_id: str) -> list[str]:
        # 数字前缀给出全局全序：所有进程都先钥匙后袋子，杜绝循环等待。
        return sorted([f"0:key:{key_id}", f"1:bag:{bag_id}"])

    def _acquire_locks(self, rid: str, key_id: str, bag_id: str) -> None:
        locks = self._lock_keys(key_id, bag_id)
        deadline = time.monotonic() + p.lock_timeout_seconds()
        while True:
            self.cc.execute("BEGIN IMMEDIATE")
            try:
                for lk in locks:
                    self.cc.execute(
                        "INSERT INTO resource_locks(lock_key,request_id,acquired_at)"
                        " VALUES(?,?,?)", (lk, rid, time.time()))
                self.cc.execute("COMMIT")
                return
            except sqlite3.IntegrityError:
                self.cc.execute("COMMIT")
                p.maybe_delay(p.DELAY_BOUNDARIES[1])
                if time.monotonic() >= deadline:
                    raise LockTimeoutError(rid)
                time.sleep(0.05)

    def _release_locks(self, rid: str) -> None:
        self.cc.execute("BEGIN IMMEDIATE")
        self.cc.execute("DELETE FROM resource_locks WHERE request_id=?", (rid,))
        self.cc.execute("COMMIT")

    # ----------------------------------------------------------------- prepare
    def _keys_prepare(self, rid: str, key_id: str) -> bool:
        self.kc.execute("BEGIN IMMEDIATE")
        row = self.kc.execute("SELECT * FROM keys WHERE key_id=?", (key_id,)).fetchone()
        if row is None or row["status"] != "AVAILABLE":
            self.kc.execute("COMMIT")
            raise _PrecheckFailed(p.REASON_KEY_NOT_AVAILABLE)
        seq = store.next_seq(self.kc, "key_events")
        self.kc.execute(
            "UPDATE keys SET status=?, request_id=?, updated_seq=? WHERE key_id=?",
            (p.RESERVED, rid, seq, key_id))
        self.kc.execute(
            "INSERT INTO key_events(seq,event_type,key_id,request_id,detail)"
            " VALUES(?, 'RESERVE', ?, ?, '{}')", (seq, key_id, rid))
        self.kc.execute("COMMIT")
        return True

    def _bags_prepare(self, rid: str, req: dict) -> bool:
        self.bc.execute("BEGIN IMMEDIATE")
        row = self.bc.execute("SELECT * FROM bags WHERE bag_id=?",
                              (req["bag_id"],)).fetchone()
        if row is None:
            self.bc.execute("COMMIT")
            raise _PrecheckFailed(p.REASON_BAG_NOT_FOUND)
        # AVAILABLE（从未交接）或 HANDED（历史交接完成）均可再次交接；
        # 只有 PREPARED（另一请求的半成品）才是忙。
        if row["status"] not in ("AVAILABLE", p.HANDED):
            self.bc.execute("COMMIT")
            raise _PrecheckFailed(p.REASON_BAG_BUSY)
        if row["current_holder"] != req["from_holder"]:
            self.bc.execute("COMMIT")
            raise _PrecheckFailed(p.REASON_BAG_HOLDER_MISMATCH)
        seq = store.next_seq(self.bc, "bag_events")
        self.bc.execute(
            "UPDATE bags SET status=?, request_id=?, prepared_holder=?, updated_seq=?"
            " WHERE bag_id=?",
            (p.BAG_PREPARED, rid, req["to_holder"], seq, req["bag_id"]))
        self.bc.execute(
            "INSERT INTO bag_events(seq,event_type,bag_id,request_id,holder,detail)"
            " VALUES(?, 'PREPARE', ?, ?, ?, '{}')",
            (seq, req["bag_id"], rid, req["to_holder"]))
        self.bc.execute("COMMIT")
        return True

    # ---------------------------------------------------------------- decision
    def _coord_decision(self, rid: str, decision: str, reason: str | None = None) -> None:
        event = "COMMIT_DECISION" if decision == p.COMMITTED else "ABORT_DECISION"
        self.cc.execute("BEGIN IMMEDIATE")
        seq = store.next_seq(self.cc, "coord_events")
        self.cc.execute(
            "UPDATE requests SET state=?, reason=?, updated_seq=? WHERE request_id=?",
            (decision, reason, seq, rid))
        self.cc.execute(
            "INSERT INTO coord_events(seq,event_type,request_id,detail) VALUES(?,?,?,?)",
            (seq, event, rid, json.dumps({"reason": reason}, sort_keys=True)))
        self.cc.execute("COMMIT")

    # -------------------------------------------------------------- commit side
    def _keys_commit(self, rid: str, key_id: str) -> bool:
        """幂等：仅当钥匙仍处于本请求的 RESERVED 时写 COMMIT。"""
        self.kc.execute("BEGIN IMMEDIATE")
        row = self.kc.execute("SELECT * FROM keys WHERE key_id=?", (key_id,)).fetchone()
        if row is not None and row["status"] == p.RESERVED and row["request_id"] == rid:
            seq = store.next_seq(self.kc, "key_events")
            self.kc.execute(
                "UPDATE keys SET status=?, updated_seq=? WHERE key_id=?",
                (p.CONSUMED, seq, key_id))
            self.kc.execute(
                "INSERT INTO key_events(seq,event_type,key_id,request_id,detail)"
                " VALUES(?, 'COMMIT', ?, ?, '{}')", (seq, key_id, rid))
            acted = True
        else:
            acted = False
        self.kc.execute("COMMIT")
        return acted

    def _bags_commit(self, rid: str, req: dict) -> bool:
        self.bc.execute("BEGIN IMMEDIATE")
        row = self.bc.execute("SELECT * FROM bags WHERE bag_id=?",
                              (req["bag_id"],)).fetchone()
        if row is not None and row["status"] == p.BAG_PREPARED and row["request_id"] == rid:
            seq = store.next_seq(self.bc, "bag_events")
            self.bc.execute(
                "UPDATE bags SET current_holder=?, status=?, request_id=?,"
                " prepared_holder=NULL, updated_seq=? WHERE bag_id=?",
                (req["to_holder"], p.HANDED, rid, seq, req["bag_id"]))
            self.bc.execute(
                "INSERT INTO bag_events(seq,event_type,bag_id,request_id,holder,detail)"
                " VALUES(?, 'COMMIT', ?, ?, ?, '{}')",
                (seq, req["bag_id"], rid, req["to_holder"]))
            acted = True
        else:
            acted = False
        self.bc.execute("COMMIT")
        return acted

    # --------------------------------------------------------------- abort side
    def _keys_abort(self, rid: str, key_id: str) -> bool:
        self.kc.execute("BEGIN IMMEDIATE")
        row = self.kc.execute("SELECT * FROM keys WHERE key_id=?", (key_id,)).fetchone()
        if row is not None and row["status"] == p.RESERVED and row["request_id"] == rid:
            seq = store.next_seq(self.kc, "key_events")
            self.kc.execute(
                "UPDATE keys SET status='AVAILABLE', request_id=NULL, updated_seq=?"
                " WHERE key_id=?", (seq, key_id))
            self.kc.execute(
                "INSERT INTO key_events(seq,event_type,key_id,request_id,detail)"
                " VALUES(?, 'ABORT', ?, ?, '{}')", (seq, key_id, rid))
            acted = True
        else:
            acted = False
        self.kc.execute("COMMIT")
        p.maybe_crash(p.CRASH_BOUNDARIES[7])  # CRASH_AFTER_KEYS_ABORTED
        return acted

    def _bags_abort(self, rid: str, bag_id: str) -> bool:
        self.bc.execute("BEGIN IMMEDIATE")
        row = self.bc.execute("SELECT * FROM bags WHERE bag_id=?", (bag_id,)).fetchone()
        if row is not None and row["status"] == p.BAG_PREPARED and row["request_id"] == rid:
            seq = store.next_seq(self.bc, "bag_events")
            self.bc.execute(
                "UPDATE bags SET status='AVAILABLE', request_id=NULL,"
                " prepared_holder=NULL, updated_seq=? WHERE bag_id=?",
                (seq, bag_id))
            self.bc.execute(
                "INSERT INTO bag_events(seq,event_type,bag_id,request_id,holder,detail)"
                " VALUES(?, 'ABORT', ?, ?, NULL, '{}')", (seq, bag_id, rid))
            acted = True
        else:
            acted = False
        self.bc.execute("COMMIT")
        return acted

    def _abort(self, rid: str, chash: str, req: dict, reason: str,
               keys_reserved: bool, bags_prepared: bool) -> dict:
        """提交点之前的终止路径：决定 -> 两边回滚 -> 回执，全部幂等。"""
        self._coord_decision(rid, p.ABORTED, reason)
        p.maybe_crash(p.CRASH_BOUNDARIES[6])  # CRASH_AFTER_ABORT_DECISION
        if keys_reserved:
            self._keys_abort(rid, req["key_id"])
        if bags_prepared:
            self._bags_abort(rid, req["bag_id"])
        seq = self._coord_cleanup_done(rid)
        receipt = p.aborted_receipt(rid, seq, chash, reason)
        self._store_receipt(rid, receipt)
        self._release_locks(rid)
        return receipt

    # --------------------------------------------------------- coord bookkeeping
    def _coord_cleanup_done(self, rid: str) -> int:
        """终止路径收尾：记录两边清理完成，state 保持 ABORTED。"""
        self.cc.execute("BEGIN IMMEDIATE")
        done = []
        for ev in ("KEYS_DONE", "BAGS_DONE"):
            seq = store.next_seq(self.cc, "coord_events")
            self.cc.execute(
                "INSERT INTO coord_events(seq,event_type,request_id,detail)"
                " VALUES(?, ?, ?, '{}')", (seq, ev, rid))
            done.append(seq)
        self.cc.execute(
            "UPDATE requests SET updated_seq=? WHERE request_id=?", (done[-1], rid))
        self.cc.execute("COMMIT")
        return done[-1]

    def _coord_finish(self, rid: str) -> int:
        """提交路径收尾：KEYS_DONE/BAGS_DONE/COMPLETED 同事务落盘。"""
        self.cc.execute("BEGIN IMMEDIATE")
        last = 0
        for ev in ("KEYS_DONE", "BAGS_DONE", "COMPLETED"):
            last = store.next_seq(self.cc, "coord_events")
            self.cc.execute(
                "INSERT INTO coord_events(seq,event_type,request_id,detail)"
                " VALUES(?, ?, ?, '{}')", (last, ev, rid))
        self.cc.execute(
            "UPDATE requests SET state=?, updated_seq=? WHERE request_id=?",
            (p.COMPLETED, last, rid))
        self.cc.execute("COMMIT")
        return last

    def _store_receipt(self, rid: str, receipt: dict) -> None:
        """固化终态回执。已存在则必须完全相同，否则拒绝改写——
        已返回成功的回执在任何恢复路径下都不允许失效或被替换。"""
        self.cc.execute("BEGIN IMMEDIATE")
        existing = self.cc.execute(
            "SELECT payload FROM receipts WHERE request_id=?", (rid,)).fetchone()
        payload = p.canonical_json(receipt)
        if existing is not None:
            if existing["payload"] != payload:
                self.cc.execute("COMMIT")
                raise RuntimeError(
                    f"请求 {rid} 的固化回执与新回执不一致，拒绝覆盖")
        else:
            self.cc.execute(
                "INSERT INTO receipts(request_id,payload) VALUES(?,?)",
                (rid, payload))
        self.cc.execute("COMMIT")

    # ----------------------------------------------------------------- recover
    def recover(self) -> dict:
        """崩溃恢复：每个未完成请求只走向全提交或全终止。

        * COMMITTED（决定已落盘）：前滚，钥匙消耗与袋子交接都补齐；
        * RECEIVED（无决定）：全量终止，两边回到 AVAILABLE；
        * ABORTED 但回执缺失：补完回滚与回执。
        残留资源锁一律清除——活进程只在短暂窗口持锁，recover 由启动方
        在没有其它 apply 进程时运行（README 明确该运维约定）。
        """
        self.cc.execute("BEGIN IMMEDIATE")
        # 注意：不能用 request_id NOT IN (SELECT ... FROM receipts)，
        # receipts 为空时子查询返回 NULL 集合，NOT IN 会整体求值为 NULL。
        rows = self.cc.execute(
            "SELECT r.* FROM requests r LEFT JOIN receipts rc"
            " ON r.request_id = rc.request_id"
            " WHERE r.state != ? OR rc.request_id IS NULL",
            (p.COMPLETED,)).fetchall()
        self.cc.execute("DELETE FROM resource_locks")
        self.cc.execute("COMMIT")

        committed, aborted = [], []
        for row in rows:
            rid = row["request_id"]
            chash = row["content_hash"]
            req = self._request_content(rid)
            if row["state"] == p.COMMITTED:
                self._recover_commit(rid, req)
                committed.append(rid)
            elif row["state"] == p.ABORTED:
                reason = row["reason"] or p.REASON_RECOVERY_ABORT
                self._recover_abort(rid, req, reason, chash)
                aborted.append(rid)
            else:  # RECEIVED：没有提交决定，只能终止
                self._recover_abort(rid, req, p.REASON_RECOVERY_ABORT, chash)
                aborted.append(rid)
        return {
            "recovered_commit": committed,
            "recovered_abort": aborted,
            "locks_cleared": True,
        }

    def _request_content(self, rid: str) -> dict:
        """从协调日志的 RECEIVED 事件还原原始请求（恢复的唯一可信来源）。"""
        ev = self.cc.execute(
            "SELECT detail FROM coord_events WHERE request_id=? AND event_type='RECEIVED'"
            " ORDER BY seq LIMIT 1", (rid,)).fetchone()
        if ev is None:
            raise RuntimeError(f"请求 {rid} 缺少 RECEIVED 日志，无法恢复")
        return json.loads(ev["detail"])["request"]

    def _recover_commit(self, rid: str, req: dict) -> None:
        # 与 apply 的第 5、6 步完全相同，状态检查保证幂等。
        if req.get("key_id"):
            self._keys_commit(rid, req["key_id"])
        p.maybe_crash(p.CRASH_BOUNDARIES[4])
        if req.get("bag_id"):
            self._bags_commit(rid, req)
        p.maybe_crash(p.CRASH_BOUNDARIES[5])
        if self._receipt_exists(rid):
            return
        seq = self._coord_finish(rid)
        chash = self._hash_of(rid)
        self._store_receipt(rid, p.committed_receipt(rid, seq, chash))

    def _recover_abort(self, rid: str, req: dict, reason: str, chash: str) -> None:
        # 无决定的请求先补 ABORT_DECISION（已有 ABORTED 决定则跳过）。
        state = self.cc.execute(
            "SELECT state FROM requests WHERE request_id=?", (rid,)).fetchone()["state"]
        if state == p.RECEIVED:
            self._coord_decision(rid, p.ABORTED, reason)
        p.maybe_crash(p.CRASH_BOUNDARIES[6])
        if req.get("key_id"):
            self._keys_abort(rid, req["key_id"])
        if req.get("bag_id"):
            self._bags_abort(rid, req["bag_id"])
        if self._receipt_exists(rid):
            return
        seq = self._coord_cleanup_done(rid)
        self._store_receipt(rid, p.aborted_receipt(rid, seq, chash, reason))

    def _receipt_exists(self, rid: str) -> bool:
        return self.cc.execute(
            "SELECT 1 FROM receipts WHERE request_id=?", (rid,)).fetchone() is not None

    def _hash_of(self, rid: str) -> str:
        return self.cc.execute(
            "SELECT content_hash FROM requests WHERE request_id=?", (rid,)
        ).fetchone()["content_hash"]


class _PrecheckFailed(Exception):
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)
