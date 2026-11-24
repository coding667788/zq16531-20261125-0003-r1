"""只读视图（read）与回执审计（audit）。

读者可以是任意独立进程，直接打开三个 SQLite 文件；本模块不做任何写操作。
关键语义：RESERVED / PREPARED 属于半成品，绝不进入"已提交视图"，
只在 pending/inflight 中明确标注；旧快照通过各库事件流按逻辑时间重放得到。
"""
from __future__ import annotations

import json

from . import protocol as p
from . import store

# 审计中判定的非法状态码
ST_UNKNOWN_REQUEST = "UNKNOWN_REQUEST"
ST_HASH_MISMATCH = "CONTENT_HASH_MISMATCH"
ST_INCOMPLETE_COMMIT = "INCOMPLETE_COMMIT"
ST_INCOMPLETE_ABORT = "INCOMPLETE_ABORT"
ST_ORPHAN_SIDE_COMMIT = "ORPHAN_SIDE_COMMIT_WITHOUT_DECISION"
ST_ORPHAN_SIDE_PREPARE = "ORPHAN_SIDE_PREPARE_WITHOUT_REQUEST"
ST_RECEIPT_MISSING = "RECEIPT_MISSING_AFTER_COMPLETION"
ST_RECEIPT_MISMATCH = "RECEIPT_PAYLOAD_MISMATCH"
ST_HOLDER_AMBIGUOUS = "HOLDER_AMBIGUOUS_ORDER_UNDETERMINED"
ST_LOCK_LEAK = "RESOURCE_LOCK_LEFT_BY_DEAD_PROCESS"


class AuditDB:
    def __init__(self, keys_path, bags_path, coord_path):
        self.kc = store.connect(keys_path)
        self.bc = store.connect(bags_path)
        self.cc = store.connect(coord_path)

    def close(self):
        for c in (self.kc, self.bc, self.cc):
            c.close()


# --------------------------------------------------------------------- read
def read_all(keys_path, bags_path, coord_path,
             as_of: dict | None = None) -> dict:
    db = AuditDB(keys_path, bags_path, coord_path)
    try:
        ka = as_of.get("keys") if as_of else None
        ba = as_of.get("bags") if as_of else None
        ca = as_of.get("coord") if as_of else None

        keys_now = store.replay_keys(db.kc, ka)
        bags_now = store.replay_bags(db.bc, ba)
        coord_now = _replay_coord(db.cc, ca)

        committed_keys = _committed_key_view(db.kc, ka)
        committed_bags = _committed_bag_view(db.bc, ba)

        inflight_keys = [k for k in keys_now.values() if k["status"] == p.RESERVED]
        inflight_bags = [b for b in bags_now.values() if b["status"] == p.BAG_PREPARED]

        pending = []
        for req in coord_now["requests"].values():
            if req["state"] in (p.COMPLETED,):
                continue
            if _has_receipt(db.cc, req["request_id"], ca):
                continue
            detail = _received_detail(db.cc, req["request_id"], ca)
            pending.append({
                "request_id": req["request_id"],
                "coord_state": req["state"],
                "content_hash": req["content_hash"],
                "key_id": detail.get("key_id") if detail else None,
                "bag_id": detail.get("bag_id") if detail else None,
                "prepared_holder": _prepared_holder(db.bc, req["request_id"], ba),
                "note": "半成品不是已提交状态：库存未消耗、持有人未变更",
            })

        return {
            "as_of": as_of,
            "logical_time": {
                "coord": store.max_seq(db.cc, "coord_events"),
                "keys": store.max_seq(db.kc, "key_events"),
                "bags": store.max_seq(db.bc, "bag_events"),
            },
            "keys": {
                "committed_view": committed_keys,
                "all_rows": sorted(keys_now.values(), key=lambda x: x["key_id"]),
                "inflight_reserved": inflight_keys,
            },
            "bags": {
                "committed_view": committed_bags,
                "all_rows": sorted(bags_now.values(), key=lambda x: x["bag_id"]),
                "inflight_prepared": inflight_bags,
            },
            "pending": pending,
        }
    finally:
        db.close()


def _replay_coord(conn, as_of: int | None) -> dict:
    sql = "SELECT * FROM coord_events"
    params: tuple = ()
    if as_of is not None:
        sql += " WHERE seq <= ?"
        params = (as_of,)
    sql += " ORDER BY seq"
    requests: dict[str, dict] = {}
    events = []
    for ev in conn.execute(sql, params):
        events.append({"seq": ev["seq"], "event_type": ev["event_type"],
                       "request_id": ev["request_id"], "detail": ev["detail"]})
        r = requests.setdefault(ev["request_id"], {
            "request_id": ev["request_id"], "state": None,
            "content_hash": None, "updated_seq": ev["seq"]})
        r["updated_seq"] = ev["seq"]
        if ev["event_type"] == "RECEIVED":
            r["state"] = p.RECEIVED
            r["content_hash"] = json.loads(ev["detail"])["content_hash"]
        elif ev["event_type"] == "COMMIT_DECISION":
            r["state"] = p.COMMITTED
        elif ev["event_type"] == "ABORT_DECISION":
            r["state"] = p.ABORTED
        elif ev["event_type"] == "COMPLETED":
            r["state"] = p.COMPLETED
    return {"requests": requests, "events": events}


def _committed_key_view(conn, as_of: int | None) -> dict:
    """只承认 SEED/COMMIT 的效果：RESERVED 钥匙仍按在库显示，绝不计消耗。"""
    sql = "SELECT * FROM key_events WHERE event_type IN ('SEED','COMMIT') ORDER BY seq"
    params: tuple = ()
    if as_of is not None:
        sql = "SELECT * FROM key_events WHERE event_type IN ('SEED','COMMIT')" \
              " AND seq <= ? ORDER BY seq"
        params = (as_of,)
    view: dict[str, str] = {}
    for ev in conn.execute(sql, params):
        view[ev["key_id"]] = "CONSUMED" if ev["event_type"] == "COMMIT" else "AVAILABLE"
    available = sorted(k for k, s in view.items() if s == "AVAILABLE")
    consumed = sorted(k for k, s in view.items() if s == "CONSUMED")
    return {"available": available, "consumed": consumed,
            "available_count": len(available), "consumed_count": len(consumed)}


def _committed_bag_view(conn, as_of: int | None) -> dict:
    """只承认 SEED/COMMIT 的效果：PREPARED 袋子保持原持有人，绝不预换。"""
    sql = "SELECT * FROM bag_events WHERE event_type IN ('SEED','COMMIT') ORDER BY seq"
    params: tuple = ()
    if as_of is not None:
        sql = "SELECT * FROM bag_events WHERE event_type IN ('SEED','COMMIT')" \
              " AND seq <= ? ORDER BY seq"
        params = (as_of,)
    holders: dict[str, str] = {}
    for ev in conn.execute(sql, params):
        holders[ev["bag_id"]] = ev["holder"]
    return [{"bag_id": b, "holder": h} for b, h in sorted(holders.items())]


def _has_receipt(conn, rid: str, as_of: int | None) -> bool:
    # receipts 表不随事件流版本化；as_of 模式下以 COMPLETED 事件为准。
    if as_of is not None:
        row = conn.execute(
            "SELECT 1 FROM coord_events WHERE request_id=? AND event_type='COMPLETED'"
            " AND seq <= ?", (rid, as_of)).fetchone()
        return row is not None
    return conn.execute(
        "SELECT 1 FROM receipts WHERE request_id=?", (rid,)).fetchone() is not None


def _received_detail(conn, rid: str, as_of: int | None) -> dict | None:
    sql = ("SELECT detail FROM coord_events WHERE request_id=?"
           " AND event_type='RECEIVED'")
    params: list = [rid]
    if as_of is not None:
        sql += " AND seq <= ?"
        params.append(as_of)
    sql += " ORDER BY seq LIMIT 1"
    row = conn.execute(sql, params).fetchone()
    return json.loads(row["detail"])["request"] if row else None


def _prepared_holder(conn, rid: str, as_of: int | None) -> str | None:
    sql = ("SELECT holder FROM bag_events WHERE request_id=? AND event_type='PREPARE'")
    params: list = [rid]
    if as_of is not None:
        sql += " AND seq <= ?"
        params.append(as_of)
    sql += " ORDER BY seq LIMIT 1"
    row = conn.execute(sql, params).fetchone()
    return row["holder"] if row else None

# --------------------------------------------------------------------- audit
def audit_receipt(keys_path, bags_path, coord_path, request_id: str) -> dict:
    db = AuditDB(keys_path, bags_path, coord_path)
    try:
        illegal: list[dict] = []
        req_row = db.cc.execute(
            "SELECT * FROM requests WHERE request_id=?", (request_id,)).fetchone()
        coord_events = [dict(r) for r in db.cc.execute(
            "SELECT * FROM coord_events WHERE request_id=? ORDER BY seq",
            (request_id,))]
        receipt_row = db.cc.execute(
            "SELECT payload FROM receipts WHERE request_id=?",
            (request_id,)).fetchone()
        key_events = [dict(r) for r in db.kc.execute(
            "SELECT * FROM key_events WHERE request_id=? ORDER BY seq", (request_id,))]
        bag_events = [dict(r) for r in db.bc.execute(
            "SELECT * FROM bag_events WHERE request_id=? ORDER BY seq", (request_id,))]
        key_row = db.kc.execute(
            "SELECT * FROM keys WHERE request_id=?", (request_id,)).fetchone()
        bag_row = db.bc.execute(
            "SELECT * FROM bags WHERE request_id=?", (request_id,)).fetchone()
        locks = [dict(r) for r in db.cc.execute(
            "SELECT * FROM resource_locks WHERE request_id=?", (request_id,))]

        detail = _received_detail(db.cc, request_id, None)
        decision = _last_event(coord_events, "COMMIT_DECISION")
        abort_decision = _last_event(coord_events, "ABORT_DECISION")
        key_commit = _last_event(key_events, "COMMIT")
        bag_commit = _last_event(bag_events, "COMMIT")
        key_reserve = _last_event(key_events, "RESERVE")
        bag_prepare = _last_event(bag_events, "PREPARE")

        # 1) 未知请求：记录后继续检查侧库孤儿事件（不能提前返回，
        #    否则"侧库有 COMMIT 但协调日志无请求"这种最危险的情况会被漏报）
        if req_row is None:
            illegal.append({"code": ST_UNKNOWN_REQUEST,
                            "detail": f"协调日志中不存在请求 {request_id}"})

        # 2) 内容哈希复核（用 RECEIVED 里保存的原始请求重算）
        if detail is not None and req_row is not None:
            recomputed = p.content_hash(detail)
            if recomputed != req_row["content_hash"]:
                illegal.append({"code": ST_HASH_MISMATCH,
                                "detail": "RECEIVED 内容重算哈希与 requests 表不一致",
                                "stored": req_row["content_hash"],
                                "recomputed": recomputed})

        # 3) 决定与两侧落实的一致性
        if decision is not None:
            if key_commit is None or bag_commit is None:
                illegal.append({
                    "code": ST_INCOMPLETE_COMMIT,
                    "detail": "提交决定已持久化但两本账尚未全部落实，可 recover 前滚",
                    "missing": [n for n, ok in
                                (("keys", key_commit), ("bags", bag_commit)) if not ok],
                })
        if abort_decision is not None:
            if key_row is not None and key_row["status"] == p.RESERVED:
                illegal.append({"code": ST_INCOMPLETE_ABORT,
                                "detail": "终止决定已落盘但钥匙仍处 RESERVED"})
            if bag_row is not None and bag_row["status"] == p.BAG_PREPARED:
                illegal.append({"code": ST_INCOMPLETE_ABORT,
                                "detail": "终止决定已落盘但证物袋仍处 PREPARED"})

        # 4) 孤儿事件：侧库有提交效果，协调方却没有提交决定。
        #    两本账各自只有本地序号，跨库持久顺序只能由协调日志裁定；
        #    裁定依据缺失时绝不臆造持有人。
        holder_ambiguous = False
        if (key_commit is not None or bag_commit is not None) and decision is None:
            illegal.append({
                "code": ST_ORPHAN_SIDE_COMMIT,
                "detail": "侧库存在 COMMIT 但协调日志无 COMMIT_DECISION，"
                          "两本账的持久顺序无法由协调日志确定",
            })
            holder_ambiguous = True
        if (key_reserve is not None or bag_prepare is not None) and req_row is None:
            illegal.append({"code": ST_ORPHAN_SIDE_PREPARE,
                            "detail": "侧库存在预留/预备但协调日志无该请求"})

        # 5) 回执核对
        receipt = json.loads(receipt_row["payload"]) if receipt_row else None
        if req_row is not None and req_row["state"] == p.COMPLETED and receipt is None:
            illegal.append({"code": ST_RECEIPT_MISSING,
                            "detail": "请求已 COMPLETED 但 receipts 表无固化回执"})
        if receipt is not None and detail is not None and req_row is not None:
            expected_kind = ("COMMITTED" if decision is not None else "ABORTED")
            if receipt.get("receipt") != expected_kind or \
                    receipt.get("content_hash") != req_row["content_hash"]:
                illegal.append({"code": ST_RECEIPT_MISMATCH,
                                "detail": "固化回执与协调日志决定/哈希不一致",
                                "stored_receipt": receipt})

        if locks:
            illegal.append({"code": ST_LOCK_LEAK,
                            "detail": "进程退出后残留资源锁，recover 会清除",
                            "locks": locks})

        # 6) 持有人结论：顺序不可裁定则显式置空，不编造交接人。
        holder = _holder_conclusion(db, bag_row, bag_events, detail, decision,
                                    abort_decision, holder_ambiguous, illegal)

        evidence = _minimal_evidence(
            coord_events, receipt_row, key_events, bag_events,
            decision, abort_decision)
        return _audit_result(request_id, illegal, holder, evidence, receipt)
    finally:
        db.close()


def _last_event(events: list[dict], kind: str) -> dict | None:
    for ev in reversed(events):
        if ev["event_type"] == kind:
            return ev
    return None


def _holder_conclusion(db, bag_row, bag_events, detail, decision, abort_decision,
                       holder_ambiguous, illegal) -> dict:
    """给出审计层面的持有人结论。

    * 协调日志存在 COMMIT_DECISION 时，才承认 bag COMMIT 事件里的新持有人；
    * 存在 ABORT_DECISION（或无决定）时，交接未生效，持有人保持交接前；
      若该请求在 bags.db 没留下任何事件（RECEIVED 后即崩溃），则从
      协调日志 RECEIVED 记录的 bag_id 直接查袋子当前持有人；
    * 两本账持久顺序无法由协调日志裁定时 holder=null，绝不编造。
    """
    commit_ev = _last_event(bag_events, "COMMIT")
    seed_ev = _last_event(bag_events, "SEED")
    prepare_ev = _last_event(bag_events, "PREPARE")
    if holder_ambiguous:
        illegal.append({
            "code": ST_HOLDER_AMBIGUOUS,
            "detail": "缺少提交决定，无法确认交接是否生效，不指定持有人",
        })
        return {"holder": None, "determined": False,
                "reason": ST_HOLDER_AMBIGUOUS}
    if decision is not None and commit_ev is not None:
        return {"holder": commit_ev["holder"], "determined": True,
                "basis": f"bag_events.seq={commit_ev['seq']} COMMIT + "
                         f"coord COMMIT_DECISION.seq={decision['seq']}"}
    # 未提交（含已落盘的终止决定）：持有人停留在 PREPARE 之前。
    prior = _last_event(bag_events, "COMMIT") or seed_ev
    if prior is not None:
        basis = f"交接未提交，保持 bag_events.seq={prior['seq']} 的持有人"
        if abort_decision is not None:
            basis += f"（coord ABORT_DECISION.seq={abort_decision['seq']}）"
        return {"holder": prior["holder"], "determined": True,
                "basis": basis,
                "pending_new_holder": prepare_ev["holder"] if prepare_ev else None}
    # 侧库无该请求的任何事件：按 bag_id 查袋子当前真实持有人。
    if detail is not None and detail.get("bag_id"):
        cur = db.bc.execute(
            "SELECT current_holder FROM bags WHERE bag_id=?",
            (detail["bag_id"],)).fetchone()
        if cur is not None:
            return {
                "holder": cur["current_holder"], "determined": True,
                "basis": f"请求未在 bags.db 留下事件，按 bags 表现状"
                         f"（bag_id={detail['bag_id']}）确认持有人未变更"}
    return {"holder": None, "determined": False, "reason": "无任何袋事件"}


def _minimal_evidence(coord_events, receipt_row, key_events, bag_events,
                      decision, abort_decision) -> dict:
    """足以支撑一张回执的最小持久证据集合（精确到表与行序号）。"""
    ev = []
    if receipt_row is not None:
        ev.append({"store": "coord.db", "table": "receipts",
                   "row": "request_id", "payload": json.loads(receipt_row["payload"])})
    decisive = decision or abort_decision
    if decisive is not None:
        ev.append({"store": "coord.db", "table": "coord_events",
                   "seq": decisive["seq"], "event_type": decisive["event_type"]})
    kc = _last_event(key_events, "COMMIT") or _last_event(key_events, "ABORT") \
        or _last_event(key_events, "RESERVE")
    if kc is not None:
        ev.append({"store": "keys.db", "table": "key_events",
                   "seq": kc["seq"], "event_type": kc["event_type"]})
    bc = _last_event(bag_events, "COMMIT") or _last_event(bag_events, "ABORT") \
        or _last_event(bag_events, "PREPARE")
    if bc is not None:
        ev.append({"store": "bags.db", "table": "bag_events",
                   "seq": bc["seq"], "event_type": bc["event_type"]})
    completed = _last_event(coord_events, "COMPLETED")
    if completed is not None:
        ev.append({"store": "coord.db", "table": "coord_events",
                   "seq": completed["seq"], "event_type": "COMPLETED"})
    return {"items": ev,
            "principle": "协调决定 + 两侧落实事件 + 固化回执；缺任一项即列为非法状态"}


def _audit_result(request_id, illegal, holder, evidence, receipt=None) -> dict:
    return {
        "request_id": request_id,
        "legal": len(illegal) == 0,
        "illegal_states": illegal,
        "holder_conclusion": holder,
        "minimal_evidence": evidence,
        "receipt": receipt,
    }
