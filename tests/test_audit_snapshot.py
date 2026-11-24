"""审计与旧快照测试。

* 正常回执：audit 合法、最小证据齐全、持有人结论有据可依；
* 崩溃未恢复：audit 报告 INCOMPLETE_* 且 pending 视图不暴露半成品为已提交；
* 人为篡改制造"两本账持久顺序无法确定"：audit 必须拒绝臆造持有人；
* read --as-of：从旧逻辑时间复核历史状态。
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from dualreceipt import CRASH_BOUNDARIES

try:
    from .support import DualReceiptTestCase
except ImportError:
    from support import DualReceiptTestCase


class AuditTests(DualReceiptTestCase):
    def test_legal_receipt_has_minimal_evidence(self):
        req = self.req("a1", "key-00", "bag-00", "vault-0", "officer-z")
        receipt = self.apply_file(req).json()
        aud = self.audit("a1")
        self.assertTrue(aud["legal"], aud)
        stores = {(i["store"], i["table"]) for i in aud["minimal_evidence"]["items"]}
        # 三个库都必须有证据
        self.assertIn(("coord.db", "receipts"), stores)
        self.assertIn(("coord.db", "coord_events"), stores)
        self.assertIn(("keys.db", "key_events"), stores)
        self.assertIn(("bags.db", "bag_events"), stores)
        self.assertEqual(aud["holder_conclusion"]["holder"], "officer-z")
        self.assertTrue(aud["holder_conclusion"]["determined"])
        self.assertEqual(aud["receipt"], receipt)

    def test_unknown_request(self):
        aud = self.audit("ghost")
        self.assertFalse(aud["legal"])
        self.assertEqual(aud["illegal_states"][0]["code"], "UNKNOWN_REQUEST")

    def test_incomplete_commit_reported_not_faked(self):
        req = self.req("ic", "key-01", "bag-01", "vault-1", "officer-q")
        crashed = self.apply_crash(req, CRASH_BOUNDARIES[3])  # 决定后、落实前
        self.assertEqual(crashed.rc, 88)
        aud = self.audit("ic")
        self.assertFalse(aud["legal"])
        codes = {s["code"] for s in aud["illegal_states"]}
        self.assertIn("INCOMPLETE_COMMIT", codes)
        # 持有人结论仍只承认已持久化的 COMMIT 事件；此时袋未换人
        self.assertEqual(aud["holder_conclusion"]["holder"], "vault-1")
        # 恢复后审计转合法
        self.assertEqual(self.recover().rc, 0)
        aud2 = self.audit("ic")
        self.assertTrue(aud2["legal"], aud2)
        self.assertEqual(aud2["holder_conclusion"]["holder"], "officer-q")

    def test_undetermined_order_must_not_invent_holder(self):
        # 正常完成一笔，然后人为在 bags.db 插入一条没有协调决定支撑的
        # COMMIT 事件（模拟两本账持久顺序无法由协调日志裁定的情形）。
        req = self.req("tamper", "key-02", "bag-02", "vault-0", "officer-real")
        self.assertEqual(self.apply_file(req).rc, 0)
        bag_db = str(self.root / "bags.db")
        conn = sqlite3.connect(bag_db)
        try:
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("BEGIN IMMEDIATE")
            seq = conn.execute("SELECT COALESCE(MAX(seq),0)+1 FROM bag_events").fetchone()[0]
            conn.execute(
                "INSERT INTO bag_events(seq,event_type,bag_id,request_id,holder,detail)"
                " VALUES(?, 'COMMIT', 'bag-02', 'rogue-req', 'fabricated-guy', '{}')",
                (seq,))
            conn.execute("COMMIT")
        finally:
            conn.close()
        aud = self.audit("rogue-req")
        self.assertFalse(aud["legal"])
        codes = {s["code"] for s in aud["illegal_states"]}
        self.assertIn("ORPHAN_SIDE_COMMIT_WITHOUT_DECISION", codes)
        self.assertIn("HOLDER_AMBIGUOUS_ORDER_UNDETERMINED", codes)
        # 关键纪律：顺序不可裁定时 holder 必须为 null，绝不编造交接人
        self.assertIsNone(aud["holder_conclusion"]["holder"])
        self.assertFalse(aud["holder_conclusion"]["determined"])

    def test_receipt_immutable_after_success(self):
        req = self.req("imm", "key-03", "bag-03", "vault-1", "officer-i")
        receipt = self.apply_file(req).json()
        # 直接篡改 coord 的 requests 行（异常操作），receipts 仍应保持原样
        conn = sqlite3.connect(str(self.root / "coord.db"))
        conn.execute("UPDATE requests SET state='RECEIVED' WHERE request_id='imm'")
        conn.commit()
        conn.close()
        # 重试仍返回固化回执，已返回成功的回执不失效
        again = self.apply_file(req, "again.json").json()
        self.assertEqual(again, receipt)
        aud = self.audit("imm")
        self.assertEqual(aud["receipt"], receipt)


class SnapshotTests(DualReceiptTestCase):
    def test_as_of_replays_old_snapshot(self):
        r1 = self.req("s1", "key-00", "bag-00", "vault-0", "officer-a")
        r2 = self.req("s2", "key-01", "bag-01", "vault-1", "officer-b")
        self.assertEqual(self.apply_file(r1, "s1.json").rc, 0)
        mid = self.read()["logical_time"]
        self.assertEqual(self.apply_file(r2, "s2.json").rc, 0)

        # 旧快照：只有 r1 生效，r2 完全不存在
        old = self.read(as_of=mid)
        consumed = old["keys"]["committed_view"]["consumed"]
        self.assertEqual(consumed, ["key-00"])
        holders = {b["bag_id"]: b["holder"] for b in old["bags"]["committed_view"]}
        self.assertEqual(holders["bag-00"], "officer-a")
        self.assertEqual(holders["bag-01"], "vault-1")

        # 当前视图：两笔都生效
        now = self.read()
        self.assertEqual(
            now["keys"]["committed_view"]["consumed"], ["key-00", "key-01"])

    def test_pending_visible_but_never_committed_during_crash(self):
        req = self.req("pv", "key-00", "bag-00", "vault-0", "officer-p")
        for boundary in (CRASH_BOUNDARIES[1],  # keys 已预留
                         CRASH_BOUNDARIES[2]):  # bags 已预备
            with self.subTest(boundary=boundary):
                self.setUp()
                self.assertEqual(self.apply_crash(req, boundary).rc, 88)
                state = self.read()
                pending = {p["request_id"] for p in state["pending"]}
                self.assertIn("pv", pending)
                # 已提交视图完全不变
                self.assertEqual(state["keys"]["committed_view"]["consumed"], [])
                holders = {b["bag_id"]: b["holder"]
                           for b in state["bags"]["committed_view"]}
                self.assertEqual(holders["bag-00"], "vault-0")
                # 半成品行本身可见，状态明确是 RESERVED / PREPARED
                self.assertEqual(state["keys"]["inflight_reserved"][0]["status"],
                                 "RESERVED")
                if boundary == CRASH_BOUNDARIES[2]:
                    # BAGS_PREPARED 边界才有袋子半成品
                    self.assertEqual(
                        state["bags"]["inflight_prepared"][0]["status"],
                        "PREPARED")
                else:
                    self.assertEqual(state["bags"]["inflight_prepared"], [])
                self.recover()
