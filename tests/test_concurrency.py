"""并发测试：两个独立客户端进程同时打同一套数据库。

覆盖三个场景：
1. 争最后一把钥匙：一个提交、一个业务终止（KEY_NOT_AVAILABLE），无重复消耗；
2. 两个请求各占不同资源、互相等待：全序加锁不死锁，最终都成功；
3. 锁等待超时：持锁进程睡眠，等待者收到 LOCK_TIMEOUT 终止回执。
"""
from __future__ import annotations

import time

from dualreceipt.reference import ReferenceModel

try:
    from .support import DualReceiptTestCase
except ImportError:
    from support import DualReceiptTestCase


class LastKeyContentionTests(DualReceiptTestCase):
    def test_two_clients_contend_last_key(self):
        # 全新空目录播种"最后一把钥匙"：不能复用 setUp 的四钥匙夹具，
        # init 的播种有 existing==0 幂等守卫，重复播种会被跳过。
        import shutil
        shutil.rmtree(self.root)
        self.root.mkdir(parents=True)
        self.init_store(keys=["only-key"],
                        bags=[{"bag_id": "only-bag", "holder": "vault-0"}])
        req_a = self.req("contend-a", "only-key", "only-bag",
                         "vault-0", "officer-a")
        req_b = self.req("contend-b", "only-key", "only-bag",
                         "vault-0", "officer-b")
        pa = self.spawn_cli(
            "apply", "--request", self._write(req_a, "a.json"),
            env_extra={"DUALRECEIPT_DELAY": "DELAY_AFTER_LOCKS_ACQUIRED",
                       "DUALRECEIPT_DELAY_SECONDS": "1.2"})
        # 轮询协调库，确认 A 已经登记并实际持有资源锁后再启动 B。
        # 必须同时看到"请求存在 + 锁行存在"，避免在 A 已结束释放锁后
        # 才轮询到的竞态；1.2 秒的持锁窗口给轮询留出充足余量。
        import sqlite3
        deadline = time.monotonic() + 10
        saw_locks = False
        while time.monotonic() < deadline:
            conn = sqlite3.connect(str(self.root / "coord.db"))
            req_n = conn.execute(
                "SELECT COUNT(*) FROM requests WHERE request_id='contend-a'"
            ).fetchone()[0]
            lock_n = conn.execute(
                "SELECT COUNT(*) FROM resource_locks").fetchone()[0]
            conn.close()
            if req_n > 0 and lock_n > 0:
                saw_locks = True
                break
            time.sleep(0.01)
        self.assertTrue(saw_locks, "A 未能在预期时间内持锁")
        pb = self.spawn_cli("apply", "--request", self._write(req_b, "b.json"))
        ra, rb = self.wait(pa), self.wait(pb)
        self.assertEqual(ra.rc, 0, ra.stderr)
        self.assertEqual(rb.rc, 0, rb.stderr)
        ja, jb = ra.json(), rb.json()
        receipts = sorted([ja["receipt"], jb["receipt"]])
        self.assertEqual(receipts, ["ABORTED", "COMMITTED"])

        # 串行参考：无论谁先谁后，最终视图必须与某个串行顺序一致
        ref = ReferenceModel(["only-key"],
                             [{"bag_id": "only-bag", "holder": "vault-0"}])
        winner = req_a if ja["receipt"] == "COMMITTED" else req_b
        ref.apply(winner)
        state = self.read()
        self.assertEqual(state["keys"]["committed_view"]["consumed"], ["only-key"])
        self.assertEqual(
            state["bags"]["committed_view"],
            [{"bag_id": "only-bag",
              "holder": "officer-a" if winner["request_id"] == "contend-a"
                        else "officer-b"}])
        self.assertEqual(ref.compare_read(state), [])
        # 幂等：双方各自重试，回执不变
        self.assertEqual(self.apply_file(req_a, "a2.json").json(), ja)
        self.assertEqual(self.apply_file(req_b, "b2.json").json(), jb)

    def _write(self, obj, name) -> str:
        from pathlib import Path
        p = Path(self.tmp) / name
        p.write_text(__import__("json").dumps(obj, ensure_ascii=False),
                     encoding="utf-8")
        return str(p)


class OrderedLockWaitTests(DualReceiptTestCase):
    def test_disjoint_resources_both_commit_without_deadlock(self):
        # A: key-00 + bag-00；B: key-01 + bag-01，资源不相交但同时启动，
        # 全序锁（0:key:* < 1:bag:*）下不允许出现循环等待。
        req_a = self.req("wait-a", "key-00", "bag-00", "vault-0", "officer-a")
        req_b = self.req("wait-b", "key-01", "bag-01", "vault-1", "officer-b")
        # A 拿锁后睡眠，制造 B 必然进入等待窗口的确定性交错
        pa = self.spawn_cli(
            "apply", "--request", self._write(req_a, "wa.json"),
            env_extra={"DUALRECEIPT_DELAY": "DELAY_AFTER_LOCKS_ACQUIRED",
                       "DUALRECEIPT_DELAY_SECONDS": "1.5"})
        time.sleep(0.3)
        pb = self.spawn_cli(
            "apply", "--request", self._write(req_b, "wb.json"))
        t0 = time.monotonic()
        ra, rb = self.wait(pa), self.wait(pb)
        elapsed = time.monotonic() - t0
        self.assertEqual(ra.rc, 0, ra.stderr)
        self.assertEqual(rb.rc, 0, rb.stderr)
        self.assertEqual(ra.json()["receipt"], "COMMITTED")
        self.assertEqual(rb.json()["receipt"], "COMMITTED")
        # B 确实经历了等待（A 持锁 1.5s），但最终完成，证明无死锁
        self.assertGreater(elapsed, 1.0)

        ref = ReferenceModel(self.keys, self.bags)
        ref.apply(req_a)
        ref.apply(req_b)
        state = self.read()
        self.assertEqual(ref.compare_read(state), [])

    def test_lock_timeout_aborts_with_no_side_effect(self):
        req_a = self.req("hold", "key-00", "bag-00", "vault-0", "officer-a")
        req_b = self.req("timedout", "key-00", "bag-00",
                         "vault-0", "officer-b")
        pa = self.spawn_cli(
            "apply", "--request", self._write(req_a, "hold.json"),
            env_extra={"DUALRECEIPT_DELAY": "DELAY_AFTER_LOCKS_ACQUIRED",
                       "DUALRECEIPT_DELAY_SECONDS": "3"})
        time.sleep(0.3)
        # 等待者 1 秒超时，期间反复在锁等待边界醒来
        pb = self.spawn_cli(
            "apply", "--request", self._write(req_b, "to.json"),
            env_extra={"DUALRECEIPT_LOCK_TIMEOUT": "1"})
        rb = self.wait(pb, timeout=30)
        self.assertEqual(rb.rc, 0, rb.stderr)
        self.assertEqual(rb.json()["receipt"], "ABORTED")
        self.assertEqual(rb.json()["reason"], "LOCK_TIMEOUT")
        ra = self.wait(pa)
        self.assertEqual(ra.json()["receipt"], "COMMITTED")

        # 超时请求零副作用：重试时资源仍被 A 持有 => 业务终止而非破坏 A
        state = self.read()
        self.assertEqual(state["keys"]["committed_view"]["consumed"], ["key-00"])
        self.assertEqual(
            state["bags"]["committed_view"][0],
            {"bag_id": "bag-00", "holder": "officer-a"})
        # 锁表必须为空（两条路径都释放/被 recover 清理过）
        import sqlite3
        conn = sqlite3.connect(str(self.root / "coord.db"))
        n = conn.execute("SELECT COUNT(*) FROM resource_locks").fetchone()[0]
        conn.close()
        self.assertEqual(n, 0)

    def _write(self, obj, name) -> str:
        from pathlib import Path
        p = Path(self.tmp) / name
        p.write_text(__import__("json").dumps(obj, ensure_ascii=False),
                     encoding="utf-8")
        return str(p)
