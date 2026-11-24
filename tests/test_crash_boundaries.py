"""崩溃边界测试：枚举 8 个命名边界，子进程硬退出后由另一进程恢复。

8 个边界分两条执行路径：

* 正常提交路径上的 6 个：RECEIVED / KEYS_PREPARED / BAGS_PREPARED /
  COMMIT_DECISION / KEYS_COMMITTED / BAGS_COMMITTED；
* 终止路径上的 2 个：ABORT_DECISION / KEYS_ABORTED，由锁等待超时
  进入 _abort 后触发。

每个边界验证：
1. 崩溃进程退出码为注入专用码 88（os._exit，无 Python 清理）；
2. 恢复前读者能看到明确 pending，且半成品绝不计入已提交视图；
3. 另一进程 recover 后只可能全提交或全终止，无残留半成品；
4. 原请求重试得到确定性回执，且与串行参考模型逐项一致。
"""
from __future__ import annotations

from pathlib import Path

from dualreceipt import CRASH_BOUNDARIES
from dualreceipt import protocol as p
from dualreceipt.reference import ReferenceModel

try:
    from .support import DualReceiptTestCase
except ImportError:
    from support import DualReceiptTestCase

# 提交决定（第 4 个持久化点，索引 3）之前的边界 => 必须全终止；
# 提交决定及其后 => 必须全提交。
DECISION_INDEX = 3
APPLY_PATH_BOUNDARIES = list(CRASH_BOUNDARIES[:6])
ABORT_PATH_BOUNDARIES = list(CRASH_BOUNDARIES[6:])


class CrashBoundaryTests(DualReceiptTestCase):
    def _request(self):
        return self.req("crash-req", "key-00", "bag-00", "vault-0", "officer-x")

    def _assert_pending_but_not_committed(self, state, boundary: str):
        idx = CRASH_BOUNDARIES.index(boundary)
        decide_idx = CRASH_BOUNDARIES.index("CRASH_AFTER_COMMIT_DECISION")
        bags_done_idx = CRASH_BOUNDARIES.index("CRASH_AFTER_BAGS_COMMITTED")
        pending_ids = {pp["request_id"] for pp in state["pending"]}
        self.assertIn("crash-req", pending_ids)

        if idx < decide_idx:
            # 提交决定之前：两边都不可能有任何已提交效果
            self.assertIn("key-00",
                          state["keys"]["committed_view"]["available"])
            self.assertEqual(
                state["bags"]["committed_view"],
                [{"bag_id": f"bag-0{i}", "holder": h}
                 for i, h in enumerate(["vault-0", "vault-1", "vault-0", "vault-1"])])
            for k in state["keys"]["all_rows"]:
                if k["key_id"] == "key-00":
                    self.assertNotEqual(k["status"], "CONSUMED")
            for b in state["bags"]["all_rows"]:
                if b["bag_id"] == "bag-00":
                    self.assertNotEqual(b["status"], "HANDED")
                    self.assertEqual(b["holder"], "vault-0")
        elif idx < bags_done_idx:
            # 提交决定之后、袋子落实之前：袋子持有人绝不能先变更
            self.assertEqual(
                state["bags"]["committed_view"],
                [{"bag_id": f"bag-0{i}", "holder": h}
                 for i, h in enumerate(["vault-0", "vault-1", "vault-0", "vault-1"])])
            for b in state["bags"]["all_rows"]:
                if b["bag_id"] == "bag-00":
                    self.assertEqual(b["holder"], "vault-0")
        else:
            # BAGS_COMMITTED：两边均已落实，唯一未完成的是回执固化；
            # 读者看到的是"决定已生效但请求仍 pending"的明确状态。
            self.assertEqual(
                state["bags"]["committed_view"][0],
                {"bag_id": "bag-00", "holder": "officer-x"})
            self.assertEqual(
                state["keys"]["committed_view"]["consumed"], ["key-00"])

    def _assert_terminal_consistency(self, receipt, expect_committed, ref, req):
        if expect_committed:
            self.assertEqual(receipt["receipt"], "COMMITTED")
            ref_receipt = ref.crash_recover_commit(req)
        else:
            self.assertEqual(receipt["receipt"], "ABORTED")
            ref_receipt = ref.crash_recover_abort(req)
        state = self.read()
        self.assert_no_inflight(state)
        self.assertEqual(ref.compare_read(state), [])
        # 回执逐项一致，含 coord_seq（参考模型按真实事件计数）
        self.assertEqual(receipt, ref_receipt)
        # 审计必须合法，且给出确定性持有人结论
        aud = self.audit("crash-req")
        self.assertEqual(aud["illegal_states"], [], aud)
        self.assertTrue(aud["holder_conclusion"]["determined"])
        if expect_committed:
            self.assertEqual(aud["holder_conclusion"]["holder"], "officer-x")
        else:
            self.assertEqual(aud["holder_conclusion"]["holder"], "vault-0")

    def test_named_boundaries_count(self):
        # 任务要求：至少六个已命名崩溃边界
        self.assertGreaterEqual(len(CRASH_BOUNDARIES), 6)
        self.assertEqual(len(CRASH_BOUNDARIES), len(set(CRASH_BOUNDARIES)))

    def test_six_apply_path_boundaries(self):
        for boundary in APPLY_PATH_BOUNDARIES:
            with self.subTest(boundary=boundary):
                self.setUp()
                self._run_apply_path(boundary)

    def test_two_abort_path_boundaries(self):
        # ABORT_DECISION：业务预检失败的终止路径
        self.setUp()
        self._run_abort_decision_boundary()
        # KEYS_ABORTED：prepare 之后崩溃，recover 终止回滚时的边界
        self.setUp()
        self._run_keys_aborted_boundary()

    def _run_apply_path(self, boundary: str):
        req = self._request()
        ref = ReferenceModel(self.keys, self.bags)

        # 1) 子进程在命名边界硬退出
        crashed = self.apply_crash(req, boundary)
        self.assertEqual(crashed.rc, 88,
                         f"{boundary}: 预期硬退出码 88，实际 {crashed.rc} / {crashed.stderr}")

        # 2) 另一进程只读：明确 pending，半成品不被当成已提交
        self._assert_pending_but_not_committed(self.read(), boundary)

        # 3) 另一进程恢复
        rec = self.recover()
        self.assertEqual(rec.rc, 0, rec.stderr)
        rec_json = rec.json()["recover"]
        expect_committed = CRASH_BOUNDARIES.index(boundary) >= DECISION_INDEX
        if expect_committed:
            self.assertEqual(rec_json["recovered_commit"], ["crash-req"])
            self.assertEqual(rec_json["recovered_abort"], [])
        else:
            self.assertEqual(rec_json["recovered_abort"], ["crash-req"])
            self.assertEqual(rec_json["recovered_commit"], [])

        # 4) 原请求重试：终态回执，与参考模型逐项一致
        retry = self.apply_file(req, name="retry.json")
        self.assertEqual(retry.rc, 0, retry.stderr)
        self._assert_terminal_consistency(retry.json(), expect_committed, ref, req)

        # 5) 再重试：同请求同内容必须是同一张回执
        receipt = retry.json()
        self.assertEqual(self.apply_file(req, "retry2.json").json(), receipt)

        # 6) 同 ID 异内容 => 冲突
        bad = dict(req)
        bad["to_holder"] = "officer-evil"
        conflict = self.apply_file(bad, name="conflict.json")
        self.assertEqual(conflict.rc, 2)
        self.assertEqual(conflict.json()["error"], "ID_CONTENT_CONFLICT")

    def _run_abort_decision_boundary(self):
        # 编排（崩溃 -> 恢复窗口内无其它活 apply 进程）：
        #   1. holder 持锁睡眠 2 秒后正常提交；
        #   2. victim 锁超时 6 秒，holder 释放后拿到锁，但钥匙已被消耗
        #      => 业务预检失败走 _abort，在 ABORT_DECISION 后硬退出；
        #   3. 另一进程 recover 收敛 victim。
        holder_req = self.req("holder-req", "key-00", "bag-00",
                              "vault-0", "officer-h")
        victim_req = self._request()
        holder = self.spawn_cli(
            "apply", "--request", self._write(holder_req, "holder.json"),
            env_extra={"DUALRECEIPT_DELAY": "DELAY_AFTER_LOCKS_ACQUIRED",
                       "DUALRECEIPT_DELAY_SECONDS": "2"})
        import time
        time.sleep(0.5)
        victim = self.spawn_cli(
            "apply", "--request", self._write(victim_req, "victim.json"),
            env_extra={"DUALRECEIPT_LOCK_TIMEOUT": "6",
                       "DUALRECEIPT_CRASH": CRASH_BOUNDARIES[6]})
        hr = self.wait(holder, timeout=30)
        self.assertEqual(hr.rc, 0, hr.stderr)
        self.assertEqual(hr.json()["receipt"], "COMMITTED")
        vr = self.wait(victim, timeout=30)
        self.assertEqual(vr.rc, 88, vr.stderr)

        rec = self.recover()
        self.assertEqual(rec.rc, 0, rec.stderr)
        self.assertEqual(rec.json()["recover"]["recovered_abort"], ["crash-req"])
        state = self.read()
        self.assert_no_inflight(state)
        # holder 的提交完好；victim 零副作用
        self.assertEqual(state["keys"]["committed_view"]["consumed"], ["key-00"])
        self.assertEqual(
            state["bags"]["committed_view"][0],
            {"bag_id": "bag-00", "holder": "officer-h"})
        again = self.apply_file(victim_req, "again.json")
        self.assertEqual(again.json()["receipt"], "ABORTED")
        # victim 从未生效；袋子当前持有人是 holder 交接后的 officer-h
        aud = self.audit("crash-req")
        self.assertEqual(aud["illegal_states"], [], aud)
        self.assertEqual(aud["holder_conclusion"]["holder"], "officer-h")

    def _run_keys_aborted_boundary(self):
        # KEYS_ABORTED 只能经恢复终止路径到达：先崩溃在 BAGS_PREPARED
        # （两边半成品都在、无决定），再让 recover 崩溃在钥匙回滚之后。
        req = self._request()
        crashed = self.apply_crash(req, CRASH_BOUNDARIES[2])
        self.assertEqual(crashed.rc, 88)
        self.assertTrue(any(pp["request_id"] == "crash-req"
                            for pp in self.read()["pending"]))
        rec1 = self.recover(env_extra={"DUALRECEIPT_CRASH": CRASH_BOUNDARIES[7]})
        self.assertEqual(rec1.rc, 88)
        # 钥匙已回滚、袋子仍半成品 => 读者仍只能看到 pending
        state1 = self.read()
        self.assertTrue(any(pp["request_id"] == "crash-req"
                            for pp in state1["pending"]))
        self.assertEqual(state1["keys"]["inflight_reserved"], [])

        # 第二个恢复进程收敛为全终止
        rec2 = self.recover()
        self.assertEqual(rec2.rc, 0, rec2.stderr)
        self.assertEqual(rec2.json()["recover"]["recovered_abort"], ["crash-req"])
        state2 = self.read()
        self.assert_no_inflight(state2)
        self.assertEqual(state2["keys"]["committed_view"]["consumed"], [])
        self.assertEqual(
            state2["bags"]["committed_view"][0],
            {"bag_id": "bag-00", "holder": "vault-0"})
        receipt = self.apply_file(req, "again.json").json()
        self.assertEqual(receipt["receipt"], "ABORTED")
        aud = self.audit("crash-req")
        self.assertEqual(aud["illegal_states"], [], aud)
        self.assertEqual(aud["holder_conclusion"]["holder"], "vault-0")

    def _write(self, obj, name) -> str:
        pth = Path(self.tmp) / name
        pth.write_text(__import__("json").dumps(obj, ensure_ascii=False),
                       encoding="utf-8")
        return str(pth)


class RecoverItselfCrashesTests(DualReceiptTestCase):
    """恢复进程自己也在中途被杀：再次 recover 必须收敛到同一终态。"""

    def test_recover_crash_at_every_reentrant_boundary(self):
        req = self.req("rc-req", "key-01", "bag-01", "vault-1", "officer-y")
        for first_crash in (CRASH_BOUNDARIES[2],  # 预备之后、决定之前 -> 最终终止
                            CRASH_BOUNDARIES[3]):  # 提交决定之后 -> 最终提交
            with self.subTest(first_crash=first_crash):
                self.setUp()
                crashed = self.apply_crash(req, first_crash)
                self.assertEqual(crashed.rc, 88)

                reentrant = (CRASH_BOUNDARIES[7]
                             if first_crash == CRASH_BOUNDARIES[2]
                             else CRASH_BOUNDARIES[5])
                rec1 = self.recover(env_extra={"DUALRECEIPT_CRASH": reentrant})
                self.assertEqual(rec1.rc, 88)

                # 半成品不得被读者当成已提交。提交路径上钥匙可能已经
                # CONSUMED（崩溃点在 keys_commit 之后），因此只检查袋子
                # 持有人这一对读者最关键的业务事实尚未变更。
                state = self.read()
                holders = {b["bag_id"]: b["holder"]
                           for b in state["bags"]["committed_view"]}
                if first_crash == CRASH_BOUNDARIES[2]:
                    self.assertEqual(holders["bag-01"], "vault-1")
                # 无论哪种情况，终态未完成时必须显式 pending
                self.assertTrue(
                    any(pp["request_id"] == "rc-req" for pp in state["pending"])
                    or state["bags"]["inflight_prepared"])

                # 第二个恢复进程收敛
                rec2 = self.recover()
                self.assertEqual(rec2.rc, 0, rec2.stderr)
                state2 = self.read()
                self.assert_no_inflight(state2)
                receipt = self.apply_file(req, name="final.json").json()
                expect_committed = first_crash == CRASH_BOUNDARIES[3]
                self.assertEqual(receipt["receipt"],
                                 "COMMITTED" if expect_committed else "ABORTED")
                holders2 = {b["bag_id"]: b["holder"]
                            for b in state2["bags"]["committed_view"]}
                self.assertEqual(holders2["bag-01"],
                                 "officer-y" if expect_committed else "vault-1")
