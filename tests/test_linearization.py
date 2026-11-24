"""合成小状态上的串行线性化对照。

每条历史都由独立子进程逐步执行，每一步后把真实 read 输出与
ReferenceModel 的串行答案逐项对照（库存、持有人、pending、半成品、回执）。
"""
from __future__ import annotations

from dualreceipt.reference import ReferenceModel

try:
    from .support import DualReceiptTestCase
except ImportError:
    from support import DualReceiptTestCase


class LinearizationTests(DualReceiptTestCase):
    def test_happy_path_matches_reference_step_by_step(self):
        ref = ReferenceModel(self.keys, self.bags)
        plan = [
            self.req("r1", "key-00", "bag-00", "vault-0", "officer-0"),
            self.req("r2", "key-01", "bag-01", "vault-1", "officer-1"),
            # 链式交接：bag-00 从 officer-0 再转到 officer-2
            self.req("r3", "key-02", "bag-00", "officer-0", "officer-2"),
        ]
        for i, req in enumerate(plan):
            real = self.apply_file(req, f"ok{i}.json")
            self.assertEqual(real.rc, 0, real.stderr)
            want = ref.apply(req)
            self.assertEqual(real.json(), want)  # 回执逐项一致（含 coord_seq）
            diffs = ref.compare_read(self.read())
            self.assertEqual(diffs, [], f"第 {i} 步状态偏离参考模型: {diffs}")

    def test_business_aborts_match_reference(self):
        ref = ReferenceModel(self.keys, self.bags)
        bad_holder = self.req("bh", "key-00", "bag-00", "wrong-guy", "officer-0")
        missing_bag = self.req("mb", "key-01", "bag-nope", "vault-0", "officer-1")
        for i, req in enumerate((bad_holder, missing_bag)):
            real = self.apply_file(req, f"ab{i}.json")
            want = ref.apply(req)
            self.assertEqual(real.json(), want)
            self.assertEqual(ref.compare_read(self.read()), [])

        # 终止后的请求不占资源：正常请求仍可成功，且参考模型同步
        ok = self.req("ok", "key-00", "bag-00", "vault-0", "officer-9")
        self.assertEqual(self.apply_file(ok, "ok.json").json(), ref.apply(ok))
        self.assertEqual(ref.compare_read(self.read()), [])

    def test_retry_semantics_match_reference(self):
        ref = ReferenceModel(self.keys, self.bags)
        req = self.req("id1", "key-03", "bag-03", "vault-1", "officer-3")
        first = self.apply_file(req, "one.json").json()
        ref.apply(req)
        # 同 ID 同内容三次：同一张回执
        for name in ("two.json", "three.json"):
            again = self.apply_file(req, name)
            self.assertEqual(again.json(), first)
        # 参考模型对同 ID 也只记一次，状态不变
        self.assertEqual(ref.compare_read(self.read()), [])

    def test_permutation_both_serial_orders_equivalent_final_state(self):
        # 两个互不相交的成功请求，A->B 与 B->A 的最终可观察状态相同
        for order in (("p1", "p2"), ("p2", "p1")):
            self.setUp()
            reqs = {
                "p1": self.req("p1", "key-00", "bag-00", "vault-0", "officer-a"),
                "p2": self.req("p2", "key-01", "bag-01", "vault-1", "officer-b"),
            }
            ref = ReferenceModel(self.keys, self.bags)
            for rid in order:
                r = self.apply_file(reqs[rid], f"{rid}.json")
                self.assertEqual(r.json(), ref.apply(reqs[rid]))
            state = self.read()
            self.assertEqual(ref.compare_read(state), [])
            self.assertEqual(
                state["keys"]["committed_view"]["consumed"],
                ["key-00", "key-01"])
