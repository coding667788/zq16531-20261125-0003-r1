"""串行线性化参考模型。

单线程、无崩溃地按给定顺序处理同样的请求序列，得到"正确答案"。
真实系统在任何崩溃/恢复/并发交错之后，其最终可观察状态都必须与
某个请求串行排列下的参考状态逐项一致；成功回执的内容也必须一致。

参考模型与生产代码刻意不共享任何业务实现，只共用协议常量与哈希函数，
避免两边犯同一个错误却互相印证。coord_seq 的计数规则则与协调器
实际写入 coord_events 的事件个数严格对齐（提交 5 个、终止 4 个事件），
使回执可以逐项（含序号）对照。
"""
from __future__ import annotations

from . import protocol as p


class ReferenceModel:
    def __init__(self, initial_keys: list[str], initial_bags: list[dict]):
        # 已提交视图：只有这些字段对业务读者算数
        self.key_status = {k: "AVAILABLE" for k in initial_keys}
        self.bag_holder = {b["bag_id"]: b["holder"] for b in initial_bags}
        # 每个 request_id 的终态回执（幂等记忆）
        self.receipts: dict[str, dict] = {}
        self.contents: dict[str, dict] = {}
        self._coord_seq = 0

    def _tick(self, n: int = 1) -> int:
        self._coord_seq += n
        return self._coord_seq

    @staticmethod
    def _content_of(req: dict) -> dict:
        return {k: req.get(k, "") for k in
                ("key_id", "bag_id", "from_holder", "to_holder", "note")}

    def apply(self, req: dict) -> dict:
        """正常处理一个请求（含业务预检终止），返回终态回执。"""
        rid = req["request_id"]
        chash = p.content_hash(req)
        if rid in self.receipts:
            if p.canonical_json(self.contents[rid]) != p.canonical_json(
                    self._content_of(req)):
                return {"error": "ID_CONTENT_CONFLICT", "request_id": rid}
            return self.receipts[rid]
        self.contents[rid] = self._content_of(req)
        self._tick()  # 1. RECEIVED

        reason = self._precheck(req)
        if reason is not None:
            return self._finish_abort(rid, chash, reason)

        # 2. 提交决定 3. KEYS_DONE 4. BAGS_DONE 5. COMPLETED
        seq = self._tick(4)
        self.key_status[req["key_id"]] = "CONSUMED"
        self.bag_holder[req["bag_id"]] = req["to_holder"]
        receipt = p.committed_receipt(rid, seq, chash)
        self.receipts[rid] = receipt
        return receipt

    def crash_recover_abort(self, req: dict,
                            reason: str = p.REASON_RECOVERY_ABORT) -> dict:
        """模拟崩溃于提交决定之前：恢复只能全量终止。"""
        rid = req["request_id"]
        chash = p.content_hash(req)
        if rid in self.receipts:
            return self.receipts[rid]
        self.contents[rid] = self._content_of(req)
        self._tick()  # RECEIVED 已在崩溃前落盘
        return self._finish_abort(rid, chash, reason)

    def crash_recover_commit(self, req: dict) -> dict:
        """模拟崩溃于提交决定之后：恢复前滚，两边都落实。"""
        rid = req["request_id"]
        chash = p.content_hash(req)
        if rid in self.receipts:
            return self.receipts[rid]
        self.contents[rid] = self._content_of(req)
        self._tick()  # RECEIVED
        seq = self._tick(4)  # 决定 + 三个完成事件
        self.key_status[req["key_id"]] = "CONSUMED"
        self.bag_holder[req["bag_id"]] = req["to_holder"]
        receipt = p.committed_receipt(rid, seq, chash)
        self.receipts[rid] = receipt
        return receipt

    def _finish_abort(self, rid: str, chash: str, reason: str) -> dict:
        # ABORT_DECISION + KEYS_DONE + BAGS_DONE（终止路径无 COMPLETED 事件，
        # 与 coordinator._coord_cleanup_done 的实际写入对齐）
        seq = self._tick(3)
        receipt = p.aborted_receipt(rid, seq, chash, reason)
        self.receipts[rid] = receipt
        return receipt

    def _precheck(self, req: dict) -> str | None:
        if self.key_status.get(req["key_id"]) != "AVAILABLE":
            return p.REASON_KEY_NOT_AVAILABLE
        if req["bag_id"] not in self.bag_holder:
            return p.REASON_BAG_NOT_FOUND
        if self.bag_holder[req["bag_id"]] != req["from_holder"]:
            return p.REASON_BAG_HOLDER_MISMATCH
        return None

    def committed_key_view(self) -> dict:
        available = sorted(k for k, s in self.key_status.items() if s == "AVAILABLE")
        consumed = sorted(k for k, s in self.key_status.items() if s == "CONSUMED")
        return {"available": available, "consumed": consumed,
                "available_count": len(available), "consumed_count": len(consumed)}

    def committed_bag_view(self) -> list[dict]:
        return [{"bag_id": b, "holder": h}
                for b, h in sorted(self.bag_holder.items())]

    def compare_read(self, real: dict) -> list[str]:
        """把真实 read 输出与参考模型逐项对照，返回差异描述列表（空=一致）。"""
        diffs = []
        if real["keys"]["committed_view"] != self.committed_key_view():
            diffs.append(f"钥匙视图不一致: real={real['keys']['committed_view']}"
                         f" ref={self.committed_key_view()}")
        if real["bags"]["committed_view"] != self.committed_bag_view():
            diffs.append(f"袋子视图不一致: real={real['bags']['committed_view']}"
                         f" ref={self.committed_bag_view()}")
        # 恢复完成后不允许残留任何 pending / 半成品
        if real["pending"]:
            diffs.append(f"存在未决 pending: {real['pending']}")
        if real["keys"]["inflight_reserved"]:
            diffs.append(f"残留 RESERVED 半成品: {real['keys']['inflight_reserved']}")
        if real["bags"]["inflight_prepared"]:
            diffs.append(f"残留 PREPARED 半成品: {real['bags']['inflight_prepared']}")
        return diffs

    def compare_receipt(self, rid: str, real_receipt: dict) -> list[str]:
        diffs = []
        ref = self.receipts.get(rid)
        if ref is None:
            diffs.append(f"参考模型中无 {rid} 的回执")
            return diffs
        if real_receipt != ref:
            diffs.append(f"回执不一致: real={real_receipt} ref={ref}")
        return diffs
