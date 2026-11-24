"""dualreceipt 命令行：init / apply / read / recover / audit / make-fixture。

所有输入输出均为 UTF-8 JSON。请求可来自文件 (--request) 或标准输入。
退出码：0 成功（含 ABORTED/PENDING 回执）；2 请求冲突或输入非法；1 其它错误。
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from . import __version__
from . import protocol as p
from . import reader
from . import store
from .coordinator import ConflictError, Coordinator

DEFAULT_NAMES = {"keys": "keys.db", "bags": "bags.db", "coord": "coord.db"}


def _emit(obj, code=0) -> int:
    sys.stdout.buffer.write(
        (json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
        .encode("utf-8"))
    return code


def _err(code_name: str, message: str, code: int = 2, **extra) -> int:
    obj = {"error": code_name, "message": message}
    obj.update(extra)
    sys.stderr.buffer.write(
        (json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
        .encode("utf-8"))
    return code


def _paths(ns) -> dict:
    if ns.root:
        root = Path(ns.root)
        return {k: str(root / v) for k, v in DEFAULT_NAMES.items()}
    missing = [n for n in ("keys", "bags", "coord") if not getattr(ns, n)]
    if missing:
        raise ValueError(f"需要 --root 或同时指定 --keys/--bags/--coord，缺: {missing}")
    return {"keys": ns.keys, "bags": ns.bags, "coord": ns.coord}


def _load_json_arg(text: str | None, file: str | None) -> dict | list:
    if file:
        with open(file, "r", encoding="utf-8") as f:
            return json.load(f)
    if text is not None:
        return json.loads(text)
    return json.loads(sys.stdin.buffer.read().decode("utf-8"))


def cmd_init(ns) -> int:
    paths = _paths(ns)
    fixture = None
    if ns.fixture:
        with open(ns.fixture, "r", encoding="utf-8") as f:
            fixture = json.load(f)
    keys_seed = ns.keys_seed
    bags_seed = ns.bags_seed
    if fixture is not None:
        keys_seed = keys_seed or fixture.get("keys")
        bags_seed = bags_seed or fixture.get("bags")
    if not keys_seed or not bags_seed:
        return _err("BAD_SEED", "init 需要 keys 与 bags 种子（参数或夹具文件）")
    store.init_keys(paths["keys"], list(keys_seed))
    store.init_bags(paths["bags"], list(bags_seed))
    store.init_coord(paths["coord"])
    return _emit({"initialized": True, "paths": paths,
                  "keys": len(keys_seed), "bags": len(bags_seed)})


def cmd_apply(ns) -> int:
    paths = _paths(ns)
    try:
        request = _load_json_arg(ns.request_json, ns.request)
    except json.JSONDecodeError as e:
        return _err("BAD_JSON", f"请求 JSON 非法: {e}")
    if not isinstance(request, dict):
        return _err("BAD_REQUEST", "请求必须是 JSON 对象")
    coord = Coordinator(paths["keys"], paths["bags"], paths["coord"])
    try:
        try:
            receipt = coord.apply(request)
        except ConflictError:
            incoming = p.content_hash(request)
            row = coord.cc.execute(
                "SELECT content_hash FROM requests WHERE request_id=?",
                (request["request_id"],)).fetchone()
            return _err("ID_CONTENT_CONFLICT",
                        "同 request_id 对应不同内容，拒绝执行",
                        existing_content_hash=row["content_hash"],
                        incoming_content_hash=incoming)
        except ValueError as e:
            return _err("BAD_REQUEST", str(e))
        code = 2 if receipt.get("error") else 0
        return _emit(receipt, code)
    finally:
        coord.close()


def cmd_read(ns) -> int:
    paths = _paths(ns)
    as_of = None
    if ns.as_of:
        as_of = json.loads(ns.as_of)
        for k in as_of:
            if k not in ("keys", "bags", "coord"):
                return _err("BAD_AS_OF", f"未知库名: {k}")
            as_of[k] = int(as_of[k])
    out = reader.read_all(paths["keys"], paths["bags"], paths["coord"], as_of)
    return _emit(out)


def cmd_recover(ns) -> int:
    paths = _paths(ns)
    coord = Coordinator(paths["keys"], paths["bags"], paths["coord"])
    try:
        summary = coord.recover()
    finally:
        coord.close()
    out = reader.read_all(paths["keys"], paths["bags"], paths["coord"])
    return _emit({"recover": summary, "state_after": out})


def cmd_audit(ns) -> int:
    paths = _paths(ns)
    out = reader.audit_receipt(paths["keys"], paths["bags"], paths["coord"],
                               ns.request_id)
    return _emit(out, 0 if out["legal"] else 2)


def cmd_make_fixture(ns) -> int:
    """生成合成夹具：初始库存、袋台账与一串确定性请求。"""
    out_dir = Path(ns.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    keys = [f"key-{i:02d}" for i in range(ns.n_keys)]
    bags = [{"bag_id": f"bag-{i:02d}", "holder": f"vault-{i % 2}"}
            for i in range(ns.n_bags)]
    fixture = {"keys": keys, "bags": bags}
    (out_dir / "fixture.json").write_text(
        json.dumps(fixture, ensure_ascii=False, indent=2), encoding="utf-8")
    requests = []
    for i in range(min(ns.n_bags, ns.n_keys)):
        requests.append({
            "request_id": f"req-{i:03d}",
            "key_id": f"key-{i:02d}",
            "bag_id": f"bag-{i:02d}",
            "from_holder": f"vault-{i % 2}",
            "to_holder": f"officer-{i % 3}",
            "note": f"synthetic transfer {i}",
        })
    (out_dir / "requests.json").write_text(
        json.dumps(requests, ensure_ascii=False, indent=2), encoding="utf-8")
    # 争最后一把钥匙的两个请求
    content = {
        "requests": requests,
        "contend_last_key": [
            {"request_id": "req-contend-a", "key_id": keys[-1],
             "bag_id": bags[-1]["bag_id"],
             "from_holder": bags[-1]["holder"], "to_holder": "officer-a",
             "note": "contend A"},
            {"request_id": "req-contend-b", "key_id": keys[-1],
             "bag_id": bags[-1]["bag_id"],
             "from_holder": bags[-1]["holder"], "to_holder": "officer-b",
             "note": "contend B"},
        ],
    }
    (out_dir / "scenarios.json").write_text(
        json.dumps(content, ensure_ascii=False, indent=2), encoding="utf-8")
    return _emit({"fixture_dir": str(out_dir), "keys": len(keys),
                  "bags": len(bags), "requests": len(requests)})


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="dualreceipt",
                                 description="两本离线安全台账的跨库事务协调器")
    ap.add_argument("--version", action="version", version=__version__)
    ap.add_argument("--root", help="数据库目录（使用默认文件名 keys.db/bags.db/coord.db）")
    ap.add_argument("--keys", help="钥匙库存 SQLite 路径")
    ap.add_argument("--bags", help="证物袋台账 SQLite 路径")
    ap.add_argument("--coord", help="协调日志 SQLite 路径")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init", help="创建并播种三个 SQLite 文件")
    p_init.add_argument("--fixture", help="夹具 JSON（含 keys 与 bags）")
    p_init.add_argument("--keys-seed", type=json.loads, help="钥匙 ID 列表 JSON")
    p_init.add_argument("--bags-seed", type=json.loads,
                        help='[{bag_id, holder}, ...] JSON')
    p_init.set_defaults(func=cmd_init)

    p_apply = sub.add_parser("apply", help="提交一次 transfer 请求")
    p_apply.add_argument("--request", help="请求 JSON 文件路径")
    p_apply.add_argument("--request-json", help="内联请求 JSON 字符串")
    p_apply.set_defaults(func=cmd_apply)

    p_read = sub.add_parser("read", help="只读视图（半成品不计入已提交视图）")
    p_read.add_argument("--as-of", help='逻辑时间快照，如 {"keys":2,"bags":1,"coord":3}')
    p_read.set_defaults(func=cmd_read)

    p_rec = sub.add_parser("recover", help="崩溃恢复：全部提交或全部终止")
    p_rec.set_defaults(func=cmd_recover)

    p_aud = sub.add_parser("audit", help="审计某回执的最小持久证据与非法状态")
    p_aud.add_argument("--request-id", required=True)
    p_aud.set_defaults(func=cmd_audit)

    p_fix = sub.add_parser("make-fixture", help="生成合成夹具")
    p_fix.add_argument("--out", required=True)
    p_fix.add_argument("--n-keys", type=int, default=4)
    p_fix.add_argument("--n-bags", type=int, default=4)
    p_fix.set_defaults(func=cmd_make_fixture)
    return ap


def main(argv: list[str] | None = None) -> int:
    ap = build_parser()
    ns = ap.parse_args(argv)
    try:
        return ns.func(ns)
    except FileNotFoundError as e:
        return _err("FILE_NOT_FOUND", str(e), code=1)
    except Exception as e:  # noqa: BLE001 - CLI 顶层兜底，保证输出结构化 JSON
        return _err("INTERNAL_ERROR", f"{type(e).__name__}: {e}", code=1)


if __name__ == "__main__":
    raise SystemExit(main())
