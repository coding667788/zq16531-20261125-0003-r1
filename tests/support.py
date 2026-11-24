"""测试公共工具：每个用例一个独立工作目录，所有 CLI 操作走独立子进程。

生产代码在仓库根的 dualreceipt 包内；测试用 sys.executable -m dualreceipt
启动全新解释器进程，崩溃注入（os._exit）与恢复因此发生在不同进程中，
真实模拟"读者直接启动另一进程"的部署形态。
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# 最小化子进程环境，避免继承外部 PYTHONPATH 等变量引入不可信模块。
def _clean_env(**overrides) -> dict:
    env = {"PATH": os.environ.get("PATH", ""),
           "SYSTEMROOT": os.environ.get("SYSTEMROOT", ""),
           "TEMP": os.environ.get("TEMP", ""),
           "TMP": os.environ.get("TMP", ""),
           "PATHEXT": os.environ.get("PATHEXT", "")}
    env.update({k: str(v) for k, v in overrides.items()})
    return env


class ProcResult:
    def __init__(self, rc: int, stdout: str, stderr: str):
        self.rc = rc
        self.stdout = stdout
        self.stderr = stderr

    def json(self) -> dict:
        # 成功回执走 stdout，错误对象走 stderr
        text = self.stdout if self.stdout.strip() else self.stderr
        return json.loads(text)


class DualReceiptTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dr_test_")
        self.root = Path(self.tmp) / "db"
        self.root.mkdir()
        self.keys = ["key-00", "key-01", "key-02", "key-03"]
        self.bags = [
            {"bag_id": "bag-00", "holder": "vault-0"},
            {"bag_id": "bag-01", "holder": "vault-1"},
            {"bag_id": "bag-02", "holder": "vault-0"},
            {"bag_id": "bag-03", "holder": "vault-1"},
        ]
        self.init_store()

    # ------------------------------------------------------------- subprocess
    def run_cli(self, *args, env_extra: dict | None = None,
                input_obj: dict | None = None, timeout: float = 60.0) -> ProcResult:
        cmd = [sys.executable, "-m", "dualreceipt",
               "--root", str(self.root), *args]
        env = _clean_env(**(env_extra or {}))
        stdin = None
        if input_obj is not None:
            stdin = json.dumps(input_obj, ensure_ascii=False).encode("utf-8")
        proc = subprocess.run(cmd, cwd=str(REPO_ROOT), env=env,
                              input=stdin, capture_output=True, timeout=timeout)
        return ProcResult(proc.returncode,
                          proc.stdout.decode("utf-8"),
                          proc.stderr.decode("utf-8"))

    def spawn_cli(self, *args, env_extra: dict | None = None,
                  stdin_obj: dict | None = None) -> subprocess.Popen:
        """不等待结束，用于并发与崩溃窗口控制。"""
        cmd = [sys.executable, "-m", "dualreceipt",
               "--root", str(self.root), *args]
        env = _clean_env(**(env_extra or {}))
        stdin = None
        if stdin_obj is not None:
            stdin = json.dumps(stdin_obj, ensure_ascii=False).encode("utf-8")
        return subprocess.Popen(cmd, cwd=str(REPO_ROOT), env=env,
                                stdin=subprocess.PIPE if stdin else None,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    @staticmethod
    def wait(proc: subprocess.Popen, timeout: float = 60.0) -> ProcResult:
        out, err = proc.communicate(timeout=timeout)
        return ProcResult(proc.returncode, out.decode("utf-8"), err.decode("utf-8"))

    # ----------------------------------------------------------------- helpers
    def init_store(self, keys=None, bags=None):
        keys = keys or self.keys
        bags = bags or self.bags
        r = self.run_cli("init", "--keys-seed", json.dumps(keys),
                         "--bags-seed", json.dumps(bags))
        self.assertEqual(r.rc, 0, r.stderr)

    def apply_file(self, req: dict, name: str = "req.json",
                   env_extra: dict | None = None) -> ProcResult:
        p = Path(self.tmp) / name
        p.write_text(json.dumps(req, ensure_ascii=False), encoding="utf-8")
        return self.run_cli("apply", "--request", str(p), env_extra=env_extra)

    def apply_crash(self, req: dict, boundary: str) -> ProcResult:
        return self.apply_file(req, env_extra={"DUALRECEIPT_CRASH": boundary})

    def recover(self, env_extra: dict | None = None) -> ProcResult:
        return self.run_cli("recover", env_extra=env_extra)

    def read(self, as_of: dict | None = None) -> dict:
        args = ["read"]
        if as_of is not None:
            args += ["--as-of", json.dumps(as_of)]
        r = self.run_cli(*args)
        self.assertEqual(r.rc, 0, r.stderr)
        return r.json()

    def audit(self, rid: str) -> dict:
        r = self.run_cli("audit", "--request-id", rid)
        self.assertIn(r.rc, (0, 2))
        return r.json()

    def req(self, rid: str, key: str, bag: str, frm: str, to: str,
            note: str = "") -> dict:
        return {"request_id": rid, "key_id": key, "bag_id": bag,
                "from_holder": frm, "to_holder": to, "note": note}

    def assert_no_inflight(self, state: dict):
        self.assertEqual(state["keys"]["inflight_reserved"], [])
        self.assertEqual(state["bags"]["inflight_prepared"], [])
        self.assertEqual(state["pending"], [])
