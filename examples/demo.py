"""端到端演示：正常 transfer、崩溃注入、另一进程恢复、审计。

用法：python examples/demo.py
全部在临时目录中完成，不污染仓库；每个 CLI 动作都是独立子进程。
"""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def _clean_env(**overrides) -> dict:
    env = {"PATH": __import__("os").environ.get("PATH", ""),
           "SYSTEMROOT": __import__("os").environ.get("SYSTEMROOT", ""),
           "TEMP": __import__("os").environ.get("TEMP", ""),
           "TMP": __import__("os").environ.get("TMP", ""),
           "PATHEXT": __import__("os").environ.get("PATHEXT", "")}
    env.update({k: str(v) for k, v in overrides.items()})
    return env


def run(root: Path, *args, env_extra: dict | None = None) -> dict:
    proc = subprocess.run(
        [sys.executable, "-m", "dualreceipt", "--root", str(root), *args],
        cwd=str(REPO), capture_output=True, text=True,
        env=_clean_env(**(env_extra or {})))
    out = proc.stdout if proc.stdout.strip() else proc.stderr
    print(f"$ dualreceipt {' '.join(args)} -> rc={proc.returncode}")
    print(json.dumps(json.loads(out), ensure_ascii=False, indent=2))
    return json.loads(out)


def main() -> None:
    work = Path(tempfile.mkdtemp(prefix="dualreceipt_demo_"))
    root = work / "data"
    root.mkdir()
    try:
        run(root, "init", "--fixture",
            str(REPO / "examples" / "fixture.json"))
        req_path = work / "req.json"
        req_path.write_text(
            (REPO / "examples" / "request.json").read_text(encoding="utf-8"),
            encoding="utf-8")
        run(root, "apply", "--request", str(req_path))
        run(root, "read")

        # 第二个请求：在提交决定之后硬退出，再由另一进程前滚
        req2 = {
            "request_id": "req-002", "key_id": "key-01", "bag_id": "bag-01",
            "from_holder": "vault-1", "to_holder": "officer-8", "note": "crash demo",
        }
        req2_path = work / "req2.json"
        req2_path.write_text(json.dumps(req2, ensure_ascii=False), encoding="utf-8")
        crashed = subprocess.run(
            [sys.executable, "-m", "dualreceipt", "--root", str(root),
             "apply", "--request", str(req2_path)],
            cwd=str(REPO), capture_output=True, text=True,
            env=_clean_env(DUALRECEIPT_CRASH="CRASH_AFTER_COMMIT_DECISION"))
        print(f"$ apply (crash injected) -> rc={crashed.returncode} (88=硬退出)")
        run(root, "read")          # 读者看到明确 pending
        run(root, "recover")       # 另一进程恢复：前滚
        run(root, "apply", "--request", str(req2_path))  # 重试取同回执
        run(root, "audit", "--request-id", "req-002")
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
