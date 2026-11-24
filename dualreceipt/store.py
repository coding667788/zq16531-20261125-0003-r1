"""三个 SQLite 文件的建表、连接与事件溯源读取。

每个文件都是程序自行创建的普通 SQLite 数据库，可独立删除后重建、
独立重启。所有连接打开 ``FULL`` 同步，事务一律 ``BEGIN IMMEDIATE``，
跨进程互斥完全依赖 SQLite 文件锁与表内行状态，不使用任何进程内锁。
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from . import protocol as p

# 连接 busy timeout：配合应用层资源锁，覆盖 Windows 上文件锁的瞬时争用。
BUSY_TIMEOUT_MS = 30000

KEYS_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS keys (
    key_id       TEXT PRIMARY KEY,
    status       TEXT NOT NULL,              -- AVAILABLE | RESERVED | CONSUMED
    request_id   TEXT,                       -- 预留/消耗所属请求
    updated_seq  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS key_events (
    seq        INTEGER PRIMARY KEY,          -- 本库本地逻辑时间
    event_type TEXT NOT NULL,                -- SEED | RESERVE | COMMIT | ABORT
    key_id     TEXT NOT NULL,
    request_id TEXT,
    detail     TEXT NOT NULL DEFAULT '{}'
);
"""

BAGS_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS bags (
    bag_id           TEXT PRIMARY KEY,
    current_holder   TEXT NOT NULL,
    status           TEXT NOT NULL,          -- AVAILABLE | PREPARED | HANDED
    request_id       TEXT,
    prepared_holder  TEXT,                   -- 预备交接的目标持有人（半成品）
    updated_seq      INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS bag_events (
    seq        INTEGER PRIMARY KEY,
    event_type TEXT NOT NULL,                -- SEED | PREPARE | COMMIT | ABORT
    bag_id     TEXT NOT NULL,
    request_id TEXT,
    holder     TEXT,
    detail     TEXT NOT NULL DEFAULT '{}'
);
"""

COORD_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS requests (
    request_id   TEXT PRIMARY KEY,
    content_hash TEXT NOT NULL,
    state        TEXT NOT NULL,              -- RECEIVED | COMMITTED | ABORTED | COMPLETED
    reason       TEXT,
    updated_seq  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS coord_events (
    seq        INTEGER PRIMARY KEY,          -- 全局协调逻辑时间
    event_type TEXT NOT NULL,                -- RECEIVED | COMMIT_DECISION |
                                             -- ABORT_DECISION | KEYS_DONE | BAGS_DONE | COMPLETED
    request_id TEXT NOT NULL,
    detail     TEXT NOT NULL DEFAULT '{}'
);
-- 跨进程资源锁：协调库是所有进程都必须访问的单点，锁表即互斥点。
CREATE TABLE IF NOT EXISTS resource_locks (
    lock_key   TEXT PRIMARY KEY,             -- 全局全序命名，见 coordinator._lock_keys
    request_id TEXT NOT NULL,
    acquired_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS receipts (
    request_id TEXT PRIMARY KEY,
    payload    TEXT NOT NULL                 -- 规范化 JSON 回执，终态只读
);
"""


def connect(path: str | Path) -> sqlite3.Connection:
    """打开一个普通 sqlite3 连接：自动提交由调用方用显式事务控制。"""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=BUSY_TIMEOUT_MS / 1000.0,
                           isolation_level=None)
    # FULL：每次事务提交都 fsync，os._exit / 断电后已提交内容必然在盘上。
    conn.execute("PRAGMA synchronous=FULL")
    # rollback journal（非 WAL），Windows 上崩溃恢复最稳，且单文件语义简单。
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_keys(path: str | Path, key_ids: list[str]) -> None:
    conn = connect(path)
    try:
        conn.executescript(KEYS_SCHEMA)
        conn.execute("BEGIN IMMEDIATE")
        existing = conn.execute("SELECT COUNT(*) FROM keys").fetchone()[0]
        if existing == 0:
            for kid in key_ids:
                seq = next_seq(conn, "key_events")
                conn.execute(
                    "INSERT INTO keys(key_id,status,request_id,updated_seq)"
                    " VALUES(?, 'AVAILABLE', NULL, ?)", (kid, seq))
                conn.execute(
                    "INSERT INTO key_events(seq,event_type,key_id,request_id,detail)"
                    " VALUES(?, 'SEED', ?, NULL, '{}')", (seq, kid))
        conn.execute("COMMIT")
    finally:
        conn.close()


def init_bags(path: str | Path, bags: list[dict]) -> None:
    conn = connect(path)
    try:
        conn.executescript(BAGS_SCHEMA)
        conn.execute("BEGIN IMMEDIATE")
        existing = conn.execute("SELECT COUNT(*) FROM bags").fetchone()[0]
        if existing == 0:
            for b in bags:
                seq = next_seq(conn, "bag_events")
                conn.execute(
                    "INSERT INTO bags(bag_id,current_holder,status,request_id,"
                    "prepared_holder,updated_seq) VALUES(?, ?, 'AVAILABLE', NULL, NULL, ?)",
                    (b["bag_id"], b["holder"], seq))
                conn.execute(
                    "INSERT INTO bag_events(seq,event_type,bag_id,request_id,holder,detail)"
                    " VALUES(?, 'SEED', ?, NULL, ?, '{}')",
                    (seq, b["bag_id"], b["holder"]))
        conn.execute("COMMIT")
    finally:
        conn.close()


def init_coord(path: str | Path) -> None:
    conn = connect(path)
    try:
        conn.executescript(COORD_SCHEMA)
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.execute(
            "INSERT OR IGNORE INTO meta(key,value) VALUES('schema','dualreceipt-1')")
        conn.execute("COMMIT")
    finally:
        conn.close()


def next_seq(conn: sqlite3.Connection, table: str) -> int:
    """下一个本地逻辑时间（同一连接内、调用方持有写事务时使用）。"""
    row = conn.execute(f"SELECT COALESCE(MAX(seq),0)+1 FROM {table}").fetchone()
    return row[0]


def replay_keys(conn: sqlite3.Connection, as_of: int | None = None) -> dict:
    """按 key_events 重放到给定逻辑时间，返回钥匙状态视图（含半成品标记）。"""
    sql = "SELECT * FROM key_events"
    params: tuple = ()
    if as_of is not None:
        sql += " WHERE seq <= ?"
        params = (as_of,)
    sql += " ORDER BY seq"
    keys: dict[str, dict] = {}
    for ev in conn.execute(sql, params):
        kid = ev["key_id"]
        if ev["event_type"] == "SEED":
            keys[kid] = {"key_id": kid, "status": "AVAILABLE",
                         "request_id": None, "updated_seq": ev["seq"]}
        elif ev["event_type"] == "RESERVE":
            keys[kid] = {"key_id": kid, "status": p.RESERVED,
                         "request_id": ev["request_id"], "updated_seq": ev["seq"]}
        elif ev["event_type"] == "COMMIT":
            keys[kid] = {"key_id": kid, "status": p.CONSUMED,
                         "request_id": ev["request_id"], "updated_seq": ev["seq"]}
        elif ev["event_type"] == "ABORT":
            keys[kid] = {"key_id": kid, "status": "AVAILABLE",
                         "request_id": None, "updated_seq": ev["seq"]}
    return keys


def replay_bags(conn: sqlite3.Connection, as_of: int | None = None) -> dict:
    """按 bag_events 重放，PREPARE 不改持有人，只记录半成品预备态。"""
    sql = "SELECT * FROM bag_events"
    params: tuple = ()
    if as_of is not None:
        sql += " WHERE seq <= ?"
        params = (as_of,)
    sql += " ORDER BY seq"
    bags: dict[str, dict] = {}
    for ev in conn.execute(sql, params):
        bid = ev["bag_id"]
        if ev["event_type"] == "SEED":
            bags[bid] = {"bag_id": bid, "holder": ev["holder"],
                         "status": "AVAILABLE", "request_id": None,
                         "prepared_holder": None, "updated_seq": ev["seq"]}
        elif ev["event_type"] == "PREPARE":
            prev = bags.get(bid)
            bags[bid] = {"bag_id": bid,
                         "holder": prev["holder"] if prev else None,
                         "status": p.BAG_PREPARED,
                         "request_id": ev["request_id"],
                         "prepared_holder": ev["holder"],
                         "updated_seq": ev["seq"]}
        elif ev["event_type"] == "COMMIT":
            bags[bid] = {"bag_id": bid, "holder": ev["holder"],
                         "status": p.HANDED, "request_id": ev["request_id"],
                         "prepared_holder": None, "updated_seq": ev["seq"]}
        elif ev["event_type"] == "ABORT":
            prev = bags.get(bid)
            bags[bid] = {"bag_id": bid,
                         "holder": prev["holder"] if prev else None,
                         "status": "AVAILABLE", "request_id": None,
                         "prepared_holder": None, "updated_seq": ev["seq"]}
    return bags


def max_seq(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f"SELECT COALESCE(MAX(seq),0) FROM {table}").fetchone()[0]
