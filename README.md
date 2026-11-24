# dualreceipt —— 两本离线安全台账的跨库事务协调器

一次性钥匙库存（`keys.db`）与证物袋交接台账（`bags.db`）是两个**不能放进
同一个数据库事务**的独立 SQLite 文件，各自可以单独重启。`dualreceipt`
用第三个 SQLite 文件（`coord.db`，协调日志）实现类两阶段提交（2PC），
保证一次 transfer 对业务读者而言**要么两边同时生效、要么两边都无效果**。

仅依赖 Python 3.12 标准库，完全离线，Windows / Linux / macOS 均可运行。

## 1. 三个数据库文件

| 文件 | 职责 | 关键表 |
|---|---|---|
| `keys.db` | 一次性钥匙库存，事件溯源 | `keys`、`key_events(seq,...)` |
| `bags.db` | 证物袋持有人台账，事件溯源 | `bags`、`bag_events(seq,...)` |
| `coord.db` | 协调日志、跨进程资源锁、终态回执 | `requests`、`coord_events(seq,...)`、`resource_locks`、`receipts` |

所有连接均 `PRAGMA synchronous=FULL` + rollback journal，每个协议步骤是
一次独立事务、一次 fsync。三个文件之间没有任何共享事务，跨进程互斥只靠
SQLite 文件锁与 `coord.db.resource_locks` 表内行状态，**不使用任何进程内
锁**——读者可以随时启动一个全新进程直接打开这些文件。

## 2. 协议（每个 `==` 是一次独立 SQLite 事务）

```
coord: RECEIVED（保存完整请求内容与 content_hash）========
lock:  全序资源锁（0:key:* < 1:bag:*，杜绝循环等待）
keys:  RESERVE  钥匙 -> RESERVED（半成品，读者不计消耗）====
bags:  PREPARE  袋子 -> PREPARED（半成品，持有人不变更）====
coord: COMMIT_DECISION =====================================  ← 唯一提交点，不可逆
keys:  COMMIT   钥匙 -> CONSUMED ===========================
bags:  COMMIT   袋子 -> HANDED，持有人变更 =================
coord: COMPLETED + 终态回执固化到 receipts =================
unlock
```

**崩溃恢复规则（`recover`）**

* 崩溃在 `COMMIT_DECISION` 之前（RECEIVED / RESERVE / PREPARE）：没有提交
  决定，恢复只能**全量终止**——补 `ABORT_DECISION`，两边回滚到 AVAILABLE；
* 崩溃在 `COMMIT_DECISION` 之后：决定不可撤销，恢复只能**前滚**——把两边
  缺失的 COMMIT 补齐；
* 所有恢复操作幂等，可重复执行；已固化在 `receipts` 表中的成功回执只读，
  恢复路径永不改写（已返回成功的回执不会失效）。

## 3. 输入格式

### 3.1 播种夹具（`init --fixture`）

```json
{
  "keys": ["key-00", "key-01"],
  "bags": [
    {"bag_id": "bag-00", "holder": "vault-0"},
    {"bag_id": "bag-01", "holder": "vault-1"}
  ]
}
```

### 3.2 transfer 请求（`apply`）

```json
{
  "request_id": "req-001",
  "key_id": "key-00",
  "bag_id": "bag-00",
  "from_holder": "vault-0",
  "to_holder": "officer-7",
  "note": "可选备注，缺省视为空字符串"
}
```

* 五个字符串字段必填且非空；`note` 可缺省。
* `content_hash = sha256(规范化JSON({key_id,bag_id,from_holder,to_holder,note}))`，
  规范化指键排序、无空白、UTF-8 字节。
* **幂等**：同 `request_id` + 同内容重试，永远返回同一张回执；
  同 `request_id` + 不同内容，返回 `ID_CONTENT_CONFLICT` 错误（退出码 2）。

### 3.3 回执

```json
{ "receipt": "COMMITTED", "request_id": "req-001",
  "content_hash": "...", "coord_seq": 5 }
```
```json
{ "receipt": "ABORTED", "request_id": "req-002",
  "content_hash": "...", "coord_seq": 9, "reason": "KEY_NOT_AVAILABLE" }
```
```json
{ "receipt": "PENDING", "request_id": "req-003",
  "content_hash": "...", "coord_state": "PREPARED",
  "hint": "决定已记录但尚未在两本账上落实，运行 recover 后重试" }
```

终止原因：`LOCK_TIMEOUT`、`KEY_NOT_AVAILABLE`、`BAG_NOT_FOUND`、
`BAG_HOLDER_MISMATCH`、`BAG_BUSY`、`RECOVERY_ABORT`。
所有 ABORTED 都发生在提交决定之前，两边无任何可见效果。

## 4. 命令

```bash
# 生成合成夹具（fixture.json / requests.json / scenarios.json）
python -m dualreceipt make-fixture --out examples/fixture --n-keys 4 --n-bags 4

# 创建并播种三个 SQLite 文件
python -m dualreceipt --root data init --fixture examples/fixture/fixture.json

# 提交一次 transfer（也可用 --request-json 内联，或标准输入）
python -m dualreceipt --root data apply --request examples/request.json

# 只读视图：已提交视图 / 半成品 / pending / 各库当前逻辑时间
python -m dualreceipt --root data read

# 旧快照复核：按各库事件流逻辑时间重放
python -m dualreceipt --root data read --as-of '{"keys":3,"bags":2,"coord":4}'

# 崩溃恢复（运维约定：运行时不要并发执行 apply）
python -m dualreceipt --root data recover

# 审计一张回执：最小持久证据 + 非法状态 + 持有人结论
python -m dualreceipt --root data audit --request-id req-001
```

退出码：`0` 成功（含 ABORTED/PENDING 回执）；`2` 冲突或输入非法；`1` 其它错误。
所有输出为 UTF-8 JSON（成功在 stdout，错误对象在 stderr）。

## 5. 读者语义（read）

* `keys.committed_view` / `bags.committed_view` 只承认 SEED/COMMIT 事件的
  效果。`RESERVED` 钥匙仍计为在库，`PREPARED` 袋子保持原持有人——
  **半成品绝不被当成已提交状态**。
* 半成品在 `inflight_reserved` / `inflight_prepared` 中明确标注。
* 未完成请求在 `pending` 中显式列出，并附带 `coord_state`。
* `--as-of` 分别给三个库指定逻辑时间，按事件流重放得到历史快照，
  支持从旧快照复核。

## 6. 审计语义（audit）

对给定 `request_id` 输出：

* `minimal_evidence`：支撑该回执的最小持久证据，精确到库/表/事件序号
  （协调决定 + 两侧落实事件 + 固化回执）；
* `illegal_states`：非法状态码列表，含 `INCOMPLETE_COMMIT`、
  `ORPHAN_SIDE_COMMIT_WITHOUT_DECISION`、`HOLDER_AMBIGUOUS_ORDER_UNDETERMINED`、
  `CONTENT_HASH_MISMATCH` 等；
* `holder_conclusion`：**只有协调日志存在 `COMMIT_DECISION` 时才承认新持有人**。
  若两本账的持久顺序无法由协调日志裁定，`holder` 为 `null`、
  `determined=false`，绝不编造交接人。

## 7. 崩溃注入与并发测试

环境变量（仅测试使用）：

| 变量 | 含义 |
|---|---|
| `DUALRECEIPT_CRASH=<边界名>` | 到达该命名边界后 `os._exit(88)` 硬退出 |
| `DUALRECEIPT_DELAY=<边界名>` + `DUALRECEIPT_DELAY_SECONDS` | 在命名边界睡眠 |
| `DUALRECEIPT_LOCK_TIMEOUT=秒` | 资源锁等待超时（默认 15s） |

八个命名崩溃边界（`dualreceipt.protocol.CRASH_BOUNDARIES`）：

1. `CRASH_AFTER_COORD_RECEIVED`
2. `CRASH_AFTER_KEYS_PREPARED`
3. `CRASH_AFTER_BAGS_PREPARED`
4. `CRASH_AFTER_COMMIT_DECISION`（提交点）
5. `CRASH_AFTER_KEYS_COMMITTED`
6. `CRASH_AFTER_BAGS_COMMITTED`
7. `CRASH_AFTER_ABORT_DECISION`
8. `CRASH_AFTER_KEYS_ABORTED`

## 8. 运行测试

全部测试用**独立子进程**执行 CLI，崩溃与恢复发生在不同进程中；
线性化用例把真实回执与状态同独立的串行参考模型
（`dualreceipt.reference.ReferenceModel`）逐项对照。

```bash
python -m unittest discover -s tests -t . -v
# 隔离模式（忽略环境变量与用户站点包，更严格）
PYTHONPATH=. python -I -m unittest discover -s tests -t .
```

覆盖：8 个命名崩溃边界的子进程崩溃 + 另一进程恢复、恢复进程自身再崩溃、
两客户端争最后一把钥匙、不相交资源的有序等待与锁超时、旧快照、
非法状态审计、串行线性化逐项对照。

## 9. 设计边界与运维约定

* `recover` 会清除 `resource_locks` 中全部残留锁，因此约定**恢复期间不并发
  运行 apply**；正常 apply 只在毫秒级窗口持锁，进程异常退出后的锁由恢复清理。
* 2PC 在提交点与最后一步之间存在经典的"不确定窗口"：本系统把该窗口内的
  状态显式暴露为 `PENDING`，而不是阻塞读者或伪装成已完成。
* 三个 SQLite 文件应位于可靠本地磁盘；`synchronous=FULL` 已保证每次提交
  fsync，但硬件级写缓存丢失不在软件协调器的防护范围内。
