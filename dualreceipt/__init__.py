"""dualreceipt —— 两本离线安全台账的跨库事务协调器。

三个彼此独立、可单独重启的 SQLite 文件：

* ``keys.db``  一次性钥匙库存（事件溯源）
* ``bags.db``  证物袋交接台账（事件溯源）
* ``coord.db`` 协调日志、跨进程资源锁、终态回执

任意两个文件都不共享数据库事务，协调器用类两阶段提交协议保证
"要么两边都提交、要么两边都终止"，且提交效果对业务读者原子可见。
"""

__version__ = "1.0.0"

from .protocol import CRASH_BOUNDARIES, DELAY_BOUNDARIES  # noqa: F401
