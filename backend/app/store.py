"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。

内存仓库同样要满足事务语义：备份策略、可恢复点、运行实例这些跨模块的写入
要么一起生效，要么一起回滚。`atomic()` 用可重入锁把一段写入串行化，并在异常时
把涉及的模块恢复到写入前的快照。
"""
from __future__ import annotations

import copy
import threading
from contextlib import contextmanager
from typing import Any, Iterator

from app.seed import SEED_ROWS


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 可恢复点是数据备份策略运行后产生的记录，单独一张表存放。
        self._tables.setdefault("recoverypoint", [])
        self._lock = threading.RLock()
        self._normalise()

    def _normalise(self) -> None:
        """给示例数据补齐后加的字段，避免旧数据缺列导致读取方各自兜底。"""
        for row in self._tables.get("backuprestore", []):
            row.setdefault("策略版本", 1)
            row.setdefault("可恢复点编号", None)

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    def module_names(self) -> list[str]:
        with self._lock:
            return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        with self._lock:
            return self._tables.setdefault(module, [])

    def snapshot(self, module: str) -> list[dict[str, Any]]:
        """返回当前模块的深拷贝，供读取方在锁外安全地筛选与排序。"""
        with self._lock:
            return copy.deepcopy(self._tables.get(module, []))

    def reset(self) -> None:
        """把仓库恢复成示例数据，供测试与本地重置使用。"""
        with self._lock:
            self._tables = {
                name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
            }
            self._tables.setdefault("recoverypoint", [])
            self._normalise()

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        with self._lock:
            for row in self.rows(module):
                if int(row.get("id", 0)) == entry_id:
                    return row
            return None

    def next_id(self, module: str) -> int:
        with self._lock:
            return max((int(row.get("id", 0)) for row in self.rows(module)), default=0) + 1

    @contextmanager
    def atomic(self, *modules: str) -> Iterator[None]:
        """把多个模块的写入合并成一个原子操作，任何异常都会整体回滚。"""
        names = modules or tuple(self._tables)
        with self._lock:
            backups = {name: copy.deepcopy(self.rows(name)) for name in names}
            try:
                yield
            except Exception:
                for name, rows in backups.items():
                    live = self.rows(name)
                    live[:] = rows
                raise

    def overview(self) -> dict[str, object]:
        with self._lock:
            modules: list[dict[str, object]] = []
            for name in self.module_names():
                rows = self.rows(name)
                modules.append({
                    "name": name,
                    "created": len(rows),
                    "pending": sum(1 for row in rows if row.get("pending")),
                    "abnormal": sum(1 for row in rows if row.get("abnormal")),
                })
            cards = [
                {"label": "业务模块", "value": len(modules)},
                {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
                {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
                {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
            ]
            return {"cards": cards, "modules": modules}


store = Store()
