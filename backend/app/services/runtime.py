"""运行实例业务规则：状态流转、备份结果回写与筛选口径都收在这里。"""
from __future__ import annotations

import copy
from typing import Any

from app.services.backuprestore import BackuprestoreService
from app.store import store

MODULE = "runtime"
REQUIRED_FIELDS = ["记录编号", "作业名称", "开始时间"]
STATUS_ORDER = ["草稿", "运行中", "成功", "失败", "已终止"]
ACTION_RULES = {"启动作业": "运行中", "终止作业": "已终止", "标记成功": "成功"}
NEGATIVE_ACTIONS = []

BACKUP_FIELDS = [
    "数据表名",
    "策略编号",
    "策略版本",
    "备份状态",
    "备份结果",
    "备份结果状态",
    "可恢复点编号",
    "本次可恢复点编号",
    "最近备份时间",
    "备份类型",
    "批次号",
]
READ_RECOVERY_ACTIONS = {"读取可恢复点", "刷新备份结果", "同步备份结果"}
START_BACKUP_ACTIONS = {"启动备份", "执行备份"}


class RuntimeService:
    def __init__(self) -> None:
        self.backup_service = BackuprestoreService()

    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.snapshot(MODULE)
        if keyword:
            needle = str(keyword)
            rows = [
                row
                for row in rows
                if needle in str(row.get("记录编号", "")) or needle in str(row.get("作业名称", ""))
            ]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        rows.sort(key=lambda row: int(row.get("id", 0)))
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        with store.lock:
            entry = store.find(MODULE, entry_id)
            return copy.deepcopy(entry) if entry else None

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        with store.atomic(MODULE):
            entry = {"id": store.next_id(MODULE)}
            entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
            entry["status"] = STATUS_ORDER[0]
            entry["pending"] = True
            entry["abnormal"] = False
            for field in BACKUP_FIELDS:
                if field in values:
                    entry[field] = values.get(field)
            store.rows(MODULE).append(entry)
            return copy.deepcopy(entry), []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        action = str(action or "").strip()
        if action in READ_RECOVERY_ACTIONS:
            return self.read_recovery_points(entry_id)
        if action in START_BACKUP_ACTIONS:
            return self.start_backup(entry_id)

        with store.atomic(MODULE):
            entry = store.find(MODULE, entry_id)
            if entry is None:
                return None, f"运行记录 {entry_id} 不存在或已归档"
            if action not in ACTION_RULES:
                return None, f"动作「{action}」不属于运行实例可执行范围"
            target = ACTION_RULES[action]
            if target not in STATUS_ORDER:
                return None, f"目标状态「{target}」不在允许的状态序列里"
            entry["status"] = target
            entry["pending"] = target != STATUS_ORDER[-1]
            entry["abnormal"] = action in NEGATIVE_ACTIONS
            return copy.deepcopy(entry), f"运行记录已{action}"

    # ------------------------------------------------------------ 备份结果
    def read_recovery_points(self, entry_id: int | None = None) -> tuple[dict[str, Any] | None, str]:
        """运行实例读取可恢复点数据记录，并据此更新备份结果。

        写入时以可恢复点和按策略版本落地的备份状态为同一来源：备份状态、可恢复点编号、
        最近备份时间三者要么一起更新，要么保留上一份可用结果。
        """
        with store.atomic(MODULE):
            targets = self._target_rows(entry_id)
            if entry_id is not None and not targets:
                return None, f"运行记录 {entry_id} 不存在或已归档"
            updated = 0
            kept = 0
            for row in targets:
                point = self._point_for(row)
                if point is None:
                    # 没有可用的可恢复点时保留上一份可用结果，不清空已有成功记录。
                    if row.get("备份结果状态") == "成功":
                        kept += 1
                        continue
                    row.setdefault("备份结果状态", "未备份")
                    continue
                status_record = self._status_record(
                    str(point.get("策略编号") or ""),
                    int(point.get("策略版本", 1)),
                )
                self._apply_point(row, point, status_record)
                updated += 1
            message = f"已读取可恢复点并更新 {updated} 条备份结果"
            if kept:
                message += f"，{kept} 条无可用可恢复点保留上一份结果"
            return (copy.deepcopy(targets[0]) if targets else None), message

    def _point_for(self, row: dict[str, Any]) -> dict[str, Any] | None:
        """按运行实例绑定的策略/数据表找到对应的可恢复点。

        运行实例记录了策略编号时优先用该策略自己的可恢复点，避免同表多策略时
        串到别人的恢复结果；只关联数据表时取该表当前生效点。
        """
        policy_number = str(row.get("策略编号") or "").strip()
        if policy_number:
            point = self.backup_service.policy_point(policy_number)
            if point is not None:
                return point
        table = str(row.get("数据表名") or "").strip()
        return self.backup_service.effective_point(table) if table else None

    def sync_backup_results(self) -> dict[str, Any]:
        """批量回写：可恢复点结果同步到运行实例清单。"""
        with store.atomic(MODULE):
            result, message = self.read_recovery_points(None)
            rows = store.snapshot(MODULE)
        return {
            "ok": True,
            "message": message,
            "updated": sum(1 for row in rows if row.get("备份结果状态") == "成功"),
            "entry": copy.deepcopy(result) if result else None,
        }

    def start_backup(self, entry_id: int | None = None) -> tuple[dict[str, Any] | None, str]:
        """只启动已启用数据表的备份，停用策略不会被运行实例拉起。"""
        policies = self.backup_service.executable_policies()
        if not policies:
            return None, "没有已启用的备份策略，未启动任何备份"

        with store.lock:
            runtime = store.find(MODULE, entry_id) if entry_id is not None else None
            if entry_id is not None and runtime is None:
                return None, f"运行记录 {entry_id} 不存在或已归档"
            target_policy = str(runtime.get("策略编号") or "") if runtime else ""
            target_table = str(runtime.get("数据表名") or "") if runtime else ""

            started: list[str] = []
            skipped: list[str] = []
            for policy in policies:
                if target_policy and str(policy.get("策略编号")) != target_policy:
                    continue
                if not target_policy and target_table and str(policy.get("数据表名")) != target_table:
                    continue
                raw = store.find("backuprestore", int(policy.get("id", 0)))
                if raw is None or raw.get("status") != "已启用":
                    skipped.append(str(policy.get("数据表名")))
                    continue
                result, message = self.backup_service.execute_backup(int(raw["id"]), {})
                if result is None:
                    skipped.append(f"{raw.get('数据表名')}（{message}）")
                elif str(raw.get("数据表名")) not in started:
                    started.append(str(raw.get("数据表名")))

        if not started:
            detail = "；".join(skipped) if skipped else "没有匹配的数据表"
            return None, f"未启动任何备份：{detail}"
        with store.atomic(MODULE):
            self.read_recovery_points(entry_id)
        message = f"已启动 {len(started)} 张已启用数据表的备份：{'、'.join(started)}"
        if skipped:
            message += f"；跳过：{'；'.join(skipped)}"
        with store.lock:
            current = store.find(MODULE, entry_id) if entry_id is not None else None
            return (copy.deepcopy(current) if current else None), message

    # ------------------------------------------------------------ 内部工具
    def _target_rows(self, entry_id: int | None) -> list[dict[str, Any]]:
        rows = store.rows(MODULE)
        if entry_id is not None:
            found = store.find(MODULE, entry_id)
            return [found] if found is not None else []
        return [
            row
            for row in rows
            if str(row.get("策略编号") or "").strip() or str(row.get("数据表名") or "").strip()
        ]

    def _status_record(self, policy_number: str, version: int) -> dict[str, Any] | None:
        rows = store.rows("backupstatus")
        for row in rows:
            if (
                str(row.get("策略编号") or "") == policy_number
                and int(row.get("策略版本", 0)) == version
            ):
                return row
        return None

    def _apply_point(
        self,
        row: dict[str, Any],
        point: dict[str, Any],
        status_record: dict[str, Any] | None,
    ) -> None:
        policy_number = str(point.get("策略编号") or "")
        version = int(point.get("策略版本", 1))
        row["数据表名"] = point.get("数据表名")
        row["策略编号"] = policy_number
        row["策略版本"] = version
        row["备份状态"] = (
            status_record.get("备份状态") if status_record else "备份成功"
        )
        row["备份结果状态"] = "成功"
        row["备份结果"] = f"成功：{point.get('可恢复点编号')}"
        row["可恢复点编号"] = point.get("可恢复点编号")
        row["本次可恢复点编号"] = point.get("可恢复点编号")
        row["最近备份时间"] = point.get("备份时间")
        row["备份类型"] = point.get("备份类型")
        row["批次号"] = point.get("批次号")
        row["执行状态"] = "成功"
        row["status"] = "成功"
        row["pending"] = False
