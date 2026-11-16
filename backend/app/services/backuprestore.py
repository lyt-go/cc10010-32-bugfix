"""数据备份业务规则：策略登记、执行边界、可恢复点生成与恢复链校验。

关键约束：

* 备份策略登记/修改时所有策略字段都要落库，不能只留策略编号、数据表名和备份周期；
* 只有「已启用」策略可以启动新备份，停用策略保留历史可恢复点；
* 同一数据表并发/重复备份时，只允许一个可恢复点处于生效状态，其余保留为历史记录；
* 策略状态、备份存储位置、最近备份时间与可恢复点、运行实例结果在同一个事务里写入；
* 可恢复点读取一律走快照，列表、详情与运行实例看到的是同一份结果。
"""
from __future__ import annotations

import copy
from datetime import datetime
from typing import Any, Iterable

from app.store import store

MODULE = "backuprestore"
RECOVERY_MODULE = "recoverypoint"
STATUS_MODULE = "backupstatus"
RUNTIME_MODULE = "runtime"
PARTITION_MODULE = "partitioncheck"

REQUIRED_FIELDS = ["策略编号", "数据表名", "备份周期"]
POLICY_FIELDS = [
    "策略编号",
    "数据表名",
    "备份周期",
    "保留天数",
    "备份存储位置",
    "压缩方式",
    "加密开关",
    "最近备份时间",
]
OPTIONAL_FIELDS = ["分区数据", "分区数", "运行实例编号", "备注"]
STATUS_ORDER = ["草稿", "策略评审中", "已启用", "已停用"]
ACTION_RULES = {"提交策略评审": "策略评审中", "启用策略": "已启用", "停用策略": "已停用"}
NEGATIVE_ACTIONS = ["停用策略"]
BACKUP_ACTIONS = {"执行备份", "启动备份", "开始备份", "生成可恢复点"}
FULL_BACKUP = "全量"
INCREMENTAL_BACKUP = "增量"
BACKUP_TYPES = (FULL_BACKUP, INCREMENTAL_BACKUP)
ACTIVE_STATUS = "生效"
SUPERSEDED_STATUS = "已替代"
BROKEN_STATUS = "恢复链失效"
SUCCESS_BACKUP_STATUS = "备份成功"
IDLE_BACKUP_STATUS = "未备份"
ENABLED_STATUS = "已启用"
DISABLED_STATUS = "已停用"

# 动作允许的起始状态：停用后的策略只能重新启用，不能直接执行备份。
ACTION_SOURCES = {
    "提交策略评审": {"草稿", "策略评审中"},
    "启用策略": {"策略评审中", "已停用", "已启用"},
    "停用策略": {"已启用", "已停用"},
}


class BackupError(Exception):
    """可直接展示给使用方的业务错误。"""


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _parse_time(value: Any) -> datetime | None:
    text = _text(value)
    if not text:
        return None
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d",
    ):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _time_key(value: Any) -> tuple[int, Any]:
    """可比较的时间键：能解析就按时间，解析不了退回原字符串，保证顺序稳定。"""
    parsed = _parse_time(value)
    if parsed is not None:
        return (0, parsed)
    return (1, _text(value))


def _as_count(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    text = _text(value)
    return int(text) if text.isdigit() else None


def _normalise_retention(value: Any) -> int | None:
    count = _as_count(value)
    return count if count is not None and count > 0 else None


def _partition_count(payload: Any) -> int | None:
    """从请求或策略里的分区数据解析分区数量。"""
    if payload is None:
        return None
    if isinstance(payload, dict):
        for key in ("分区数", "分区数量", "实际分区数", "count", "partition_count"):
            count = _as_count(payload.get(key))
            if count is not None:
                return count
        for key in ("分区数据", "分区列表", "partitions", "partition_list"):
            if key in payload:
                return _partition_count(payload[key])
        return None
    if isinstance(payload, (list, tuple, set)):
        return len(payload)
    return _as_count(payload)


def _partition_payload(values: dict[str, Any], policy: dict[str, Any]) -> Any:
    for source in (values, policy):
        for key in ("分区数据", "分区列表", "分区", "实际分区数", "分区数", "分区数量"):
            if source.get(key) not in (None, ""):
                return source.get(key)
    return None


def sort_points(points: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """可恢复点排序：备份时间优先，同一时间全量优先，再按写入顺序兜底。"""
    return sorted(
        points,
        key=lambda row: (
            _time_key(row.get("备份时间")),
            1 if row.get("备份类型") == FULL_BACKUP else 0,
            int(row.get("id", 0)),
        ),
        reverse=True,
    )


class BackuprestoreService:
    # ------------------------------------------------------------------ 查询
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = self._decorated_rows()
        if keyword:
            needle = _text(keyword)
            rows = [
                row
                for row in rows
                if needle in _text(row.get("策略编号")) or needle in _text(row.get("数据表名"))
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
            return self._decorate(entry) if entry else None

    def get_entry_by_number(self, policy_number: str) -> dict[str, Any] | None:
        with store.lock:
            for row in store.rows(MODULE):
                if _text(row.get("策略编号")) == _text(policy_number):
                    return row
            return None

    def recovery_points(
        self,
        *,
        policy_number: str | None = None,
        table: str | None = None,
        effective: bool | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.snapshot(RECOVERY_MODULE)
        if policy_number:
            rows = [row for row in rows if _text(row.get("策略编号")) == _text(policy_number)]
        if table:
            rows = [row for row in rows if _text(row.get("数据表名")) == _text(table)]
        if effective is not None:
            rows = [row for row in rows if bool(row.get("是否生效")) is effective]
        rows = sort_points(rows)
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def effective_points(self) -> list[dict[str, Any]]:
        """每张数据表当前只有一个生效可恢复点。"""
        with store.lock:
            effective = [row for row in store.rows(RECOVERY_MODULE) if row.get("是否生效")]
        return sort_points(copy.deepcopy(effective))

    def effective_point(self, table: str) -> dict[str, Any] | None:
        with store.lock:
            points = [
                row
                for row in store.rows(RECOVERY_MODULE)
                if _text(row.get("数据表名")) == _text(table) and row.get("是否生效")
            ]
            return copy.deepcopy(sort_points(points)[0]) if points else None

    def policy_point(self, policy_number: str) -> dict[str, Any] | None:
        """某条策略自己的最新可用可恢复点，用于运行实例按策略回写结果。"""
        with store.lock:
            point = self._policy_latest_point(_text(policy_number))
            return copy.deepcopy(point) if point else None

    def executable_policies(self) -> list[dict[str, Any]]:
        """可执行范围只包含已启用策略，停用策略不会进入新的备份批次。"""
        return [row for row in self._decorated_rows() if row.get("status") == ENABLED_STATUS]

    # -------------------------------------------------------------- 登记修改
    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not _text(values.get(field))]
        if missing:
            return None, missing
        if _text(values.get("保留天数")) and _normalise_retention(values.get("保留天数")) is None:
            return None, ["保留天数（需为正整数）"]

        policy_number = _text(values.get("策略编号"))
        with store.atomic(MODULE):
            existing = self.get_entry_by_number(policy_number)
            if existing is not None:
                # 策略编号是业务主键：重复登记沿用原有策略记录，不再生成第二条策略。
                self._apply_values(existing, values)
                return self._decorate(existing), []
            entry: dict[str, Any] = {
                "id": store.next_id(MODULE),
                "status": STATUS_ORDER[0],
                "pending": True,
                "abnormal": False,
                "是否可执行": False,
                "策略版本": 1,
                "备份状态": IDLE_BACKUP_STATUS,
                "可恢复点编号": None,
                "最近批次号": None,
                "最近备份类型": None,
            }
            self._apply_values(entry, values, bump_version=False)
            store.rows(MODULE).append(entry)
            return self._decorate(entry), []

    def update_entry(self, entry_id: int, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        with store.atomic(MODULE):
            entry = store.find(MODULE, entry_id)
            if entry is None:
                return None, []
            missing = [
                field
                for field in REQUIRED_FIELDS
                if field in values and not _text(values.get(field))
            ]
            if missing:
                return None, missing
            if "保留天数" in values and _text(values.get("保留天数")):
                if _normalise_retention(values.get("保留天数")) is None:
                    return None, ["保留天数（需为正整数）"]
            self._apply_values(entry, values)
            return self._decorate(entry), []

    def _apply_values(
        self,
        entry: dict[str, Any],
        values: dict[str, Any],
        *,
        bump_version: bool = True,
    ) -> None:
        """把提交上来的策略字段全部写入，未提交的字段保留原值。"""
        material_changed = False
        for field in POLICY_FIELDS:
            if field not in values:
                continue
            raw = values.get(field)
            if field == "保留天数":
                new_value: Any = _normalise_retention(raw) if _text(raw) else None
            else:
                new_value = _text(raw)
            if entry.get(field) != new_value:
                if field != "最近备份时间" and entry.get(field) is not None:
                    material_changed = True
                entry[field] = new_value
        for field in OPTIONAL_FIELDS:
            if field in values:
                entry[field] = values.get(field)
        if entry.get("id") is None:
            # 新记录尚未占用 id，字段赋值不算版本变更。
            material_changed = False
        if material_changed and bump_version:
            entry["策略版本"] = int(entry.get("策略版本", 1)) + 1
        entry.setdefault("策略版本", 1)
        entry.setdefault("备份状态", IDLE_BACKUP_STATUS)
        entry.setdefault("可恢复点编号", None)
        entry["是否可执行"] = entry.get("status") == ENABLED_STATUS

    # ---------------------------------------------------------------- 动作
    def run_action(
        self,
        entry_id: int,
        action: str,
        values: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        action = _text(action)
        if action in BACKUP_ACTIONS:
            return self.execute_backup(entry_id, values or {})

        entry = self._raw_entry(entry_id)
        if entry is None:
            return None, f"备份策略 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于数据备份可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        current = entry.get("status")
        if current == target:
            # 幂等动作：重复点同一个动作不报错，保持原有可恢复点不变。
            return self._decorate(entry), f"备份策略已{action}"
        if current not in ACTION_SOURCES[action]:
            return None, f"策略当前状态「{current}」不允许执行「{action}」"

        with store.atomic(MODULE):
            raw = store.find(MODULE, entry_id)
            if raw is None:
                return None, f"备份策略 {entry_id} 不存在或已归档"
            raw["status"] = target
            raw["pending"] = target in ("草稿", "策略评审中")
            raw["abnormal"] = action in NEGATIVE_ACTIONS
            raw["是否可执行"] = target == ENABLED_STATUS
            # 停用只关闭新的备份入口：可恢复点与最近备份时间保持原样。
            return self._decorate(raw), f"备份策略已{action}"

    # ------------------------------------------------------------ 备份执行
    def execute_backup(
        self,
        entry_id: int,
        values: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        values = dict(values or {})
        with store.lock:
            policy = store.find(MODULE, entry_id)
            if policy is None:
                return None, f"备份策略 {entry_id} 不存在或已归档"
            if policy.get("status") != ENABLED_STATUS:
                return None, f"备份策略 {entry_id} 未启用，停用策略不能启动新的备份"
            return self._execute_backup_locked(policy, values)

    def _execute_backup_locked(
        self,
        policy: dict[str, Any],
        values: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, str]:
        table = _text(policy.get("数据表名"))
        policy_number = _text(policy.get("策略编号"))
        backup_type = _text(values.get("备份类型")) or FULL_BACKUP
        if backup_type not in BACKUP_TYPES:
            return None, f"备份类型「{backup_type}」不在允许范围内，只支持全量或增量"
        backup_time = _text(
            values.get("备份时间")
            or values.get("完成时间")
            or values.get("备份完成时间")
        ) or _now()
        if _parse_time(backup_time) is None:
            return None, f"备份时间「{backup_time}」不是可识别的时间格式"
        storage = _text(values.get("备份存储位置")) or _text(policy.get("备份存储位置"))
        if not storage:
            return None, "备份存储位置为空，无法生成可恢复点"
        retention_count = _normalise_retention(values.get("保留天数", policy.get("保留天数")))
        if retention_count is None:
            return None, "保留天数为空或不是正整数，无法生成可恢复点"

        try:
            partition = self._validate_partitions(policy, values)
        except BackupError as exc:
            return None, str(exc)

        batch = _text(
            values.get("批次号") or values.get("备份批次") or values.get("批次")
        ) or self._next_batch_number(policy_number)
        runtime_id = _as_count(values.get("运行实例编号") or values.get("运行实例ID"))

        with store.atomic(MODULE, RECOVERY_MODULE, STATUS_MODULE, RUNTIME_MODULE):
            before = copy.deepcopy(store.rows(RECOVERY_MODULE))
            point = self._find_point(policy_number, batch, backup_type)
            if point is None:
                point = {
                    "id": store.next_id(RECOVERY_MODULE),
                    "可恢复点编号": self._next_point_number(policy_number),
                    "策略编号": policy_number,
                    "策略版本": int(policy.get("策略版本", 1)),
                    "数据表名": table,
                    "备份类型": backup_type,
                    "批次号": batch,
                    "运行实例编号": runtime_id,
                }
                store.rows(RECOVERY_MODULE).append(point)
            point.update(
                {
                    "备份时间": backup_time,
                    "备份存储位置": storage,
                    "保留天数": retention_count,
                    "分区数据": partition["数据"],
                    "分区数": partition["数量"],
                    "分区校验": partition["校验"],
                    "策略版本": int(policy.get("策略版本", 1)),
                }
            )
            if runtime_id is not None:
                point["运行实例编号"] = runtime_id

            # 每次写入都重新校验整张表的恢复链：晚到的全量可以修复先前失效的增量，
            # 只有本次写入的可恢复点自身不可用时才回滚策略与备份状态，
            # 可恢复点本身保留为失效记录，供核对时看到“为什么没有生效”。
            self._refresh_chain(table)
            executed = self._find_point(policy_number, batch, backup_type)
            assert executed is not None  # 同一事务里刚写入
            if not executed.get("是否可用", True):
                # 只有本次写入的可恢复点失效时回滚可恢复点表，
                # 策略状态、备份存储位置、最近备份时间保持上一份可用结果。
                store.rows(RECOVERY_MODULE)[:] = before
                return (
                    None,
                    f"恢复链校验失败：{executed.get('恢复链状态')}；"
                    "已保留上一份可用可恢复点与最近备份时间",
                )

            effective = self._select_effective(table)
            self._mark_effective(table, effective)
            executed = self._find_point(policy_number, batch, backup_type)
            assert executed is not None

            # 策略状态、存储位置、最近备份时间与可恢复点在同一事务内落库。
            # 最近备份时间以本策略最新可恢复点为准，晚到的旧批次不会把它改回去。
            current = self._apply_result_to_policy(policy, executed)
            self._record_status(policy, executed, current)
            self._sync_runtime(policy, executed, current, runtime_id)

            if executed.get("是否生效"):
                return (
                    self._decorate(policy),
                    f"备份已完成，可恢复点 {executed.get('可恢复点编号')} 已生效",
                )
            effective_number = effective.get("可恢复点编号") if effective else "无"
            return (
                self._decorate(policy),
                f"备份已完成，可恢复点 {executed.get('可恢复点编号')} 保留为历史记录"
                f"（当前生效：{effective_number}）",
            )

    # ------------------------------------------------------------ 分区校验
    def _validate_partitions(
        self,
        policy: dict[str, Any],
        values: dict[str, Any],
    ) -> dict[str, Any]:
        table = _text(policy.get("数据表名"))
        payload = _partition_payload(values, policy)
        actual = _partition_count(payload)
        rules = [
            row
            for row in store.snapshot(PARTITION_MODULE)
            if _text(row.get("数据表名")) == table
        ]
        # 已启用的分区检测规则优先；规则尚未启用时仍参与期望分区数校验，只是不作为唯一依据。
        enabled_rules = [row for row in rules if row.get("status") == ENABLED_STATUS]
        rule = enabled_rules[0] if enabled_rules else (rules[0] if rules else None)
        expected = _as_count(rule.get("期望分区数")) if rule else None
        if actual is None and expected is not None:
            actual = expected
        if rule is not None and actual is not None and expected is not None and actual < expected:
            raise BackupError(
                f"数据表「{table}」分区数据不完整：实际 {actual} 个分区，低于期望 {expected} 个"
            )
        if payload is None and rule is None:
            # 既没有分区检测规则也没有请求侧分区数据时降级为跳过，
            # 不因为环境里尚未登记分区规则就完全阻断备份。
            return {"数据": None, "数量": None, "校验": "跳过（未登记分区数据）"}
        return {
            "数据": payload,
            "数量": actual if actual is not None else expected,
            "校验": "通过",
        }

    # ------------------------------------------------------------ 恢复链
    def _refresh_chain(self, table: str) -> None:
        """重算整张表的恢复链：增量必须能挂到不晚于自身的可用全量点上。"""
        points = [
            row
            for row in store.rows(RECOVERY_MODULE)
            if _text(row.get("数据表名")) == _text(table)
        ]
        fulls = [
            row
            for row in points
            if row.get("备份类型") == FULL_BACKUP and row.get("是否可用", True)
        ]
        for row in points:
            row.setdefault("是否可用", True)
            if row.get("备份类型") == FULL_BACKUP:
                row["恢复链状态"] = "有效"
                row["是否可用"] = True
                row.setdefault("状态", SUPERSEDED_STATUS)
                continue
            anchors = [
                full
                for full in fulls
                if full.get("是否可用", True)
                and _time_key(full.get("备份时间")) <= _time_key(row.get("备份时间"))
            ]
            if not anchors:
                row["恢复链状态"] = "失效：缺少可用的全量基线"
                row["是否可用"] = False
                row["状态"] = BROKEN_STATUS
            else:
                row["恢复链状态"] = "有效"
                row["是否可用"] = True
                if row.get("状态") in (None, BROKEN_STATUS):
                    row["状态"] = SUPERSEDED_STATUS
                row.setdefault("状态", SUPERSEDED_STATUS)

    def _select_effective(self, table: str) -> dict[str, Any] | None:
        candidates = [
            row
            for row in store.rows(RECOVERY_MODULE)
            if _text(row.get("数据表名")) == _text(table) and row.get("是否可用", True)
        ]
        return sort_points(candidates)[0] if candidates else None

    def _mark_effective(self, table: str, effective: dict[str, Any] | None) -> None:
        effective_id = effective.get("id") if effective else None
        for row in store.rows(RECOVERY_MODULE):
            if _text(row.get("数据表名")) != _text(table):
                continue
            if not row.get("是否可用", True):
                row["是否生效"] = False
                continue
            is_effective = effective_id is not None and row.get("id") == effective_id
            row["是否生效"] = is_effective
            if is_effective:
                row["状态"] = ACTIVE_STATUS
            elif row.get("状态") == ACTIVE_STATUS:
                row["状态"] = SUPERSEDED_STATUS

    # ------------------------------------------------------------ 结果同步
    def _apply_result_to_policy(
        self,
        policy: dict[str, Any],
        executed: dict[str, Any],
    ) -> dict[str, Any]:
        """策略状态、备份存储位置、最近备份时间与可恢复点同源落库。

        最近备份时间取本策略自己的最新可用可恢复点，晚到的旧批次不会把它改回去；
        保留天数属于策略配置，不被可恢复点反向覆盖。
        """
        current = self._policy_latest_point(_text(policy.get("策略编号"))) or executed
        policy["备份存储位置"] = current.get("备份存储位置")
        policy["最近备份时间"] = current.get("备份时间")
        policy["可恢复点编号"] = current.get("可恢复点编号")
        policy["最近批次号"] = current.get("批次号")
        policy["最近备份类型"] = current.get("备份类型")
        policy["本次可恢复点编号"] = executed.get("可恢复点编号")
        policy["备份状态"] = SUCCESS_BACKUP_STATUS
        return current

    def _policy_latest_point(self, policy_number: str) -> dict[str, Any] | None:
        points = [
            row
            for row in store.rows(RECOVERY_MODULE)
            if _text(row.get("策略编号")) == policy_number and row.get("是否可用", True)
        ]
        return sort_points(points)[0] if points else None

    def _record_status(
        self,
        policy: dict[str, Any],
        executed: dict[str, Any],
        current: dict[str, Any],
    ) -> None:
        """按策略版本记录业务侧备份状态，运行实例与策略详情从这里取同一份结果。"""
        policy_number = _text(policy.get("策略编号"))
        version = int(policy.get("策略版本", 1))
        rows = store.rows(STATUS_MODULE)
        record = next(
            (
                row
                for row in rows
                if _text(row.get("策略编号")) == policy_number
                and int(row.get("策略版本", 0)) == version
            ),
            None,
        )
        if record is None:
            record = {"id": store.next_id(STATUS_MODULE)}
            rows.append(record)
        record.update(
            {
                "策略编号": policy_number,
                "策略版本": version,
                "数据表名": policy.get("数据表名"),
                "备份状态": policy.get("备份状态"),
                "可恢复点编号": current.get("可恢复点编号"),
                "可恢复点状态": current.get("状态"),
                "本次可恢复点编号": executed.get("可恢复点编号"),
                "最近备份时间": current.get("备份时间"),
                "备份类型": current.get("备份类型"),
                "批次号": current.get("批次号"),
                "备份存储位置": policy.get("备份存储位置"),
                "更新时间": _now(),
            }
        )

    def _sync_runtime(
        self,
        policy: dict[str, Any],
        executed: dict[str, Any],
        current: dict[str, Any],
        runtime_id: int | None,
    ) -> None:
        """把可恢复点结果回写到运行实例清单，两处结果使用同一份可恢复点数据。"""
        rows = store.rows(RUNTIME_MODULE)
        targets: list[dict[str, Any]] = []
        if runtime_id is not None:
            found = store.find(RUNTIME_MODULE, runtime_id)
            if found is not None:
                targets.append(found)
        if not targets:
            policy_number = _text(policy.get("策略编号"))
            targets = [
                row
                for row in rows
                if _text(row.get("策略编号")) == policy_number
            ]
        if not targets:
            # 运行实例清单里还没有这张数据表的备份记录时补一条，保证结果可回看。
            record = {
                "id": store.next_id(RUNTIME_MODULE),
                "status": "成功",
                "pending": False,
                "abnormal": False,
                "记录编号": f"RT-{policy.get('策略编号')}-{int(policy.get('策略版本', 1)):04d}",
                "作业名称": f"{policy.get('数据表名')} 备份",
                "开始时间": executed.get("备份时间"),
                "结束时间": executed.get("备份时间"),
                "执行状态": "成功",
                "读取行数": 0,
                "写入行数": 0,
                "错误信息": "",
            }
            rows.append(record)
            targets.append(record)
        for row in targets:
            row["数据表名"] = policy.get("数据表名")
            row["策略编号"] = policy.get("策略编号")
            row["策略版本"] = int(policy.get("策略版本", 1))
            row["备份状态"] = policy.get("备份状态")
            row["备份结果"] = f"成功：{current.get('可恢复点编号')}"
            row["备份结果状态"] = "成功"
            row["可恢复点编号"] = current.get("可恢复点编号")
            row["本次可恢复点编号"] = executed.get("可恢复点编号")
            row["最近备份时间"] = current.get("备份时间")
            row["备份类型"] = current.get("备份类型")
            row["批次号"] = current.get("批次号")
            row["执行状态"] = "成功"
            row["status"] = "成功"
            row["pending"] = False

    # ------------------------------------------------------------ 小工具
    def _raw_entry(self, entry_id: int) -> dict[str, Any] | None:
        with store.lock:
            entry = store.find(MODULE, entry_id)
            return copy.deepcopy(entry) if entry else None

    def _decorated_rows(self) -> list[dict[str, Any]]:
        with store.lock:
            rows = copy.deepcopy(store.rows(MODULE))
            points = copy.deepcopy(store.rows(RECOVERY_MODULE))
        return [self._decorate(row, points) for row in rows]

    def _decorate(
        self,
        entry: dict[str, Any],
        points: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """返回副本并补齐详情口径，列表与详情共用同一套字段来源。"""
        row = dict(entry)
        for field in POLICY_FIELDS:
            row.setdefault(field, None)
        row.setdefault("策略版本", 1)
        row.setdefault("备份状态", IDLE_BACKUP_STATUS)
        row.setdefault("可恢复点编号", None)
        row["是否可执行"] = row.get("status") == ENABLED_STATUS

        if points is None:
            with store.lock:
                points = copy.deepcopy(store.rows(RECOVERY_MODULE))
        policy_number = _text(row.get("策略编号"))
        own_points = [
            item
            for item in points
            if _text(item.get("策略编号")) == policy_number and item.get("是否可用", True)
        ]
        if own_points:
            current = sort_points(own_points)[0]
            row["可恢复点编号"] = current.get("可恢复点编号")
            row["最近备份时间"] = current.get("备份时间")
            row["最近备份类型"] = current.get("备份类型")
            row["最近批次号"] = current.get("批次号")
        table_points = [
            item
            for item in points
            if _text(item.get("数据表名")) == _text(row.get("数据表名")) and item.get("是否生效")
        ]
        if table_points:
            table_current = sort_points(table_points)[0]
            row["数据表可恢复点编号"] = table_current.get("可恢复点编号")
            row["数据表最近备份时间"] = table_current.get("备份时间")

        normalised = _normalise_retention(row.get("保留天数"))
        if normalised is not None:
            row["保留天数"] = normalised
        return row

    def _find_point(
        self,
        policy_number: str,
        batch: str,
        backup_type: str,
    ) -> dict[str, Any] | None:
        for row in store.rows(RECOVERY_MODULE):
            if (
                _text(row.get("策略编号")) == policy_number
                and _text(row.get("批次号")) == batch
                and row.get("备份类型") == backup_type
            ):
                return row
        return None

    def _next_point_number(self, policy_number: str) -> str:
        rows = store.rows(RECOVERY_MODULE)
        seq = len(rows) + 1
        existing = {_text(row.get("可恢复点编号")) for row in rows}
        while f"RP-{policy_number}-{seq:04d}" in existing:
            seq += 1
        return f"RP-{policy_number}-{seq:04d}"

    def _next_batch_number(self, policy_number: str) -> str:
        rows = [
            row
            for row in store.rows(RECOVERY_MODULE)
            if _text(row.get("策略编号")) == policy_number
        ]
        return f"BATCH-{policy_number}-{len(rows) + 1:04d}"
