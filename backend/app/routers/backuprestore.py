"""数据备份接口：维护备份策略与可恢复点，覆盖评审、启用、停用和备份执行。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.backuprestore import BackuprestoreService

router = APIRouter(prefix="/api/backuprestore", tags=["数据备份"])

service = BackuprestoreService()

LIST_FIELDS = [
    "策略编号",
    "数据表名",
    "备份周期",
    "保留天数",
    "备份存储位置",
    "压缩方式",
    "加密开关",
    "最近备份时间",
    "策略版本",
    "备份状态",
    "可恢复点编号",
]
STATUSES = ["草稿", "策略评审中", "已启用", "已停用"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按策略编号或数据表名检索"),
    status: str | None = Query(default=None, description="草稿、策略评审中、已启用、已停用"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按策略编号与状态过滤数据备份列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出数据备份清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "backuprestore", "total": total, "items": items}


@router.get("/executable", response_model=PageResult[dict])
def executable_policies(page: int = 1, size: int = 20) -> PageResult[dict]:
    """可执行范围只包含已启用策略：停用策略不会出现在新的备份批次里。"""
    items = service.executable_policies()
    total = len(items)
    start = max(page - 1, 0) * size
    return PageResult(items=items[start:start + size], total=total, page=page, size=size)


@router.get("/recoverypoints", response_model=PageResult[dict])
def list_recovery_points(
    policy_number: str | None = Query(default=None, description="按策略编号过滤"),
    table: str | None = Query(default=None, description="按数据表名过滤"),
    effective: bool | None = Query(default=None, description="只看生效/历史可恢复点"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """读取可恢复点数据记录：同一数据表只有一个生效点，其余保留为历史记录。"""
    items, total = service.recovery_points(
        policy_number=policy_number,
        table=table,
        effective=effective,
        page=page,
        size=size,
    )
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/recoverypoints/effective", response_model=PageResult[dict])
def effective_recovery_points() -> PageResult[dict]:
    """每张数据表当前生效的可恢复点，供运行实例回写备份结果使用。"""
    items = service.effective_points()
    return PageResult(items=items, total=len(items), page=1, size=len(items) or 20)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条备份策略明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"备份策略 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条备份策略：所有策略字段一起落库，缺字段时说明原因。"""
    entry, problems = service.create_entry(payload.values)
    if problems:
        return ActionResult(ok=False, message=f"字段缺失或不合法：{'、'.join(problems)}")
    return ActionResult(ok=True, message="备份策略已登记", entry=entry)


@router.patch("/{entry_id}", response_model=ActionResult)
def update_entry(entry_id: int, payload: EntryPayload) -> ActionResult:
    """修改备份策略字段；保留天数与最近备份时间不会被静默丢弃。"""
    entry, problems = service.update_entry(entry_id, payload.values)
    if entry is None:
        if problems:
            return ActionResult(ok=False, message=f"字段缺失或不合法：{'、'.join(problems)}")
        return ActionResult(ok=False, message=f"备份策略 {entry_id} 不存在或已归档")
    return ActionResult(ok=True, message="备份策略已更新", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """执行策略动作或备份：停用策略不能启动新备份。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action, payload.values)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("/{entry_id}/backups", response_model=ActionResult)
def execute_backup(entry_id: int, payload: EntryPayload) -> ActionResult:
    """启动一次备份并生成可恢复点，返回落库后的策略结果。"""
    entry, message = service.execute_backup(entry_id, payload.values)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
