"""运行实例接口：维护运行记录，覆盖启动作业、终止作业、标记成功等动作。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.runtime import RuntimeService

router = APIRouter(prefix="/api/runtime", tags=["运行实例"])

service = RuntimeService()

LIST_FIELDS = [
    "记录编号",
    "作业名称",
    "开始时间",
    "结束时间",
    "执行状态",
    "读取行数",
    "写入行数",
    "错误信息",
    "数据表名",
    "策略编号",
    "策略版本",
    "备份状态",
    "可恢复点编号",
    "最近备份时间",
]
STATUSES = ["草稿", "运行中", "成功", "失败", "已终止"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按记录编号或作业名称检索"),
    status: str | None = Query(default=None, description="草稿、运行中、成功、失败、已终止"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按记录编号与状态过滤运行实例列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


# 静态路径必须放在 /{entry_id} 之前，否则会被当成整数主键解析。
@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出运行实例清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "runtime", "total": total, "items": items}


@router.get("/backup-results", response_model=dict)
def backup_results() -> dict[str, Any]:
    """运行实例清单里的备份结果汇总：与可恢复点数据记录对得上。"""
    items, total = service.list_entries(page=1, size=10000)
    return {
        "module": "runtime",
        "total": total,
        "items": [row for row in items if row.get("备份结果状态")],
        "effective_points": service.backup_service.effective_points(),
    }


@router.post("/backup-results/sync", response_model=ActionResult)
def sync_backup_results() -> ActionResult:
    """读取可恢复点数据记录后批量更新运行实例的备份结果。"""
    result = service.sync_backup_results()
    return ActionResult(
        ok=True,
        message=str(result.get("message")),
        entry=result.get("entry"),
    )


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条运行记录明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"运行记录 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条运行记录，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="运行记录已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条运行记录执行启动作业、终止作业、标记成功；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
