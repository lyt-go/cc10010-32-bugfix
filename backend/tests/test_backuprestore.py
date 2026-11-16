"""数据备份策略与可恢复点的回归测试。

覆盖：策略字段落库、执行边界、停用保留历史、同表并发只留一个生效点、
晚到批次冲突、恢复链失败保留上一份结果、运行实例与策略结果同源。

运行：cd backend && .venv/bin/python -m unittest discover -s tests -v
"""
from __future__ import annotations

import unittest

from app.services.backuprestore import BackuprestoreService
from app.services.runtime import RuntimeService
from app.store import store


class BackupTestCase(unittest.TestCase):
    def setUp(self) -> None:
        store.reset()
        self.policies = BackuprestoreService()
        self.runtime = RuntimeService()

    def enable(self, policy_id: int) -> None:
        entry = self.policies.get_entry(policy_id)
        self.assertIsNotNone(entry)
        if entry["status"] == "草稿":
            _, message = self.policies.run_action(policy_id, "提交策略评审")
            self.assertNotIn("不允许", message)
        entry, message = self.policies.run_action(policy_id, "启用策略")
        if entry is None:
            self.fail(message)

    def backup(self, policy_id: int, **values):
        return self.policies.execute_backup(policy_id, values)


class PolicyFieldTests(BackupTestCase):
    def test_create_persists_all_policy_fields(self) -> None:
        entry, missing = self.policies.create_entry({
            "策略编号": "BACK-9001",
            "数据表名": "dws_sales_summary",
            "备份周期": "每日",
            "保留天数": 12,
            "备份存储位置": "hdfs://dw-backup/x",
            "压缩方式": "zstd",
            "加密开关": "开启",
        })
        self.assertEqual(missing, [])
        self.assertIsNotNone(entry)
        detail = self.policies.get_entry(entry["id"])
        self.assertEqual(detail["保留天数"], 12)
        self.assertEqual(detail["备份存储位置"], "hdfs://dw-backup/x")
        self.assertEqual(detail["压缩方式"], "zstd")
        self.assertEqual(detail["加密开关"], "开启")

    def test_list_and_detail_share_retention_and_time(self) -> None:
        self.enable(3)
        self.backup(3, 备份类型="全量", 备份时间="2026-10-01 01:00:00", 分区数=7)
        rows, _ = self.policies.list_entries(size=200)
        row = next(item for item in rows if item["id"] == 3)
        detail = self.policies.get_entry(3)
        for field in ("保留天数", "最近备份时间", "可恢复点编号", "备份存储位置", "策略版本"):
            self.assertEqual(row[field], detail[field], field)
        self.assertEqual(detail["保留天数"], 7)

    def test_update_keeps_unsubmitted_fields(self) -> None:
        entry, _ = self.policies.update_entry(1, {"保留天数": "45"})
        self.assertEqual(entry["保留天数"], 45)
        self.assertEqual(entry["最近备份时间"], "2026-09-01")

    def test_duplicate_policy_number_reuses_record(self) -> None:
        first, _ = self.policies.create_entry({
            "策略编号": "BACK-9002",
            "数据表名": "dwd_order_detail",
            "备份周期": "每日",
            "保留天数": 9,
        })
        second, _ = self.policies.create_entry({
            "策略编号": "BACK-9002",
            "数据表名": "dwd_order_detail",
            "备份周期": "每周",
        })
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(second["备份周期"], "每周")
        self.assertEqual(second["保留天数"], 9)
        self.assertEqual(len([row for row in store.snapshot("backuprestore") if row["策略编号"] == "BACK-9002"]), 1)


class ExecutionBoundaryTests(BackupTestCase):
    def test_disabled_policy_cannot_start_backup(self) -> None:
        entry, message = self.backup(1, 备份类型="全量")
        self.assertIsNone(entry)
        self.assertIn("未启用", message)
        self.assertEqual(store.snapshot("recoverypoint"), [])

    def test_review_only_policy_is_not_executable(self) -> None:
        self.policies.run_action(2, "提交策略评审")  # 已是评审中，幂等
        entry, message = self.backup(2, 备份类型="全量")
        self.assertIsNone(entry)
        self.assertIn("未启用", message)

    def test_disabled_policy_keeps_history_points(self) -> None:
        self.enable(3)
        self.backup(3, 备份类型="全量", 备份时间="2026-10-01 01:00:00", 分区数=7)
        before = self.policies.get_entry(3)
        self.policies.run_action(3, "停用策略")
        after = self.policies.get_entry(3)
        self.assertEqual(after["最近备份时间"], before["最近备份时间"])
        self.assertEqual(after["可恢复点编号"], before["可恢复点编号"])
        self.assertFalse(after["是否可执行"])
        self.assertEqual(self.policies.recovery_points(policy_number="BACK-0003")[1], 1)
        self.assertNotIn("BACK-0003", [row["策略编号"] for row in self.policies.executable_policies()])

    def test_invalid_action_rejected(self) -> None:
        entry, message = self.policies.run_action(3, "回滚策略")
        self.assertIsNone(entry)
        self.assertIn("不属于数据备份可执行范围", message)

    def test_state_transition_order_enforced(self) -> None:
        entry, message = self.policies.run_action(1, "启用策略")
        self.assertIsNone(entry)
        self.assertIn("不允许", message)


class RecoveryPointTests(BackupTestCase):
    def test_partition_validation_blocks_incomplete_backup(self) -> None:
        self.enable(3)
        entry, message = self.backup(3, 备份类型="全量", 备份时间="2026-10-01 01:00:00", 分区数=3)
        self.assertIsNone(entry)
        self.assertIn("分区数据不完整", message)
        self.assertEqual(self.policies.recovery_points(policy_number="BACK-0003")[1], 0)

    def test_partition_payload_generates_point(self) -> None:
        self.enable(3)
        partitions = [{"分区": f"dt=2026-10-0{day}"} for day in range(1, 8)]
        entry, message = self.backup(3, 备份类型="全量", 备份时间="2026-10-01 01:00:00", 分区数据=partitions)
        self.assertIsNotNone(entry, message)
        point = self.policies.recovery_points(policy_number="BACK-0003")[0][0]
        self.assertEqual(point["分区数"], 7)
        self.assertEqual(point["分区校验"], "通过")
        self.assertEqual(point["保留天数"], 7)
        self.assertEqual(point["备份存储位置"], "hdfs://dw-backup/backuprestore/dws_sales_summary")

    def test_only_one_effective_point_per_table(self) -> None:
        self.enable(3)
        for day in range(1, 5):
            entry, message = self.backup(
                3, 备份类型="全量", 备份时间=f"2026-10-0{day} 01:00:00", 分区数=7
            )
            self.assertIsNotNone(entry, message)
        points, total = self.policies.recovery_points(table="dws_sales_summary")
        self.assertEqual(total, 4)
        self.assertEqual(sum(1 for point in points if point["是否生效"]), 1)
        effective = self.policies.effective_point("dws_sales_summary")
        self.assertEqual(effective["备份时间"], "2026-10-04 01:00:00")
        self.assertEqual(effective["可恢复点编号"], points[0]["可恢复点编号"])

    def test_late_older_increment_does_not_move_latest_time(self) -> None:
        self.enable(3)
        self.backup(3, 备份类型="全量", 备份时间="2026-10-01 01:00:00", 分区数=7)
        self.backup(3, 备份类型="增量", 备份时间="2026-10-03 01:00:00", 分区数=7)
        before = self.policies.get_entry(3)
        entry, message = self.backup(3, 备份类型="增量", 备份时间="2026-10-02 01:00:00", 分区数=7)
        self.assertIsNotNone(entry, message)
        after = self.policies.get_entry(3)
        self.assertEqual(after["最近备份时间"], before["最近备份时间"])
        self.assertEqual(after["可恢复点编号"], before["可恢复点编号"])
        self.assertNotEqual(after["本次可恢复点编号"], after["可恢复点编号"])
        kept = self.policies.recovery_points(policy_number="BACK-0003")[0]
        late = next(point for point in kept if point["可恢复点编号"] == after["本次可恢复点编号"])
        self.assertFalse(late["是否生效"])
        self.assertEqual(late["状态"], "已替代")

    def test_late_newer_full_takes_effect(self) -> None:
        self.enable(3)
        self.backup(3, 备份类型="全量", 备份时间="2026-10-01 01:00:00", 分区数=7)
        self.backup(3, 备份类型="全量", 备份时间="2026-10-05 01:00:00", 分区数=7)
        entry = self.policies.get_entry(3)
        self.assertEqual(entry["最近备份时间"], "2026-10-05 01:00:00")
        self.assertTrue(self.policies.effective_point("dws_sales_summary")["是否生效"])

    def test_replay_same_batch_is_idempotent(self) -> None:
        self.enable(3)
        values = dict(备份类型="全量", 备份时间="2026-10-01 01:00:00", 分区数=7, 批次号="BATCH-FIXED")
        self.backup(3, **values)
        self.backup(3, **values)
        self.assertEqual(self.policies.recovery_points(policy_number="BACK-0003")[1], 1)

    def test_increment_without_full_base_is_rejected_and_keeps_result(self) -> None:
        entry, message = self.policies.create_entry({
            "策略编号": "BACK-9100",
            "数据表名": "ods_no_base",
            "备份周期": "每日",
            "保留天数": 5,
            "备份存储位置": "s3://raw",
        })
        self.assertIsNotNone(entry)
        self.enable(entry["id"])
        result, message = self.backup(entry["id"], 备份类型="增量", 备份时间="2026-10-01 01:00:00")
        self.assertIsNone(result)
        self.assertIn("恢复链校验失败", message)
        self.assertEqual(self.policies.recovery_points(policy_number="BACK-9100")[1], 0)
        detail = self.policies.get_entry(entry["id"])
        self.assertEqual(detail["备份状态"], "未备份")

    def test_failed_backup_keeps_previous_usable_result(self) -> None:
        self.enable(3)
        self.backup(3, 备份类型="全量", 备份时间="2026-10-01 01:00:00", 分区数=7)
        before = self.policies.get_entry(3)
        entry, message = self.backup(3, 备份类型="全量", 备份时间="2026-10-02 01:00:00", 分区数=2)
        self.assertIsNone(entry)
        self.assertIn("分区数据不完整", message)
        after = self.policies.get_entry(3)
        self.assertEqual(after["最近备份时间"], before["最近备份时间"])
        self.assertEqual(after["可恢复点编号"], before["可恢复点编号"])
        self.assertEqual(self.policies.recovery_points(policy_number="BACK-0003")[1], 1)


class RuntimeSyncTests(BackupTestCase):
    def test_runtime_result_matches_recovery_point(self) -> None:
        self.enable(3)
        self.backup(3, 备份类型="全量", 备份时间="2026-10-01 01:00:00", 分区数=7, 运行实例编号=2)
        point = self.policies.effective_point("dws_sales_summary")
        row = self.runtime.get_entry(2)
        self.assertEqual(row["可恢复点编号"], point["可恢复点编号"])
        self.assertEqual(row["最近备份时间"], point["备份时间"])
        self.assertEqual(row["备份结果"], f"成功：{point['可恢复点编号']}")
        self.assertEqual(row["策略版本"], point["策略版本"])

    def test_runtime_read_after_new_backup_updates_result(self) -> None:
        self.enable(3)
        runtime, missing = self.runtime.create_entry({
            "记录编号": "RUNT-9002",
            "作业名称": "dws_sales_summary 备份",
            "开始时间": "2026-10-01 00:30:00",
            "数据表名": "dws_sales_summary",
            "策略编号": "BACK-0003",
        })
        self.assertEqual(missing, [])
        self.backup(3, 备份类型="全量", 备份时间="2026-10-01 01:00:00", 分区数=7)
        first, message = self.runtime.read_recovery_points(runtime["id"])
        self.assertIsNotNone(first, message)
        self.backup(3, 备份类型="增量", 备份时间="2026-10-02 01:00:00", 分区数=7)
        entry, message = self.runtime.read_recovery_points(runtime["id"])
        self.assertIsNotNone(entry, message)
        self.assertEqual(entry["可恢复点编号"], self.policies.effective_point("dws_sales_summary")["可恢复点编号"])
        self.assertNotEqual(entry["可恢复点编号"], first["可恢复点编号"])
        self.assertEqual(entry["最近备份时间"], "2026-10-02 01:00:00")

    def test_runtime_keeps_previous_result_when_no_point(self) -> None:
        entry, missing = self.runtime.create_entry({
            "记录编号": "RUNT-9001",
            "作业名称": "no point job",
            "开始时间": "2026-10-01 00:00:00",
            "数据表名": "ods_unknown",
            "备份结果状态": "成功",
            "可恢复点编号": "RP-OLD-0001",
            "最近备份时间": "2026-09-01 00:00:00",
        })
        self.assertEqual(missing, [])
        current, message = self.runtime.read_recovery_points(entry["id"])
        self.assertIsNotNone(current, message)
        self.assertEqual(current["可恢复点编号"], "RP-OLD-0001")
        self.assertEqual(current["最近备份时间"], "2026-09-01 00:00:00")

    def test_start_backup_only_enabled_tables(self) -> None:
        self.enable(3)
        self.policies.run_action(2, "提交策略评审")
        entry, message = self.runtime.start_backup(3)
        self.assertIsNotNone(entry, message)
        self.assertIn("dws_sales_summary", message)
        self.assertNotIn("dwd_payment_flow", message)
        self.assertNotIn("dwd_order_detail", message)
        points, _ = self.policies.recovery_points()
        self.assertEqual({point["数据表名"] for point in points}, {"dws_sales_summary"})

    def test_start_backup_skips_all_when_nothing_enabled(self) -> None:
        self.policies.run_action(3, "停用策略")
        entry, message = self.runtime.start_backup()
        self.assertIsNone(entry)
        self.assertIn("没有已启用的备份策略", message)
        self.assertEqual(self.policies.recovery_points()[1], 0)


if __name__ == "__main__":
    unittest.main()
