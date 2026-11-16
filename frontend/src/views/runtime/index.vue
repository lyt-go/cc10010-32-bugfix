<template>
  <section class="page" data-module="runtime">
    <header class="page-head">
      <div>
        <h2>运行实例管理</h2>
        <p class="page-desc">记录每次ETL作业的运行实例，追踪执行状态和数据吞吐量。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记运行记录</button>
        <button class="btn" type="button" @click="exportRows">导出运行实例清单</button>
        <button class="btn" type="button" @click="syncBackupResults">读取可恢复点</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无运行实例数据，可先登记运行记录</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条运行实例记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/runtime'
const columns = [
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
const actions = ["启动作业", "终止作业", "标记成功", "启动备份", "读取可恢复点"]
const statuses = ["草稿", "运行中", "成功", "失败", "已终止"]
const stats = computed(() => [
  { label: "运行记录总数", value: total.value },
  { label: "备份结果已回写", value: rows.value.filter((row) => row.备份结果状态 === "成功").length },
  { label: "失败记录数", value: rows.value.filter((row) => row.status === "失败").length },
])

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '运行记录登记入口尚未接入审批流'
}

async function syncBackupResults() {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/backup-results/sync`, { method: 'POST' })
    const payload = await response.json()
    if (!payload.ok) {
      throw new Error(payload.message || '可恢复点读取失败')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '可恢复点读取失败'
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ values: { action } }),
    })
    const payload = await response.json()
    if (!payload.ok) {
      throw new Error(payload.message || '运行实例动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '运行实例操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('运行记录列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '运行实例列表读取失败'
  }
}

onMounted(reload)
</script>
