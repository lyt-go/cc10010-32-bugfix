<template>
  <section class="page" data-module="backuprestore">
    <header class="page-head">
      <div>
        <h2>数据备份管理</h2>
        <p class="page-desc">管理ETL产出数据的备份策略，配置备份周期和保留规则，并核对可恢复点。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记备份策略</button>
        <button class="btn" type="button" @click="exportRows">导出数据备份清单</button>
        <button class="btn" type="button" @click="reload">刷新</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form v-if="showForm" class="filter-bar form-panel" @submit.prevent="submitCreate">
      <label v-for="field in formFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="form[field]" :placeholder="`请输入${field}`" />
      </label>
      <button class="btn primary" type="submit">保存策略</button>
      <button class="btn ghost" type="button" @click="showForm = false">取消</button>
    </form>

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
          <td :colspan="columns.length + 1" class="empty-state">暂无数据备份数据，可先登记备份策略</td>
        </tr>
      </tbody>
    </table>

    <section class="page-section">
      <header class="section-head">
        <h3>可恢复点</h3>
        <span class="section-desc">同一数据表只保留一个生效可恢复点，历史点可核对</span>
      </header>
      <table class="data-table">
        <thead>
          <tr>
            <th v-for="column in pointColumns" :key="column">{{ column }}</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="point in points" :key="String(point.id)">
            <td v-for="column in pointColumns" :key="column">{{ point[column] ?? '—' }}</td>
          </tr>
          <tr v-if="!points.length">
            <td :colspan="pointColumns.length" class="empty-state">暂无可恢复点，启用策略后可执行备份生成</td>
          </tr>
        </tbody>
      </table>
    </section>

    <footer class="page-foot">
      <span>共 {{ total }} 条数据备份记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/backuprestore'
const columns = [
  '策略编号',
  '数据表名',
  '备份周期',
  '保留天数',
  '备份存储位置',
  '压缩方式',
  '加密开关',
  '最近备份时间',
  '策略版本',
  '备份状态',
  '可恢复点编号',
]
const pointColumns = ['可恢复点编号', '策略编号', '数据表名', '备份类型', '批次号', '备份时间', '状态', '分区校验']
const actions = ['提交策略评审', '启用策略', '停用策略', '执行备份']
const formFields = ['策略编号', '数据表名', '备份周期', '保留天数', '备份存储位置', '压缩方式', '加密开关']

const rows = ref<Row[]>([])
const points = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const showForm = ref(false)
const filters = ref<Record<string, string>>({})
const form = reactive<Record<string, string>>({})
const filterFields = ['策略编号', '数据表名']

const stats = computed(() => [
  { label: '备份策略总数', value: total.value },
  { label: '已启用策略', value: rows.value.filter((row) => row.status === '已启用').length },
  { label: '评审中策略', value: rows.value.filter((row) => row.status === '策略评审中').length },
])

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = ''
  showForm.value = true
}

async function submitCreate() {
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST',
      body: JSON.stringify({ values: { ...form } }),
    })
    const payload = await response.json()
    if (!payload.ok) {
      throw new Error(payload.message || '备份策略登记失败')
    }
    showForm.value = false
    Object.keys(form).forEach((key) => delete form[key])
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '备份策略登记失败'
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
      throw new Error(payload.message || '数据备份动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '数据备份操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const [listResponse, pointResponse] = await Promise.all([
      request(`${ENDPOINT}?${query}`),
      request(`${ENDPOINT}/recoverypoints?size=200`),
    ])
    if (!listResponse.ok) {
      throw new Error('备份策略列表读取失败')
    }
    const payload = await listResponse.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
    if (pointResponse.ok) {
      const pointPayload = await pointResponse.json()
      points.value = pointPayload.items ?? []
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '数据备份列表读取失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.page-section { margin-top: 18px; }
.section-head { display: flex; align-items: baseline; gap: 10px; }
.section-head h3 { margin: 0 0 8px; font-size: 15px; }
.section-desc { color: var(--muted); font-size: 12px; }
.form-panel { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 10px 12px; }
</style>
