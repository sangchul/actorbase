export interface Member {
  node_id: string
  address: string
  status: string
}

export interface RouteEntry {
  partition_id: string
  actor_type: string
  key_range_start: string
  key_range_end: string
  node_id: string
  node_address: string
}

export interface RoutingTable {
  version: number
  entries: RouteEntry[]
}

export interface PartitionStats {
  partition_id: string
  actor_type: string
  key_count: number
  rps: number
}

export interface NodeStats {
  node_id: string
  node_addr: string
  node_rps: number
  partition_count: number
  partitions: PartitionStats[]
}

export interface ClusterStats {
  nodes: NodeStats[]
}

export interface PolicyInfo {
  active: boolean
  yaml: string
}

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const r = await fetch(path, options)
  const data = await r.json()
  if (!r.ok) throw new Error((data as { error?: string }).error ?? 'request failed')
  return data as T
}

export const api = {
  getMembers: () => request<Member[]>('/api/members'),
  getRouting: () => request<RoutingTable>('/api/routing'),
  getStats: () => request<ClusterStats>('/api/stats'),
  getPolicy: () => request<PolicyInfo>('/api/policy'),

  split: (body: { actor_type: string; partition_id: string; split_key: string }) =>
    request<{ new_partition_id: string }>('/api/split', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    }),

  migrate: (body: { actor_type: string; partition_id: string; target_node_id: string }) =>
    request<{ status: string }>('/api/migrate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    }),
}
