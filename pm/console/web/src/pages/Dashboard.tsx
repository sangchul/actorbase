import { useEffect, useState } from 'react'
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { api, NodeStats } from '../api'
import { usePolling } from '../hooks/usePolling'

const NODE_COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899']
const MAX_POINTS = 20

type ChartPoint = Record<string, string | number>

export default function Dashboard() {
  const { data: stats, error } = usePolling(api.getStats)
  const [chartData, setChartData] = useState<ChartPoint[]>([])

  useEffect(() => {
    if (!stats) return
    const now = new Date().toLocaleTimeString()
    const point: ChartPoint = { time: now }
    stats.nodes.forEach(n => {
      point[n.node_id] = Math.round(n.node_rps * 10) / 10
    })
    setChartData(prev => [...prev.slice(-(MAX_POINTS - 1)), point])
  }, [stats])

  const nodeIds = stats?.nodes.map(n => n.node_id) ?? []

  return (
    <div>
      <h1 className="text-2xl font-bold mb-6">Dashboard</h1>

      {error && (
        <div className="mb-4 p-3 bg-red-50 border border-red-200 text-red-700 rounded text-sm">
          {error}
        </div>
      )}

      {/* Node cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mb-6">
        {stats?.nodes.map((node, i) => (
          <NodeCard key={node.node_id} node={node} color={NODE_COLORS[i % NODE_COLORS.length]} />
        ))}
        {!stats && !error && (
          <div className="col-span-3 text-gray-400 text-sm">Loading nodes…</div>
        )}
      </div>

      {/* RPS chart */}
      <div className="bg-white rounded-lg p-4 shadow mb-6">
        <h2 className="text-base font-semibold mb-4">Node RPS (last {MAX_POINTS} samples)</h2>
        <ResponsiveContainer width="100%" height={220}>
          <LineChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
            <XAxis dataKey="time" tick={{ fontSize: 10 }} interval="preserveStartEnd" />
            <YAxis tick={{ fontSize: 11 }} />
            <Tooltip />
            <Legend />
            {nodeIds.map((id, i) => (
              <Line
                key={id}
                type="monotone"
                dataKey={id}
                stroke={NODE_COLORS[i % NODE_COLORS.length]}
                dot={false}
                isAnimationActive={false}
                strokeWidth={2}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Partition distribution table */}
      {stats && stats.nodes.length > 0 && (
        <div className="bg-white rounded-lg p-4 shadow">
          <h2 className="text-base font-semibold mb-3">Partition Distribution</h2>
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-gray-500 border-b">
                <th className="pb-2 font-medium">Node</th>
                <th className="pb-2 font-medium">Address</th>
                <th className="pb-2 font-medium text-right">Partitions</th>
                <th className="pb-2 font-medium text-right">RPS</th>
              </tr>
            </thead>
            <tbody>
              {stats.nodes.map(n => (
                <tr key={n.node_id} className="border-b last:border-0">
                  <td className="py-2 font-medium">{n.node_id}</td>
                  <td className="py-2 text-gray-500">{n.node_addr}</td>
                  <td className="py-2 text-right">{n.partition_count}</td>
                  <td className="py-2 text-right">{n.node_rps.toFixed(1)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

function NodeCard({ node, color }: { node: NodeStats; color: string }) {
  return (
    <div className="bg-white rounded-lg p-4 shadow border-l-4" style={{ borderLeftColor: color }}>
      <div className="font-semibold text-gray-800 mb-1">{node.node_id}</div>
      <div className="text-xs text-gray-400 mb-3">{node.node_addr}</div>
      <div className="flex justify-between text-sm">
        <span className="text-gray-600">
          <span className="font-medium">{node.partition_count}</span> partitions
        </span>
        <span style={{ color }}>
          <span className="font-medium">{node.node_rps.toFixed(1)}</span> RPS
        </span>
      </div>
    </div>
  )
}
