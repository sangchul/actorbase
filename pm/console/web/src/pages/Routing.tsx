import { api } from '../api'
import { usePolling } from '../hooks/usePolling'

export default function Routing() {
  const { data: routing, error } = usePolling(api.getRouting)

  return (
    <div>
      <div className="flex items-baseline justify-between mb-4">
        <h1 className="text-2xl font-bold">Routing Table</h1>
        {routing && (
          <span className="text-xs text-gray-400">
            v{routing.version} · {routing.entries.length} partitions · auto-refresh 3 s
          </span>
        )}
      </div>

      {error && (
        <div className="mb-4 p-3 bg-red-50 border border-red-200 text-red-700 rounded text-sm">
          {error}
        </div>
      )}

      <div className="bg-white rounded-lg shadow overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 border-b">
            <tr>
              <th className="px-4 py-3 text-left font-medium text-gray-600">Partition ID</th>
              <th className="px-4 py-3 text-left font-medium text-gray-600">Actor Type</th>
              <th className="px-4 py-3 text-left font-medium text-gray-600">Key Range</th>
              <th className="px-4 py-3 text-left font-medium text-gray-600">Node</th>
              <th className="px-4 py-3 text-left font-medium text-gray-600">Node Address</th>
            </tr>
          </thead>
          <tbody>
            {routing?.entries.map(e => (
              <tr key={e.partition_id} className="border-b last:border-0 hover:bg-gray-50">
                <td className="px-4 py-3 font-mono text-xs text-gray-600" title={e.partition_id}>
                  {e.partition_id.length > 12 ? e.partition_id.slice(0, 12) + '…' : e.partition_id}
                </td>
                <td className="px-4 py-3">
                  <span className="px-2 py-0.5 bg-blue-50 text-blue-700 rounded text-xs font-medium">
                    {e.actor_type}
                  </span>
                </td>
                <td className="px-4 py-3 font-mono text-xs text-gray-600">
                  [{e.key_range_start || '""'}, {e.key_range_end || '""'})
                </td>
                <td className="px-4 py-3 font-medium">{e.node_id}</td>
                <td className="px-4 py-3 text-gray-500">{e.node_address}</td>
              </tr>
            ))}
            {!routing && !error && (
              <tr>
                <td colSpan={5} className="px-4 py-6 text-center text-gray-400">
                  Loading…
                </td>
              </tr>
            )}
            {routing?.entries.length === 0 && (
              <tr>
                <td colSpan={5} className="px-4 py-6 text-center text-gray-400">
                  No partitions
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
