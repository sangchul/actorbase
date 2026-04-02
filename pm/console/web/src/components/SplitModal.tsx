import { useState } from 'react'
import { api, RouteEntry } from '../api'

interface Props {
  entry: RouteEntry
  onClose: () => void
  onSuccess: (newPartitionId: string) => void
}

export default function SplitModal({ entry, onClose, onSuccess }: Props) {
  const [splitKey, setSplitKey] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const handleSubmit = async () => {
    setLoading(true)
    setError(null)
    try {
      const result = await api.split({
        actor_type: entry.actor_type,
        partition_id: entry.partition_id,
        split_key: splitKey,
      })
      onSuccess(result.new_partition_id)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'split failed')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg p-6 w-[420px] shadow-xl">
        <h2 className="text-lg font-semibold mb-4">Split Partition</h2>

        <div className="text-sm space-y-1.5 mb-4 bg-gray-50 rounded p-3">
          <Row label="Partition" value={entry.partition_id} mono />
          <Row label="Actor type" value={entry.actor_type} />
          <Row
            label="Range"
            value={`[${entry.key_range_start || '""'}, ${entry.key_range_end || '""'})`}
            mono
          />
          <Row label="Node" value={entry.node_id} />
        </div>

        <div className="mb-4">
          <label className="block text-sm font-medium mb-1">
            Split key{' '}
            <span className="text-gray-400 font-normal">(empty = midpoint auto)</span>
          </label>
          <input
            type="text"
            value={splitKey}
            onChange={e => setSplitKey(e.target.value)}
            className="w-full border rounded px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-blue-500"
            placeholder="e.g. m"
            autoFocus
            onKeyDown={e => e.key === 'Enter' && handleSubmit()}
          />
        </div>

        {error && (
          <div className="mb-4 p-2 bg-red-50 border border-red-200 text-red-700 rounded text-sm">
            {error}
          </div>
        )}

        <div className="flex gap-2 justify-end">
          <button
            className="px-4 py-2 bg-gray-100 hover:bg-gray-200 rounded text-sm"
            onClick={onClose}
          >
            Cancel
          </button>
          <button
            className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded text-sm disabled:opacity-50"
            onClick={handleSubmit}
            disabled={loading}
          >
            {loading ? 'Splitting…' : 'Split'}
          </button>
        </div>
      </div>
    </div>
  )
}

function Row({
  label,
  value,
  mono = false,
}: {
  label: string
  value: string
  mono?: boolean
}) {
  return (
    <div className="flex gap-2">
      <span className="text-gray-500 w-24 flex-shrink-0">{label}</span>
      <span className={mono ? 'font-mono text-xs break-all' : ''}>{value}</span>
    </div>
  )
}
