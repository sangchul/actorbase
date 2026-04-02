import { api } from '../api'
import { usePolling } from '../hooks/usePolling'

export default function Policy() {
  const { data: policy, error } = usePolling(api.getPolicy)

  return (
    <div>
      <h1 className="text-2xl font-bold mb-6">Policy</h1>

      {error && (
        <div className="mb-4 p-3 bg-red-50 border border-red-200 text-red-700 rounded text-sm">
          {error}
        </div>
      )}

      <div className="bg-white rounded-lg p-6 shadow">
        <div className="flex items-center gap-3 mb-6">
          <span className="text-sm font-medium text-gray-700">AutoPolicy:</span>
          {policy ? (
            <span
              className={`px-2.5 py-0.5 rounded-full text-xs font-semibold ${
                policy.active
                  ? 'bg-green-100 text-green-800'
                  : 'bg-gray-100 text-gray-500'
              }`}
            >
              {policy.active ? 'Active' : 'Inactive'}
            </span>
          ) : (
            <span className="text-gray-400 text-sm">Loading…</span>
          )}
        </div>

        {policy?.yaml ? (
          <div>
            <p className="text-sm font-medium text-gray-700 mb-2">Policy YAML (read-only):</p>
            <pre className="bg-gray-50 border rounded p-4 text-xs font-mono overflow-auto whitespace-pre-wrap">
              {policy.yaml}
            </pre>
          </div>
        ) : (
          policy && (
            <div className="text-sm text-gray-400 italic">
              No policy configured. Use{' '}
              <code className="font-mono bg-gray-100 px-1 rounded">abctl policy apply</code> to
              set an AutoPolicy.
            </div>
          )
        )}
      </div>
    </div>
  )
}
