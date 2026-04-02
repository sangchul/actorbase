import { DndContext, DragEndEvent, useDraggable, useDroppable } from '@dnd-kit/core'
import { useState } from 'react'
import { api, RouteEntry } from '../api'
import SplitModal from '../components/SplitModal'
import { usePolling } from '../hooks/usePolling'

export default function Operations() {
  const [tab, setTab] = useState<'split' | 'migrate'>('split')
  const { data: routing } = usePolling(api.getRouting)
  const { data: stats } = usePolling(api.getStats)
  const [splitEntry, setSplitEntry] = useState<RouteEntry | null>(null)
  const [migrateConfirm, setMigrateConfirm] = useState<{
    entry: RouteEntry
    targetNodeId: string
  } | null>(null)
  const [toast, setToast] = useState<{ type: 'ok' | 'error'; msg: string } | null>(null)

  const showToast = (type: 'ok' | 'error', msg: string) => {
    setToast({ type, msg })
    setTimeout(() => setToast(null), 4000)
  }

  // Group entries by node for the migrate DnD view.
  const partitionsByNode: Record<string, RouteEntry[]> = {}
  routing?.entries.forEach(e => {
    ;(partitionsByNode[e.node_id] ??= []).push(e)
  })
  // Include nodes that are registered but have no partitions yet.
  stats?.nodes.forEach(n => {
    partitionsByNode[n.node_id] ??= []
  })
  const nodeIds = Object.keys(partitionsByNode).sort()

  const handleDragEnd = (event: DragEndEvent) => {
    const { active, over } = event
    if (!over) return
    const entry = routing?.entries.find(e => e.partition_id === active.id)
    if (!entry) return
    const targetNodeId = over.id as string
    if (targetNodeId === entry.node_id) return
    setMigrateConfirm({ entry, targetNodeId })
  }

  const confirmMigrate = async () => {
    if (!migrateConfirm) return
    const { entry, targetNodeId } = migrateConfirm
    setMigrateConfirm(null)
    try {
      await api.migrate({
        actor_type: entry.actor_type,
        partition_id: entry.partition_id,
        target_node_id: targetNodeId,
      })
      showToast('ok', `Migrate of ${entry.partition_id.slice(0, 8)}… to ${targetNodeId} initiated`)
    } catch (e) {
      showToast('error', e instanceof Error ? e.message : 'migrate failed')
    }
  }

  return (
    <div>
      <h1 className="text-2xl font-bold mb-6">Operations</h1>

      {toast && (
        <div
          className={`mb-4 p-3 rounded text-sm flex justify-between items-center ${
            toast.type === 'ok'
              ? 'bg-green-50 border border-green-200 text-green-800'
              : 'bg-red-50 border border-red-200 text-red-700'
          }`}
        >
          <span>{toast.msg}</span>
          <button className="ml-4 font-bold text-lg leading-none" onClick={() => setToast(null)}>
            ×
          </button>
        </div>
      )}

      {/* Tab buttons */}
      <div className="flex gap-2 mb-6">
        {(['split', 'migrate'] as const).map(t => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`px-4 py-2 rounded capitalize text-sm font-medium ${
              tab === t ? 'bg-blue-600 text-white' : 'bg-white text-gray-700 hover:bg-gray-50 shadow-sm'
            }`}
          >
            {t}
          </button>
        ))}
      </div>

      {/* Split tab */}
      {tab === 'split' && (
        <div className="bg-white rounded-lg shadow overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 border-b">
              <tr>
                <th className="px-4 py-3 text-left font-medium text-gray-600">Partition ID</th>
                <th className="px-4 py-3 text-left font-medium text-gray-600">Actor Type</th>
                <th className="px-4 py-3 text-left font-medium text-gray-600">Key Range</th>
                <th className="px-4 py-3 text-left font-medium text-gray-600">Node</th>
                <th className="px-4 py-3" />
              </tr>
            </thead>
            <tbody>
              {routing?.entries.map(e => (
                <tr key={e.partition_id} className="border-b last:border-0">
                  <td className="px-4 py-3 font-mono text-xs" title={e.partition_id}>
                    {e.partition_id.slice(0, 12)}…
                  </td>
                  <td className="px-4 py-3">
                    <span className="px-2 py-0.5 bg-blue-50 text-blue-700 rounded text-xs">
                      {e.actor_type}
                    </span>
                  </td>
                  <td className="px-4 py-3 font-mono text-xs">
                    [{e.key_range_start || '""'}, {e.key_range_end || '""'})
                  </td>
                  <td className="px-4 py-3">{e.node_id}</td>
                  <td className="px-4 py-3 text-right">
                    <button
                      className="px-3 py-1 bg-blue-600 hover:bg-blue-700 text-white rounded text-xs"
                      onClick={() => setSplitEntry(e)}
                    >
                      Split
                    </button>
                  </td>
                </tr>
              ))}
              {!routing && (
                <tr>
                  <td colSpan={5} className="px-4 py-6 text-center text-gray-400">
                    Loading…
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}

      {/* Migrate tab — DnD */}
      {tab === 'migrate' && (
        <>
          <p className="text-sm text-gray-500 mb-4">
            Drag a partition card and drop it onto the target node area.
          </p>
          <DndContext onDragEnd={handleDragEnd}>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {nodeIds.map(nodeId => (
                <NodeDropArea
                  key={nodeId}
                  nodeId={nodeId}
                  partitions={partitionsByNode[nodeId] ?? []}
                />
              ))}
            </div>
          </DndContext>
          {nodeIds.length === 0 && (
            <div className="text-gray-400 text-sm">No nodes found</div>
          )}
        </>
      )}

      {/* Split modal */}
      {splitEntry && (
        <SplitModal
          entry={splitEntry}
          onClose={() => setSplitEntry(null)}
          onSuccess={newId => {
            setSplitEntry(null)
            showToast('ok', `Split initiated → new partition ${newId.slice(0, 8)}…`)
          }}
        />
      )}

      {/* Migrate confirm modal */}
      {migrateConfirm && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg p-6 w-96 shadow-xl">
            <h2 className="text-lg font-semibold mb-4">Confirm Migrate</h2>
            <p className="text-sm mb-2">
              Move partition{' '}
              <span className="font-mono bg-gray-100 px-1 rounded">
                {migrateConfirm.entry.partition_id.slice(0, 12)}…
              </span>
            </p>
            <p className="text-sm mb-1">
              From: <strong>{migrateConfirm.entry.node_id}</strong>
            </p>
            <p className="text-sm mb-4">
              To: <strong>{migrateConfirm.targetNodeId}</strong>
            </p>
            <div className="flex gap-2 justify-end">
              <button
                className="px-4 py-2 bg-gray-100 hover:bg-gray-200 rounded text-sm"
                onClick={() => setMigrateConfirm(null)}
              >
                Cancel
              </button>
              <button
                className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded text-sm"
                onClick={confirmMigrate}
              >
                Confirm
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

function NodeDropArea({
  nodeId,
  partitions,
}: {
  nodeId: string
  partitions: RouteEntry[]
}) {
  const { isOver, setNodeRef } = useDroppable({ id: nodeId })
  return (
    <div
      ref={setNodeRef}
      className={`bg-white rounded-lg p-4 shadow min-h-32 transition-colors ${
        isOver ? 'ring-2 ring-blue-400 bg-blue-50' : ''
      }`}
    >
      <h3 className="font-semibold text-sm mb-3 text-gray-700">{nodeId}</h3>
      <div className="space-y-2">
        {partitions.map(p => (
          <DraggablePartition key={p.partition_id} entry={p} />
        ))}
        {partitions.length === 0 && (
          <div className="text-xs text-gray-300 italic text-center py-4">drop here</div>
        )}
      </div>
    </div>
  )
}

function DraggablePartition({ entry }: { entry: RouteEntry }) {
  const { attributes, listeners, setNodeRef, isDragging } = useDraggable({
    id: entry.partition_id,
  })
  return (
    <div
      ref={setNodeRef}
      {...listeners}
      {...attributes}
      className={`p-2 bg-blue-50 border border-blue-200 rounded cursor-grab text-xs select-none transition-opacity ${
        isDragging ? 'opacity-40' : 'hover:bg-blue-100'
      }`}
    >
      <div className="font-mono text-gray-700" title={entry.partition_id}>
        {entry.partition_id.slice(0, 12)}…
      </div>
      <div className="text-gray-500 mt-0.5">
        {entry.actor_type} · [{entry.key_range_start || '""'}, {entry.key_range_end || '""'})
      </div>
    </div>
  )
}
