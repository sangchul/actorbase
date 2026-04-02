import { useEffect, useRef, useState } from 'react'

export function usePolling<T>(fetcher: () => Promise<T>, interval = 3000) {
  const [data, setData] = useState<T | null>(null)
  const [error, setError] = useState<string | null>(null)
  // Use ref so the effect doesn't re-run when fetcher changes identity.
  const fetcherRef = useRef(fetcher)
  fetcherRef.current = fetcher

  useEffect(() => {
    let active = true
    const run = async () => {
      try {
        const result = await fetcherRef.current()
        if (active) {
          setData(result)
          setError(null)
        }
      } catch (e) {
        if (active) setError(e instanceof Error ? e.message : String(e))
      }
    }
    run()
    const id = setInterval(run, interval)
    return () => {
      active = false
      clearInterval(id)
    }
  }, [interval])

  return { data, error }
}
