import { useCallback, useEffect, useRef, useState } from 'react'

export type MetricsSnapshot = {
  total_logs_received?: number
  logs_per_second?: number
  error_rate?: number
  queue_backlog?: number
  rate_limited_requests?: number
  dead_lettered_logs?: number
  write_failures?: number
  logs_by_level?: Record<string, number>
  logs_by_service?: Record<string, number>
  throughput_history?: Array<{ t: string; y: number }>
  error_rate_history?: Array<{ t: string; y: number }>
}

export type ConnectionStatus = 'connecting' | 'connected' | 'error'

// Connects to the server-sent events endpoint and returns the latest metrics snapshot. EventSource reconnects automatically on connection loss.
export function useMetricsEvents() {
  const [snapshot, setSnapshot] = useState<MetricsSnapshot>({})
  const [status, setStatus] = useState<ConnectionStatus>('connecting')
  const [lastUpdated, setLastUpdated] = useState<string | null>(null)

  const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? ''
  const eventsUrl = apiBaseUrl ? `${apiBaseUrl}/metrics/events` : '/metrics/events'

  const initialReceived = useRef(false)

  const handleMessage = useCallback((event: MessageEvent) => {
    try {
      const data = JSON.parse(event.data) as MetricsSnapshot
      setSnapshot(data)
      setStatus('connected')
      if (initialReceived.current) {
        setLastUpdated(new Date().toLocaleTimeString())
      } else {
        initialReceived.current = true
      }
    } catch {
      setStatus('error')
    }
  }, [])

  useEffect(() => {
    const eventSource = new EventSource(eventsUrl)

    eventSource.onopen = () => {
      setStatus('connected')
    }

    eventSource.onerror = () => {
      setStatus('error')
      eventSource.close()
    }

    eventSource.onmessage = handleMessage

    return () => {
      eventSource.close()
    }
  }, [eventsUrl, handleMessage])

  return { snapshot, status, lastUpdated }
}
