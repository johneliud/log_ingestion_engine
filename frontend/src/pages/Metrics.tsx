import { useMetricsEvents } from '../hooks/useMetricsEvents'

type StatCardProps = {
  label: string
  value: React.ReactNode
}

function StatCard({ label, value }: StatCardProps) {
  return (
    <article className="rounded-xl bg-slate-50 p-4">
      <h2 className="text-sm text-slate-500">{label}</h2>
      <p className="mt-1 text-2xl font-semibold">{value}</p>
    </article>
  )
}

function KeyValueTable({ title, rows }: { title: string; rows: [string, number][] }) {
  return (
    <article className="rounded-xl bg-slate-50 p-4">
      <h2 className="text-sm text-slate-500">{title}</h2>
      <table className="mt-4 w-full text-left text-sm">
        <thead>
          <tr className="text-slate-500">
            <th className="py-1">{title.split(' ')[0]}</th>
            <th className="py-1 text-right">Logs received</th>
          </tr>
        </thead>
        <tbody>
          {rows.length === 0 ? (
            <tr>
              <td className="py-1">waiting for the first snapshot</td>
              <td className="py-1 text-right">-</td>
            </tr>
          ) : (
            rows.map(([key, value]) => (
              <tr key={key}>
                <td className="py-1 text-slate-900">{key}</td>
                <td className="py-1 text-right text-slate-900">{value.toLocaleString()}</td>
              </tr>
            ))
          )}
        </tbody>
      </table>
    </article>
  )
}

export default function Metrics() {
  const { snapshot, status, lastUpdated } = useMetricsEvents()

  const formatNumber = (value: number | undefined) =>
    value === undefined ? '-' : value.toLocaleString()

  const levelRows = Object.entries(snapshot.logs_by_level ?? {}).sort(
    ([a], [b]) => a.localeCompare(b)
  )
  const serviceRows = Object.entries(snapshot.logs_by_service ?? {}).sort(
    ([, a], [, b]) => b - a
  )

  return (
    <div className="space-y-6">
      <p className="text-sm text-slate-500">
        <span className={`capitalize font-medium ${status === 'connected' ? 'text-green-600' : 'text-slate-400'}`}>
          {status}
        </span>
        {lastUpdated ? ` · Last SSE update: ${lastUpdated}` : ''}
      </p>

      <section className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard label="Total logs received" value={formatNumber(snapshot.total_logs_received)} />
        <StatCard label="Logs per second" value={formatNumber(snapshot.logs_per_second)} />
        <StatCard label="Error rate" value={snapshot.error_rate === undefined ? '-' : snapshot.error_rate.toFixed(2) + '%'} />
        <StatCard label="Queue backlog" value={formatNumber(snapshot.queue_backlog)} />
      </section>

      <section className="grid grid-cols-1 gap-4 sm:grid-cols-3">
        <StatCard label="Rate-limited requests" value={formatNumber(snapshot.rate_limited_requests)} />
        <StatCard label="Dead-lettered logs" value={formatNumber(snapshot.dead_lettered_logs)} />
        <StatCard label="Write failures" value={formatNumber(snapshot.write_failures)} />
      </section>

      <section className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <KeyValueTable title="Logs by level" rows={levelRows} />
        <KeyValueTable title="Logs by service" rows={serviceRows} />
      </section>

      <p className="text-sm text-slate-500">
        The raw snapshot is served as JSON at{' '}
        <a href="/metrics.json" className="font-medium text-slate-900 underline" target="_blank" rel="noopener noreferrer">
          GET /metrics.json
        </a>
        .
      </p>
    </div>
  )
}
