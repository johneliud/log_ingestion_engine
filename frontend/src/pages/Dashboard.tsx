import {
  Line as ChartLine,
  Bar as ChartBar,
} from 'react-chartjs-2'
import 'chartjs-adapter-date-fns'
import { Chart, registerables } from 'chart.js'
import { useMetricsEvents } from '../hooks/useMetricsEvents'

Chart.register(...registerables)

type StatCardProps = {
  label: string
  value: React.ReactNode
}

function StatCard({ label, value }: StatCardProps) {
  return (
    <article className="rounded-xl bg-slate-50 p-4">
      <h2 className="text-sm text-slate-500">{label}</h2>
      <p className="mt-1 text-3xl font-bold">{value}</p>
    </article>
  )
}

export default function Dashboard() {
  const { snapshot, status, lastUpdated } = useMetricsEvents()

  const formatNumber = (value: number | undefined) =>
    value === undefined ? '-' : value.toLocaleString()
  const formatPercent = (value: number | undefined) =>
    value === undefined ? '-' : (value * 100).toFixed(2) + '%'

  // The chart library plots points that carry an x value, so the history points are mapped from their timestamp.
  const toChartPoints = (points?: Array<{ t: string; y: number }>) =>
    (points ?? []).map((point) => ({ x: new Date(point.t), y: point.y }))

  const throughputData = {
    datasets: [
      {
        label: 'Logs per second',
        data: toChartPoints(snapshot.throughput_history),
        borderColor: '#0f172a',
        backgroundColor: 'rgba(15, 23, 42, 0.1)',
        fill: true,
        tension: 0.3,
        pointRadius: 0,
      },
    ],
  }
  const errorRateData = {
    datasets: [
      {
        label: 'Error rate',
        data: toChartPoints(snapshot.error_rate_history),
        borderColor: '#b45309',
        backgroundColor: 'rgba(180, 83, 9, 0.1)',
        fill: true,
        tension: 0.3,
        pointRadius: 0,
      },
    ],
  }
  const topServices = Object.entries(snapshot.logs_by_service ?? {})
    .sort(([, left], [, right]) => right - left)
    .slice(0, 5)
  const topServicesData = {
    labels: topServices.map(([service]) => service),
    datasets: [
      {
        label: 'Logs received',
        data: topServices.map(([, count]) => count),
        backgroundColor: '#0f172a',
      },
    ],
  }
  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { display: false },
      title: { display: false },
    },
    scales: {
      y: { beginAtZero: true, ticks: { font: { size: 11 } } },
      x: { ticks: { font: { size: 11 } } },
    },
  } as const

  const timeChartOptions = {
    ...chartOptions,
    scales: {
      y: { beginAtZero: true, ticks: { font: { size: 11 } } },
      x: { type: 'time' as const, ticks: { font: { size: 11 } } },
    },
  } as const

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
        <StatCard label="Error rate" value={formatPercent(snapshot.error_rate)} />
        <StatCard label="Queue backlog" value={formatNumber(snapshot.queue_backlog)} />
      </section>

      <section className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <article className="rounded-xl bg-slate-50 p-4">
          <h2 className="text-sm text-slate-500">Logs per second over time</h2>
          <div className="mt-2 h-64">
            <ChartLine
              data={throughputData}
              options={timeChartOptions}
              height={320}
              width={640}
            />
          </div>
        </article>
        <article className="rounded-xl bg-slate-50 p-4">
          <h2 className="text-sm text-slate-500">Error rate over time</h2>
          <div className="mt-2 h-64">
            <ChartLine data={errorRateData} options={timeChartOptions} height={320} width={640} />
          </div>
        </article>
        <article className="rounded-xl bg-slate-50 p-4 lg:col-span-2">
          <h2 className="text-sm text-slate-500">Top five services by volume</h2>
          <div className="mt-2 h-64">
            <ChartBar
              data={topServicesData}
              options={chartOptions}
              height={320}
              width={960}
            />
          </div>
          {Object.keys(snapshot.logs_by_service ?? {}).length === 0 && (
            <p className="mt-4 text-sm text-slate-500">
              waiting for the first snapshot
            </p>
          )}
        </article>
      </section>
    </div>
  )
}
