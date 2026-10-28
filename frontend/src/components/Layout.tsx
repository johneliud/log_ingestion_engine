import { useLocation } from 'react-router-dom'
import Sidebar from './Sidebar'
import { Routes, Route } from 'react-router-dom'
import Home from '../pages/Home'
import Dashboard from '../pages/Dashboard'
import Metrics from '../pages/Metrics'

const titles: Record<string, string> = {
  '/': 'Overview',
  '/dashboard': 'Dashboard',
  '/metrics': 'Metrics',
}

export default function Layout() {
  const { pathname } = useLocation()

  return (
    <div className="flex min-h-screen bg-white text-slate-900">
      <Sidebar />
      <main className="flex-1 flex flex-col min-w-0">
        <header className="sticky top-0 z-20 flex items-center justify-between gap-2 border-b border-slate-200 bg-white px-6 py-4 shadow-sm">
          <h1 className="text-2xl font-semibold">{titles[pathname] ?? ''}</h1>
        </header>
        <div className="flex-1 overflow-y-auto p-6 space-y-6">
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/dashboard" element={<Dashboard />} />
            <Route path="/metrics" element={<Metrics />} />
          </Routes>
        </div>
      </main>
    </div>
  )
}
