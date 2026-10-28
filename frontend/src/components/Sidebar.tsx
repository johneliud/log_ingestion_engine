import { NavLink } from 'react-router-dom'

export default function Sidebar() {
  const links = [
    { to: '/', label: 'Home' },
    { to: '/dashboard', label: 'Dashboard' },
    { to: '/metrics', label: 'Metrics' },
  ]

  return (
    <aside className="sticky top-0 flex h-screen w-64 shrink-0 flex-col border-r border-slate-200 p-6">
      <a href="/" className="text-base font-semibold">
        Log ingestion engine
      </a>
      <p className="mt-1 text-xs text-slate-500">
        HTTP log ingestion, queuing, storage and metrics
      </p>
      <nav className="mt-6 flex flex-col gap-1 text-sm">
        {links.map((link) => (
          <NavLink
            key={link.label}
            to={link.to}
            className={({ isActive }: { isActive: boolean }) =>
              isActive
                ? 'rounded-md bg-slate-100 px-3 py-2 font-medium text-slate-900'
                : 'rounded-md px-3 py-2 text-slate-500 hover:bg-slate-50 hover:text-slate-900'
            }
          >
            {link.label}
          </NavLink>
        ))}
      </nav>
    </aside>
  )
}
