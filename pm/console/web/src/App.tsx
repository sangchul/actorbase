import { BrowserRouter, NavLink, Route, Routes } from 'react-router-dom'
import Dashboard from './pages/Dashboard'
import Routing from './pages/Routing'
import Operations from './pages/Operations'
import Policy from './pages/Policy'

const navItems = [
  { to: '/', label: 'Dashboard' },
  { to: '/routing', label: 'Routing' },
  { to: '/operations', label: 'Operations' },
  { to: '/policy', label: 'Policy' },
]

export default function App() {
  return (
    <BrowserRouter>
      <div className="flex h-screen bg-gray-100">
        <aside className="w-48 bg-gray-900 text-white flex-shrink-0 p-4">
          <h1 className="text-lg font-bold mb-1">Actorbase</h1>
          <p className="text-xs text-gray-400 mb-6">Console</p>
          <nav className="space-y-1">
            {navItems.map(item => (
              <NavLink
                key={item.to}
                to={item.to}
                end={item.to === '/'}
                className={({ isActive }) =>
                  `block px-3 py-2 rounded text-sm transition-colors ${
                    isActive ? 'bg-blue-600 text-white' : 'text-gray-300 hover:bg-gray-700'
                  }`
                }
              >
                {item.label}
              </NavLink>
            ))}
          </nav>
        </aside>
        <main className="flex-1 overflow-auto p-6">
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/routing" element={<Routing />} />
            <Route path="/operations" element={<Operations />} />
            <Route path="/policy" element={<Policy />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  )
}
