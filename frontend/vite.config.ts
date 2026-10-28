import react from '@vitejs/plugin-react'
import { defineConfig, loadEnv } from 'vite'
import tailwindcss from '@tailwindcss/vite'

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
  const apiTarget = loadEnv(mode, process.cwd(), 'VITE_').VITE_API_BASE_URL || 'http://localhost:8080'
  return {
    plugins: [react(), tailwindcss()],
    server: {
      proxy: {
        // The api paths are listed one by one so the prefix rule does not swallow the metrics page route.
        '/metrics.json': apiTarget,
        '/metrics/events': apiTarget,
        '/logs': apiTarget,
      },
    },
  }
})
