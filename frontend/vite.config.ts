import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'
import tailwindcss from '@tailwindcss/vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    proxy: {
      // The api paths are listed one by one so the prefix rule does not swallow the metrics page route.
      '/metrics.json': import.meta.env?.VITE_API_BASE_URL ?? 'http://localhost:8080',
      '/metrics/events': import.meta.env?.VITE_API_BASE_URL ?? 'http://localhost:8080',
      '/logs': import.meta.env?.VITE_API_BASE_URL ?? 'http://localhost:8080',
    },
  },
})
