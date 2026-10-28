import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'
import tailwindcss from '@tailwindcss/vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    proxy: {
      '/metrics': import.meta.env?.VITE_API_BASE_URL ?? 'http://localhost:8080',
      '/logs': import.meta.env?.VITE_API_BASE_URL ?? 'http://localhost:8080',
    },
  },
})
