# Log Ingestion Engine Frontend

React (TypeScript) + Vite dashboard for the [log ingestion engine](../README.md).

## Setup

```bash
cd frontend

# 1. Install dependencies
npm install

# 2. Configure the backend URL. Defaults to http://localhost:8080
cp .env.example .env
# Edit .env: VITE_API_BASE_URL=http://localhost:8081

# 3. Start the dev server
npm run dev   # http://localhost:5173

# 4. Production build
npm run build # output to frontend/dist/
```

## Pages

| Route | Description |
|-------|-------------|
| `/` | Dashboard: live metrics via server-sent events |
| `/metrics` | Counters and per-level/per-service tables |

## Environment

| Variable | Default | Description |
|----------|---------|-------------|
| `VITE_API_BASE_URL` | `http://localhost:8080` | Backend base URL for `/metrics/events` and `/logs` |

## Scripts

| Script | Description |
|--------|-------------|
| `npm run dev` | Start Vite dev server |
| `npm run build` | Type-check + production build to `dist/` |
| `npm run lint` | Run oxlint |

## Backend

Start the Go backend first (`cd .. && go run ./cmd/server`). The frontend proxies `/metrics` and `/logs` to the backend via Vite in development.
