# counter-app

A learning project. We build a simple counter app and grow it **one layer at a
time** — from plain code, to Docker, to Kubernetes, to a scalable distributed
system. The goal is understanding each step, not speed.

## Where we are right now: Stage 1 — just the app

```
Browser → Frontend (React)  → Backend (FastAPI) → Redis
```

Right now the repo contains **only the application code**. No Docker, no
Kubernetes yet — we add those deliberately, each as its own lesson.

```
counter-app/
├── backend/
│   ├── app/main.py        FastAPI service (stores the count in Redis)
│   └── requirements.txt
└── frontend/
    ├── src/               React UI (App.jsx, main.jsx, index.css)
    ├── index.html
    ├── package.json
    └── vite.config.js
```

## The roadmap (we add one layer per stage)

| Stage | What we add | Why |
| ----- | ----------- | --- |
| 1 ✅ | Backend + frontend code only | Understand the app itself |
| 2 | Run locally + a Redis container by hand | See the full chain work |
| 3 | A `Dockerfile` (backend, then frontend) | Containerize the app |
| 4 | `docker-compose` for all containers | Run everything together |
| 5 | Kubernetes (k3s on the VM) | Transition Docker → k8s |
| 6 | PostgreSQL | Persist the Redis value durably on intervals |
| 7 | More replicas + Kafka queue | Scale out; buffer writes so Redis isn't overwhelmed |
| 8 | CI/CD + Prometheus/Grafana | Auto-deploy + monitoring |

## Running it (Stage 2 — next lesson)

You'll start a Redis container by hand, then run the two apps:

```bash
# Redis (in a container)
docker run -d --name redis -p 6379:6379 redis:7-alpine

# Backend
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload      # http://localhost:8000

# Frontend (separate terminal)
cd frontend
npm install
npm run dev                        # http://localhost:5173
```

## API

| Method | Path                     | Description        |
| ------ | ------------------------ | ------------------ |
| GET    | `/api/counter`           | Current value      |
| POST   | `/api/counter/increment` | +1                 |
| POST   | `/api/counter/decrement` | −1                 |
| POST   | `/api/counter/reset`     | Set to 0           |
| GET    | `/healthz`               | Liveness           |
| GET    | `/readyz`                | Readiness (pings Redis) |
