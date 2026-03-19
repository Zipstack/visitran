# Visitran OSS — Docker Setup

## Quick Start

```bash
# From the repo root:
cp backend/sample.env backend/.env
# Edit backend/.env — set SECRET_KEY and VISITRAN_ENCRYPTION_KEY

cd docker
docker compose up --build -d
```

Open http://localhost:3000 and **Sign Up** to create your account.

## Common Commands

```bash
# Build and start all services
docker compose up --build -d

# Start services (without rebuilding)
docker compose up -d

# Stop all services
docker compose down

# Stop and delete all data (PostgreSQL volume)
docker compose down -v

# Rebuild a specific service (e.g., after code changes)
docker compose build --no-cache backend
docker compose up -d backend

# View logs
docker compose logs -f backend
docker compose logs -f frontend
```

## Services

| Service | Container Name | Port | Description |
|---------|---------------|------|-------------|
| frontend | visitran-frontend | 3000 | React UI (Nginx) |
| backend | visitran-backend | 8000 | Django REST API (Gunicorn) |
| postgres | visitran-postgres | 5432 | PostgreSQL database |
| redis | visitran-redis | 6379 | WebSockets and async task broker |
| celery-worker | visitran-celery-worker | — | Background job processing |
| celery-beat | visitran-celery-beat | — | Scheduled task processing |

## Environment Variables

The backend reads its config from `backend/.env` (copied from `backend/sample.env`).

Key variables for Docker:

| Variable | Docker Value | Description |
|----------|-------------|-------------|
| `DB_HOST` | `postgres` | PostgreSQL service name |
| `REDIS_HOST` | `redis` | Redis service name |
| `DB_SAMPLE_HOST` | `postgres` | Same PostgreSQL for sample project |
| `SECRET_KEY` | *(generate)* | Django secret key |
| `VISITRAN_ENCRYPTION_KEY` | *(generate)* | Fernet encryption key |

> **Note:** The `sample.env` is pre-configured for Docker — no hostname changes needed. Just set `SECRET_KEY` and `VISITRAN_ENCRYPTION_KEY`.

## Troubleshooting

**Backend using SQLite instead of PostgreSQL?**
- Check that `DB_HOST=postgres` is set in `backend/.env` (not empty)
- After changing `.env`, recreate containers: `docker compose up -d` (not `docker compose restart`)

**PostgreSQL password authentication failed?**
- The Docker volume may have stale data from a previous setup. Reset with: `docker compose down -v && docker compose up -d`

**Sample project not working?**
- Ensure `DB_SAMPLE_HOST=postgres` is set in `backend/.env`
- Rebuild: `docker compose build --no-cache backend && docker compose up -d`
