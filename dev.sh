#!/usr/bin/env bash
set -e

# Kill any running backend/frontend processes
lsof -ti :8000 | xargs kill -9 2>/dev/null || true
lsof -ti :5173 | xargs kill -9 2>/dev/null || true

trap 'kill 0' EXIT

# Postgres
docker start pariyesana-postgres 2>/dev/null || \
  docker run -d --name pariyesana-postgres -p 5432:5432 \
    -e POSTGRES_USER=pariyesana -e POSTGRES_PASSWORD=pariyesana -e POSTGRES_DB=pariyesana \
    -v pariyesana_postgres:/var/lib/postgresql/data \
    postgres:17
echo "Postgres running on :5432"

# Qdrant
docker start pariyesana-qdrant 2>/dev/null || \
  docker run -d --name pariyesana-qdrant -p 6333:6333 -v pariyesana_qdrant:/qdrant/storage qdrant/qdrant:latest
echo "Qdrant running on :6333"

# Backend
export DATABASE_URL="postgresql+psycopg://pariyesana:pariyesana@localhost:5432/pariyesana"
(cd backend && uv run uvicorn pariyesana.main:app --reload --port 8000) &
echo "Backend starting on :8000"

# Frontend
(cd frontend && bun run dev --port 5173) &
echo "Frontend starting on :5173"

wait
