#!/usr/bin/env bash
set -e

# Kill any running backend/frontend processes
lsof -ti :8000 | xargs kill -9 2>/dev/null || true
lsof -ti :5173 | xargs kill -9 2>/dev/null || true

trap 'kill 0' EXIT

# Qdrant
docker start pariyesana-qdrant 2>/dev/null || \
  docker run -d --name pariyesana-qdrant -p 6333:6333 -v pariyesana_qdrant:/qdrant/storage qdrant/qdrant:latest
echo "Qdrant running on :6333"

# Backend
(cd backend && uv run uvicorn pariyesana.main:app --reload --port 8000) &
echo "Backend starting on :8000"

# Frontend
(cd frontend && bun run dev --port 5173) &
echo "Frontend starting on :5173"

wait
