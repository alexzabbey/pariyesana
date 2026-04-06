FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_PYTHON_DOWNLOADS=never

# Git needed for transformers git dependency
RUN apt-get update && apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app/backend

# Copy db package (maintains ../db relative path)
COPY db/ /app/db/

# Copy dependency files first (cached layer)
COPY backend/pyproject.toml pyproject.toml

# Install deps only (generate lock on the fly since uv.lock is environment-specific)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-install-project --no-dev --no-editable

# Copy source
COPY backend/src/ src/
COPY backend/scripts/ scripts/

# Install project itself
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-dev --no-editable

CMD ["uv", "run", "uvicorn", "pariyesana.main:app", "--host", "0.0.0.0", "--port", "8000"]
