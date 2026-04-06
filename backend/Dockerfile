FROM python:3.12-slim

RUN pip install uv

WORKDIR /app

# Copy and install shared db package first
COPY db/ /tmp/db/
RUN uv pip install --system /tmp/db/

# Copy backend source
COPY backend/pyproject.toml .
COPY backend/src/ src/
COPY backend/scripts/ scripts/

# Install backend deps (pariyesana-db already installed above)
RUN UV_NO_SOURCES=1 uv pip install --system .

ENV PYTHONPATH=/app/src

CMD ["uvicorn", "pariyesana.main:app", "--host", "0.0.0.0", "--port", "8000"]
