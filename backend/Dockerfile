FROM python:3.12-slim

RUN pip install uv

WORKDIR /app

# Install shared db package first
COPY db/ /tmp/db/
RUN uv pip install --system /tmp/db/

# Install backend
COPY backend/pyproject.toml .
COPY backend/src/ src/
COPY backend/scripts/ scripts/

RUN uv pip install --system .

ENV PYTHONPATH=/app/src

CMD ["uvicorn", "pariyesana.main:app", "--host", "0.0.0.0", "--port", "8000"]
