FROM python:3.12-slim

RUN pip install uv

WORKDIR /app
COPY pyproject.toml .
COPY src/ src/
COPY scripts/ scripts/

RUN uv pip install --system .

ENV PYTHONPATH=/app/src

CMD ["uvicorn", "pariyesana.main:app", "--host", "0.0.0.0", "--port", "8000"]
