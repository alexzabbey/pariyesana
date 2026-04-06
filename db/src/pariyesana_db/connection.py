import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

# Load .env from the project root
_env_path = Path(__file__).resolve().parents[3] / ".env"
if _env_path.exists():
    load_dotenv(_env_path)


def _build_url() -> str:
    """Build DATABASE_URL from env, supporting either DATABASE_URL directly or PG_PASSWORD."""
    if url := os.environ.get("DATABASE_URL"):
        return url
    pw = os.environ.get("PG_PASSWORD", "pariyesana")
    return f"postgresql+psycopg://pariyesana:{pw}@localhost:5432/pariyesana"


def get_engine(database_url: str | None = None) -> Engine:
    url = database_url or _build_url()
    return create_engine(url, pool_pre_ping=True)


def get_session(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(engine, expire_on_commit=False)
