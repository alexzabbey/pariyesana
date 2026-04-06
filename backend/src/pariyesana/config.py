from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    qdrant_url: str = "http://localhost:6333"
    collection_name: str = "passages"
    database_url: str = "postgresql+psycopg://pariyesana:pariyesana@localhost:5432/pariyesana"
    transcripts_dir: str = "../transcripts"
    embedding_model: str = "google/EmbeddingGemma-300M"
    vector_size: int = 768
    cors_origins: list[str] = ["http://localhost:5173", "http://localhost:3000"]

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()
