import logging

import numpy as np
from sentence_transformers import SentenceTransformer

from pariyesana.config import settings

logger = logging.getLogger(__name__)


class EmbeddingService:
    def __init__(self) -> None:
        self.model: SentenceTransformer | None = None

    def load(self) -> None:
        logger.info("Loading embedding model: %s", settings.embedding_model)
        self.model = SentenceTransformer(settings.embedding_model)
        logger.info("Embedding model loaded")

    def embed_query(self, query: str) -> list[float]:
        """Embed a search query (applies 'task: search result | query: ' prefix)."""
        assert self.model is not None, "Embedding model not loaded"
        vec = self.model.encode_query(query, normalize_embeddings=True)
        return vec.tolist()

    def embed_documents(self, texts: list[str], batch_size: int = 256) -> np.ndarray:
        """Embed document chunks (applies 'title: none | text: ' prefix)."""
        assert self.model is not None, "Embedding model not loaded"
        return self.model.encode_document(texts, normalize_embeddings=True, batch_size=batch_size, show_progress_bar=True)

    def embed_sentences(self, texts: list[str]) -> np.ndarray:
        """Embed a small batch of sentences for re-ranking (no progress bar)."""
        assert self.model is not None, "Embedding model not loaded"
        return self.model.encode_document(texts, normalize_embeddings=True, batch_size=len(texts))


embedding_service = EmbeddingService()
