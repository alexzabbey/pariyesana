import logging

import numpy as np
from sentence_transformers import SentenceTransformer

from pariyesana.config import settings

logger = logging.getLogger(__name__)


class EmbeddingService:
    def __init__(self) -> None:
        self.model: SentenceTransformer | None = None

    def load(self) -> None:
        import torch

        logger.info(
            "Loading embedding model: %s | torch %s (cuda build=%s, cuda available=%s, devices=%d)",
            settings.embedding_model,
            torch.__version__,
            torch.version.cuda,
            torch.cuda.is_available(),
            torch.cuda.device_count(),
        )
        self.model = SentenceTransformer(
            settings.embedding_model,
            device=settings.embedding_device or None,
            # Gemma is natively bf16; fp32 on MPS is ~2x slower and doubles the attention buffer.
            model_kwargs={"dtype": torch.bfloat16},
        )
        logger.info("Embedding model loaded on device: %s", self.model.device)

    def embed_query(self, query: str) -> list[float]:
        """Embed a search query (applies 'task: search result | query: ' prefix)."""
        assert self.model is not None, "Embedding model not loaded"
        vec = self.model.encode_query(query, normalize_embeddings=True)
        return vec.tolist()

    def embed_documents(self, texts: list[str], batch_size: int = 32) -> np.ndarray:
        """Embed document chunks (applies 'title: none | text: ' prefix)."""
        assert self.model is not None, "Embedding model not loaded"
        return self.model.encode_document(
            texts,
            normalize_embeddings=True,
            batch_size=batch_size,
            show_progress_bar=True,
        )

    def embed_sentences(self, texts: list[str]) -> np.ndarray:
        """Embed a small batch of sentences for re-ranking (no progress bar)."""
        assert self.model is not None, "Embedding model not loaded"
        return self.model.encode_document(
            texts, normalize_embeddings=True, batch_size=len(texts)
        )


embedding_service = EmbeddingService()
