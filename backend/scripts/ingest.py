"""CLI: Ingest transcripts into Qdrant.

Usage:
    cd backend/
    uv run python -m scripts.ingest --transcripts ../transcripts/
"""

import argparse
import logging
import time

from pariyesana.config import settings
from pariyesana.services.embedding import embedding_service
from pariyesana.services.ingestion import run_ingestion
from pariyesana.services.search import search_service


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest dharma talk transcripts into Qdrant")
    parser.add_argument("--database-url", default=settings.database_url, help="Postgres connection URL")
    parser.add_argument("--transcripts", default=settings.transcripts_dir, help="Path to transcripts directory")
    parser.add_argument("--embed-batch-size", type=int, default=256, help="Embedding batch size")
    parser.add_argument("--upsert-batch-size", type=int, default=100, help="Qdrant upsert batch size")
    parser.add_argument("--full", action="store_true", help="Re-embed and upsert all talks (default: incremental, only new)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")
    logger = logging.getLogger(__name__)

    logger.info("Connecting to Qdrant at %s", settings.qdrant_url)
    search_service.connect()

    logger.info("Loading embedding model")
    embedding_service.load()

    logger.info("Starting ingestion")
    start = time.time()
    run_ingestion(args.database_url, args.transcripts, args.embed_batch_size, args.upsert_batch_size, full=args.full)
    elapsed = time.time() - start
    logger.info("Done in %.1f seconds", elapsed)


if __name__ == "__main__":
    main()
