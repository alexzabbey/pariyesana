import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from pariyesana.api.search import router
from pariyesana.config import settings
from pariyesana.services.embedding import embedding_service
from pariyesana.services.metadata import metadata_store
from pariyesana.services.search import search_service

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Loading metadata from database")
    metadata_store.load()
    logger.info("Loaded %d talks", len(metadata_store.talks))

    embedding_service.load()

    logger.info("Connecting to Qdrant at %s", settings.qdrant_url)
    search_service.connect()
    logger.info("Qdrant connected, %d passages indexed", search_service.count_passages())

    yield

    logger.info("Shutting down")


app = FastAPI(title="Pariyesana", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(router)
