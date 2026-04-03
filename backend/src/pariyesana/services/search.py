import logging
import re

from qdrant_client import QdrantClient, models

from pariyesana.config import settings
from pariyesana.models.schemas import Snippet, TalkSearchResult
from pariyesana.services.embedding import embedding_service
from pariyesana.services.metadata import metadata_store

logger = logging.getLogger(__name__)

_SENTENCE_SPLIT = re.compile(r'(?<=[.!?])\s+')
_FILLERS = re.compile(
    r'(?:^|(?<=\s))'           # must be at start or after whitespace
    r'(?:uh+|um+|uhm|hmm+|hm+|mm+)'  # only unambiguous fillers
    r'[,.]?\s*',               # optional trailing punctuation
    re.IGNORECASE,
)


def _strip_fillers(text: str) -> str:
    """Remove unambiguous filler words (uh, um, hmm, etc.) from text."""
    cleaned = _FILLERS.sub('', text)
    return re.sub(r'  +', ' ', cleaned).strip()


def _extract_highlight(text: str, query_words: set[str]) -> str:
    """Pick the best sentence by keyword overlap and return it with one sentence of context on each side."""
    sentences = _SENTENCE_SPLIT.split(text)
    if len(sentences) <= 3:
        return text

    best_idx = 0
    best_score = -1
    for i, s in enumerate(sentences):
        words = set(s.lower().split())
        score = len(query_words & words)
        if score > best_score:
            best_score = score
            best_idx = i

    start = max(0, best_idx - 1)
    end = min(len(sentences), best_idx + 2)
    return " ".join(sentences[start:end])


class SearchService:
    def __init__(self) -> None:
        self.client: QdrantClient | None = None

    def connect(self) -> None:
        self.client = QdrantClient(url=settings.qdrant_url)
        self._ensure_collection()

    def _ensure_collection(self) -> None:
        assert self.client is not None
        collections = [c.name for c in self.client.get_collections().collections]
        if settings.collection_name not in collections:
            logger.info("Creating collection: %s", settings.collection_name)
            self.client.create_collection(
                collection_name=settings.collection_name,
                vectors_config=models.VectorParams(
                    size=settings.vector_size,
                    distance=models.Distance.COSINE,
                ),
            )
            self._create_indexes()

    def _create_indexes(self) -> None:
        assert self.client is not None
        for field in ("teacher", "center", "language", "talk_id", "date"):
            schema = (
                models.PayloadSchemaType.INTEGER
                if field == "talk_id"
                else models.PayloadSchemaType.KEYWORD
            )
            self.client.create_payload_index(
                collection_name=settings.collection_name,
                field_name=field,
                field_schema=schema,
            )

    def is_connected(self) -> bool:
        if self.client is None:
            return False
        try:
            self.client.get_collections()
            return True
        except Exception:
            return False

    def count_passages(self) -> int:
        if self.client is None:
            return 0
        try:
            info = self.client.get_collection(settings.collection_name)
            return info.points_count or 0
        except Exception:
            return 0

    def get_indexed_talk_ids(self) -> set[int]:
        """Return the set of talk_ids already indexed in Qdrant."""
        assert self.client is not None
        talk_ids: set[int] = set()
        offset = None
        while True:
            result = self.client.scroll(
                collection_name=settings.collection_name,
                scroll_filter=None,
                limit=1000,
                offset=offset,
                with_payload=models.PayloadSelectorInclude(include=["talk_id"]),
                with_vectors=False,
            )
            points, next_offset = result
            for p in points:
                if p.payload and "talk_id" in p.payload:
                    talk_ids.add(p.payload["talk_id"])
            if next_offset is None:
                break
            offset = next_offset
        return talk_ids

    def search(
        self,
        query: str,
        *,
        teacher: str | None = None,
        center: str | None = None,
        language: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int = 20,
        max_snippets: int = 1,
    ) -> list[TalkSearchResult]:
        assert self.client is not None
        query_vector = embedding_service.embed_query(query)

        must_conditions: list[models.Condition] = []
        if teacher:
            must_conditions.append(
                models.FieldCondition(
                    key="teacher",
                    match=models.MatchValue(value=teacher),
                )
            )
        if center:
            must_conditions.append(
                models.FieldCondition(
                    key="center",
                    match=models.MatchValue(value=center),
                )
            )
        if language:
            must_conditions.append(
                models.FieldCondition(
                    key="language",
                    match=models.MatchValue(value=language),
                )
            )
        if date_from:
            must_conditions.append(
                models.FieldCondition(
                    key="date",
                    range=models.Range(gte=date_from),
                )
            )
        if date_to:
            must_conditions.append(
                models.FieldCondition(
                    key="date",
                    range=models.Range(lte=date_to),
                )
            )

        query_filter = models.Filter(must=must_conditions) if must_conditions else None

        # Fetch more chunks to allow grouping by talk
        hits = self.client.query_points(
            collection_name=settings.collection_name,
            query=query_vector,
            query_filter=query_filter,
            limit=limit * 5,
            with_payload=True,
        )

        # Group chunks by talk
        talks: dict[int, list] = {}
        for hit in hits.points:
            p = hit.payload or {}
            talk_id = p.get("talk_id", 0)
            talks.setdefault(talk_id, []).append(hit)

        # Build talk-level results
        query_words = set(query.lower().split())
        results: list[TalkSearchResult] = []

        for talk_id, chunks in talks.items():
            # Sort chunks by score descending
            chunks.sort(key=lambda h: h.score or 0.0, reverse=True)
            best = chunks[0]
            p = best.payload or {}
            talk = metadata_store.get_talk(talk_id)

            # Extract snippets from top chunks
            snippets: list[Snippet] = []
            for hit in chunks[:max_snippets]:
                hp = hit.payload or {}
                snippet_text = _strip_fillers(_extract_highlight(hp.get("text", ""), query_words))
                snippets.append(
                    Snippet(
                        text=snippet_text,
                        start_time=hp.get("start_time", 0.0),
                        end_time=hp.get("end_time", 0.0),
                        score=hit.score or 0.0,
                    )
                )

            # Sort snippets chronologically
            snippets.sort(key=lambda s: s.start_time)

            results.append(
                TalkSearchResult(
                    talk_id=talk_id,
                    title=p.get("title", ""),
                    teacher=p.get("teacher", ""),
                    date=p.get("date", ""),
                    center=p.get("center", ""),
                    language=p.get("language", "English"),
                    description=talk.description if talk else "",
                    duration=talk.duration if talk else "",
                    dharmaseed_url=talk.dharmaseed_url if talk else f"https://dharmaseed.org/talks/{talk_id}/",
                    audio_url=talk.audio_url if talk else "",
                    score=best.score or 0.0,
                    snippets=snippets,
                )
            )

        # Sort by best score, take top `limit`
        results.sort(key=lambda r: r.score, reverse=True)
        return results[:limit]


search_service = SearchService()
