import json
import logging
import re
import uuid
from pathlib import Path

from qdrant_client import models

from pariyesana.config import settings
from pariyesana_db import get_all_talks, get_engine, get_session

from pariyesana.services.embedding import embedding_service
from pariyesana.services.search import search_service

logger = logging.getLogger(__name__)

NAMESPACE = uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

OUTRO_PATTERNS = re.compile(
    r"(thank\s+you\s+for\s+listening|"
    r"support.*dharmaseed|"
    r"dharmaseed\.org|"
    r"please\s+(consider\s+)?donat|"
    r"your\s+donation|"
    r"this\s+talk\s+was\s+offered)",
    re.IGNORECASE,
)


def _strip_outro(segments: list[dict]) -> list[dict]:
    """Remove donation outro segments from the end of a talk."""
    if len(segments) < 3:
        return segments
    # Check last 5 segments for outro patterns
    cutoff = len(segments)
    for i in range(max(0, len(segments) - 5), len(segments)):
        if OUTRO_PATTERNS.search(segments[i].get("text", "")):
            cutoff = i
            break
    return segments[:cutoff]


def _estimate_tokens(text: str) -> int:
    """Rough token estimate: ~0.75 tokens per word."""
    return int(len(text.split()) * 1.33)


def chunk_segments(
    segments: list[dict],
    target_tokens: int = 500,
    overlap_tokens: int = 50,
) -> list[dict]:
    """Group segments into chunks of ~target_tokens with overlap."""
    segments = _strip_outro(segments)
    if not segments:
        return []

    chunks: list[dict] = []
    current_segs: list[dict] = []
    current_tokens = 0

    for seg in segments:
        seg_tokens = _estimate_tokens(seg.get("text", ""))
        current_segs.append(seg)
        current_tokens += seg_tokens

        if current_tokens >= target_tokens:
            chunk = _build_chunk(current_segs, len(chunks))
            chunks.append(chunk)

            # Overlap: keep last segments that fit within overlap_tokens
            overlap_segs: list[dict] = []
            overlap_tok = 0
            for s in reversed(current_segs):
                t = _estimate_tokens(s.get("text", ""))
                if overlap_tok + t > overlap_tokens:
                    break
                overlap_segs.insert(0, s)
                overlap_tok += t

            current_segs = overlap_segs
            current_tokens = overlap_tok

    # Final chunk
    if current_segs:
        chunk = _build_chunk(current_segs, len(chunks))
        chunks.append(chunk)

    return chunks


def _build_chunk(segs: list[dict], index: int) -> dict:
    return {
        "text": " ".join(s.get("text", "") for s in segs),
        "start_time": segs[0].get("start", 0.0),
        "end_time": segs[-1].get("end", 0.0),
        "has_audience": any(s.get("speaker") == "audience" for s in segs),
        "chunk_index": index,
    }


def _read_and_chunk_talk(talk_id: int, jsonl_path: Path) -> list[dict]:
    """Read a JSONL transcript and return chunks with talk_id attached."""
    segments: list[dict] = []
    with open(jsonl_path) as f:
        for line in f:
            line = line.strip()
            if line:
                segments.append(json.loads(line))

    if not segments:
        return []

    chunks = chunk_segments(segments)
    for c in chunks:
        c["talk_id"] = talk_id
    return chunks


def run_ingestion(
    database_url: str,
    transcripts_dir: str,
    embed_batch_size: int = 256,
    upsert_batch_size: int = 100,
    full: bool = False,
) -> None:
    """Run ingestion pipeline. By default only indexes new talks (incremental)."""
    assert search_service.client is not None

    engine = get_engine(database_url)
    Session = get_session(engine)
    with Session() as session:
        all_talks = get_all_talks(session)

    transcripts = Path(transcripts_dir)
    transcribed = {
        t.talk_id: t for t in all_talks if t.status == "done"
    }
    logger.info("Found %d transcribed talks in DB", len(transcribed))

    if not full:
        existing_ids = search_service.get_indexed_talk_ids()
        before = len(transcribed)
        transcribed = {tid: t for tid, t in transcribed.items() if tid not in existing_ids}
        logger.info("Incremental mode: %d already indexed, %d new to process", before - len(transcribed), len(transcribed))
        if not transcribed:
            logger.info("Nothing new to ingest")
            return

    # Phase 1: Read and chunk all talks
    logger.info("Chunking transcripts...")
    all_chunks: list[dict] = []
    meta_by_id: dict[int, dict] = {}
    skipped = 0

    for talk_id, talk in transcribed.items():
        jsonl_path = transcripts / f"{talk_id}.jsonl"
        if not jsonl_path.exists():
            skipped += 1
            continue

        meta_by_id[talk_id] = {
            "teacher": talk.teacher or "",
            "title": talk.title or "",
            "date": talk.date or "",
            "center": talk.center or "",
            "language": talk.language or "English",
        }
        all_chunks.extend(_read_and_chunk_talk(talk_id, jsonl_path))

    logger.info("Chunked %d talks into %d passages (%d skipped, no JSONL)", len(meta_by_id), len(all_chunks), skipped)

    if not all_chunks:
        return

    # Phase 2: Embed all chunks in large batches
    logger.info("Embedding %d passages (batch_size=%d)...", len(all_chunks), embed_batch_size)
    all_texts = [c["text"] for c in all_chunks]
    all_vectors = embedding_service.embed_documents(all_texts, batch_size=embed_batch_size)
    logger.info("Embedding complete")

    # Phase 3: Build points and upsert
    logger.info("Upserting to Qdrant...")
    points: list[models.PointStruct] = []
    for chunk, vec in zip(all_chunks, all_vectors):
        talk_id = chunk["talk_id"]
        meta = meta_by_id[talk_id]
        point_id = str(uuid.uuid5(NAMESPACE, f"{talk_id}:{chunk['chunk_index']}"))
        points.append(
            models.PointStruct(
                id=point_id,
                vector=vec.tolist(),
                payload={
                    "talk_id": talk_id,
                    "teacher": meta["teacher"],
                    "title": meta["title"],
                    "date": meta["date"],
                    "center": meta["center"],
                    "language": meta["language"],
                    "chunk_index": chunk["chunk_index"],
                    "start_time": chunk["start_time"],
                    "end_time": chunk["end_time"],
                    "text": chunk["text"],
                    "has_audience": chunk["has_audience"],
                },
            )
        )

    for i in range(0, len(points), upsert_batch_size):
        batch = points[i : i + upsert_batch_size]
        search_service.client.upsert(
            collection_name=settings.collection_name,
            points=batch,
        )
        if (i // upsert_batch_size) % 10 == 0:
            logger.info("Upserted %d/%d points", min(i + upsert_batch_size, len(points)), len(points))

    logger.info("Ingestion complete: %d talks, %d passages", len(meta_by_id), len(points))
