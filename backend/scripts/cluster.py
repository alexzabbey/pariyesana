"""CLI: Cluster dharma talks by topic using embeddings from Qdrant.

Usage:
    cd backend/
    uv run python -m scripts.cluster
    uv run python -m scripts.cluster --level passages --min-cluster-size 20
"""

import argparse
import logging

import hdbscan
import numpy as np
from qdrant_client import models

from pariyesana.config import settings
from pariyesana.services.search import search_service


def fetch_vectors(level: str) -> tuple[np.ndarray, list[dict]]:
    """Pull vectors and metadata from Qdrant. Returns (vectors, metadata_list)."""
    assert search_service.client is not None

    all_points: list = []
    offset = None
    while True:
        points, next_offset = search_service.client.scroll(
            collection_name=settings.collection_name,
            limit=1000,
            offset=offset,
            with_payload=models.PayloadSelectorInclude(
                include=["talk_id", "teacher", "title", "date", "center", "text", "chunk_index"]
            ),
            with_vectors=True,
        )
        all_points.extend(points)
        if next_offset is None:
            break
        offset = next_offset

    logger.info("Fetched %d passage vectors from Qdrant", len(all_points))

    if level == "talks":
        # Average passage vectors per talk
        talk_vectors: dict[int, list[np.ndarray]] = {}
        talk_meta: dict[int, dict] = {}
        for p in all_points:
            tid = p.payload["talk_id"]
            talk_vectors.setdefault(tid, []).append(np.array(p.vector))
            if tid not in talk_meta:
                talk_meta[tid] = {
                    "talk_id": tid,
                    "teacher": p.payload.get("teacher", ""),
                    "title": p.payload.get("title", ""),
                    "date": p.payload.get("date", ""),
                    "center": p.payload.get("center", ""),
                }

        vectors = np.array([np.mean(talk_vectors[tid], axis=0) for tid in talk_meta])
        meta = list(talk_meta.values())
        logger.info("Averaged into %d talk-level vectors", len(meta))
    else:
        vectors = np.array([p.vector for p in all_points])
        meta = [
            {
                "talk_id": p.payload["talk_id"],
                "teacher": p.payload.get("teacher", ""),
                "title": p.payload.get("title", ""),
                "date": p.payload.get("date", ""),
                "chunk_index": p.payload.get("chunk_index", 0),
                "text": p.payload.get("text", "")[:120],
            }
            for p in all_points
        ]

    return vectors, meta


def run_clustering(vectors: np.ndarray, min_cluster_size: int, min_samples: int | None) -> np.ndarray:
    """Run HDBSCAN and return labels."""
    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=min_samples,
        metric="euclidean",
    )
    labels = clusterer.fit_predict(vectors)
    return labels


def print_results(labels: np.ndarray, meta: list[dict], level: str, top_n: int) -> None:
    """Print cluster summary."""
    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    n_noise = int(np.sum(labels == -1))
    total = len(labels)

    print(f"\n{'=' * 60}")
    print(f"Clustering results ({level}-level)")
    print(f"{'=' * 60}")
    print(f"Total items:  {total}")
    print(f"Clusters:     {n_clusters}")
    print(f"Noise:        {n_noise} ({100 * n_noise / total:.1f}%)")

    # Group by cluster
    clusters: dict[int, list[dict]] = {}
    for label, m in zip(labels, meta):
        clusters.setdefault(int(label), []).append(m)

    # Print each cluster (sorted by size)
    for cid in sorted(clusters, key=lambda c: len(clusters[c]), reverse=True):
        if cid == -1:
            continue
        items = clusters[cid]
        print(f"\n--- Cluster {cid} ({len(items)} items) ---")

        if level == "talks":
            # Show teachers breakdown
            teachers: dict[str, int] = {}
            for item in items:
                teachers[item["teacher"]] = teachers.get(item["teacher"], 0) + 1
            top_teachers = sorted(teachers.items(), key=lambda x: x[1], reverse=True)[:5]
            print(f"  Teachers: {', '.join(f'{t} ({c})' for t, c in top_teachers)}")

            for item in items[:top_n]:
                print(f"  - [{item['date']}] {item['teacher']}: {item['title']}")
            if len(items) > top_n:
                print(f"  ... and {len(items) - top_n} more")
        else:
            for item in items[:top_n]:
                print(f"  - [talk {item['talk_id']}, chunk {item['chunk_index']}] {item['text']}")
            if len(items) > top_n:
                print(f"  ... and {len(items) - top_n} more")

    if -1 in clusters:
        print(f"\n--- Noise ({len(clusters[-1])} items, not shown) ---")


logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Cluster dharma talks by topic")
    parser.add_argument("--level", choices=["talks", "passages"], default="talks", help="Cluster at talk or passage level")
    parser.add_argument("--min-cluster-size", type=int, default=5, help="HDBSCAN min_cluster_size")
    parser.add_argument("--min-samples", type=int, default=None, help="HDBSCAN min_samples (defaults to min_cluster_size)")
    parser.add_argument("--top-n", type=int, default=10, help="Items to show per cluster")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")

    logger.info("Connecting to Qdrant at %s", settings.qdrant_url)
    search_service.connect()

    logger.info("Fetching vectors (%s-level)...", args.level)
    vectors, meta = fetch_vectors(args.level)

    logger.info("Running HDBSCAN (min_cluster_size=%d)...", args.min_cluster_size)
    labels = run_clustering(vectors, args.min_cluster_size, args.min_samples)

    print_results(labels, meta, args.level, args.top_n)


if __name__ == "__main__":
    main()
