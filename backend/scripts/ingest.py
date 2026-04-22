"""CLI: Ingest transcripts into Qdrant.

Opens SSH tunnels to the OCI host for Postgres and Qdrant, syncs JSONL transcripts
from the remote, then runs incremental ingestion.

Usage:
    cd backend/
    uv run python -m scripts.ingest
"""

import argparse
import logging
import os
import subprocess
import time

from pariyesana_db.tunnel import ensure_tunnel

SSH_TUNNEL_HOST = "oci-pariyesana"
REMOTE_TRANSCRIPTS_PATH = "~/pariyesana/transcripts/"
LOCAL_PG_PORT = 5433   # avoid clash with local Postgres on 5432
REMOTE_PG_PORT = 5432
LOCAL_QDRANT_PORT = 6334  # avoid clash with local Qdrant on 6333
REMOTE_QDRANT_PORT = 6333


def _set_tunnel_env() -> None:
    """Point DATABASE_URL and QDRANT_URL at the tunnel unless already set."""
    if not os.environ.get("DATABASE_URL"):
        pw = os.environ.get("PG_PASSWORD", "pariyesana")
        os.environ["DATABASE_URL"] = (
            f"postgresql+psycopg://pariyesana:{pw}@localhost:{LOCAL_PG_PORT}/pariyesana"
        )
    if not os.environ.get("QDRANT_URL"):
        os.environ["QDRANT_URL"] = f"http://localhost:{LOCAL_QDRANT_PORT}"


def _format_size(n: int) -> str:
    if n < 1024**2:
        return f"{n / 1024:.1f} KB"
    if n < 1024**3:
        return f"{n / 1024**2:.1f} MB"
    return f"{n / 1024**3:.2f} GB"


_RSYNC_FILTER = ["--include=*.jsonl", "--exclude=*"]


def _sync_jsonls(transcripts_dir: str) -> bool:
    """rsync JSONLs from remote, dry-run first, confirm, then transfer. Returns False on cancel/fail."""
    os.makedirs(transcripts_dir, exist_ok=True)
    dest = transcripts_dir.rstrip("/") + "/"
    remote = f"{SSH_TUNNEL_HOST}:{REMOTE_TRANSCRIPTS_PATH}"

    print("SYNC | Checking what would be transferred...")
    dry = subprocess.run(
        ["rsync", "-az", "--dry-run", "--stats", *_RSYNC_FILTER, remote, dest],
        capture_output=True, text=True,
    )
    if dry.returncode != 0:
        print(f"SYNC | Dry run failed (exit code {dry.returncode})")
        if dry.stderr:
            print(dry.stderr.strip())
        return False

    transfer_bytes = 0
    file_count = 0
    for line in dry.stdout.splitlines():
        if line.startswith("Total transferred file size:"):
            try:
                transfer_bytes = int(line.split(":", 1)[1].strip().split()[0].replace(",", ""))
            except (ValueError, IndexError):
                pass
        elif line.startswith("Number of regular files transferred:"):
            try:
                file_count = int(line.split(":", 1)[1].strip().replace(",", ""))
            except ValueError:
                pass

    if transfer_bytes == 0:
        print("SYNC | JSONLs already up to date")
        return True

    print(f"SYNC | {file_count} JSONL files, {_format_size(transfer_bytes)} to download")
    answer = input("Proceed? [y/N] ").strip().lower()
    if answer != "y":
        print("SYNC | Cancelled")
        return False

    print("SYNC | Pulling JSONLs from server...")
    result = subprocess.run(
        ["rsync", "-az", "--progress", *_RSYNC_FILTER, remote, dest],
    )
    if result.returncode == 0:
        print("SYNC | Complete")
        return True
    print(f"SYNC | Failed (exit code {result.returncode})")
    return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest dharma talk transcripts into Qdrant (via SSH tunnel to OCI)")
    parser.add_argument("--transcripts", default=None, help="Path to transcripts directory")
    parser.add_argument("--embed-batch-size", type=int, default=256, help="Embedding batch size")
    parser.add_argument("--upsert-batch-size", type=int, default=100, help="Qdrant upsert batch size")
    parser.add_argument("--full", action="store_true", help="Re-embed and upsert all talks (default: incremental, only new)")
    parser.add_argument("--no-tunnel", action="store_true", help="Skip SSH tunnel setup (use existing DATABASE_URL/QDRANT_URL)")
    parser.add_argument("--no-sync", action="store_true", help="Skip pulling JSONLs from the remote before ingesting")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")
    logger = logging.getLogger(__name__)

    if not args.no_tunnel:
        ensure_tunnel(
            SSH_TUNNEL_HOST,
            [(LOCAL_PG_PORT, REMOTE_PG_PORT), (LOCAL_QDRANT_PORT, REMOTE_QDRANT_PORT)],
        )
        _set_tunnel_env()

    # Import after env vars are set so settings pick them up
    from pariyesana.config import settings
    from pariyesana.services.embedding import embedding_service
    from pariyesana.services.ingestion import run_ingestion
    from pariyesana.services.search import search_service

    transcripts_dir = args.transcripts or settings.transcripts_dir

    if not args.no_sync:
        if not _sync_jsonls(transcripts_dir):
            return

    logger.info("Connecting to Qdrant at %s", settings.qdrant_url)
    search_service.connect()

    logger.info("Loading embedding model")
    embedding_service.load()

    logger.info("Starting ingestion")
    start = time.time()
    run_ingestion(settings.database_url, transcripts_dir, args.embed_batch_size, args.upsert_batch_size, full=args.full)
    elapsed = time.time() - start
    logger.info("Done in %.1f seconds", elapsed)


if __name__ == "__main__":
    main()
