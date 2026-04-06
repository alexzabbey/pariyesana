"""One-time migration: import talks.csv into Postgres.

Usage:
    DATABASE_URL=postgresql+psycopg://pariyesana:pariyesana@localhost:5432/pariyesana \
        uv run python -m pariyesana_db.migrate --csv talks.csv --transcripts transcripts/
"""

import argparse
from pathlib import Path

from pariyesana_db.connection import get_engine, get_session
from pariyesana_db.models import Base, Talk


STATUS_MAP = {
    "": "pending",
    "done": "done",
    "skip_language": "skip_language",
    "no_mp3": "no_mp3",
}


def migrate(csv_path: str, transcripts_dir: str | None = None) -> None:
    import polars as pl

    engine = get_engine()
    Base.metadata.create_all(engine)
    Session = get_session(engine)

    df = pl.read_csv(csv_path, schema_overrides={"talk_id": pl.Utf8, "teacher_id": pl.Utf8})

    transcripts = Path(transcripts_dir) if transcripts_dir else None

    talks = []
    for row in df.iter_rows(named=True):
        talk_id = row["talk_id"]
        csv_status = (row.get("transcribed") or "").strip()
        status = STATUS_MAP.get(csv_status, csv_status or "pending")

        # Check disk for transcript files if status wasn't already "done"
        if status != "done" and transcripts:
            txt = transcripts / f"{talk_id}.txt"
            jsonl = transcripts / f"{talk_id}.jsonl"
            if txt.exists() and jsonl.exists():
                status = "done"

        talks.append(Talk(
            talk_id=int(talk_id),
            date=row.get("date", "") or "",
            title=row.get("title", "") or "",
            teacher=row.get("teacher", "") or "",
            teacher_id=str(row.get("teacher_id", "") or ""),
            center=row.get("center", "") or "",
            duration=row.get("duration", "") or "",
            description=row.get("description", "") or "",
            mp3_url=row.get("mp3_url", "") or "",
            language=row.get("language", "") or "English",
            status=status,
        ))

    with Session() as session:
        session.add_all(talks)
        session.commit()

    # Print summary
    from collections import Counter
    counts = Counter(t.status for t in talks)
    print(f"Migrated {len(talks)} talks:")
    for status, count in sorted(counts.items()):
        print(f"  {status}: {count}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate talks.csv to Postgres")
    parser.add_argument("--csv", required=True, help="Path to talks.csv")
    parser.add_argument("--transcripts", default=None, help="Path to transcripts directory")
    args = parser.parse_args()
    migrate(args.csv, args.transcripts)


if __name__ == "__main__":
    main()
