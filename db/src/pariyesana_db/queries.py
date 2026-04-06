from datetime import datetime, timedelta, timezone

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from pariyesana_db.models import Talk

STALE_CLAIM_MINUTES = 60


def upsert_talks(session: Session, talks: list[dict]) -> int:
    """Insert new talks, skip any that already exist. Returns count inserted."""
    if not talks:
        return 0
    stmt = pg_insert(Talk).values([
        {
            "talk_id": int(t["talk_id"]),
            "date": t.get("date", ""),
            "title": t.get("title", ""),
            "teacher": t.get("teacher", ""),
            "teacher_id": t.get("teacher_id", ""),
            "center": t.get("center", ""),
            "duration": t.get("duration", ""),
            "description": t.get("description", ""),
            "mp3_url": t.get("mp3_url", ""),
            "language": t.get("language", "English"),
            "status": t.get("status", "pending"),
        }
        for t in talks
    ]).on_conflict_do_nothing(index_elements=["talk_id"])
    result = session.execute(stmt)
    session.commit()
    return result.rowcount


def claim_talk(session: Session, talk_id: int, worker_id: str) -> bool:
    """Claim a specific talk for transcription. Returns False if already claimed."""
    now = datetime.now(timezone.utc)
    stale = now - timedelta(minutes=STALE_CLAIM_MINUTES)

    row = session.execute(
        select(Talk)
        .where(Talk.talk_id == talk_id)
        .where(
            (Talk.status == "pending")
            | ((Talk.status == "claimed") & (Talk.claimed_at < stale))
        )
        .with_for_update(skip_locked=True),
    ).scalar_one_or_none()

    if row is None:
        return False

    row.status = "claimed"
    row.claimed_by = worker_id
    row.claimed_at = now
    session.commit()
    return True


def claim_next_talk(session: Session, worker_id: str) -> Talk | None:
    """Claim the next available pending talk. Returns None if nothing available."""
    now = datetime.now(timezone.utc)
    stale = now - timedelta(minutes=STALE_CLAIM_MINUTES)

    row = session.execute(
        select(Talk)
        .where(
            (Talk.status == "pending")
            | ((Talk.status == "claimed") & (Talk.claimed_at < stale))
        )
        .order_by(Talk.talk_id)
        .limit(1)
        .with_for_update(skip_locked=True),
    ).scalar_one_or_none()

    if row is None:
        return None

    row.status = "claimed"
    row.claimed_by = worker_id
    row.claimed_at = now
    session.commit()
    return row


def mark_done(session: Session, talk_id: int) -> None:
    """Mark a talk as successfully transcribed."""
    row = session.get(Talk, talk_id)
    if row:
        row.status = "done"
        row.claimed_by = None
        row.claimed_at = None
        session.commit()


def mark_error(session: Session, talk_id: int) -> None:
    """Release a failed talk back to pending so it can be retried."""
    row = session.get(Talk, talk_id)
    if row:
        row.status = "pending"
        row.claimed_by = None
        row.claimed_at = None
        session.commit()


def get_all_talks(session: Session) -> list[Talk]:
    """Return all talks. Used by the backend MetadataStore."""
    return list(session.execute(select(Talk)).scalars().all())


def get_known_talk_ids(session: Session) -> set[str]:
    """Return all known talk_ids as strings. Used by the scraper for dedup."""
    rows = session.execute(select(Talk.talk_id)).scalars().all()
    return {str(tid) for tid in rows}
