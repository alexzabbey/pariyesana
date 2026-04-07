from datetime import datetime, timedelta, timezone

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from pariyesana_db.models import Talk, Worker

STALE_CLAIM_MINUTES = 60
WORKER_TIMEOUT_MINUTES = 2


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


def worker_heartbeat(
    session: Session,
    worker_id: str,
    status: str = "idle",
    current_talk_id: int | None = None,
    inc_completed: bool = False,
) -> None:
    """Upsert a worker heartbeat. Call at key lifecycle points."""
    now = datetime.now(timezone.utc)
    stmt = pg_insert(Worker).values(
        worker_id=worker_id,
        status=status,
        current_talk_id=current_talk_id,
        last_heartbeat=now,
        started_at=now,
        talks_completed=1 if inc_completed else 0,
    ).on_conflict_do_update(
        index_elements=["worker_id"],
        set_={
            "status": status,
            "current_talk_id": current_talk_id,
            "last_heartbeat": now,
            **({"talks_completed": Worker.talks_completed + 1} if inc_completed else {}),
        },
    )
    session.execute(stmt)
    session.commit()


def get_dashboard_stats(session: Session) -> dict:
    """Return aggregated stats for the dashboard."""
    from sqlalchemy import func as sa_func

    # Status counts
    status_rows = session.execute(
        select(Talk.status, sa_func.count()).group_by(Talk.status)
    ).all()
    status_counts = {status: count for status, count in status_rows}

    # Workers seen in the last 10 minutes (frontend determines active/inactive
    # based on whether last_heartbeat is within 2 minutes)
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=10)
    recent_workers = session.execute(
        select(Worker)
        .where(Worker.last_heartbeat >= cutoff)
        .order_by(Worker.started_at)
    ).scalars().all()
    workers = [
        {
            "worker_id": w.worker_id,
            "status": w.status,
            "current_talk_id": w.current_talk_id,
            "last_heartbeat": w.last_heartbeat.isoformat(),
            "started_at": w.started_at.isoformat(),
            "talks_completed": w.talks_completed,
        }
        for w in recent_workers
    ]

    # Last 5 talks whose status changed (by updated_at)
    recent_talks = session.execute(
        select(Talk)
        .order_by(Talk.updated_at.desc())
        .limit(5)
    ).scalars().all()
    recent = [
        {
            "talk_id": t.talk_id,
            "title": t.title,
            "status": t.status,
            "updated_at": t.updated_at.isoformat(),
        }
        for t in recent_talks
    ]

    # Total count
    total = sum(status_counts.values())

    return {
        "total": total,
        "status_counts": status_counts,
        "workers": workers,
        "recent_talks": recent,
    }
