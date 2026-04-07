from fastapi import APIRouter, HTTPException, Query

from pariyesana_db import get_dashboard_stats, get_engine, get_session

from pariyesana.config import settings
from pariyesana.models.schemas import (
    DashboardResponse,
    FiltersResponse,
    HealthResponse,
    RecentTalk,
    SearchResponse,
    WorkerInfo,
)
from pariyesana.services.metadata import metadata_store
from pariyesana.services.search import search_service

router = APIRouter(prefix="/api")


@router.get("/search", response_model=SearchResponse)
async def search(
    q: str = Query(..., min_length=1, max_length=500),
    teacher: str | None = Query(None),
    center: str | None = Query(None),
    language: str | None = Query(None),
    date_from: str | None = Query(None),
    date_to: str | None = Query(None),
    duration_min: int | None = Query(None, description="Min duration in minutes"),
    duration_max: int | None = Query(None, description="Max duration in minutes"),
    limit: int = Query(20, ge=1, le=100),
) -> SearchResponse:
    if not search_service.is_connected():
        raise HTTPException(503, "Search service unavailable")

    results = search_service.search(
        query=q,
        teacher=teacher,
        center=center,
        language=language,
        date_from=date_from,
        date_to=date_to,
        limit=limit * 3 if (duration_min or duration_max) else limit,
    )

    # Post-filter by duration
    if duration_min is not None or duration_max is not None:
        min_secs = (duration_min or 0) * 60
        max_secs = (duration_max or 9999) * 60
        filtered = []
        for r in results:
            talk = metadata_store.get_talk(r.talk_id)
            if talk and min_secs <= talk.duration_secs <= max_secs:
                filtered.append(r)
        results = filtered[:limit]

    return SearchResponse(results=results, query=q, total=len(results))


@router.get("/filters", response_model=FiltersResponse)
async def filters(
    teacher: str | None = Query(None),
    center: str | None = Query(None),
    language: str | None = Query(None),
) -> FiltersResponse:
    return FiltersResponse(
        teachers=metadata_store.list_teachers(center=center, language=language),
        centers=metadata_store.list_centers(teacher=teacher, language=language),
        languages=metadata_store.list_languages(teacher=teacher, center=center),
    )


@router.get("/dashboard", response_model=DashboardResponse)
async def dashboard() -> DashboardResponse:
    engine = get_engine(settings.database_url)
    Session = get_session(engine)
    with Session() as session:
        stats = get_dashboard_stats(session)
    return DashboardResponse(
        total=stats["total"],
        status_counts=stats["status_counts"],
        workers=[WorkerInfo(**w) for w in stats["workers"]],
        recent_talks=[RecentTalk(**t) for t in stats["recent_talks"]],
    )


@router.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    return HealthResponse(
        status="ok" if search_service.is_connected() else "degraded",
        qdrant_connected=search_service.is_connected(),
        metadata_loaded=len(metadata_store.talks) > 0,
        talks_count=len(metadata_store.talks),
        passages_count=search_service.count_passages(),
    )
