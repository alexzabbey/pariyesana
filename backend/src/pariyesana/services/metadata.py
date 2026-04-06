from pariyesana_db import get_all_talks, get_engine, get_session

from pariyesana.config import settings
from pariyesana.models.schemas import (
    CenterSummary,
    LanguageSummary,
    TalkMetadata,
    TeacherSummary,
)

DHARMASEED_BASE = "https://dharmaseed.org"


def _parse_duration(d: str) -> int:
    """Parse 'MM:SS' or 'H:MM:SS' to seconds."""
    if not d:
        return 0
    parts = d.split(":")
    if len(parts) == 2:
        return int(parts[0]) * 60 + int(parts[1])
    if len(parts) == 3:
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    return 0


class MetadataStore:
    def __init__(self) -> None:
        self.talks: dict[int, TalkMetadata] = {}

    def load(self) -> None:
        engine = get_engine(settings.database_url)
        Session = get_session(engine)
        with Session() as session:
            rows = get_all_talks(session)

        for row in rows:
            talk_id = row.talk_id
            teacher_id = int(row.teacher_id) if row.teacher_id and row.teacher_id.isdigit() else 0
            duration = row.duration or ""
            mp3_url = row.mp3_url or ""
            self.talks[talk_id] = TalkMetadata(
                talk_id=talk_id,
                date=row.date or "",
                title=row.title or "",
                teacher=row.teacher or "",
                teacher_id=teacher_id,
                center=row.center or "",
                duration=duration,
                duration_secs=_parse_duration(duration),
                description=row.description or "",
                mp3_url=mp3_url,
                audio_url=f"{DHARMASEED_BASE}{mp3_url}" if mp3_url else "",
                dharmaseed_url=f"{DHARMASEED_BASE}/talks/{talk_id}/",
                language=row.language or "English",
                transcribed=row.status or "",
            )

    def get_talk(self, talk_id: int) -> TalkMetadata | None:
        return self.talks.get(talk_id)

    def _filtered_talks(
        self,
        teacher: str | None = None,
        center: str | None = None,
        language: str | None = None,
    ) -> list[TalkMetadata]:
        talks = self.talks.values()
        if teacher:
            talks = [t for t in talks if t.teacher == teacher]
        if center:
            talks = [t for t in talks if t.center == center]
        if language:
            talks = [t for t in talks if t.language == language]
        return list(talks)

    def list_teachers(
        self,
        center: str | None = None,
        language: str | None = None,
    ) -> list[TeacherSummary]:
        counts: dict[str, int] = {}
        for t in self._filtered_talks(center=center, language=language):
            if t.teacher:
                counts[t.teacher] = counts.get(t.teacher, 0) + 1
        return sorted(
            [TeacherSummary(name=n, talk_count=c) for n, c in counts.items()],
            key=lambda x: x.talk_count,
            reverse=True,
        )

    def list_centers(
        self,
        teacher: str | None = None,
        language: str | None = None,
    ) -> list[CenterSummary]:
        counts: dict[str, int] = {}
        for t in self._filtered_talks(teacher=teacher, language=language):
            if t.center:
                counts[t.center] = counts.get(t.center, 0) + 1
        return sorted(
            [CenterSummary(name=n, talk_count=c) for n, c in counts.items()],
            key=lambda x: x.talk_count,
            reverse=True,
        )

    def list_languages(
        self,
        teacher: str | None = None,
        center: str | None = None,
    ) -> list[LanguageSummary]:
        counts: dict[str, int] = {}
        for t in self._filtered_talks(teacher=teacher, center=center):
            if t.language:
                counts[t.language] = counts.get(t.language, 0) + 1
        return sorted(
            [LanguageSummary(name=n, talk_count=c) for n, c in counts.items()],
            key=lambda x: x.talk_count,
            reverse=True,
        )


metadata_store = MetadataStore()
