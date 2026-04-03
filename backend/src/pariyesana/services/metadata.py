import polars as pl

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

    def load(self, csv_path: str | None = None) -> None:
        path = csv_path or settings.metadata_csv_path
        df = pl.read_csv(path, schema_overrides={"talk_id": pl.Utf8, "teacher_id": pl.Utf8})
        for row in df.iter_rows(named=True):
            talk_id = int(row["talk_id"])
            teacher_id = int(row["teacher_id"]) if row.get("teacher_id") else 0
            duration = row.get("duration", "") or ""
            mp3_url = row.get("mp3_url", "") or ""
            self.talks[talk_id] = TalkMetadata(
                talk_id=talk_id,
                date=row.get("date", "") or "",
                title=row.get("title", "") or "",
                teacher=row.get("teacher", "") or "",
                teacher_id=teacher_id,
                center=row.get("center", "") or "",
                duration=duration,
                duration_secs=_parse_duration(duration),
                description=row.get("description", "") or "",
                mp3_url=mp3_url,
                audio_url=f"{DHARMASEED_BASE}{mp3_url}" if mp3_url else "",
                dharmaseed_url=f"{DHARMASEED_BASE}/talks/{talk_id}/",
                language=row.get("language", "") or "English",
                transcribed=row.get("transcribed", "") or "",
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
