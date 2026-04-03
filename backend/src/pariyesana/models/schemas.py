from pydantic import BaseModel


class SearchResult(BaseModel):
    talk_id: int
    title: str
    teacher: str
    date: str
    center: str
    language: str
    text: str
    start_time: float
    end_time: float
    dharmaseed_url: str
    audio_url: str
    score: float


class Snippet(BaseModel):
    text: str
    start_time: float
    end_time: float
    score: float


class TalkSearchResult(BaseModel):
    talk_id: int
    title: str
    teacher: str
    date: str
    center: str
    language: str
    description: str
    duration: str
    dharmaseed_url: str
    audio_url: str
    score: float
    snippets: list[Snippet]


class SearchResponse(BaseModel):
    results: list[TalkSearchResult]
    query: str
    total: int


class TeacherSummary(BaseModel):
    name: str
    talk_count: int


class CenterSummary(BaseModel):
    name: str
    talk_count: int


class LanguageSummary(BaseModel):
    name: str
    talk_count: int


class FiltersResponse(BaseModel):
    teachers: list[TeacherSummary]
    centers: list[CenterSummary]
    languages: list[LanguageSummary]


class HealthResponse(BaseModel):
    status: str
    qdrant_connected: bool
    metadata_loaded: bool
    talks_count: int
    passages_count: int


class TalkMetadata(BaseModel):
    talk_id: int
    date: str
    title: str
    teacher: str
    teacher_id: int
    center: str
    duration: str
    duration_secs: int
    description: str
    mp3_url: str
    audio_url: str
    dharmaseed_url: str
    language: str
    transcribed: str
