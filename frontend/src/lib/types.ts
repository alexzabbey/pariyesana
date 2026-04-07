export interface Snippet {
	text: string;
	start_time: number;
	end_time: number;
	score: number;
}

export interface TalkSearchResult {
	talk_id: number;
	title: string;
	teacher: string;
	date: string;
	center: string;
	language: string;
	description: string;
	duration: string;
	dharmaseed_url: string;
	audio_url: string;
	score: number;
	snippets: Snippet[];
}

export interface SearchResponse {
	results: TalkSearchResult[];
	query: string;
	total: number;
}

export interface TeacherSummary {
	name: string;
	talk_count: number;
}

export interface CenterSummary {
	name: string;
	talk_count: number;
}

export interface LanguageSummary {
	name: string;
	talk_count: number;
}

export interface FiltersResponse {
	teachers: TeacherSummary[];
	centers: CenterSummary[];
	languages: LanguageSummary[];
}

export interface WorkerInfo {
	worker_id: string;
	status: string;
	current_talk_id: number | null;
	last_heartbeat: string;
	started_at: string;
	talks_completed: number;
}

export interface RecentTalk {
	talk_id: number;
	title: string;
	status: string;
	updated_at: string;
}

export interface DashboardResponse {
	total: number;
	status_counts: Record<string, number>;
	workers: WorkerInfo[];
	recent_talks: RecentTalk[];
}
