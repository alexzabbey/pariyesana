import type { SearchResponse, FiltersResponse } from './types';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export async function search(params: {
	q: string;
	teacher?: string;
	center?: string;
	language?: string;
	date_from?: string;
	date_to?: string;
	duration_min?: number;
	duration_max?: number;
	limit?: number;
}): Promise<SearchResponse> {
	const url = new URL(`${API_URL}/api/search`);
	url.searchParams.set('q', params.q);
	if (params.teacher) url.searchParams.set('teacher', params.teacher);
	if (params.center) url.searchParams.set('center', params.center);
	if (params.language) url.searchParams.set('language', params.language);
	if (params.date_from) url.searchParams.set('date_from', params.date_from);
	if (params.date_to) url.searchParams.set('date_to', params.date_to);
	if (params.duration_min) url.searchParams.set('duration_min', String(params.duration_min));
	if (params.duration_max) url.searchParams.set('duration_max', String(params.duration_max));
	if (params.limit) url.searchParams.set('limit', String(params.limit));

	const res = await fetch(url.toString());
	if (!res.ok) throw new Error(`Search failed: ${res.status}`);
	return res.json();
}

export async function fetchFilters(params?: {
	teacher?: string;
	center?: string;
	language?: string;
}): Promise<FiltersResponse> {
	const url = new URL(`${API_URL}/api/filters`);
	if (params?.teacher) url.searchParams.set('teacher', params.teacher);
	if (params?.center) url.searchParams.set('center', params.center);
	if (params?.language) url.searchParams.set('language', params.language);

	const res = await fetch(url.toString());
	if (!res.ok) throw new Error(`Failed to load filters: ${res.status}`);
	return res.json();
}
