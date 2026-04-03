<script lang="ts">
    import { search } from "$lib/api";
    import type { TalkSearchResult } from "$lib/types";
    import { Button } from "$lib/components/ui/button/index.js";
    import SearchBar from "$lib/components/search-bar.svelte";
    import SearchResultCard from "$lib/components/search-result-card.svelte";
    import LotusLoader from "$lib/components/lotus-loader.svelte";

    let query = $state("");
    let results = $state<TalkSearchResult[]>([]);
    let hasSearched = $state(false);
    let loading = $state(false);
    let error = $state("");

    let selectedTeacher = $state("");
    let selectedCenter = $state("");
    let selectedLanguage = $state("");
    let durationMin = $state(0);
    let durationMax = $state(120);

    const allExamples = [
        "dependent origination",
        "working with fear",
        "metta meditation",
        "emptiness of self",
        "mindfulness of breathing",
        "letting go of attachment",
        "compassion in daily life",
        "the five hindrances",
        "what is nibbana",
        "dealing with anger",
        "the nature of suffering",
        "walking meditation",
        "equanimity practice",
        "body scan meditation",
        "impermanence of all things",
        "the eightfold path",
        "forgiveness practice",
        "cultivating joy",
        "working with grief",
        "the four noble truths",
        "concentration and jhana",
        "self and not-self",
        "patience and endurance",
        "generosity as practice",
        "right speech",
        "doubt on the path",
        "awareness of awareness",
        "refuge in the dharma",
        "desire and craving",
        "karma and intention",
    ];

    const examples = allExamples
        .sort(() => Math.random() - 0.5)
        .slice(0, 7);

    async function doSearch(q?: string) {
        const searchQuery = q ?? query;
        if (!searchQuery.trim()) return;
        query = searchQuery;
        hasSearched = true;
        loading = true;
        error = "";
        try {
            const res = await search({
                q: searchQuery,
                teacher: selectedTeacher || undefined,
                center: selectedCenter || undefined,
                language: selectedLanguage || undefined,
                duration_min: durationMin || undefined,
                duration_max: durationMax < 120 ? durationMax : undefined,
            });
            results = res.results;
        } catch (e) {
            error = e instanceof Error ? e.message : "Search failed";
            results = [];
        } finally {
            loading = false;
        }
    }

    function handleSearch() {
        if (hasSearched && query.trim()) doSearch();
    }

    function goHome() {
        hasSearched = false;
        results = [];
        error = "";
    }
</script>

<svelte:head>
    <title>Pariyesan&#x101; &mdash; Search the Dharma</title>
</svelte:head>

<main
    class="search-layout px-6 max-sm:px-4"
    class:landed={!hasSearched}
    class:docked={hasSearched}
>
    <div
        class="search-container mx-auto w-full max-w-[600px]"
    >
        <!-- Header -->
        <header class="search-header text-center">
            <button
                class="title-text font-heading font-semibold text-primary -tracking-wide shrink-0 border-none bg-transparent p-0"
                class:cursor-pointer={hasSearched}
                onclick={hasSearched ? goHome : undefined}
            >
                Pariyesan&#x101;
            </button>
            <p class="subtitle text-muted-foreground">
                Search by meaning across dharma talks from <a
                    href="https://dharmaseed.org"
                    target="_blank"
                    rel="noopener">dharmaseed.org</a
                >
            </p>
        </header>

        <!-- Search bar -->
        <div class="search-bar-wrapper">
            <SearchBar
                bind:query
                bind:selectedTeacher
                bind:selectedCenter
                bind:selectedLanguage
                bind:durationMin
                bind:durationMax
                {loading}
                onsearch={() => doSearch()}
            />
        </div>

        <!-- Landing extras -->
        <div class="landing-extras" class:visible={!hasSearched}>
            <div class="text-center text-muted-foreground">
                <div
                    class="mt-4 flex flex-wrap items-center justify-center gap-2"
                >
                    <span class="text-sm text-muted-foreground">Try:</span>
                    {#each examples as ex}
                        <Button
                            variant="outline"
                            size="sm"
                            class="rounded-full"
                            onclick={() => doSearch(ex)}>{ex}</Button
                        >
                    {/each}
                </div>
            </div>
        </div>

        <!-- Results -->
        <div class="results-section" class:visible={hasSearched}>
            {#if error}
                <div class="p-8 text-center text-destructive">{error}</div>
            {:else if loading}
                <div class="flex flex-col items-center gap-3 py-16">
                    <LotusLoader size={56} />
                    <span class="text-sm text-muted-foreground">Searching...</span>
                </div>
            {:else if hasSearched && results.length === 0}
                <div class="py-12 text-center text-muted-foreground">
                    No results found for &ldquo;{query}&rdquo;
                </div>
            {:else}
                <div class="flex flex-col gap-4">
                    {#each results as r, i (r.talk_id)}
                        <div class="result-item" style="animation-delay: {i * 60}ms">
                            <SearchResultCard result={r} />
                        </div>
                    {/each}
                </div>
            {/if}
        </div>
    </div>
</main>

<style>
    .search-layout {
        display: flex;
        flex-direction: column;
        align-items: center;
        transition: padding-top 0.5s cubic-bezier(0.4, 0, 0.2, 1);
    }

    .search-layout.landed {
        min-height: 100svh;
        padding-top: calc(50svh - 120px);
        padding-bottom: 0;
    }

    .search-layout.docked {
        min-height: auto;
        padding-top: 1.5rem;
        padding-bottom: 4rem;
    }

    .search-container {
        transition: max-width 0.5s cubic-bezier(0.4, 0, 0.2, 1);
    }

    .search-header {
        transition: margin-bottom 0.4s cubic-bezier(0.4, 0, 0.2, 1);
    }

    .landed .search-header {
        margin-bottom: 2rem;
    }

    .docked .search-header {
        margin-bottom: 0.25rem;
    }

    .title-text {
        transition: font-size 0.4s cubic-bezier(0.4, 0, 0.2, 1);
    }

    .landed .title-text {
        font-size: 3rem;
        margin-bottom: 0.5rem;
    }

    .docked .title-text {
        font-size: 1.25rem;
    }

    .subtitle {
        overflow: hidden;
    }

    .landed .subtitle {
        display: block;
    }

    .docked .subtitle {
        display: none;
    }

    .search-bar-wrapper {
        transition: margin-bottom 0.4s cubic-bezier(0.4, 0, 0.2, 1);
    }

    .landed .search-bar-wrapper {
        margin-bottom: 1.5rem;
    }

    .docked .search-bar-wrapper {
        margin-bottom: 1rem;
    }

    .docked .search-header {
        text-align: center;
    }

    .landing-extras {
        opacity: 0;
        max-height: 0;
        overflow: hidden;
        transition:
            opacity 0.3s ease,
            max-height 0.3s ease;
    }

    .landing-extras.visible {
        opacity: 1;
        max-height: 16rem;
    }

    .results-section {
        opacity: 0;
        transition: opacity 0.4s ease 0.2s;
    }

    .results-section.visible {
        opacity: 1;
    }

    .result-item {
        opacity: 0;
        transform: translateY(8px);
        animation: fade-up 0.35s ease forwards;
    }

    @keyframes fade-up {
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }

    @media (max-width: 640px) {
        .landed .title-text {
            font-size: 2.25rem;
        }
    }
</style>
