<script lang="ts">
    import { fetchDashboard } from "$lib/api";
    import type { DashboardResponse } from "$lib/types";
    import { onMount } from "svelte";
    import * as Card from "$lib/components/ui/card/index.js";
    import { Badge } from "$lib/components/ui/badge/index.js";
    import { Separator } from "$lib/components/ui/separator/index.js";

    let data = $state<DashboardResponse | null>(null);
    let error = $state("");
    let lastUpdated = $state<Date | null>(null);

    const STATUS_ORDER = ["done", "pending", "claimed", "skip_language", "no_mp3", "error"] as const;

    const STATUS_META: Record<string, { label: string; variant: "default" | "secondary" | "outline" | "destructive" }> = {
        done: { label: "Transcribed", variant: "default" },
        pending: { label: "Pending", variant: "secondary" },
        claimed: { label: "In Progress", variant: "outline" },
        skip_language: { label: "Skipped", variant: "secondary" },
        no_mp3: { label: "No Audio", variant: "secondary" },
        error: { label: "Error", variant: "destructive" },
    };

    async function load() {
        try {
            data = await fetchDashboard();
            lastUpdated = new Date();
            error = "";
        } catch (e) {
            error = e instanceof Error ? e.message : "Failed to load";
        }
    }

    onMount(() => {
        load();
        const interval = setInterval(load, 10_000);
        return () => clearInterval(interval);
    });

    let sortedStatuses = $derived(
        STATUS_ORDER.filter((s) => data && (data.status_counts[s] ?? 0) > 0)
    );

    let doneCount = $derived(data?.status_counts["done"] ?? 0);
    let totalTranscribable = $derived(
        (data?.total ?? 0) -
            (data?.status_counts["skip_language"] ?? 0) -
            (data?.status_counts["no_mp3"] ?? 0)
    );
    let progressPct = $derived(
        totalTranscribable > 0 ? (doneCount / totalTranscribable) * 100 : 0
    );

    function timeAgo(iso: string): string {
        const diff = Date.now() - new Date(iso).getTime();
        const mins = Math.floor(diff / 60000);
        if (mins < 1) return "just now";
        if (mins < 60) return `${mins}m ago`;
        const hrs = Math.floor(mins / 60);
        return `${hrs}h ${mins % 60}m ago`;
    }

    function uptime(iso: string): string {
        const diff = Date.now() - new Date(iso).getTime();
        const mins = Math.floor(diff / 60000);
        if (mins < 60) return `${mins}m`;
        const hrs = Math.floor(mins / 60);
        const rem = mins % 60;
        if (hrs < 24) return rem > 0 ? `${hrs}h ${rem}m` : `${hrs}h`;
        const days = Math.floor(hrs / 24);
        return `${days}d ${hrs % 24}h`;
    }
</script>

<svelte:head>
    <title>Dashboard &mdash; Pariyesan&#x101;</title>
</svelte:head>

<main class="mx-auto w-full max-w-[640px] px-6 pt-6 pb-16 max-sm:px-4">
    <!-- Header -->
    <header class="mb-8 text-center">
        <a
            href="/"
            class="font-heading text-xl font-semibold text-primary -tracking-wide no-underline hover:no-underline"
        >
            Pariyesan&#x101;
        </a>
        <p class="mt-1 text-sm text-muted-foreground">Transcription Pipeline</p>
    </header>

    {#if error}
        <Card.Root class="mb-6 border-destructive/30 bg-destructive/5">
            <Card.Content class="py-3 text-center text-sm text-destructive">
                {error}
            </Card.Content>
        </Card.Root>
    {/if}

    {#if data}
        <!-- Progress -->
        <section class="mb-8 fade-in" style="animation-delay: 0ms">
            <div class="mb-2 flex items-baseline justify-between">
                <span class="text-xs font-medium uppercase tracking-widest text-muted-foreground">
                    Progress
                </span>
                <span class="tabular-nums">
                    <span class="text-2xl font-semibold text-primary">{doneCount.toLocaleString()}</span>
                    <span class="text-sm text-muted-foreground">
                        / {totalTranscribable.toLocaleString()}
                    </span>
                    <span class="ml-1.5 text-xs text-muted-foreground">
                        ({progressPct.toFixed(1)}%)
                    </span>
                </span>
            </div>
            <div class="h-2 overflow-hidden rounded-full bg-secondary">
                <div
                    class="h-full rounded-full bg-primary transition-all duration-700 ease-out"
                    style="width: {progressPct}%"
                ></div>
            </div>
        </section>

        <!-- Status cards -->
        <section class="mb-8 fade-in" style="animation-delay: 80ms">
            <h2 class="mb-3 text-xs font-medium uppercase tracking-widest text-muted-foreground">
                By Status
            </h2>
            <div class="grid grid-cols-2 gap-3 sm:grid-cols-3">
                {#each sortedStatuses as status, i}
                    {@const count = data.status_counts[status] ?? 0}
                    {@const meta = STATUS_META[status]}
                    {@const pct = data.total > 0 ? (count / data.total) * 100 : 0}
                    <Card.Root class="card-item" style="animation-delay: {i * 60 + 160}ms">
                        <Card.Content class="p-4">
                            <div class="mb-1 flex items-center justify-between">
                                <span class="font-heading text-2xl font-semibold tabular-nums text-foreground">
                                    {count.toLocaleString()}
                                </span>
                                <Badge variant={meta.variant}>{meta.label}</Badge>
                            </div>
                            <div class="mt-2 h-1 overflow-hidden rounded-full bg-secondary">
                                <div
                                    class="h-full rounded-full transition-all duration-500 ease-out"
                                    class:bg-primary={status === "done"}
                                    class:bg-muted-foreground={status === "pending" || status === "skip_language" || status === "no_mp3"}
                                    class:bg-chart-2={status === "claimed"}
                                    class:bg-destructive={status === "error"}
                                    style="width: {pct}%"
                                ></div>
                            </div>
                        </Card.Content>
                    </Card.Root>
                {/each}
            </div>
        </section>

        <Separator class="mb-8" />

        <!-- Workers -->
        <section class="mb-8 fade-in" style="animation-delay: 160ms">
            <h2 class="mb-3 text-xs font-medium uppercase tracking-widest text-muted-foreground">
                Active Workers
            </h2>
            {#if data.workers.length === 0}
                <Card.Root>
                    <Card.Content class="py-8 text-center text-sm text-muted-foreground">
                        No workers currently active
                    </Card.Content>
                </Card.Root>
            {:else}
                <div class="flex flex-col gap-2">
                    {#each data.workers as worker, i}
                        <Card.Root class="card-item" style="animation-delay: {i * 60 + 240}ms">
                            <Card.Content class="flex items-center gap-3 p-4">
                                <span class="relative flex h-2.5 w-2.5 shrink-0">
                                    {#if worker.status === "processing"}
                                        <span class="absolute inline-flex h-full w-full animate-ping rounded-full bg-primary opacity-60"></span>
                                        <span class="relative inline-flex h-2.5 w-2.5 rounded-full bg-primary"></span>
                                    {:else}
                                        <span class="relative inline-flex h-2.5 w-2.5 rounded-full bg-muted-foreground"></span>
                                    {/if}
                                </span>
                                <div class="min-w-0 flex-1">
                                    <div class="truncate text-sm font-medium text-foreground">
                                        {worker.worker_id}
                                    </div>
                                    <div class="flex items-center gap-1.5 text-xs text-muted-foreground">
                                        {#if worker.status === "processing" && worker.current_talk_id}
                                            <span>talk #{worker.current_talk_id}</span>
                                            <span class="text-border">&middot;</span>
                                        {/if}
                                        <span>{worker.talks_completed} done</span>
                                        <span class="text-border">&middot;</span>
                                        <span>up {uptime(worker.started_at)}</span>
                                    </div>
                                </div>
                                <Badge variant={worker.status === "processing" ? "default" : "outline"} class="shrink-0">
                                    {worker.status === "processing" ? "processing" : "idle"}
                                </Badge>
                            </Card.Content>
                        </Card.Root>
                    {/each}
                </div>
            {/if}
        </section>

        <!-- Footer -->
        <footer class="text-center text-xs text-muted-foreground fade-in" style="animation-delay: 240ms">
            {data.total.toLocaleString()} talks tracked
            {#if lastUpdated}
                &middot; updated {lastUpdated.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}
            {/if}
        </footer>
    {:else if !error}
        <div class="flex flex-col items-center gap-3 py-16">
            <div class="h-6 w-6 animate-spin rounded-full border-2 border-secondary border-t-primary"></div>
            <span class="text-sm text-muted-foreground">Connecting...</span>
        </div>
    {/if}
</main>

<style>
    .fade-in {
        opacity: 0;
        transform: translateY(8px);
        animation: fade-up 0.4s ease forwards;
    }

    :global(.card-item) {
        opacity: 0;
        transform: translateY(6px);
        animation: fade-up 0.35s ease forwards;
    }

    @keyframes fade-up {
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }
</style>
