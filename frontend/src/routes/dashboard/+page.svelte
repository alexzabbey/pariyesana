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

    function isWorkerActive(iso: string): boolean {
        return Date.now() - new Date(iso).getTime() < 2 * 60 * 1000;
    }
</script>

<svelte:head>
    <title>Dashboard &mdash; Pariyesan&#x101;</title>
</svelte:head>

<main class="mx-auto w-full max-w-[960px] px-6 pt-4 pb-8 max-sm:px-4">
    <!-- Header -->
    <header class="mb-4 text-center">
        <a
            href="/"
            class="font-heading text-lg font-semibold text-primary -tracking-wide no-underline hover:no-underline"
        >
            Pariyesan&#x101;
        </a>
        <p class="text-xs text-muted-foreground">Transcription Pipeline</p>
    </header>

    {#if error}
        <Card.Root class="mb-4 border-destructive/30 bg-destructive/5">
            <Card.Content class="py-2 text-center text-sm text-destructive">
                {error}
            </Card.Content>
        </Card.Root>
    {/if}

    {#if data}
        <!-- Progress -->
        <section class="mb-4 fade-in" style="animation-delay: 0ms">
            <div class="mb-1.5 flex items-baseline justify-between">
                <span class="text-xs font-medium uppercase tracking-widest text-muted-foreground">
                    Progress
                </span>
                <span class="tabular-nums">
                    <span class="text-xl font-semibold text-primary">{doneCount.toLocaleString()}</span>
                    <span class="text-sm text-muted-foreground">
                        / {totalTranscribable.toLocaleString()}
                    </span>
                    <span class="ml-1 text-xs text-muted-foreground">
                        ({progressPct.toFixed(1)}%)
                    </span>
                </span>
            </div>
            <div class="h-1.5 overflow-hidden rounded-full bg-secondary">
                <div
                    class="h-full rounded-full bg-primary transition-all duration-700 ease-out"
                    style="width: {progressPct}%"
                ></div>
            </div>
        </section>

        <!-- Status cards -->
        <section class="mb-4 fade-in" style="animation-delay: 60ms">
            <div class="grid grid-cols-3 gap-2 sm:grid-cols-6">
                {#each sortedStatuses as status, i}
                    {@const count = data.status_counts[status] ?? 0}
                    {@const meta = STATUS_META[status]}
                    <Card.Root class="card-item" style="animation-delay: {i * 40 + 100}ms">
                        <Card.Content class="px-3 py-2">
                            <div class="flex items-center justify-between gap-1">
                                <span class="font-heading text-lg font-semibold tabular-nums text-foreground">
                                    {count.toLocaleString()}
                                </span>
                                <Badge variant={meta.variant} class="text-[10px] px-1.5 py-0">{meta.label}</Badge>
                            </div>
                        </Card.Content>
                    </Card.Root>
                {/each}
            </div>
        </section>

        <Separator class="mb-4" />

        <!-- Workers + Recent activity side by side -->
        <div class="grid grid-cols-1 gap-4 sm:grid-cols-2 fade-in" style="animation-delay: 120ms">
            <!-- Workers -->
            <section>
                <h2 class="mb-2 text-xs font-medium uppercase tracking-widest text-muted-foreground">
                    Workers
                </h2>
                {#if data.workers.length === 0}
                    <Card.Root>
                        <Card.Content class="py-6 text-center text-sm text-muted-foreground">
                            No workers
                        </Card.Content>
                    </Card.Root>
                {:else}
                    <div class="flex flex-col gap-1.5">
                        {#each data.workers as worker, i}
                            {@const active = isWorkerActive(worker.last_heartbeat)}
                            <Card.Root class="card-item" style="animation-delay: {i * 40 + 160}ms">
                                <Card.Content class="flex items-center gap-2.5 px-3 py-2">
                                    <span class="relative flex h-2 w-2 shrink-0">
                                        {#if active}
                                            <span class="absolute inline-flex h-full w-full animate-ping rounded-full bg-primary opacity-60"></span>
                                            <span class="relative inline-flex h-2 w-2 rounded-full bg-primary"></span>
                                        {:else}
                                            <span class="relative inline-flex h-2 w-2 rounded-full bg-muted-foreground"></span>
                                        {/if}
                                    </span>
                                    <div class="min-w-0 flex-1">
                                        <div class="truncate text-sm font-medium text-foreground">
                                            {worker.worker_id}
                                        </div>
                                        <div class="flex items-center gap-1 text-[11px] text-muted-foreground">
                                            <span>{worker.talks_completed} done</span>
                                            <span class="text-border">&middot;</span>
                                            <span>up {uptime(worker.started_at)}</span>
                                            {#if !active}
                                                <span class="text-border">&middot;</span>
                                                <span>{timeAgo(worker.last_heartbeat)}</span>
                                            {/if}
                                        </div>
                                    </div>
                                    <Badge variant={active ? "default" : "outline"} class="shrink-0 text-[10px] px-1.5 py-0">
                                        {active ? "active" : "inactive"}
                                    </Badge>
                                </Card.Content>
                            </Card.Root>
                        {/each}
                    </div>
                {/if}
            </section>

            <!-- Recent activity -->
            <section>
                <h2 class="mb-2 text-xs font-medium uppercase tracking-widest text-muted-foreground">
                    Recent Activity
                </h2>
                {#if data.recent_talks.length === 0}
                    <Card.Root>
                        <Card.Content class="py-6 text-center text-sm text-muted-foreground">
                            No recent activity
                        </Card.Content>
                    </Card.Root>
                {:else}
                    <div class="flex flex-col gap-1">
                        {#each data.recent_talks as talk, i}
                            {@const meta = STATUS_META[talk.status]}
                            <div class="flex items-center gap-2 rounded-md border px-2.5 py-1.5 card-item" style="animation-delay: {i * 40 + 200}ms">
                                <div class="min-w-0 flex-1">
                                    <div class="truncate text-sm text-foreground">
                                        #{talk.talk_id}
                                        {#if talk.title}
                                            <span class="text-muted-foreground">&mdash;</span>
                                            <span class="text-muted-foreground">{talk.title}</span>
                                        {/if}
                                    </div>
                                    <div class="flex items-center gap-1 text-[11px] text-muted-foreground">
                                        <span>{timeAgo(talk.updated_at)}</span>
                                        {#if talk.claimed_by}
                                            <span class="text-border">&middot;</span>
                                            <span>{talk.claimed_by}</span>
                                        {/if}
                                    </div>
                                </div>
                                {#if meta}
                                    <Badge variant={meta.variant} class="shrink-0 text-[10px] px-1.5 py-0">{meta.label}</Badge>
                                {/if}
                            </div>
                        {/each}
                    </div>
                {/if}
            </section>
        </div>

        <!-- Footer -->
        <footer class="mt-4 text-center text-xs text-muted-foreground fade-in" style="animation-delay: 200ms">
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
