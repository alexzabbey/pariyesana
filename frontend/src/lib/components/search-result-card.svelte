<script lang="ts">
    import type { TalkSearchResult } from "$lib/types";
    import * as Card from "$lib/components/ui/card/index.js";
    import { Badge } from "$lib/components/ui/badge/index.js";

    let { result }: { result: TalkSearchResult } = $props();

    let expanded = $state(false);

    const descriptionPreview = $derived(() => {
        if (!result.description) return { short: "", full: "", truncated: false };
        const sentences = result.description.match(/[^.!?]+[.!?]+/g) || [result.description];
        const short = sentences.slice(0, 2).join("").trim();
        return {
            short,
            full: result.description,
            truncated: sentences.length > 2,
        };
    });

    function formatTime(seconds: number): string {
        const h = Math.floor(seconds / 3600);
        const m = Math.floor((seconds % 3600) / 60);
        const s = Math.floor(seconds % 60);
        if (h > 0)
            return `${h}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
        return `${m}:${String(s).padStart(2, "0")}`;
    }
</script>

<a
    href={result.dharmaseed_url}
    target="_blank"
    rel="noopener"
    class="block no-underline hover:no-underline"
>
    <Card.Root class="group relative cursor-pointer transition-shadow hover:shadow-md">
        <Card.Header class="pb-2 group/card">
            <div class="flex flex-wrap items-center gap-1.5 text-sm text-muted-foreground">
                <span class="font-medium text-foreground/70">{result.teacher}</span>
                {#if result.center}
                    <span class="text-border">&middot;</span>
                    <span>{result.center}</span>
                {/if}
                {#if result.date}
                    <span class="text-border">&middot;</span>
                    <span>{result.date}</span>
                {/if}
                {#if result.duration}
                    <span class="text-border">&middot;</span>
                    <span>{result.duration}</span>
                {/if}
                {#if result.language && result.language !== "English"}
                    <span class="text-border">&middot;</span>
                    <span>{result.language}</span>
                {/if}
            </div>
            <Card.Title class="font-heading text-xl leading-snug text-foreground">
                {result.title}
            </Card.Title>
            {#if result.description}
                {@const desc = descriptionPreview()}
                <p class="mt-1 text-sm leading-relaxed text-muted-foreground">
                    {#if expanded || !desc.truncated}
                        {desc.full}
                    {:else}
                        {desc.short}
                        {" "}<button
                            class="inline cursor-pointer border-none bg-transparent p-0 text-sm text-primary hover:underline"
                            onclick={(e) => { e.preventDefault(); e.stopPropagation(); expanded = true; }}
                        >see more...</button>
                    {/if}
                </p>
            {/if}
        </Card.Header>
        <Card.Content class="pb-3">
            <div class="flex flex-col gap-2">
                {#if result.snippets[0]}
                    {@const snippet = result.snippets[0]}
                    <div class="flex items-baseline gap-3">
                        <span class="min-w-[3.5em] shrink-0 text-sm tabular-nums text-primary">
                            {formatTime(snippet.start_time)}
                        </span>
                        <p class="text-base italic leading-relaxed text-muted-foreground font-heading">
                            &ldquo;{snippet.text}&rdquo;
                        </p>
                    </div>
                {/if}
            </div>
        </Card.Content>
        <span class="absolute bottom-3 right-4 text-xs font-medium text-primary opacity-0 transition-opacity group-hover:opacity-100">
            Listen on dharmaseed.org &rarr;
        </span>
    </Card.Root>
</a>
