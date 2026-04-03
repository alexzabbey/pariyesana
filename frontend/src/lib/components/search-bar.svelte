<script lang="ts">
    import { tick } from "svelte";
    import CheckIcon from "@lucide/svelte/icons/check";
    import ChevronDownIcon from "@lucide/svelte/icons/chevron-down";
    import { fetchFilters } from "$lib/api";
    import type { FiltersResponse } from "$lib/types";
    import { cn } from "$lib/utils.js";
    import { Button } from "$lib/components/ui/button/index.js";
    import { Input } from "$lib/components/ui/input/index.js";
    import * as Command from "$lib/components/ui/command/index.js";
    import * as Popover from "$lib/components/ui/popover/index.js";
    import { Slider } from "$lib/components/ui/slider/index.js";

    let {
        query = $bindable(""),
        selectedTeacher = $bindable(""),
        selectedCenter = $bindable(""),
        selectedLanguage = $bindable(""),
        durationMin = $bindable(0),
        durationMax = $bindable(120),
        loading = false,
        onsearch,
    }: {
        query: string;
        selectedTeacher: string;
        selectedCenter: string;
        selectedLanguage: string;
        durationMin: number;
        durationMax: number;
        loading: boolean;
        onsearch: () => void;
    } = $props();

    let filters = $state<FiltersResponse | null>(null);
    let filtersLoaded = $state(false);

    let teacherOpen = $state(false);
    let centerOpen = $state(false);
    let languageOpen = $state(false);
    let durationOpen = $state(false);

    let durationRange = $state([durationMin, durationMax]);
    let hasDurationFilter = $derived(durationRange[0] > 0 || durationRange[1] < 120);

    let hasActiveFilters = $derived(
        !!selectedTeacher || !!selectedCenter || !!selectedLanguage || hasDurationFilter,
    );

    function formatDuration(mins: number): string {
        if (mins >= 120) return "2h+";
        if (mins >= 60) return `${Math.floor(mins / 60)}h${mins % 60 ? ` ${mins % 60}m` : ""}`;
        return `${mins}m`;
    }

    function applyDuration() {
        durationMin = durationRange[0];
        durationMax = durationRange[1];
        durationOpen = false;
        onsearch();
    }

    function loadFilters() {
        fetchFilters({
            teacher: selectedTeacher || undefined,
            center: selectedCenter || undefined,
            language: selectedLanguage || undefined,
        })
            .then((f) => {
                filters = f;
                filtersLoaded = true;
            })
            .catch(() => {});
    }

    function ensureFiltersLoaded() {
        if (!filtersLoaded) loadFilters();
    }

    function selectFilter(
        which: "teacher" | "center" | "language",
        value: string,
    ) {
        if (which === "teacher") {
            selectedTeacher = selectedTeacher === value ? "" : value;
            teacherOpen = false;
        } else if (which === "center") {
            selectedCenter = selectedCenter === value ? "" : value;
            centerOpen = false;
        } else {
            selectedLanguage = selectedLanguage === value ? "" : value;
            languageOpen = false;
        }
        loadFilters();
        tick().then(() => onsearch());
    }

    function clearFilter(which: "teacher" | "center" | "language") {
        if (which === "teacher") selectedTeacher = "";
        else if (which === "center") selectedCenter = "";
        else selectedLanguage = "";
        loadFilters();
        onsearch();
    }

    function clearAllFilters() {
        selectedTeacher = "";
        selectedCenter = "";
        selectedLanguage = "";
        durationRange = [0, 120];
        durationMin = 0;
        durationMax = 120;
        loadFilters();
        onsearch();
    }

    function handleKeydown(e: KeyboardEvent) {
        if (e.key === "Enter") onsearch();
    }
</script>

<div>
    <div class="flex gap-2">
        <Input
            type="text"
            bind:value={query}
            onkeydown={handleKeydown}
            placeholder="Search the dharma..."
            class="flex-1 h-11 !text-base"
        />
        <Button
            onclick={() => onsearch()}
            class="h-11 min-w-[90px] text-base"
        >
            Search
        </Button>
    </div>

    <div class="mt-2 flex flex-wrap items-center gap-3">
        <!-- Teacher filter -->
        <Popover.Root
            bind:open={teacherOpen}
            onOpenChange={(open) => {
                if (open) ensureFiltersLoaded();
            }}
        >
            <Popover.Trigger>
                {#snippet child({ props })}
                    <button
                        {...props}
                        class="inline-flex items-center gap-1 border-none bg-transparent p-0 text-sm cursor-pointer text-muted-foreground hover:text-foreground transition-colors whitespace-nowrap"
                        role="combobox"
                        aria-expanded={teacherOpen}
                    >
                        {#if selectedTeacher}
                            <span class="text-foreground"
                                >{selectedTeacher}</span
                            >
                        {:else}
                            Teacher
                        {/if}
                        <ChevronDownIcon class="size-3.5 opacity-50" />
                    </button>
                {/snippet}
            </Popover.Trigger>
            <Popover.Content class="w-[220px] p-0" align="start">
                <Command.Root>
                    <Command.Input placeholder="Search teachers..." />
                    <Command.List>
                        {#if filters}
                            <Command.Empty>No teacher found.</Command.Empty>
                            <Command.Group>
                                {#each filters.teachers as t (t.name)}
                                    <Command.Item
                                        value={t.name}
                                        onSelect={() =>
                                            selectFilter("teacher", t.name)}
                                    >
                                        <CheckIcon
                                            class={cn(
                                                "mr-2 size-3.5",
                                                selectedTeacher !== t.name &&
                                                    "text-transparent",
                                            )}
                                        />
                                        <span class="flex-1 truncate"
                                            >{t.name}</span
                                        >
                                        <span
                                            class="text-sm text-muted-foreground"
                                            >{t.talk_count}</span
                                        >
                                    </Command.Item>
                                {/each}
                            </Command.Group>
                        {:else}
                            <div
                                class="flex items-center justify-center gap-2 py-6 text-sm text-muted-foreground"
                            >
                                <span
                                    class="inline-block h-3.5 w-3.5 animate-spin rounded-full border-[1.5px] border-muted-foreground/20 border-t-muted-foreground"
                                ></span>
                                Loading
                            </div>
                        {/if}
                    </Command.List>
                </Command.Root>
            </Popover.Content>
        </Popover.Root>

        <!-- Center filter -->
        <Popover.Root
            bind:open={centerOpen}
            onOpenChange={(open) => {
                if (open) ensureFiltersLoaded();
            }}
        >
            <Popover.Trigger>
                {#snippet child({ props })}
                    <button
                        {...props}
                        class="inline-flex items-center gap-1 border-none bg-transparent p-0 text-sm cursor-pointer text-muted-foreground hover:text-foreground transition-colors whitespace-nowrap"
                        role="combobox"
                        aria-expanded={centerOpen}
                    >
                        {#if selectedCenter}
                            <span class="text-foreground">{selectedCenter}</span
                            >
                        {:else}
                            Center
                        {/if}
                        <ChevronDownIcon class="size-3.5 opacity-50" />
                    </button>
                {/snippet}
            </Popover.Trigger>
            <Popover.Content class="w-[220px] p-0" align="start">
                <Command.Root>
                    <Command.Input placeholder="Search centers..." />
                    <Command.List>
                        {#if filters}
                            <Command.Empty>No center found.</Command.Empty>
                            <Command.Group>
                                {#each filters.centers as c (c.name)}
                                    <Command.Item
                                        value={c.name}
                                        onSelect={() =>
                                            selectFilter("center", c.name)}
                                    >
                                        <CheckIcon
                                            class={cn(
                                                "mr-2 size-3.5",
                                                selectedCenter !== c.name &&
                                                    "text-transparent",
                                            )}
                                        />
                                        <span class="flex-1 truncate"
                                            >{c.name}</span
                                        >
                                        <span
                                            class="text-sm text-muted-foreground"
                                            >{c.talk_count}</span
                                        >
                                    </Command.Item>
                                {/each}
                            </Command.Group>
                        {:else}
                            <div
                                class="flex items-center justify-center gap-2 py-6 text-sm text-muted-foreground"
                            >
                                <span
                                    class="inline-block h-3.5 w-3.5 animate-spin rounded-full border-[1.5px] border-muted-foreground/20 border-t-muted-foreground"
                                ></span>
                                Loading
                            </div>
                        {/if}
                    </Command.List>
                </Command.Root>
            </Popover.Content>
        </Popover.Root>

        <!-- Language filter -->
        <Popover.Root
            bind:open={languageOpen}
            onOpenChange={(open) => {
                if (open) ensureFiltersLoaded();
            }}
        >
            <Popover.Trigger>
                {#snippet child({ props })}
                    <button
                        {...props}
                        class="inline-flex items-center gap-1 border-none bg-transparent p-0 text-sm cursor-pointer text-muted-foreground hover:text-foreground transition-colors whitespace-nowrap"
                        role="combobox"
                        aria-expanded={languageOpen}
                    >
                        {#if selectedLanguage}
                            <span class="text-foreground"
                                >{selectedLanguage}</span
                            >
                        {:else}
                            Language
                        {/if}
                        <ChevronDownIcon class="size-3.5 opacity-50" />
                    </button>
                {/snippet}
            </Popover.Trigger>
            <Popover.Content class="w-[200px] p-0" align="start">
                <Command.Root>
                    <Command.Input placeholder="Search languages..." />
                    <Command.List>
                        {#if filters}
                            <Command.Empty>No language found.</Command.Empty>
                            <Command.Group>
                                {#each filters.languages as l (l.name)}
                                    <Command.Item
                                        value={l.name}
                                        onSelect={() =>
                                            selectFilter("language", l.name)}
                                    >
                                        <CheckIcon
                                            class={cn(
                                                "mr-2 size-3.5",
                                                selectedLanguage !== l.name &&
                                                    "text-transparent",
                                            )}
                                        />
                                        <span class="flex-1 truncate"
                                            >{l.name}</span
                                        >
                                        <span
                                            class="text-sm text-muted-foreground"
                                            >{l.talk_count}</span
                                        >
                                    </Command.Item>
                                {/each}
                            </Command.Group>
                        {:else}
                            <div
                                class="flex items-center justify-center gap-2 py-6 text-sm text-muted-foreground"
                            >
                                <span
                                    class="inline-block h-3.5 w-3.5 animate-spin rounded-full border-[1.5px] border-muted-foreground/20 border-t-muted-foreground"
                                ></span>
                                Loading
                            </div>
                        {/if}
                    </Command.List>
                </Command.Root>
            </Popover.Content>
        </Popover.Root>

        <!-- Duration filter -->
        <Popover.Root bind:open={durationOpen}>
            <Popover.Trigger>
                {#snippet child({ props })}
                    <button
                        {...props}
                        class="inline-flex items-center gap-1 border-none bg-transparent p-0 text-sm cursor-pointer text-muted-foreground hover:text-foreground transition-colors whitespace-nowrap"
                    >
                        {#if hasDurationFilter}
                            <span class="text-foreground">{formatDuration(durationRange[0])} &ndash; {formatDuration(durationRange[1])}</span>
                        {:else}
                            Duration
                        {/if}
                        <ChevronDownIcon class="size-3.5 opacity-50" />
                    </button>
                {/snippet}
            </Popover.Trigger>
            <Popover.Content class="w-[260px] p-4" align="start">
                <div class="flex flex-col gap-4">
                    <div class="flex items-center justify-between text-sm">
                        <span class="text-muted-foreground">Duration</span>
                        <span class="font-medium">{formatDuration(durationRange[0])} &ndash; {formatDuration(durationRange[1])}</span>
                    </div>
                    <Slider
                        type="multiple"
                        bind:value={durationRange}
                        min={0}
                        max={120}
                        step={5}
                    />
                    <div class="flex items-center justify-between text-xs text-muted-foreground">
                        <span>0m</span>
                        <span>30m</span>
                        <span>1h</span>
                        <span>2h+</span>
                    </div>
                    <Button size="sm" onclick={applyDuration}>Apply</Button>
                </div>
            </Popover.Content>
        </Popover.Root>

        {#if hasActiveFilters}
            <button
                class="cursor-pointer border-none bg-transparent p-0 text-sm text-muted-foreground hover:text-foreground"
                onclick={clearAllFilters}
            >
                Clear
            </button>
        {/if}
    </div>
</div>
