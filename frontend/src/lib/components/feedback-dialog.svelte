<script lang="ts">
    import * as Dialog from "$lib/components/ui/dialog/index.js";
    import { Button } from "$lib/components/ui/button/index.js";
    import { Textarea } from "$lib/components/ui/textarea/index.js";

    const TO = "itsme@alyo.sh";

    let open = $state(false);
    let text = $state("");

    function send() {
        const body = text.trim();
        if (!body) return;
        // mailto hands off to the visitor's mail client (zero backend).
        // ponytail: some clients cap the mailto URL near ~2000 chars — plenty for feedback;
        // switch to a POST /api/feedback endpoint if you ever need longer/guaranteed delivery.
        const url = `mailto:${TO}?subject=${encodeURIComponent("Pariyesanā feedback")}&body=${encodeURIComponent(body)}`;
        window.location.href = url;
        open = false;
        text = "";
    }
</script>

<Dialog.Root bind:open>
    <Dialog.Trigger>
        {#snippet child({ props })}
            <Button
                {...props}
                variant="ghost"
                size="sm"
                class="fixed bottom-3 right-3 z-40 text-xs text-muted-foreground hover:text-foreground"
            >
                Feedback
            </Button>
        {/snippet}
    </Dialog.Trigger>
    <Dialog.Content>
        <Dialog.Header>
            <Dialog.Title>Send feedback</Dialog.Title>
            <Dialog.Description>
                Thoughts, bugs, or talks you'd love to see. Opens your mail app.
            </Dialog.Description>
        </Dialog.Header>
        <Textarea
            bind:value={text}
            rows={5}
            placeholder="What's on your mind?"
            aria-label="Feedback message"
        />
        <Dialog.Footer>
            <Button onclick={send} disabled={!text.trim()}>Send</Button>
        </Dialog.Footer>
    </Dialog.Content>
</Dialog.Root>
