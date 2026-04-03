<script lang="ts">
    let { size = 48, class: className = "" }: { size?: number; class?: string } = $props();
</script>

<svg
    width={size}
    height={size}
    viewBox="0 0 100 100"
    class="lotus-loader {className}"
    xmlns="http://www.w3.org/2000/svg"
>
    <!-- Center petal -->
    <path
        class="petal p-center"
        d="M50,32.3c4.4,3.5,7.1,9.4,7.1,15.8c0,6.4-2.7,12.3-7.1,15.8c-4.4-3.5-7.1-9.4-7.1-15.8C42.9,41.7,45.6,35.8,50,32.3z"
        fill="currentColor"
    />
    <!-- Right inner petal -->
    <path
        class="petal p-right-inner"
        d="M65.6,38.7c1.9,4.9,1.3,10.9-1.6,16c-2.1,3.7-5.2,6.5-8.7,8.1c3.1-4,4.9-9.2,4.9-14.7c0-2.4-0.3-4.7-1-6.9C61.2,39.9,63.4,39.1,65.6,38.7z"
        fill="currentColor"
    />
    <!-- Left inner petal -->
    <path
        class="petal p-left-inner"
        d="M34.4,38.7c2.3,0.3,4.5,1.2,6.5,2.5c-0.6,2.2-1,4.5-1,6.9c0,5.5,1.8,10.7,4.9,14.7c-3.5-1.6-6.6-4.4-8.7-8.1C33,49.6,32.4,43.6,34.4,38.7z"
        fill="currentColor"
    />
    <!-- Right outer petal -->
    <path
        class="petal p-right-outer"
        d="M65.6,63.4c-2.7,1.6-5.8,2.4-8.8,2.4c-0.2,0-0.5,0-0.7,0c4.2-1.8,8-5.2,10.5-9.5c1-1.7,1.7-3.4,2.2-5.2c1.8,0,3.5,0.3,5.1,0.9C73.2,56.5,70.1,60.8,65.6,63.4z"
        fill="currentColor"
    />
    <!-- Left outer petal -->
    <path
        class="petal p-left-outer"
        d="M43.2,65.8c-3,0-6-0.8-8.8-2.4c-4.5-2.6-7.6-6.9-8.3-11.4c1.6-0.6,3.3-0.9,5.1-0.9c0.5,1.8,1.3,3.5,2.2,5.2c2.5,4.3,6.2,7.7,10.5,9.5C43.6,65.8,43.4,65.8,43.2,65.8z"
        fill="currentColor"
    />
</svg>

<style>
    .lotus-loader {
        --dim: color-mix(in oklch, var(--primary) 15%, transparent);
        --lit: var(--primary);
    }

    .petal {
        transform-origin: 50px 50px;
        fill: var(--dim);
    }

    .p-center {
        animation: pulse-color 2s ease-in-out infinite;
    }

    .p-right-inner,
    .p-left-inner {
        animation: pulse-color 2s ease-in-out infinite;
        animation-delay: 0.4s;
    }

    .p-right-outer,
    .p-left-outer {
        animation: pulse-color 2s ease-in-out infinite;
        animation-delay: 0.8s;
    }

    @keyframes pulse-color {
        0%, 100% {
            fill: var(--dim);
        }
        30%, 50% {
            fill: var(--lit);
        }
    }
</style>
