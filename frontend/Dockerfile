FROM oven/bun:slim AS build

WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile

COPY . .
RUN bun run svelte-kit sync
RUN bun run build

FROM oven/bun:slim
WORKDIR /app

RUN adduser --system --uid 1001 sveltekit
COPY --from=build --chown=sveltekit:sveltekit /app/build build/
COPY --from=build --chown=sveltekit:sveltekit /app/package.json .
COPY --from=build --chown=sveltekit:sveltekit /app/node_modules node_modules/

USER sveltekit
ENV PORT=3000
EXPOSE 3000
CMD ["bun", "build/index.js"]
