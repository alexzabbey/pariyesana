FROM node:22-slim AS build

RUN corepack enable && corepack prepare pnpm@latest --activate

WORKDIR /app
COPY package.json .
RUN pnpm install

COPY . .
RUN pnpm build

FROM node:22-slim
WORKDIR /app
COPY --from=build /app/build build/
COPY --from=build /app/package.json .
COPY --from=build /app/node_modules node_modules/

ENV PORT=3000
EXPOSE 3000
CMD ["node", "build"]
