FROM node:20-alpine AS base
WORKDIR /app
ENV NEXT_PUBLIC_API_BASE_URL=http://localhost:8000
ENV NEXT_PUBLIC_DEV_MODE=true

# 依存関係のインストール（共通）
FROM base AS deps
COPY web_frontend/package*.json ./
COPY web_frontend/pnpm-lock.yaml* ./
RUN corepack enable && corepack prepare pnpm@latest --activate
# pnpm-lock.yamlとpackage.jsonの整合性が取れていない場合があるため、frozen-lockfileを無効化
RUN --mount=type=cache,target=/root/.local/share/pnpm/store pnpm install --no-frozen-lockfile

# 開発用ステージ（ソースコードはマウント前提だが、初期構築用にコピーもしておく）
FROM base AS dev
COPY --from=deps /app/node_modules ./node_modules
COPY web_frontend .
# 開発サーバー起動
CMD ["npm", "run", "dev"]

# ビルダー（本番用）
FROM base AS builder
COPY --from=deps /app/node_modules ./node_modules
COPY web_frontend .
RUN corepack enable && corepack prepare pnpm@latest --activate
RUN pnpm run build

# ランナー（本番用）
FROM node:20-alpine AS runner
WORKDIR /app
ENV NODE_ENV=production
COPY --from=builder /app/package*.json ./
COPY --from=builder /app/next.config.mjs ./
COPY --from=builder /app/.next ./.next
COPY --from=builder /app/public ./public
COPY --from=builder /app/node_modules ./node_modules
EXPOSE 3000
ENV PORT=3000
CMD ["node_modules/.bin/next", "start"]
