# personal-radar

Self-hosted veille agent. Collects from RSS / GitHub / Reddit / X (Twitter),
deduplicates, ranks, synthesizes a daily "why it matters" briefing, and
persists everything in Postgres. There is **no built-in user interface**: the
chat agent (Hermes) reads the database and posts briefings to a Discord
channel — every interaction (read, like, deep dive) happens in chat, not in
code.

```
 rss ─┐
reddit├─▶ collectors ─▶ ingest ─▶ dedup ─▶ rank ─▶ briefing ─▶ Postgres
github│  (per source)                                    (daily slots)
  x  ─┘                                                    │
                                                     chat agent (Hermes)
                                                     reads DB → Discord
```

## Commands

```bash
radar migrate    # apply database migrations
radar collect    # run one collection cycle across all enabled collectors
radar rank       # score pending items
radar briefing   # generate the daily briefing (persisted in briefings table)
radar run        # start the scheduler (collect every 20m + briefing slots)
```

## Sources

| Source      | Access                                   | Notes                      |
|-------------|------------------------------------------|----------------------------|
| RSS         | public feeds, conditional GET            | 9 feeds, ETag cached       |
| Reddit      | OAuth (script app) or public RSS fallback| own 60m poll (rate limits) |
| GitHub      | REST API + read-only token               | releases + new org repos   |
| X (Twitter) | twscrape sidecar (Python), session cookies | accounts / queries / lists |

LinkedIn was dropped (login-wall + anti-scrape, 0 items) — see
`docs/adr/0002-linkedin-inaccessible.md`. The web dashboard and Telegram bot
were removed in `docs/adr/0003-chat-agent-only.md`.

## Setup

1. `cp .env.example .env` and fill in:
   - `GITHUB_TOKEN` — read-only PAT
   - `OPENAI_API_KEY` — OpenAI-compatible endpoint (ranking + synthesis)
   - `X_AUTH_TOKEN` / `X_CT0` — X session cookies for twscrape (rotate regularly)
   - `REDDIT_CLIENT_ID` / `REDDIT_CLIENT_SECRET` — optional (public RSS fallback)
2. `cd deploy && docker compose up -d --build`

The scheduler collects every 20 minutes and generates briefings at the
configured slots (`briefing.schedules` in `config/radar.yaml`).

## Reading the data

The chat agent is the interface. Useful queries:

```bash
# top-scored items of the last 24h
docker exec deploy-postgres-1 psql -U radar -d radar -c "
  SELECT i.source, i.title, s.final_score FROM items i
  JOIN scores s ON s.item_id = i.id
  WHERE i.collected_at > now() - interval '24 hours'
  ORDER BY s.final_score DESC LIMIT 10;"

# last briefing (what the agent posts to Discord)
docker exec deploy-postgres-1 psql -U radar -d radar -t -c "
  SELECT content FROM briefings ORDER BY date DESC LIMIT 1;"

# pipeline health
docker exec deploy-postgres-1 psql -U radar -d radar -c "
  SELECT kind, source, count(*), max(end_time) FROM runs
  GROUP BY kind, source ORDER BY 3 DESC LIMIT 10;"
```

## Repo layout

```
cmd/radar/          entrypoint (migrate | collect | rank | briefing | run)
internal/
  collectors/       rss, github, reddit (oauth + public), x  (each → model.Item)
  ingestion/        dedup (canonical URL + content hash), item_sources
  ranking/          heuristic BM25 scorer, optional LLM stage
  briefing/         selection (source quota), trends, markdown rendering
  scheduler/        cron-like jobs (collect 20m, briefing slots)
  store/            all SQL (items, scores, briefings, runs, feed_state)
  db/               connection + append-only migrations
  config/           radar.yaml + env overrides
  model/            Item normalization, content hash
  topics/           topic enrichment
  textutil/         text helpers
  logging/          slog wrapper
xscraper/           twscrape Python sidecar
deploy/             docker-compose.yml + Dockerfile + entrypoint.sh
scripts/            verify-and-send.sh, run-local.sh
docs/adr/           architecture decision records
```

## Development

```bash
make build    # compile for the current platform
make test     # short test suite
make ci       # vet + test + cross-compile
```
