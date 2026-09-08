# ADR 0003: Chat-agent-only — no web dashboard, no Telegram bot

Date: 2026-09-08
Status: Accepted

## Context

The radar shipped with two user-facing surfaces: a bookmark web dashboard
(`internal/web`, on 127.0.0.1:8081, exposed through the Cloudflare tunnel) and
a Telegram bot (`internal/telegram`, `@PersoRadarBot`) that pushed briefings
and handled commands (`/save`, `/deepdive`, reactions). Both required the user
to actively open an app. In practice the dashboard was never opened — reading
happened (or didn't) in chat.

The user decided to drop both surfaces and read everything in a Discord
channel through the Hermes agent instead.

## Decision

- Delete `internal/web`, `internal/telegram`, `internal/summary`,
  `internal/personalization` and the Telegram-only handlers
  (`app/handlers.go`, `app/deepdive.go`, `app/fs.go`).
- The Go binary becomes a pure pipeline: collect → rank → briefings persisted
  in Postgres. No HTTP server, no bot listener, no delivery code.
- The chat agent reads `items` / `briefings` directly (psql) and posts to
  Discord via a scheduled Hermes job. Feedback (like/dislike) lives in the
  chat history, not in the database.
- Dashboard-only DB columns (`is_bookmarked`, `is_read`, `is_liked`,
  `is_pinned`, `summary_fr`, `summary_title`, `scores.personalization`) and
  the `user_preferences` / `feedback` tables are dropped by migration 007.

## Consequences

- ~2 900 lines of Go removed (8 094 → ~5 200); 17 → 13 internal packages.
- Reading requires asking the agent in chat — there is no scrollback UI
  beyond Discord's own history.
- The briefing renders as Discord markdown instead of Telegram HTML.
- If a dashboard is ever wanted again, resurrect it as a Hermes skill reading
  the same Postgres tables — not as Go code in this repo.
