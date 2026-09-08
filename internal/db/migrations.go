package db

import (
	"context"
	"fmt"
)

// migrations is the ordered list of schema migrations. Append-only.
var migrations = []string{
	// 001 — initial schema
	`
CREATE TABLE IF NOT EXISTS sources (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    kind        TEXT NOT NULL,
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    last_run_at TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS items (
    id            BIGSERIAL PRIMARY KEY,
    source        TEXT NOT NULL,
    source_id     TEXT NOT NULL,
    url           TEXT NOT NULL DEFAULT '',
    canonical_url TEXT NOT NULL DEFAULT '',
    author        TEXT NOT NULL DEFAULT '',
    author_id     TEXT NOT NULL DEFAULT '',
    title         TEXT NOT NULL DEFAULT '',
    content       TEXT NOT NULL DEFAULT '',
    published_at  TIMESTAMPTZ,
    collected_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    content_hash  TEXT NOT NULL DEFAULT '',
    topics        TEXT[] NOT NULL DEFAULT '{}',
    language      TEXT NOT NULL DEFAULT '',
    engagement    BIGINT NOT NULL DEFAULT 0,
    metadata      JSONB NOT NULL DEFAULT '{}',
    UNIQUE (source, source_id)
);

CREATE INDEX IF NOT EXISTS items_canonical_url_idx ON items (canonical_url) WHERE canonical_url <> '';
CREATE INDEX IF NOT EXISTS items_content_hash_idx  ON items (content_hash)  WHERE content_hash  <> '';
CREATE INDEX IF NOT EXISTS items_collected_at_idx  ON items (collected_at);
CREATE INDEX IF NOT EXISTS items_published_at_idx  ON items (published_at);

CREATE TABLE IF NOT EXISTS item_sources (
    item_id     BIGINT NOT NULL REFERENCES items(id) ON DELETE CASCADE,
    source      TEXT NOT NULL,
    source_ref  TEXT NOT NULL,
    first_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (item_id, source, source_ref)
);

CREATE TABLE IF NOT EXISTS scores (
    item_id     BIGINT PRIMARY KEY REFERENCES items(id) ON DELETE CASCADE,
    importance  REAL NOT NULL DEFAULT 0,
    relevance   REAL NOT NULL DEFAULT 0,
    novelty     REAL NOT NULL DEFAULT 0,
    actionability REAL NOT NULL DEFAULT 0,
    personalization REAL NOT NULL DEFAULT 0,
    final_score REAL NOT NULL DEFAULT 0,
    model       TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS briefings (
    id          BIGSERIAL PRIMARY KEY,
    date        DATE NOT NULL UNIQUE,
    content     TEXT NOT NULL,
    item_ids    BIGINT[] NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS feedback (
    id         BIGSERIAL PRIMARY KEY,
    item_id    BIGINT NOT NULL REFERENCES items(id) ON DELETE CASCADE,
    action     TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS user_preferences (
    kind   TEXT NOT NULL,          -- topic | source | author | cluster
    name   TEXT NOT NULL,
    weight REAL NOT NULL DEFAULT 0,
    PRIMARY KEY (kind, name)
);

CREATE TABLE IF NOT EXISTS runs (
    id              BIGSERIAL PRIMARY KEY,
    kind            TEXT NOT NULL, -- collect | rank | briefing
    source          TEXT NOT NULL DEFAULT '',
    start_time      TIMESTAMPTZ NOT NULL,
    end_time        TIMESTAMPTZ NOT NULL,
    items_collected INTEGER NOT NULL DEFAULT 0,
    items_failed    INTEGER NOT NULL DEFAULT 0,
    error           TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS feed_state (
    name          TEXT PRIMARY KEY,
    etag          TEXT NOT NULL DEFAULT '',
    last_modified TEXT NOT NULL DEFAULT '',
    last_fetched  TIMESTAMPTZ
);
`,
	// 002-006 — dashboard era (bookmarks, read/like/pin flags, French
	// summaries). Superseded by 007 which drops every column they added;
	// kept here because migrations are append-only.
	`
ALTER TABLE items
    ADD COLUMN IF NOT EXISTS is_bookmarked BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS is_read       BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS items_bookmarked_unread_idx
    ON items (collected_at DESC)
    WHERE is_bookmarked = TRUE AND is_read = FALSE;

CREATE INDEX IF NOT EXISTS items_bookmarked_all_idx
    ON items (collected_at DESC)
    WHERE is_bookmarked = TRUE;
`,
	`
ALTER TABLE items
    ADD COLUMN IF NOT EXISTS summary_fr TEXT NOT NULL DEFAULT '';
`,
	`
ALTER TABLE items
    ADD COLUMN IF NOT EXISTS is_liked BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS items_liked_idx
    ON items (collected_at DESC)
    WHERE is_liked = TRUE;
`,
	`
ALTER TABLE items
    ADD COLUMN IF NOT EXISTS summary_title TEXT NOT NULL DEFAULT '';
`,
	`
ALTER TABLE items
    ADD COLUMN IF NOT EXISTS is_pinned BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS items_pinned_idx
    ON items (collected_at DESC)
    WHERE is_pinned = TRUE;
`,
	// 007 — dashboard removal: the interface moved to the chat agent
	// (Hermes reads the DB directly). Drop every dashboard-only column
	// and index. The 002-006 statements above stay for existing
	// databases that already applied them; this migration undoes them.
	`
DROP INDEX IF EXISTS items_bookmarked_unread_idx;
DROP INDEX IF EXISTS items_bookmarked_all_idx;
DROP INDEX IF EXISTS items_liked_idx;
DROP INDEX IF EXISTS items_pinned_idx;

ALTER TABLE items
    DROP COLUMN IF EXISTS is_bookmarked,
    DROP COLUMN IF EXISTS is_read,
    DROP COLUMN IF EXISTS is_liked,
    DROP COLUMN IF EXISTS is_pinned,
    DROP COLUMN IF EXISTS summary_fr,
    DROP COLUMN IF EXISTS summary_title;

-- The personalization loop lost its only inputs (Telegram reactions and
-- dashboard likes are gone); the chat agent records feedback in its own
-- history now.
ALTER TABLE scores
    DROP COLUMN IF EXISTS personalization;

DROP TABLE IF EXISTS user_preferences;
DROP TABLE IF EXISTS feedback;
`,
}

// Migrate applies pending migrations inside a transaction per migration.
// It uses an advisory lock so concurrent `radar migrate` calls are safe.
func (d *DB) Migrate(ctx context.Context) error {
	if _, err := d.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version INT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("create migrations table: %w", err)
	}

	if _, err := d.ExecContext(ctx, `SELECT pg_advisory_lock(727272)`); err != nil {
		return fmt.Errorf("advisory lock: %w", err)
	}
	defer d.ExecContext(ctx, `SELECT pg_advisory_unlock(727272)`)

	var current int
	if err := d.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&current); err != nil {
		return fmt.Errorf("read migration version: %w", err)
	}

	for i, m := range migrations {
		v := i + 1
		if v <= current {
			continue
		}
		tx, err := d.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, m); err != nil {
			tx.Rollback()
			return fmt.Errorf("migration %d: %w", v, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, v); err != nil {
			tx.Rollback()
			return fmt.Errorf("record migration %d: %w", v, err)
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
