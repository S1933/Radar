package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/S1933/personal-radar/internal/briefing"
	"github.com/S1933/personal-radar/internal/collectors/github"
	"github.com/S1933/personal-radar/internal/collectors/reddit"
	"github.com/S1933/personal-radar/internal/collectors/rss"
	"github.com/S1933/personal-radar/internal/collectors/x"
	"github.com/S1933/personal-radar/internal/config"
	"github.com/S1933/personal-radar/internal/db"
	"github.com/S1933/personal-radar/internal/ingestion"
	"github.com/S1933/personal-radar/internal/logging"
	"github.com/S1933/personal-radar/internal/ranking"
	"github.com/S1933/personal-radar/internal/scheduler"
	"github.com/S1933/personal-radar/internal/store"
)

// App wires every component of the radar together. It owns the database
// pool and the lifecycle of the scheduler. There is no user-facing
// surface in here anymore: the chat agent (Hermes) reads the Postgres
// data directly (items, briefings) and handles every interaction.
type App struct {
	Cfg   *config.Config
	Log   *logging.Logger
	DB    *db.DB
	Store *store.Store
	Ingest *ingestion.Service
	Ranker  *ranking.Service
	Briefer   *briefing.Service
	Scheduler *scheduler.Scheduler
}

// New builds the App: opens the DB pool and wires services. Collectors are
// registered per collection cycle based on configuration and credentials.
func New(ctx context.Context, cfg *config.Config, log *logging.Logger) (*App, error) {
	database, err := db.Open(ctx, cfg.Database.DSN())
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	st := store.New(database)

	ranker := ranking.New(cfg.Models, st, log.With("sub", "ranking"))

	briefer := briefing.New(briefing.Options{
		MaxItems:  cfg.Briefing.MaxItems,
		MaxTrends: cfg.Briefing.MaxTrends,
		Location:  cfg.Location(),
	}, st, ranker, log.With("sub", "briefing"))

	svc := ingestion.New(st, log.With("sub", "ingestion"))

	return &App{
		Cfg:       cfg,
		Log:       log,
		DB:        database,
		Store:     st,
		Ingest:    svc,
		Ranker:    ranker,
		Briefer:   briefer,
		Scheduler: scheduler.New(log.With("sub", "scheduler")),
	}, nil
}

// Close releases resources held by the app (DB pool).
func (a *App) Close() {
	if a.DB != nil {
		a.DB.Close()
	}
}

// Migrate applies all pending migrations.
func (a *App) Migrate(ctx context.Context) error {
	return a.DB.Migrate(ctx)
}

// briefingSlots merges the configured daily slots with the legacy single
// schedule, de-duplicating and preserving order.
//
// The legacy field only applies when no modern slot is configured: it
// carries a "07:00" default, and blindly appending it produced a fourth
// briefing nobody asked for. The copy also matters — appending straight
// onto cfg.Briefing.Schedules can write into the config's backing array.
func briefingSlots(schedules []string, legacy string) []string {
	out := make([]string, 0, len(schedules)+1)
	seen := map[string]bool{}
	add := func(s string) {
		if s == "" || seen[s] {
			return
		}
		seen[s] = true
		out = append(out, s)
	}
	for _, s := range schedules {
		add(s)
	}
	if len(out) == 0 {
		add(legacy)
	}
	if len(out) == 0 {
		add("07:00")
	}
	return out
}

// collectors builds the enabled collector set for this cycle.
func (a *App) collectors(ctx context.Context) []ingestion.Collector {
	var out []ingestion.Collector
	if a.Cfg.RSS.Enabled && len(a.Cfg.RSS.Feeds) > 0 {
		c := rss.NewCollector(a.Cfg.RSS, a.Log.With("sub", "rss"))
		// Conditional GET: without a state store, ETag/Last-Modified
		// tokens are never persisted and the 9 feeds are re-downloaded
		// in full every cycle (≈ 650 requests/day toward Le Monde,
		// Cloudflare, HN, etc.). *store.Store already satisfies the
		// rss.StateStore interface (GetFeedState / SaveFeedState).
		c.SetStateStore(a.Store)
		out = append(out, c)
	}
	if a.Cfg.Reddit.Enabled && len(a.Cfg.Reddit.Subreddits) > 0 && a.Cfg.Reddit.Every == 0 {
		c, err := reddit.NewCollector(ctx, a.Cfg.Reddit, a.Log.With("sub", "reddit"))
		if err == nil {
			out = append(out, c)
		} else {
			// No OAuth credentials: fall back to the public RSS adapter
			// (best-effort).
			a.Log.Warn("reddit oauth unavailable, using public adapter", "error", err.Error())
			out = append(out, reddit.NewPublicCollector(a.Cfg.Reddit, a.Log.With("sub", "reddit-public")))
		}
	}
	if a.Cfg.GitHub.Enabled && (len(a.Cfg.GitHub.Repositories) > 0 || len(a.Cfg.GitHub.Organizations) > 0) {
		if c, err := github.NewCollector(a.Cfg.GitHub, a.Log.With("sub", "github")); err == nil {
			out = append(out, c)
		} else {
			a.Log.Warn("github disabled for this cycle", "error", err)
		}
	}
	if a.Cfg.X.Enabled && (len(a.Cfg.X.Accounts) > 0 || len(a.Cfg.X.Queries) > 0) {
		py := os.Getenv("X_PYTHON")
		if py == "" {
			py = "/usr/bin/python3" // system python with twscrape (Docker image)
		}
		c, err := x.NewCollector(a.Cfg.X, a.Log.With("sub", "x"),
			x.WithScriptPath("xscraper/collect.py"),
			x.WithVenvPython(py),
			x.WithTwscrapeDB("/app/x_accounts.db"),
		)
		if err != nil {
			a.Log.Warn("x disabled", "error", err.Error())
		} else {
			out = append(out, c)
		}
	}
	return out
}

// CollectOnce runs all enabled collectors, normalizes, dedupes and stores.
// A failing collector never aborts the cycle (isolation requirement).
func (a *App) CollectOnce(ctx context.Context) (int, error) {
	collectors := a.collectors(ctx)
	if len(collectors) == 0 {
		return 0, errors.New("no collector enabled")
	}
	var total int
	for _, c := range collectors {
		total += a.runCollector(ctx, c)
	}
	return total, nil
}

// collectReddit runs only the Reddit collector on its own (slower) interval
// to stay under Reddit's anonymous-RSS rate limit. It is used when
// reddit.every is configured; the global CollectOnce then skips Reddit.
func (a *App) collectReddit(ctx context.Context) error {
	var c ingestion.Collector
	var err error
	if c, err = reddit.NewCollector(ctx, a.Cfg.Reddit, a.Log.With("sub", "reddit")); err != nil {
		a.Log.Warn("reddit oauth unavailable, using public adapter", "error", err.Error())
		c = reddit.NewPublicCollector(a.Cfg.Reddit, a.Log.With("sub", "reddit-public"))
	}
	a.runCollector(ctx, c)
	return nil
}

// perCollectorBudget bounds a single collector's run. Collectors
// execute sequentially, and scheduler.loop calls Run in series — so
// one that hangs blocks every source behind it indefinitely. 5 minutes
// is the largest the slowest sidecar (X with 5 accounts + lists) has
// ever needed in practice, doubled for safety.
const perCollectorBudget = 5 * time.Minute

// runCollector runs one collector under a time budget, ingests any
// partial result the collector managed to return, logs the outcome,
// and never propagates a failure: one broken source must not abort the
// cycle. The function also records the run for observability (T13).
func (a *App) runCollector(ctx context.Context, c ingestion.Collector) int {
	start := time.Now()
	cctx, cancel := context.WithTimeout(ctx, perCollectorBudget)
	defer cancel()

	var inserted, failed int
	var errMsg string

	items, err := c.Collect(cctx)
	if err != nil {
		errMsg = err.Error()
		failed = len(items)
		a.Log.Warn("collector failed", "collector", c.Name(), "error", err,
			"duration_ms", time.Since(start).Milliseconds())
	}
	// A collector may return a partial result with an error
	// (isolation rule): ingest what arrived, then record the run.
	if len(items) > 0 {
		n, ierr := a.Ingest.IngestBatch(ctx, c.Name(), items)
		if ierr != nil {
			if errMsg == "" {
				errMsg = ierr.Error()
			} else {
				errMsg = errMsg + "; " + ierr.Error()
			}
			a.Log.Warn("ingest failed", "collector", c.Name(), "error", ierr)
		} else {
			inserted = n
			a.Log.Info("collect ok", "collector", c.Name(), "items", len(items),
				"new", inserted, "duration_ms", time.Since(start).Milliseconds())
		}
	}

	if err := a.Store.SaveRun(ctx, "collect", c.Name(), start, time.Now(),
		inserted, failed, errMsg); err != nil {
		a.Log.Warn("save run", "collector", c.Name(), "error", err)
	}
	return inserted
}

// RankPending scores items that do not have a score yet.
func (a *App) RankPending(ctx context.Context) (int, error) {
	return a.Ranker.RankPending(ctx)
}

// Briefing generates the daily briefing and persists it (no delivery —
// the chat agent picks it up from the briefings table).
func (a *App) Briefing(ctx context.Context) (string, error) {
	return a.Briefer.Generate(ctx)
}

// Run starts the scheduler (collection + briefing).
func (a *App) Run(ctx context.Context) error {
	// Apply migrations at boot so fresh deployments work without manual steps.
	if err := a.Migrate(ctx); err != nil {
		a.Log.Warn("auto-migrate failed", "error", err.Error())
	}
	// Collection every 20 minutes (RSS, GitHub, X, and Reddit only when
	// Reddit has no dedicated interval configured).
	a.Scheduler.Add("collect", scheduler.Spec{
		Every: 20 * time.Minute,
		Run: func(ctx context.Context) {
			if _, err := a.CollectOnce(ctx); err != nil {
				a.Log.Warn("collect cycle", "error", err)
			}
		},
	})
	// Reddit may run on its own slower interval to stay under Reddit's
	// anonymous-RSS rate limit. When reddit.every is set, CollectOnce
	// skips Reddit (it has its own job below) so we never double-collect.
	if a.Cfg.Reddit.Enabled && a.Cfg.Reddit.Every > 0 {
		a.Scheduler.Add("reddit-collect", scheduler.Spec{
			Every: a.Cfg.Reddit.Every,
			Run: func(ctx context.Context) {
				if err := a.collectReddit(ctx); err != nil {
					a.Log.Warn("reddit collect", "error", err)
				}
			},
		})
	}
	// Briefing: one job per daily slot (default single 07:00, or the
	// configured schedules list). Each slot generates and persists the
	// briefing; delivery is the chat agent's job.
	slots := briefingSlots(a.Cfg.Briefing.Schedules, a.Cfg.Briefing.Schedule)
	for i, slot := range slots {
		s := slot // capture loop var
		a.Scheduler.Add(fmt.Sprintf("briefing-%02d", i), scheduler.Spec{
			DailyAt: s,
			Loc:     a.Cfg.Location(),
			Run: func(ctx context.Context) {
				if _, err := a.Briefer.Generate(ctx); err != nil {
					a.Log.Error("briefing", "slot", s, "error", err)
				}
			},
		})
	}
	a.Scheduler.Start(ctx)

	<-ctx.Done()
	return nil
}
