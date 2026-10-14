package x

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/S1933/personal-radar/internal/config"
	"github.com/S1933/personal-radar/internal/model"
)

// Collector pulls recent tweets from targeted X accounts via the twscrape
// sidecar (Python). twscrape uses an authenticated X session (cookies from
// X_AUTH_TOKEN / X_CT0 env vars) to call X's internal GraphQL endpoints,
// bypassing the paid API tier. The session is required and managed by the
// user; the radar only orchestrates the sidecar.
type Collector struct {
	cfg        config.XConfig
	log        Logger
	scriptPath string
	venvPython string
	twscrapeDB string
	timeout    time.Duration
}

type Logger interface {
	Warn(msg string, kv ...any)
}

// NewCollector wires the twscrape sidecar. scriptPath is the absolute path to
// xscraper/collect.py; if empty it is resolved relative to the repo root.
func NewCollector(cfg config.XConfig, log Logger, opts ...Option) (*Collector, error) {
	if len(cfg.Accounts) == 0 && len(cfg.Queries) == 0 && len(cfg.Lists) == 0 {
		return nil, fmt.Errorf("x: no accounts, queries or lists configured")
	}
	c := &Collector{
		cfg:        cfg,
		log:        log,
		scriptPath: "xscraper/collect.py",
		venvPython: "python3",
		twscrapeDB: "x_accounts.db",
		timeout:    60 * time.Second,
	}
	for _, o := range opts {
		o(c)
	}
	return c, nil
}

// Option overrides collector defaults (used in tests / deployment).
type Option func(*Collector)

func WithScriptPath(p string) Option     { return func(c *Collector) { c.scriptPath = p } }
func WithVenvPython(p string) Option     { return func(c *Collector) { c.venvPython = p } }
func WithTwscrapeDB(p string) Option     { return func(c *Collector) { c.twscrapeDB = p } }
func WithTimeout(d time.Duration) Option { return func(c *Collector) { c.timeout = d } }

// num extracts an int64 from a JSON-decoded number (float64, json.Number)
// or a numeric string ("123" — twscrape serializes counts as strings).
func num(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), n > 0
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return i, i > 0
		}
	case string:
		var i int64
		if _, err := fmt.Sscanf(n, "%d", &i); err == nil {
			return i, i > 0
		}
	}
	return 0, false
}

func (c *Collector) Name() string { return "x" }

// Collect invokes the twscrape sidecar and parses its JSON output.
func (c *Collector) Collect(ctx context.Context) ([]model.Item, error) {
	// The sidecar talks to X's internal GraphQL endpoints: a stuck
	// socket there would freeze the whole collection cycle, since
	// the scheduler runs jobs sequentially. Derive a bounded context
	// from c.cfg.Timeout (defaults to 3 minutes) so a runaway process
	// cannot wedge the pipeline indefinitely.
	if c.cfg.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.cfg.Timeout)
		defer cancel()
	}

	limit := c.cfg.Limit
	if limit <= 0 {
		limit = 15
	}
	args := []string{c.scriptPath, "--limit", strconv.Itoa(limit)}
	for _, a := range c.cfg.Accounts {
		args = append(args, "--accounts", a)
	}
	for _, q := range c.cfg.Queries {
		args = append(args, "--queries", q)
	}
	for _, l := range c.cfg.Lists {
		args = append(args, "--lists", l)
	}

	cmd := exec.CommandContext(ctx, c.venvPython, args...)
	cmd.Env = append(os.Environ(),
		"X_AUTH_TOKEN="+c.cfg.APIKey, // reused as auth_token cookie
		"X_CT0="+c.cfg.APISecret,     // reused as ct0 cookie
		"TWSCRAPE_DB="+c.twscrapeDB,
	)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		// Distinguish a timeout (ctx.Err() != nil) from a script error:
		// the cause is different and the log message should reflect it.
		if ctx.Err() != nil {
			return nil, fmt.Errorf("x sidecar timed out after %s: %w", c.cfg.Timeout, ctx.Err())
		}
		return nil, fmt.Errorf("x sidecar failed: %w: %s", err, stderr.String())
	}

	var raw []struct {
		SourceID    string         `json:"source_id"`
		Author      string         `json:"author"`
		Title       string         `json:"title"`
		URL         string         `json:"url"`
		Content     string         `json:"content"`
		PublishedAt *time.Time     `json:"published_at"`
		Topics      []string       `json:"topics"`
		Language    string         `json:"language"`
		Metadata    map[string]any `json:"metadata"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &raw); err != nil {
		return nil, fmt.Errorf("x: parse sidecar output: %w", err)
	}

	now := time.Now().UTC()
	items := make([]model.Item, 0, len(raw))
	for _, r := range raw {
		if r.SourceID == "" {
			continue
		}
		it := model.Item{
			Source:      "x",
			SourceID:    r.SourceID,
			Author:      r.Author,
			Title:       r.Title,
			URL:         r.URL,
			Content:     r.Content,
			PublishedAt: now,
			CollectedAt: now,
			Topics:      r.Topics,
			Language:    r.Language,
			Metadata:    toStringMap(r.Metadata),
		}
		// twscrape metadata carries the engagement signals that users care
		// about — feed them into the normalised Engagement field so the
		// ranker can weigh virality, not just raw text.
		if likes, ok := num(r.Metadata["likes"]); ok {
			it.Engagement.Score = likes
		}
		if rt, ok := num(r.Metadata["retweets"]); ok {
			it.Engagement.Comments = rt
		}
		if r.PublishedAt != nil {
			it.PublishedAt = *r.PublishedAt
		}
		if it.Language == "" {
			it.Language = "en"
		}
		if len(it.Topics) == 0 {
			it.Topics = []string{"software-engineering", "open-source"}
		}
		items = append(items, it)
	}
	return items, nil
}

func toStringMap(in map[string]any) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		switch val := v.(type) {
		case string:
			out[k] = val
		case nil:
			out[k] = ""
		case []any:
			// Media lists (image URLs) degrade to a space-joined string;
			// the card builder splits on whitespace to re-embed them.
			parts := make([]string, 0, len(val))
			for _, e := range val {
				if s, ok := e.(string); ok {
					parts = append(parts, s)
				}
			}
			out[k] = strings.Join(parts, " ")
		default:
			out[k] = fmt.Sprintf("%v", val)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
