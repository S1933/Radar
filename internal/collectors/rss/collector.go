package rss

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/mmcdole/gofeed"

	"github.com/S1933/personal-radar/internal/config"
	"github.com/S1933/personal-radar/internal/model"
)

// Collector polls RSS/Atom feeds with conditional GET (ETag + Last-Modified).
type Collector struct {
	cfg     config.RSSConfig
	log     Logger
	client  *http.Client
	parser  *gofeed.Parser
	fetcher Fetcher
	state   StateStore
}

// Fetcher abstracts HTTP GET for testability.
type Fetcher interface {
	Get(ctx context.Context, url, etag, lastModified string) (*FetchResult, error)
}

type FetchResult struct {
	Body         io.ReadCloser
	ETag         string
	LastModified string
	NotModified  bool
}

type Logger interface {
	Warn(msg string, kv ...any)
}

func NewCollector(cfg config.RSSConfig, log Logger) *Collector {
	c := &Collector{
		cfg:    cfg,
		log:    log,
		client: &http.Client{Timeout: 30 * time.Second},
	}
	c.fetcher = &httpFetcher{client: c.client}
	c.parser = gofeed.NewParser()
	return c
}

// SetFetcher overrides the HTTP layer (tests).
func (c *Collector) SetFetcher(f Fetcher) { c.fetcher = f }

func (c *Collector) Name() string { return "rss" }

// StateStore persists conditional-GET tokens per feed (optional in tests).
type StateStore interface {
	GetFeedState(ctx context.Context, name string) (etag, lastModified string, err error)
	SaveFeedState(ctx context.Context, name, etag, lastModified string) error
}

func (c *Collector) SetStateStore(s StateStore) { c.state = s }

func (c *Collector) Collect(ctx context.Context) ([]model.Item, error) {
	var items []model.Item
	var lastErr error
	for _, feed := range c.cfg.Feeds {
		feedItems, err := c.collectFeed(ctx, feed)
		if err != nil {
			c.log.Warn("feed failed", "feed", feed.Name, "error", err)
			lastErr = err
			continue
		}
		items = append(items, feedItems...)
	}
	if len(items) == 0 && lastErr != nil {
		return nil, lastErr
	}
	return items, nil
}

func (c *Collector) collectFeed(ctx context.Context, feed config.RSSFeed) ([]model.Item, error) {
	etag, lastMod := "", ""
	if c.state != nil {
		e, lm, err := c.state.GetFeedState(ctx, feed.Name)
		if err == nil {
			etag, lastMod = e, lm
		}
	}

	res, err := c.fetcher.Get(ctx, feed.URL, etag, lastMod)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", feed.URL, err)
	}
	defer func() {
		if res.Body != nil {
			res.Body.Close()
		}
	}()
	if res.NotModified {
		return nil, nil
	}

	parsed, err := c.parser.Parse(res.Body)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", feed.Name, err)
	}
	if c.state != nil {
		if err := c.state.SaveFeedState(ctx, feed.Name, res.ETag, res.LastModified); err != nil {
			c.log.Warn("save feed state", "feed", feed.Name, "error", err)
		}
	}

	var items []model.Item
	now := time.Now().UTC()
	for _, entry := range parsed.Items {
		it := model.Item{
			Source:       "rss",
			SourceID:     guid(entry, feed.Name),
			Author:       author(entry),
			Title:        strings.TrimSpace(entry.Title),
			Content:      content(entry),
			URL:          entry.Link,
			CanonicalURL: canonical(entry.Link),
			PublishedAt:  publishedAt(entry, now),
			CollectedAt:  now,
			Topics:       feed.Topics,
			Language:     parsed.Language,
			Metadata:     map[string]string{"feed": feed.Name},
		}
		// Feed-item cover (rare, but some blogs publish one per post). Falls
		// back to the OG image fetch at card-build time when absent.
		if entry.Image != nil && entry.Image.URL != "" {
			it.Metadata["media"] = entry.Image.URL
		}
		if it.Title == "" && it.Content == "" {
			continue
		}
		items = append(items, it)
	}
	return items, nil
}

func guid(entry *gofeed.Item, feedName string) string {
	if entry.GUID != "" {
		return entry.GUID
	}
	if entry.Link != "" {
		return entry.Link
	}
	return feedName + ":" + entry.Title
}

func author(entry *gofeed.Item) string {
	if entry.Author != nil && entry.Author.Name != "" {
		return entry.Author.Name
	}
	if len(entry.Authors) > 0 && entry.Authors[0].Name != "" {
		return entry.Authors[0].Name
	}
	return ""
}

func content(entry *gofeed.Item) string {
	if entry.Content != "" {
		return entry.Content
	}
	return entry.Description
}

func publishedAt(entry *gofeed.Item, fallback time.Time) time.Time {
	if entry.PublishedParsed != nil {
		return *entry.PublishedParsed
	}
	if entry.UpdatedParsed != nil {
		return *entry.UpdatedParsed
	}
	return fallback
}

// canonical normalizes a URL for cross-source dedup (strip query/fragment,
// lowercase scheme+host, strip common tracking params).
func canonical(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	u.Fragment = ""
	u.RawQuery = ""
	u.Host = strings.ToLower(u.Host)
	u.Path = strings.TrimSuffix(u.Path, "/")
	return u.String()
}

type httpFetcher struct{ client *http.Client }

func (h *httpFetcher) Get(ctx context.Context, url, etag, lastModified string) (*FetchResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "personal-radar/0.1 (+https://github.com/S1933)")
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	if lastModified != "" {
		req.Header.Set("If-Modified-Since", lastModified)
	}
	res, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode == http.StatusNotModified {
		res.Body.Close()
		return &FetchResult{NotModified: true}, nil
	}
	if res.StatusCode != http.StatusOK {
		res.Body.Close()
		return nil, fmt.Errorf("HTTP %d", res.StatusCode)
	}
	return &FetchResult{
		Body:         res.Body,
		ETag:         res.Header.Get("ETag"),
		LastModified: res.Header.Get("Last-Modified"),
	}, nil
}
