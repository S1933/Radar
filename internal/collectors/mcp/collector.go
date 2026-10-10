// Package mcp collects items from a remote MCP server over the streamable
// HTTP transport (JSON-RPC 2.0, one POST per tool call).
//
// It is the bridge for "Hermes France MCP", the read-only veille of the
// Hermes France Discord community: the server exposes the messages of the
// community as data (tools list_channels / get_recent / search / digest /
// top_links) and this collector turns the ones carrying a link into regular
// radar items, so community-shared links are scored, deduped against RSS/X
// and proposed as cards like any other source.
//
// Trust boundary: every message is written by a third party. It is stored as
// DATA, never interpreted — no message content is executed, and the
// collector only reads it.
package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/S1933/personal-radar/internal/config"
	"github.com/S1933/personal-radar/internal/model"
	"github.com/S1933/personal-radar/internal/textutil"
)

// SourceName is the value stored in items.source. Every item coming from the
// MCP transport is a Discord message, so "discord" is the honest source label
// (the same way the X sidecar labels its rows "x").
const SourceName = "discord"

// Logger is the minimal logging surface this collector needs.
type Logger interface {
	Warn(msg string, kv ...any)
	Info(msg string, kv ...any)
}

// Caller performs one MCP tools/call and returns the raw JSON-RPC result.
// httpCaller implements it; tests substitute a fake so the item-shaping rules
// can be exercised without a live server.
type Caller interface {
	Call(ctx context.Context, tool string, args map[string]any) (json.RawMessage, error)
}

// Collector reads the recent messages of an MCP-backed community feed.
type Collector struct {
	cfg    config.MCPConfig
	log    Logger
	caller Caller
	now    func() time.Time
}

// NewCollector builds the collector with the HTTP transport.
func NewCollector(cfg config.MCPConfig, log Logger) *Collector {
	c := &Collector{cfg: cfg, log: log, now: func() time.Time { return time.Now().UTC() }}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	c.caller = &httpCaller{url: cfg.URL, client: &http.Client{Timeout: timeout}}
	return c
}

// SetCaller overrides the transport (tests).
func (c *Collector) SetCaller(caller Caller) { c.caller = caller }

// SetClock overrides the clock (tests).
func (c *Collector) SetClock(now func() time.Time) { c.now = now }

// Name returns the collector name recorded in the runs table.
func (c *Collector) Name() string { return SourceName }

// backfillDelay spaces per-channel refetches so a clipped window does not
// burst the server's per-client rate limit. The service page warns the rate
// is capped per client; hammering 30 channels back-to-back trips it.
const backfillDelay = 400 * time.Millisecond

// Collect fetches the messages of the lookback window and shapes them into
// items. A failure on the whole-window call is returned (the pipeline records
// it and moves on); a per-channel backfill failure only stops the backfill,
// so a partial result still gets ingested.
func (c *Collector) Collect(ctx context.Context) ([]model.Item, error) {
	hours := c.cfg.Hours
	if hours <= 0 {
		hours = 6
	}
	limit := c.cfg.Limit
	if limit <= 0 {
		limit = 100
	}

	msgs, err := c.recent(ctx, "", hours, limit)
	if err != nil {
		return nil, err
	}
	// The get_recent tool has no pagination and caps at 100 messages, so a
	// busy window would silently drop the oldest messages of the window.
	// When the cap is hit, re-fetch channel by channel to recover them.
	if len(msgs) >= limit {
		c.log.Warn("mcp window clipped, backfilling per channel",
			"server", c.cfg.Name, "messages", len(msgs))
		for _, ch := range c.channels(ctx) {
			more, cerr := c.recent(ctx, ch, hours, limit)
			if cerr != nil {
				// 429 / "rate limited": the server throttles per client
				// and the rest of this burst would be throttled too, so
				// stop instead of hammering it. The no-clip steady state
				// never reaches this branch.
				c.log.Warn("mcp channel fetch failed, stopping backfill",
					"channel", ch, "error", cerr)
				break
			}
			msgs = append(msgs, more...)
			select {
			case <-time.After(backfillDelay):
			case <-ctx.Done():
				return c.buildItems(msgs), nil
			}
		}
	}
	return c.buildItems(msgs), nil
}

// buildItems dedupes the fetched messages by id and shapes the survivors into
// items, applying the configured filters.
func (c *Collector) buildItems(msgs []message) []model.Item {
	now := c.now()
	seen := make(map[string]bool, len(msgs))
	var items []model.Item
	for _, m := range msgs {
		if m.ID == "" || seen[m.ID] {
			continue
		}
		seen[m.ID] = true
		it, keep := c.item(m, now)
		if !keep {
			continue
		}
		items = append(items, it)
	}
	if len(items) == 0 {
		c.log.Info("mcp collect: no message matched the filter",
			"server", c.cfg.Name, "messages", len(msgs))
	}
	return items
}

// channels returns the channel list to use for a backfill: the configured
// ones, or every channel the server reports when none is configured.
func (c *Collector) channels(ctx context.Context) []string {
	if len(c.cfg.Channels) > 0 {
		return c.cfg.Channels
	}
	raw, err := c.caller.Call(ctx, "list_channels", map[string]any{})
	if err != nil {
		c.log.Warn("mcp list_channels failed", "error", err)
		return nil
	}
	var out []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		c.log.Warn("mcp list_channels decode failed", "error", err)
		return nil
	}
	names := make([]string, 0, len(out))
	for _, ch := range out {
		if ch.Name != "" {
			names = append(names, ch.Name)
		}
	}
	return names
}

// recent calls get_recent and decodes the message list.
func (c *Collector) recent(ctx context.Context, channel string, hours float64, limit int) ([]message, error) {
	args := map[string]any{"hours": hours, "limit": limit}
	if channel != "" {
		args["channel"] = channel
	}
	raw, err := c.caller.Call(ctx, "get_recent", args)
	if err != nil {
		if channel == "" {
			return nil, err
		}
		return nil, fmt.Errorf("channel %q: %w", channel, err)
	}
	var msgs []message
	if err := json.Unmarshal(raw, &msgs); err != nil {
		return nil, fmt.Errorf("decode messages: %w", err)
	}
	return msgs, nil
}

// message is one Discord message as returned by the server.
type message struct {
	ID        string   `json:"id"`
	Channel   string   `json:"channel"`
	Time      string   `json:"time"`
	Author    string   `json:"author"`
	Content   string   `json:"content"`
	ReplyTo   *string  `json:"reply_to"`
	Reactions int      `json:"reactions"`
	Links     []string `json:"links"`
}

// item shapes one message into a radar item. keep is false when the message
// is filtered out (no link by default, too short, not enough reactions).
func (c *Collector) item(m message, now time.Time) (model.Item, bool) {
	text := strings.TrimSpace(m.Content)
	links := externalLinks(m.Links)

	if len(links) == 0 && !c.cfg.AllowLinkless {
		return model.Item{}, false
	}
	if text == "" && len(links) == 0 {
		return model.Item{}, false
	}
	if c.cfg.MinChars > 0 && len([]rune(text)) < c.cfg.MinChars {
		return model.Item{}, false
	}
	if m.Reactions < c.cfg.MinReactions {
		return model.Item{}, false
	}

	title := firstLine(text)
	if title == "" {
		title = fmt.Sprintf("Message de %s dans %s", m.Author, m.Channel)
	}
	title = textutil.TruncateWords(title, 110, "…")

	link := ""
	if len(links) > 0 {
		link = links[0]
	}

	body := text
	if link != "" {
		body = strings.TrimSpace(body + "\n\nLien partagé : " + link)
	}
	body = strings.TrimSpace(body + fmt.Sprintf("\n\nSalon : %s · Auteur : %s", m.Channel, m.Author))

	meta := map[string]string{
		"mode":       "mcp",
		"mcp_server": c.cfg.Name,
		"channel":    m.Channel,
		"author":     m.Author,
		"message_id": m.ID,
		"reactions":  fmt.Sprintf("%d", m.Reactions),
	}
	if len(links) > 0 {
		meta["links"] = strings.Join(links, " ")
	}
	if m.ReplyTo != nil && *m.ReplyTo != "" {
		meta["reply_to"] = *m.ReplyTo
	}

	it := model.Item{
		Source:       SourceName,
		SourceID:     m.ID,
		Author:       m.Author,
		Title:        title,
		Content:      body,
		URL:          link,
		CanonicalURL: canonicalURL(link),
		PublishedAt:  parseTime(m.Time, now),
		CollectedAt:  now,
		Topics:       append([]string(nil), c.cfg.Topics...),
		Language:     "fr",
		Engagement:   model.Engagement{Score: int64(m.Reactions)},
		Metadata:     meta,
	}
	return it, true
}

// firstLine returns the first non-empty line of s, collapsed to single
// spaces. Discord messages are usually one paragraph; a multi-line message
// keeps only its opening line as the title.
func firstLine(s string) string {
	for _, line := range strings.Split(s, "\n") {
		line = strings.Join(strings.Fields(line), " ")
		if line != "" {
			return line
		}
	}
	return ""
}

// externalLinks keeps http(s) links and drops Discord-internal ones
// (invites, channel/message permalinks): those point back at the chat, not at
// content worth a radar card.
func externalLinks(links []string) []string {
	var out []string
	for _, l := range links {
		l = strings.TrimSpace(l)
		if !strings.HasPrefix(l, "http://") && !strings.HasPrefix(l, "https://") {
			continue
		}
		u, err := url.Parse(l)
		if err != nil || u.Host == "" {
			continue
		}
		host := strings.ToLower(u.Host)
		if host == "discord.com" || host == "discord.gg" || host == "www.discord.com" ||
			strings.HasSuffix(host, ".discord.com") {
			continue
		}
		out = append(out, l)
	}
	return out
}

// parseTime accepts RFC3339 (what the server returns) and falls back to now
// so a malformed timestamp never loses an item.
func parseTime(raw string, fallback time.Time) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t.UTC()
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t.UTC()
	}
	return fallback
}

// canonicalURL normalizes a link for cross-source dedup (same rule as the RSS
// collector: drop query and fragment, lowercase host, trim trailing slash).
// Returns "" for an empty link, which makes store.FindDuplicate skip its
// canonical-URL probe instead of matching every other URL-less item.
func canonicalURL(raw string) string {
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

// httpCaller is the streamable-HTTP MCP transport: one POST per tools/call,
// the response is either an SSE stream (the transport the server advertises)
// or a plain JSON body.
type httpCaller struct {
	url    string
	client *http.Client
	nextID int
}

func (h *httpCaller) Call(ctx context.Context, tool string, args map[string]any) (json.RawMessage, error) {
	if args == nil {
		args = map[string]any{}
	}
	h.nextID++
	reqBody, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      h.nextID,
		"method":  "tools/call",
		"params":  map[string]any{"name": tool, "arguments": args},
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url, bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("MCP-Protocol-Version", "2025-06-18")
	req.Header.Set("User-Agent", "personal-radar/0.1 (+https://github.com/S1933)")

	res, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", res.StatusCode, textutil.Truncate(string(body), 200, "…"))
	}
	return decodeRPC(body)
}

// rpcResponse is the JSON-RPC envelope subset we care about.
type rpcResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// toolResult is the MCP tools/call result: the structured payload (preferred,
// when the server declares an outputSchema) or the text content blocks.
type toolResult struct {
	StructuredContent struct {
		Result json.RawMessage `json:"result"`
	} `json:"structuredContent"`
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	IsError bool `json:"isError"`
}

// decodeRPC extracts the payload of a tools/call response. The transport may
// answer with an SSE stream ("event: message" + "data: {...}") or with a bare
// JSON body; both are handled so a server upgrade cannot silently break the
// collector.
func decodeRPC(body []byte) (json.RawMessage, error) {
	payloads := ssePayloads(body)
	if len(payloads) == 0 {
		payloads = [][]byte{body}
	}
	var lastErr error
	for _, p := range payloads {
		var env rpcResponse
		if err := json.Unmarshal(p, &env); err != nil {
			lastErr = err
			continue
		}
		if env.Error != nil {
			return nil, fmt.Errorf("MCP error %d: %s", env.Error.Code, env.Error.Message)
		}
		if len(env.Result) == 0 {
			continue
		}
		var tr toolResult
		if err := json.Unmarshal(env.Result, &tr); err != nil {
			return nil, fmt.Errorf("decode tool result: %w", err)
		}
		if tr.IsError {
			return nil, fmt.Errorf("MCP tool reported an error")
		}
		if len(tr.StructuredContent.Result) > 0 {
			return tr.StructuredContent.Result, nil
		}
		for _, blk := range tr.Content {
			if blk.Type == "text" && strings.TrimSpace(blk.Text) != "" {
				return json.RawMessage(strings.TrimSpace(blk.Text)), nil
			}
		}
		return json.RawMessage("[]"), nil
	}
	if lastErr != nil {
		return nil, fmt.Errorf("decode response: %w", lastErr)
	}
	return nil, fmt.Errorf("empty MCP response")
}

// ssePayloads returns the JSON payloads of an SSE body, or nil when the body
// is not an event stream.
func ssePayloads(body []byte) [][]byte {
	text := string(body)
	if !strings.Contains(text, "data:") {
		return nil
	}
	var out [][]byte
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimRight(line, "\r")
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "" || data == "[DONE]" {
			continue
		}
		out = append(out, []byte(data))
	}
	return out
}
