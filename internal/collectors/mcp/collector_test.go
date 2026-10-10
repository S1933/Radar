package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/S1933/personal-radar/internal/config"
)

// fakeCaller serves canned tool payloads shaped like the real server's
// structuredContent.result.
type fakeCaller struct {
	messages []map[string]any
	channels []string
	calls    []string
}

func (f *fakeCaller) Call(_ context.Context, tool string, args map[string]any) (json.RawMessage, error) {
	f.calls = append(f.calls, tool)
	switch tool {
	case "get_recent":
		b, err := json.Marshal(f.messages)
		return b, err
	case "list_channels":
		out := make([]map[string]any, 0, len(f.channels))
		for _, c := range f.channels {
			out = append(out, map[string]any{"name": c})
		}
		b, err := json.Marshal(out)
		return b, err
	}
	return nil, fmt.Errorf("unexpected tool %q", tool)
}

func newTestCollector(cfg config.MCPConfig, caller Caller) *Collector {
	c := NewCollector(cfg, nopLogger{})
	c.SetCaller(caller)
	c.SetClock(func() time.Time { return time.Date(2026, 10, 10, 8, 0, 0, 0, time.UTC) })
	return c
}

type nopLogger struct{}

func (nopLogger) Warn(string, ...any) {}
func (nopLogger) Info(string, ...any) {}

func msg(id, channel, author, content string, reactions int, links ...string) map[string]any {
	return map[string]any{
		"id": id, "channel": channel, "author": author, "content": content,
		"time": "2026-10-09T22:06:56.782000+00:00", "reactions": reactions,
		"links": links, "reply_to": nil,
	}
}

func TestCollectKeepsOnlyMessagesWithAnExternalLink(t *testing.T) {
	caller := &fakeCaller{messages: []map[string]any{
		msg("1", "Conseils en IA locale", "Waevile", "Un guide d'installation pas mal", 2, "https://tekbrut.fr/tutos/hermes-agent/"),
		msg("2", "Second brain", "Tetrax", "J'utilise obsidian pour mes projets", 0),
		msg("3", "général-🏡", "Mike", "regarde ici https://discord.gg/hermesfrance", 1, "https://discord.gg/hermesfrance"),
	}}
	c := newTestCollector(config.MCPConfig{Name: "hermes-france", Hours: 3, Limit: 100}, caller)

	items, err := c.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("want 1 item (linked message only), got %d", len(items))
	}
	it := items[0]
	if it.Source != SourceName || it.SourceID != "1" {
		t.Errorf("source/source_id = %q/%q", it.Source, it.SourceID)
	}
	if it.URL != "https://tekbrut.fr/tutos/hermes-agent/" {
		t.Errorf("url = %q", it.URL)
	}
	if it.CanonicalURL != "https://tekbrut.fr/tutos/hermes-agent" {
		t.Errorf("canonical_url = %q", it.CanonicalURL)
	}
	if it.Author != "Waevile" || it.Metadata["channel"] != "Conseils en IA locale" {
		t.Errorf("author/channel = %q/%q", it.Author, it.Metadata["channel"])
	}
	if it.Metadata["mode"] != "mcp" || it.Metadata["mcp_server"] != "hermes-france" {
		t.Errorf("metadata = %v", it.Metadata)
	}
	if it.Engagement.Score != 2 {
		t.Errorf("engagement = %d", it.Engagement.Score)
	}
	if !strings.Contains(it.Content, "Lien partagé : https://tekbrut.fr") {
		t.Errorf("content does not carry the shared link: %q", it.Content)
	}
	if it.Language != "fr" || it.PublishedAt.IsZero() {
		t.Errorf("language/published_at = %q/%v", it.Language, it.PublishedAt)
	}
}

func TestAllowLinklessKeepsMessagesWithoutURL(t *testing.T) {
	caller := &fakeCaller{messages: []map[string]any{
		msg("1", "Second brain", "Tetrax", "J'utilise obsidian pour ma gestion des projets", 0),
	}}
	c := newTestCollector(config.MCPConfig{AllowLinkless: true}, caller)

	items, err := c.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("want 1 item, got %d", len(items))
	}
	if items[0].URL != "" || items[0].CanonicalURL != "" {
		t.Errorf("url/canonical = %q/%q, want empty", items[0].URL, items[0].CanonicalURL)
	}
	if items[0].Title == "" {
		t.Error("title is empty")
	}
}

func TestFiltersMinCharsAndMinReactions(t *testing.T) {
	caller := &fakeCaller{messages: []map[string]any{
		msg("1", "général-🏡", "Mike", "top", 5, "https://a.example/1"),
		msg("2", "général-🏡", "Mike", "un message long sur la gestion de contexte dans les agents", 0, "https://a.example/2"),
		msg("3", "général-🏡", "Mike", "encore un message assez long pour passer le filtre de taille", 1, "https://a.example/3"),
	}}
	c := newTestCollector(config.MCPConfig{MinChars: 20, MinReactions: 1}, caller)

	items, err := c.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("want only message 3, got %d items", len(items))
	}
	if items[0].SourceID != "3" {
		t.Errorf("kept source_id %q, want 3", items[0].SourceID)
	}
}

func TestCollectDedupesRepeatedMessagesAndBackfills(t *testing.T) {
	// The unfiltered call returns exactly `limit` messages (the server cap is
	// hit), so the collector must ask for the channel list and re-fetch per
	// channel; duplicates by message id are dropped.
	caller := &fakeCaller{channels: []string{"général-🏡", "Second brain"}}
	for i := 0; i < 2; i++ {
		caller.messages = append(caller.messages,
			msg(fmt.Sprintf("%d", i), "général-🏡", "Mike", "message avec lien", 0, "https://a.example/x"))
	}
	c := newTestCollector(config.MCPConfig{Limit: 2, Channels: []string{"général-🏡"}}, caller)

	items, err := c.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("want 2 deduped items, got %d", len(items))
	}
	if len(caller.calls) != 2 {
		t.Errorf("want the unfiltered call + one backfill, got %v", caller.calls)
	}
}

func TestHTTPCallerParsesSSEAndJSON(t *testing.T) {
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		if strings.Contains(gotBody, `"get_recent"`) {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", `{"jsonrpc":"2.0","id":1,"result":{"structuredContent":{"result":[{"id":"42","channel":"général-🏡","author":"Waevile","time":"2026-10-09T22:06:56.782Z","content":"hello","reactions":3,"links":["https://tekbrut.fr/x"]}]},"content":[{"type":"text","text":"ignored"}],"isError":false}}`)
			return
		}
		// Plain JSON body, text-only content block (no outputSchema).
		fmt.Fprint(w, `{"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"[{\"name\":\"Second brain\"}]"}],"isError":false}}`)
	}))
	defer srv.Close()

	c := NewCollector(config.MCPConfig{URL: srv.URL, Name: "hermes-france", Hours: 3, Limit: 100}, nopLogger{})
	items, err := c.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(items) != 1 || items[0].SourceID != "42" || items[0].URL != "https://tekbrut.fr/x" {
		t.Fatalf("unexpected items from SSE payload: %+v", items)
	}
	if !strings.Contains(gotBody, `"method":"tools/call"`) || !strings.Contains(gotBody, `"name":"get_recent"`) {
		t.Errorf("request body is not a tools/call: %s", gotBody)
	}

	// The same transport must decode a text-only content block.
	raw, err := c.caller.Call(context.Background(), "list_channels", nil)
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if string(raw) != `[{"name":"Second brain"}]` {
		t.Errorf("text content block not decoded: %s", raw)
	}
}

func TestHTTPCallerReportsMCPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"error\":{\"code\":-32602,\"message\":\"bad args\"}}\n\n")
	}))
	defer srv.Close()

	caller := &httpCaller{url: srv.URL, client: srv.Client()}
	if _, err := caller.Call(context.Background(), "get_recent", nil); err == nil {
		t.Fatal("want an error for a JSON-RPC error envelope")
	} else if !strings.Contains(err.Error(), "bad args") {
		t.Errorf("error = %v", err)
	}
}

func TestExternalLinksDropsDiscordTargets(t *testing.T) {
	got := externalLinks([]string{
		"https://discord.com/channels/1/2/3",
		"https://discord.gg/abc",
		"not-a-url",
		"https://github.com/usestrix/strix",
	})
	if len(got) != 1 || got[0] != "https://github.com/usestrix/strix" {
		t.Fatalf("externalLinks = %v", got)
	}
}

func TestBackfillStopsOnRateLimit(t *testing.T) {
	// A clipped window fans out per channel; the moment one returns a
	// per-client rate limit, the collector must stop hammering the server
	// and still ingest the messages it already has (the unfiltered call).
	cf := &fakeCaller{messages: []map[string]any{}, channels: []string{"a", "b", "c"}}
	cf.messages = append(cf.messages, msg("1", "général", "Mike", "un lien partagé", 0, "https://a.example/1"))
	flaky := &flakyCaller{Caller: cf}
	c := newTestCollector(config.MCPConfig{Limit: 1, Channels: []string{"a", "b", "c"}}, flaky)

	items, err := c.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect should not fail on a rate-limited backfill: %v", err)
	}
	if len(items) != 1 || items[0].SourceID != "1" {
		t.Fatalf("want the unfiltered-call item ingested, got %+v", items)
	}
}

// flakyCaller fails the FIRST per-channel get_recent (any call that carries a
// channel argument), standing in for the server's per-client rate limit.
type flakyCaller struct {
	Caller
}

func (f *flakyCaller) Call(ctx context.Context, tool string, args map[string]any) (json.RawMessage, error) {
	if tool == "get_recent" {
		ch, _ := args["channel"].(string)
		if ch != "" {
			return nil, fmt.Errorf("HTTP 429: rate limited")
		}
	}
	return f.Caller.Call(ctx, tool, args)
}

func TestCanonicalURL(t *testing.T) {
	cases := map[string]string{
		"https://www.damyr.fr/p/hermes/?utm_source=x#top": "https://www.damyr.fr/p/hermes",
		"": "",
	}
	for in, want := range cases {
		if got := canonicalURL(in); got != want {
			t.Errorf("canonicalURL(%q) = %q, want %q", in, got, want)
		}
	}
}
