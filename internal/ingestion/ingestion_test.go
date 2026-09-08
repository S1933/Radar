package ingestion

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/S1933/personal-radar/internal/model"
)

// fakeStore is an in-memory implementation of the Store interface,
// sufficient to exercise the cross-source dedup path without a real
// Postgres. The map is keyed by (source, source_id) — the same
// surface identity InsertItem uses as its unique constraint.
type fakeStore struct {
	mu        sync.Mutex
	rows      map[string]int64      // (source, source_id) → id
	canonical map[string]int64      // canonical_url → id (first match)
	hashes    map[string]int64      // content_hash → id (first match)
	sources   map[int64][]string    // id → additional source refs
	nextID    int64
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		rows:      map[string]int64{},
		canonical: map[string]int64{},
		hashes:    map[string]int64{},
		sources:   map[int64][]string{},
	}
}

func (f *fakeStore) key(src, sid string) string { return src + "\x00" + sid }

func (f *fakeStore) InsertItem(_ context.Context, it model.Item) (int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := f.key(it.Source, it.SourceID)
	if id, ok := f.rows[k]; ok {
		return id, false, nil
	}
	f.nextID++
	f.rows[k] = f.nextID
	if it.CanonicalURL != "" {
		if _, exists := f.canonical[it.CanonicalURL]; !exists {
			f.canonical[it.CanonicalURL] = f.nextID
		}
	}
	if h := model.ContentHash(it); h != "" {
		if _, exists := f.hashes[h]; !exists {
			f.hashes[h] = f.nextID
		}
	}
	return f.nextID, true, nil
}

func (f *fakeStore) AddItemSource(_ context.Context, id int64, _, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sources[id] = append(f.sources[id], "x")
	return nil
}

func (f *fakeStore) FindDuplicate(_ context.Context, canonicalURL, hash string) (int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if canonicalURL != "" {
		if id, ok := f.canonical[canonicalURL]; ok {
			return id, true, nil
		}
	}
	if hash != "" {
		if id, ok := f.hashes[hash]; ok {
			return id, true, nil
		}
	}
	return 0, false, nil
}

func (f *fakeStore) ItemSource(_ context.Context, id int64) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for k, v := range f.rows {
		if v == id {
			// key is "source\x00sourceID" (see key() above)
			for i := 0; i < len(k); i++ {
				if k[i] == 0 {
					return k[:i], nil
				}
			}
		}
	}
	return "", nil
}

type captureLog struct{ infos []string }

func (c *captureLog) Info(_ string, _ ...any) { c.infos = append(c.infos, "info") }
func (c *captureLog) Warn(_ string, _ ...any) {}

func TestIngestBatch_DedupByCanonicalURL(t *testing.T) {
	fs := newFakeStore()
	log := &captureLog{}
	s := New(fs, log)

	common := model.Item{
		Title:        "OpenAI releases Agent SDK",
		Content:      "The SDK is now generally available.",
		CanonicalURL: "https://openai.com/blog/agent-sdk",
		PublishedAt:  time.Now(),
	}
	rss := common
	rss.Source = "rss"
	rss.SourceID = "rss-feed-1:42"
	rss.URL = "https://example.com/feed.xml"

	x := common
	x.Source = "x"
	x.SourceID = "tweet-1234"
	x.URL = "https://x.com/openai/status/1234"

	// RSS arrives first.
	n, err := s.IngestBatch(context.Background(), "rss", []model.Item{rss})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("rss devrait être nouveau, got inserted=%d", n)
	}

	// X arrives second: same canonical URL → must merge, not insert.
	n, err = s.IngestBatch(context.Background(), "x", []model.Item{x})
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("x aurait dû être fusionné (inserted=%d), pas inséré", n)
	}
	// The dedup merge must have logged an info line.
	found := false
	for _, m := range log.infos {
		if m == "info" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("dedup merge aurait dû être logué, got %v", log.infos)
	}
}

func TestIngestBatch_DedupByContentHash(t *testing.T) {
	// Same title + same content, no canonical URL on either side.
	// FindDuplicate should fall back to the content hash.
	fs := newFakeStore()
	s := New(fs, &captureLog{})

	mk := func(src, sid string) model.Item {
		return model.Item{
			Source:      src,
			SourceID:    sid,
			Title:       "Anthropic releases Claude 5",
			Content:     "Today we are announcing Claude 5, with significant improvements.",
			URL:         "https://anthropic.com/news/claude-5-" + sid,
			PublishedAt: time.Now(),
		}
	}
	rss := mk("rss", "1")
	x := mk("x", "2")

	if _, err := s.IngestBatch(context.Background(), "rss", []model.Item{rss}); err != nil {
		t.Fatal(err)
	}
	n, err := s.IngestBatch(context.Background(), "x", []model.Item{x})
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("x aurait dû fusionner par content_hash, inserted=%d", n)
	}
}

func TestIngestBatch_NoFalsePositive(t *testing.T) {
	// Empty canonical URL + empty content hash → FindDuplicate returns
	// (0, false, nil) and IngestBatch falls through to InsertItem.
	fs := newFakeStore()
	s := New(fs, &captureLog{})

	items := []model.Item{
		{Source: "rss", SourceID: "1", Title: "Different story A"},
		{Source: "rss", SourceID: "2", Title: "Different story B"},
	}
	n, err := s.IngestBatch(context.Background(), "rss", items)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("deux items distincts auraient dû être insérés, got %d", n)
	}
}

func TestIngestBatch_DedupLogSilentOnSelfMatch(t *testing.T) {
	// The previous code logged "dedup merge" for every match. An
	// item with a canonical_url or content_hash that matches
	// itself (the same row the cycle just inserted) used to
	// flood the log with non-merge noise. The new gate logs
	// only when the originating source differs.
	fs := newFakeStore()
	log := &captureLog{}
	s := New(fs, log)

	common := model.Item{
		Title:        "OpenAI ships GPT",
		Content:      "The new model is now generally available.",
		CanonicalURL: "https://openai.com/blog/gpt",
		Source:       "rss",
		SourceID:     "rss-1",
	}
	// First arrival creates the row.
	if _, err := s.IngestBatch(context.Background(), "rss", []model.Item{common}); err != nil {
		t.Fatal(err)
	}
	before := len(log.infos)
	// Second arrival with a different surface id but identical
	// canonical_url — the dedup branch fires, but the source
	// matches the row already in store, so we must stay silent.
	again := common
	again.SourceID = "rss-2"
	if _, err := s.IngestBatch(context.Background(), "rss", []model.Item{again}); err != nil {
		t.Fatal(err)
	}
	if got := len(log.infos); got != before {
		t.Errorf("self-merge should not log; infos went from %d to %d", before, got)
	}
}

func TestIngestBatch_DedupLogNoisyOnCrossSource(t *testing.T) {
	// The same story via a different collector must still log
	// — that is the only signal that surfaces cross-source
	// coverage in production.
	fs := newFakeStore()
	log := &captureLog{}
	s := New(fs, log)

	common := model.Item{
		Title:        "OpenAI ships GPT",
		Content:      "The new model is now generally available.",
		CanonicalURL: "https://openai.com/blog/gpt",
	}
	rss := common
	rss.Source = "rss"
	rss.SourceID = "rss-1"
	if _, err := s.IngestBatch(context.Background(), "rss", []model.Item{rss}); err != nil {
		t.Fatal(err)
	}
	before := len(log.infos)
	x := common
	x.Source = "x"
	x.SourceID = "tweet-1"
	if _, err := s.IngestBatch(context.Background(), "x", []model.Item{x}); err != nil {
		t.Fatal(err)
	}
	if got := len(log.infos); got <= before {
		t.Errorf("cross-source merge should log; infos stayed at %d", got)
	}
}
