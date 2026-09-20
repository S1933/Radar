package ranking

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/S1933/personal-radar/internal/config"
	"github.com/S1933/personal-radar/internal/logging"
	"github.com/S1933/personal-radar/internal/store"
)

func testLogger() *logging.Logger { return logging.New("test", logging.ErrorLevel) }

func newJevTestScorer(t *testing.T, h http.HandlerFunc) *jevScorer {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return newJevScorer(config.JevConfig{
		BaseURL:          srv.URL,
		Model:            "jev-latest",
		APIKey:           "test-key",
		Timeout:          5 * time.Second,
		IncludeThreshold: 0.6,
	})
}

// jevBody builds a well-formed answer payload from the level of each sub-score.
func jevBody(levels map[string]float64, include float64) string {
	answers := map[string]any{
		"include": map[string]any{"type": "noul", "noul": include},
	}
	for _, k := range []string{"relevance", "importance", "novelty", "actionability"} {
		answers[k] = map[string]any{"type": "score", "score": levels[k], "confidence": 0.9}
	}
	body, _ := json.Marshal(map[string]any{
		"model": "jev-1.13.0", "answers": answers,
		"usage": map[string]int{"input_tokens": 412, "output_tokens": 61},
	})
	return string(body)
}

func testItem() store.ScoredItem {
	return store.ScoredItem{
		DBID: 1, Source: "x", Title: "A new coding agent",
		Content: strings.Repeat("content ", 300), Engagement: 42,
	}
}

func TestJevScorerNormalizesLevelsToUnitRange(t *testing.T) {
	var gotBody map[string]any
	sc := newJevTestScorer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/systemone" {
			t.Errorf("path = %q, want /systemone", r.URL.Path)
		}
		if auth := r.Header.Get("Authorization"); auth != "Bearer test-key" {
			t.Errorf("authorization = %q", auth)
		}
		json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(jevBody(map[string]float64{
			"relevance": 4, "importance": 3, "novelty": 2, "actionability": 0}, 0.9)))
	})

	scored, err := sc.Score(context.Background(), testItem())
	if err != nil {
		t.Fatalf("Score: %v", err)
	}

	if scored.Relevance != 1 || scored.Importance != 0.75 || scored.Novelty != 0.5 || scored.Actionability != 0 {
		t.Fatalf("sub-scores not normalized: %+v", scored)
	}
	want := 0.40*1 + 0.25*0.75 + 0.15*0.5 + 0.20*0
	if diff := scored.Final - want; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("Final = %v, want %v", scored.Final, want)
	}
	if scored.Model != "jev:jev-latest" {
		t.Errorf("Model tag = %q, want jev:jev-latest", scored.Model)
	}

	// One call carries every question, and the state stays scoped to the item.
	questions, _ := gotBody["questions"].(map[string]any)
	if len(questions) != 5 {
		t.Errorf("questions = %d, want 5 (4 scores + include)", len(questions))
	}
	if _, ok := questions["include"].(map[string]any)["criteria"]; !ok {
		t.Error("include question lost its criteria")
	}
	state, _ := gotBody["state"].(map[string]any)
	if state["source"] != "x" || state["title"] != "A new coding agent" {
		t.Errorf("state = %v", state)
	}
	if content, _ := state["content"].(string); len([]rune(content)) > 1201 {
		t.Errorf("content not truncated: %d runes", len([]rune(content)))
	}
}

func TestJevScorerVerdictExposesInclude(t *testing.T) {
	sc := newJevTestScorer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(jevBody(map[string]float64{
			"relevance": 2, "importance": 2, "novelty": 2, "actionability": 2}, 0.31)))
	})
	v, err := sc.Verdict(context.Background(), testItem())
	if err != nil {
		t.Fatalf("Verdict: %v", err)
	}
	if v.Include != 0.31 {
		t.Errorf("Include = %v, want 0.31", v.Include)
	}
	if v.InputTokens != 412 || v.Model != "jev-1.13.0" {
		t.Errorf("usage/model not propagated: %+v", v)
	}
}

func TestJevScorerRetriesRateLimit(t *testing.T) {
	var calls int32
	sc := newJevTestScorer(t, func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte(`{"error":"rate_limited"}`))
			return
		}
		w.Write([]byte(jevBody(map[string]float64{
			"relevance": 3, "importance": 3, "novelty": 3, "actionability": 3}, 0.7)))
	})

	if _, err := sc.Score(context.Background(), testItem()); err != nil {
		t.Fatalf("Score after retry: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("calls = %d, want 2", got)
	}
}

func TestJevScorerFailsFastOnAuthError(t *testing.T) {
	var calls int32
	sc := newJevTestScorer(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error":"invalid api key"}`))
	})

	_, err := sc.Score(context.Background(), testItem())
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("err = %v, want a 401 error", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("calls = %d, want 1 (no retry on auth failure)", got)
	}
}

func TestJevScorerRejectsIncompleteAnswers(t *testing.T) {
	sc := newJevTestScorer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"model":"jev-1.13.0","answers":{"relevance":{"type":"score","score":3}}}`))
	})
	if _, err := sc.Score(context.Background(), testItem()); err == nil {
		t.Fatal("expected an error when answers are missing")
	}
}

func TestPickScorerFallsBackWhenJevKeyMissing(t *testing.T) {
	svc := New(config.ModelsConfig{RankEngine: "jev"}, nil, testLogger())
	if _, ok := svc.scorer.(*heuristicScorer); !ok {
		t.Fatalf("scorer = %T, want heuristic when TYPESAFE_API_KEY is empty", svc.scorer)
	}

	svc = New(config.ModelsConfig{
		RankEngine: "jev",
		Jev:        config.JevConfig{APIKey: "k", BaseURL: "https://example.invalid/v1"},
	}, nil, testLogger())
	jev, ok := svc.JevScorer()
	if !ok {
		t.Fatalf("scorer = %T, want jev", svc.scorer)
	}
	if tag := jev.Tag(); !strings.HasPrefix(tag, "jev:") {
		t.Errorf("Tag = %q", tag)
	}
}
