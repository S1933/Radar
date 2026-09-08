package ranking

import (
	"context"
	"math"
	"testing"

	"github.com/S1933/personal-radar/internal/store"
)

func TestHeuristicScoring(t *testing.T) {
	s := newHeuristicScorer()

	ai := store.ScoredItem{
		Title:      "OpenAI releases new coding agent SDK",
		Content:    "The agent SDK for LLM developers is out.",
		Engagement: 5000,
	}
	sc, err := s.Score(context.Background(), ai)
	if err != nil {
		t.Fatal(err)
	}
	if sc.Relevance < 0.4 {
		// Threshold reflects the new BM25-based relevance: 0.4 is
		// enough headroom for the "coding agent" topic to win, and
		// still well above the 0 an "all-avoid" item gets.
		t.Errorf("AI item should be relevant, got %.2f", sc.Relevance)
	}
	if sc.Novelty < 0.5 {
		t.Errorf("release item should be novel, got %.2f", sc.Novelty)
	}

	noise := store.ScoredItem{Title: "Celebrity sports news: crypto pump alert", Content: "kardashian"}
	sc2, _ := s.Score(context.Background(), noise)
	if sc2.Relevance != 0 || sc2.Importance != 0 {
		t.Errorf("avoid-list item must be zeroed, got %+v", sc2)
	}
}

func TestFinalScoreWeights(t *testing.T) {
	sc := store.Score{Relevance: 1, Importance: 1, Novelty: 1, Actionability: 1}
	got := finalScore(sc)
	if math.Abs(got-1.0) > 1e-9 {
		t.Errorf("all-ones score should be 1.0, got %.2f", got)
	}
	empty := finalScore(store.Score{})
	if empty != 0 {
		t.Errorf("zero score must be 0, got %.2f", empty)
	}
}
