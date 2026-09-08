package ranking

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/S1933/personal-radar/internal/config"
	"github.com/S1933/personal-radar/internal/store"
	"github.com/S1933/personal-radar/internal/textutil"
)

// llmScorer calls an OpenAI-compatible chat endpoint to produce the
// structured sub-scores for one item. Used as an optional Stage-2 ranker
// when models.base_url and models.api_key are configured.
type llmScorer struct {
	cfg    config.ModelsConfig
	client *http.Client
}

func newLLMScorer(cfg config.ModelsConfig) *llmScorer {
	return &llmScorer{
		cfg:    cfg,
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

// Score asks the LLM to return JSON with the four sub-scores plus include.
func (s *llmScorer) Score(ctx context.Context, it store.ScoredItem) (store.Score, error) {
	prompt := buildRankPrompt(it)
	body, err := json.Marshal(map[string]any{
		"model": s.cfg.Rank.Model,
		"messages": []map[string]string{
			{"role": "system", "content": "You are a strict relevance scorer. Reply ONLY with JSON: {\"importance\":0-10,\"personal_relevance\":0-10,\"novelty\":0-10,\"actionability\":0-10,\"include\":true|false}"},
			{"role": "user", "content": prompt},
		},
		"temperature": 0,
	})
	if err != nil {
		return store.Score{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		s.cfg.BaseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return store.Score{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+s.cfg.APIKey)
	}

	res, err := s.client.Do(req)
	if err != nil {
		return store.Score{}, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(res.Body, 512))
		return store.Score{}, fmt.Errorf("llm rank: HTTP %d %s", res.StatusCode, string(b))
	}

	var parsed struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		return store.Score{}, err
	}
	if len(parsed.Choices) == 0 {
		return store.Score{}, fmt.Errorf("llm rank: empty choices")
	}

	var out struct {
		Importance        float64 `json:"importance"`
		PersonalRelevance float64 `json:"personal_relevance"`
		Novelty           float64 `json:"novelty"`
		Actionability     float64 `json:"actionability"`
		Include           bool    `json:"include"`
	}
	if err := json.Unmarshal([]byte(parsed.Choices[0].Message.Content), &out); err != nil {
		return store.Score{}, fmt.Errorf("llm rank: parse json: %w", err)
	}

	sc := store.Score{
		Importance:    clamp(out.Importance, 0, 10),
		Relevance:     clamp(out.PersonalRelevance, 0, 10),
		Novelty:       clamp(out.Novelty, 0, 10),
		Actionability: clamp(out.Actionability, 0, 10),
		Model:         s.cfg.Rank.Model,
	}
	sc.Final = weightedScore(sc)
	return sc, nil
}

func buildRankPrompt(it store.ScoredItem) string {
	// Virality is a first-class signal: a 280-char tweet can't be judged
	// fairly on text alone, so hand the LLM the engagement numbers too.
	engagement := ""
	if it.Engagement > 0 {
		engagement = fmt.Sprintf(" (viral: %d likes/shares)", it.Engagement)
	}
	return fmt.Sprintf(
		"Title: %s\nSource: %s%s\nContent: %s",
		it.Title, it.Source, engagement, textutil.Truncate(it.Content, 1200, "…"))
}

func weightedScore(sc store.Score) float64 {
	return sc.Relevance*Weights.Relevance +
		sc.Importance*Weights.Importance +
		sc.Novelty*Weights.Novelty +
		sc.Actionability*Weights.Actionability
}
