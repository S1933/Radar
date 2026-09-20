package ranking

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/S1933/personal-radar/internal/config"
	"github.com/S1933/personal-radar/internal/store"
	"github.com/S1933/personal-radar/internal/textutil"
)

// jevScorer is the Stage-2 ranker backed by TypeSafe's System One model (Jev).
//
// One POST per item returns four Score answers plus an include/no-include Noul,
// all evaluated in parallel: no text generation, no JSON to parse out of prose,
// no malformed-response fallback. Sub-scores come back on five descriptive
// levels and are normalized to 0..1 so they share the heuristic scorer's scale
// (and therefore the configured notify.score_threshold).
type jevScorer struct {
	cfg    config.JevConfig
	client *http.Client
}

func newJevScorer(cfg config.JevConfig) *jevScorer {
	return &jevScorer{cfg: cfg, client: &http.Client{Timeout: cfg.Timeout}}
}

// Tag identifies the scores this scorer writes. Changing the model or the
// question set must change the tag, otherwise stale rows survive:
//
//	DELETE FROM scores WHERE model = '<tag>';
func (j *jevScorer) Tag() string { return "jev:" + j.cfg.Model }

// jevVerdict is what one System One call returns: the four sub-scores plus the
// briefing gate. Include is not folded into Final — the notification threshold
// stays in config where it is auditable.
type jevVerdict struct {
	Score       store.Score
	Include     float64
	Latency     time.Duration
	InputTokens int
	Model       string
}

// jevTopLevel is the highest index of a Score rubric (five levels: 0..4).
const jevTopLevel = 4

// jevQuestions is the fixed question set. Each question asks one narrow thing:
// a judgment a knowledgeable reader makes in a second given the item.
func jevQuestions() map[string]any {
	return map[string]any{
		"relevance": map[string]any{
			"type": "score",
			"instructions": "How relevant is this item to a senior backend/platform " +
				"engineer working on Go, DevOps, AI coding agents and open source?",
			"criteria": []string{
				"Off topic", "Tangential", "Useful background",
				"Directly relevant", "Core interest",
			},
		},
		"importance": map[string]any{
			"type":         "score",
			"instructions": "How important is this item for the field it belongs to?",
			"criteria":     []string{"Negligible", "Minor", "Notable", "Significant", "Major"},
		},
		"novelty": map[string]any{
			"type":         "score",
			"instructions": "How new is this compared to what was already public knowledge?",
			"criteria":     []string{"Old news", "Incremental", "Somewhat new", "New", "Genuinely novel"},
		},
		"actionability": map[string]any{
			"type": "score",
			"instructions": "How actionable is this for the reader: something to try, " +
				"adopt, migrate to, or decide on?",
			"criteria": []string{
				"Nothing to do", "Curiosity only", "Worth a look", "Worth trying", "Act on it now",
			},
		},
		"include": map[string]any{
			"type":         "noul",
			"instructions": "Should this item appear in the reader's daily briefing?",
			"criteria": map[string]string{
				"true":  "Worth the reader's attention today",
				"false": "Noise, marketing, duplicate, or too minor",
			},
		},
	}
}

type jevAnswer struct {
	Type          string             `json:"type"`
	Score         float64            `json:"score"`
	Noul          float64            `json:"noul"`
	Choice        string             `json:"choice"`
	Confidence    float64            `json:"confidence"`
	Probabilities map[string]float64 `json:"probabilities"`
}

type jevResponse struct {
	Model   string               `json:"model"`
	Answers map[string]jevAnswer `json:"answers"`
	Usage   struct {
		InputTokens  int `json:"input_tokens"`
		OutputTokens int `json:"output_tokens"`
	} `json:"usage"`
}

// Verdict runs the single System One call for one item.
func (j *jevScorer) Verdict(ctx context.Context, it store.ScoredItem) (jevVerdict, error) {
	// Keep the state to what the questions need: accuracy drops when the state
	// carries unrelated detail (documented Jev failure mode).
	state := map[string]any{
		"source":  it.Source,
		"title":   it.Title,
		"content": textutil.Truncate(it.Content, 1200, "…"),
	}
	if it.Engagement > 0 {
		state["engagement"] = it.Engagement
	}

	body, err := json.Marshal(map[string]any{
		"state":     state,
		"model":     j.cfg.Model,
		"questions": jevQuestions(),
	})
	if err != nil {
		return jevVerdict{}, err
	}

	started := time.Now()
	res, err := j.post(ctx, body)
	if err != nil {
		return jevVerdict{}, err
	}
	latency := time.Since(started)

	var parsed jevResponse
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		return jevVerdict{}, fmt.Errorf("jev rank: decode: %w", err)
	}

	var v jevVerdict
	v.Latency = latency
	v.InputTokens = parsed.Usage.InputTokens
	v.Model = parsed.Model
	for _, key := range []string{"relevance", "importance", "novelty", "actionability"} {
		if _, ok := parsed.Answers[key]; !ok {
			return v, fmt.Errorf("jev rank: missing answer %q", key)
		}
	}
	inc, ok := parsed.Answers["include"]
	if !ok {
		return v, fmt.Errorf("jev rank: missing answer %q", "include")
	}

	v.Score = store.Score{
		Relevance:     normalizeLevel(parsed.Answers["relevance"].Score),
		Importance:    normalizeLevel(parsed.Answers["importance"].Score),
		Novelty:       normalizeLevel(parsed.Answers["novelty"].Score),
		Actionability: normalizeLevel(parsed.Answers["actionability"].Score),
		Model:         j.Tag(),
	}
	v.Score.Final = finalScore(v.Score)
	v.Include = clamp(inc.Noul, 0, 1)
	return v, nil
}

// Score satisfies the Scorer interface used by RankPending.
func (j *jevScorer) Score(ctx context.Context, it store.ScoredItem) (store.Score, error) {
	v, err := j.Verdict(ctx, it)
	return v.Score, err
}

// post performs the request with bounded retries on the statuses TypeSafe
// documents as transient (429 rate limit, 5xx, 529 overloaded).
func (j *jevScorer) post(ctx context.Context, body []byte) (*http.Response, error) {
	const attempts = 4
	var lastErr error

	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(1<<uint(attempt-1)) * 500 * time.Millisecond
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			j.cfg.BaseURL+"/systemone", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+j.cfg.APIKey)

		res, err := j.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		if res.StatusCode == http.StatusOK {
			return res, nil
		}

		msg, _ := io.ReadAll(io.LimitReader(res.Body, 400))
		res.Body.Close()
		lastErr = fmt.Errorf("jev rank: HTTP %d %s", res.StatusCode, msg)

		switch res.StatusCode {
		case http.StatusTooManyRequests, http.StatusInternalServerError,
			http.StatusBadGateway, http.StatusServiceUnavailable, 529:
			if wait := retryAfter(res); wait > 0 {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(wait):
				}
			}
		default:
			return nil, lastErr
		}
	}
	return nil, lastErr
}

func retryAfter(res *http.Response) time.Duration {
	secs, err := strconv.Atoi(res.Header.Get("retry-after"))
	if err != nil || secs <= 0 || secs > 30 {
		return 0
	}
	return time.Duration(secs) * time.Second
}

// normalizeLevel maps a Score answer (0..4, possibly fractional) onto 0..1.
func normalizeLevel(level float64) float64 {
	return clamp(level/jevTopLevel, 0, 1)
}
