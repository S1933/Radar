package ranking

import (
	"context"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/S1933/personal-radar/internal/config"
	"github.com/S1933/personal-radar/internal/logging"
	"github.com/S1933/personal-radar/internal/store"
)

// Weights of the final score. Configurable in future config revisions.
var Weights = struct {
	Relevance     float64
	Importance    float64
	Novelty       float64
	Actionability float64
}{0.40, 0.25, 0.15, 0.20}

// Service scores unscored items. POC uses a deterministic heuristic scorer;
// an LLM stage can be plugged via Scorer.
type Service struct {
	models config.ModelsConfig
	store  *store.Store
	log    *logging.Logger
	scorer Scorer
}

// ModelsConfig exposes the loaded model configuration (used by the briefing
// synthesizer to reuse the same endpoint/key).
func (s *Service) ModelsConfig() config.ModelsConfig { return s.models }

// Scorer produces sub-scores for one item (heuristic or LLM-backed).
type Scorer interface {
	Score(ctx context.Context, it store.ScoredItem) (store.Score, error)
}

func New(models config.ModelsConfig, st *store.Store, log *logging.Logger) *Service {
	s := &Service{models: models, store: st, log: log}
	s.scorer = s.pickScorer()
	return s
}

// pickScorer resolves models.rank_engine into a Scorer. It always falls back to
// the deterministic heuristic when the selected engine cannot run (missing key
// or endpoint): a ranker that fails to build must never stop the pipeline.
func (s *Service) pickScorer() Scorer {
	engine := strings.ToLower(strings.TrimSpace(s.models.RankEngine))
	switch engine {
	case "jev":
		if s.models.Jev.APIKey == "" {
			s.log.Warn("rank_engine=jev without TYPESAFE_API_KEY, using heuristic")
			break
		}
		s.log.Info("stage-2 ranker", "engine", "jev", "model", s.models.Jev.Model)
		return newJevScorer(s.models.Jev)
	case "llm":
		if s.models.BaseURL == "" || s.models.APIKey == "" {
			s.log.Warn("rank_engine=llm without endpoint/key, using heuristic")
			break
		}
		return newLLMScorer(s.models)
	case "heuristic":
		return newHeuristicScorer()
	case "":
		// Legacy behaviour: llm_rank opts into the chat-completion ranker.
		if s.models.LLMRank && s.models.BaseURL != "" && s.models.APIKey != "" {
			return newLLMScorer(s.models)
		}
	default:
		s.log.Warn("unknown rank_engine, using heuristic", "engine", s.models.RankEngine)
	}
	return newHeuristicScorer()
}

// scorerTag labels the rows written by the active scorer, so a formula or model
// change can invalidate them:
//
//	DELETE FROM scores WHERE model = '<tag>';
func (s *Service) scorerTag() string {
	switch sc := s.scorer.(type) {
	case *jevScorer:
		return sc.Tag()
	case *llmScorer:
		return "llm:" + s.models.Rank.Model
	default:
		return "heuristic-v2"
	}
}

// JevScorer returns the Jev scorer when it is the active engine (used by the
// compare command to score the backlog without writing anything).
func (s *Service) JevScorer() (*jevScorer, bool) {
	sc, ok := s.scorer.(*jevScorer)
	return sc, ok
}

// SetScorer overrides the scoring strategy (tests, future LLM stage).
func (s *Service) SetScorer(sc Scorer) { s.scorer = sc }

// RankPending scores items collected in the last 48h without a score.
func (s *Service) RankPending(ctx context.Context) (int, error) {
	items, err := s.store.UnscoredItems(ctx, 48*time.Hour)
	if err != nil {
		return 0, err
	}

	var n int
	for _, it := range items {
		sc, err := s.scorer.Score(ctx, it)
		tag := s.scorerTag()
		if err != nil {
			// LLM/Jev stage failed (rate-limit, transport) — fall back to the
			// deterministic heuristic so the item is still scored and the
			// pending queue does not loop forever on the same items. The tag
			// must follow the values actually written.
			s.log.Warn("score item (stage-2 failed, heuristic fallback)", "id", it.DBID, "error", err)
			sc, _ = newHeuristicScorer().Score(ctx, it)
			tag = "heuristic-v2"
		}
		sc.Final = finalScore(sc)
		// Bump on every change of formula — the model tag is
		// what lets us invalidate stale scores via
		//   DELETE FROM scores WHERE model = '<previous tag>';
		// and let RankPending recompute them. heuristic-v2
		// ships with the BM25 relevance; v1 was the
		// substring-count relevance.
		sc.Model = tag
		if err := s.store.SaveScore(ctx, it.DBID, sc); err != nil {
			s.log.Warn("save score", "id", it.DBID, "error", err)
			continue
		}
		n++
	}
	if n > 0 {
		s.log.Info("ranked", "items", n)
	}
	return n, nil
}

func finalScore(sc store.Score) float64 {
	return sc.Relevance*Weights.Relevance +
		sc.Importance*Weights.Importance +
		sc.Novelty*Weights.Novelty +
		sc.Actionability*Weights.Actionability
}

func clamp(v, lo, hi float64) float64 { return math.Max(lo, math.Min(hi, v)) }

// heuristicScorer computes deterministic sub-scores from item fields.
type heuristicScorer struct {
	bm25          *bm25
	topicKeywords map[string][]string // exposed for tests
}

func newHeuristicScorer() *heuristicScorer {
	return &heuristicScorer{
		bm25:          newBM25(),
		topicKeywords: topicKeywords,
	}
}

var avoidRe = regexp.MustCompile(`(?i)\b(celebrity|sport|kardashian|crypto\s*pump|nft\s*drop|horoscope)\b`)

var topicKeywords = map[string][]string{
	"ai":                   {"ai", "llm", "gpt", "model", "inference", "agent", "openai", "anthropic", "claude", "gemini", "deepseek", "mistral"},
	"coding-agents":        {"coding agent", "copilot", "cursor", "code assistant", "agentic", "swe-bench"},
	"software-engineering": {"engineering", "developer", "api", "typescript", "rust", "compiler", "refactor", "architecture"},
	"devops":               {"kubernetes", "docker", "ci/cd", "devops", "terraform", "observability", "deployment"},
	"open-source":          {"open source", "open-source", "github", "release", "license", "mit", "repository"},
	"go":                   {"golang", " go ", "goroutine", "go 1.", "go team"},
	"php":                  {"php", "symfony", "drupal", "laravel", "composer"},
	"typescript":           {"typescript", "tsx", "bun", "deno", "node.js", "vite"},
}

func (h *heuristicScorer) Score(_ context.Context, it store.ScoredItem) (store.Score, error) {
	text := strings.ToLower(it.Title + "\n" + it.Content)

	var sc store.Score

	// Relevance: BM25 match against each topic's keyword list. The
	// previous code double-counted repeated substrings; BM25 with
	// k1=1.5 saturates TF contribution so a single title like
	// "GPT-4o GPT-4o GPT-4o" does not dominate a richer one.
	var topScore float64
	for _, kw := range h.topicKeywords {
		s := h.bm25.score(text, kw)
		if s > topScore {
			topScore = s
		}
	}
	sc.Relevance = clamp(topScore, 0, 1)

	// Avoid-list hard penalty.
	if avoidRe.MatchString(text) {
		sc.Relevance = 0
		sc.Importance = 0
	}

	// Importance: engagement (log scale) + title length sanity.
	sc.Importance = clamp(math.Log1p(float64(it.Engagement))/10, 0, 1)

	// Novelty: release/new keywords boost.
	if strings.Contains(text, "release") || strings.Contains(text, "v1.") || strings.Contains(text, "launch") ||
		strings.Contains(text, "announc") || strings.Contains(text, "new repo") {
		sc.Novelty = 0.8
	} else {
		sc.Novelty = 0.3
	}

	// Actionability: tutorials, how-tos, migrations.
	if strings.Contains(text, "how to") || strings.Contains(text, "tutorial") ||
		strings.Contains(text, "guide") || strings.Contains(text, "migration") ||
		strings.Contains(text, "breaking change") {
		sc.Actionability = 0.7
	} else {
		sc.Actionability = 0.2
	}

	return sc, nil
}
