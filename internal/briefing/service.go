package briefing

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/S1933/personal-radar/internal/logging"
	"github.com/S1933/personal-radar/internal/ranking"
	"github.com/S1933/personal-radar/internal/store"
)

// Options configures briefing generation.
type Options struct {
	MaxItems  int
	MaxTrends int
	Location  *time.Location
}

// Service generates the daily briefing markdown. Delivery is not the
// briefing's job anymore: the content is persisted in the briefings table
// and the chat agent (Hermes) reads it from there and posts it to Discord.
type Service struct {
	opts   Options
	store  *store.Store
	ranker *ranking.Service
	synth  *synthesizer
	log    *logging.Logger
}

func New(opts Options, st *store.Store, ranker *ranking.Service, log *logging.Logger) *Service {
	s := &Service{opts: opts, store: st, ranker: ranker, log: log}
	// Stage-2 LLM synthesis is active only when an endpoint + key are set.
	if cfg := ranker.ModelsConfig(); cfg.BaseURL != "" && cfg.APIKey != "" {
		s.synth = newSynthesizer(cfg)
	}
	return s
}

// drainPendingBatches caps the RankPending loop: UnscoredItems is capped
// (150/query), so the queue is drained in batches — but a runaway loop
// would spin forever if scoring keeps failing without erroring.
const drainPendingBatches = 5

// Generate builds the briefing over the last 24h and persists it.
func (s *Service) Generate(ctx context.Context) (string, error) {
	// T13: record the run for observability. items_collected counts
	// what made it into the message, items_failed is 0 for the
	// briefing (errors surface as the returned err below).
	start := time.Now()
	var runErr error
	var selectedCount int
	defer func() {
		if err := s.store.SaveRun(ctx, "briefing", "", start, time.Now(),
			selectedCount, 0, runErrMsg(runErr)); err != nil {
			s.log.Warn("save run", "error", err)
		}
	}()

	// Rank anything pending first so the selection is fresh.
	for i := 0; i < drainPendingBatches; i++ {
		n, err := s.ranker.RankPending(ctx)
		if err != nil {
			s.log.Warn("rank before briefing", "error", err)
			break
		}
		if n == 0 {
			break
		}
		s.log.Info("ranked pending before briefing", "batch", i+1, "items", n)
	}

	items, err := s.store.TopScoredItems(ctx, 24*time.Hour, s.opts.MaxItems*3)
	if err != nil {
		runErr = fmt.Errorf("top items: %w", err)
		return "", runErr
	}
	if len(items) == 0 {
		runErr = fmt.Errorf("no items in the last 24h")
		return "", runErr
	}

	// Source quota: a pure top-N ranking lets one source crowd out the
	// rest (a wall of long Reddit posts buries short X tweets even when
	// they are viral). Ensure every active source keeps a seat — this is
	// a Trello-style "one column is not allowed to eat the whole board".
	selected := applySourceQuota(items, s.opts.MaxItems)

	// Dedupe by title before ids are computed so the persisted item_ids
	// match exactly what the reader sees.
	selected = dedupeByTitle(selected)
	selectedCount = len(selected)

	trends := s.detectTrends(items)

	content := s.render(ctx, selected, trends)

	date := time.Now().In(s.loc()).Format("2006-01-02")
	ids := make([]int64, 0, len(selected))
	for _, it := range selected {
		ids = append(ids, it.DBID)
	}
	if err := s.store.SaveBriefing(ctx, date, content, ids); err != nil {
		s.log.Warn("save briefing", "error", err)
	}
	return content, nil
}

// runErrMsg returns the error message for the runs table, or "" on
// success. Centralised so the defer above stays compact.
func runErrMsg(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// applySourceQuota diversifies a ranked list so no single source can
// crowd out the rest. Every source present in the candidates keeps at
// least minSeats items (when it has that many), and no source exceeds
// maxSeats. Selection is still score-ordered within those constraints.
func applySourceQuota(items []store.ScoredItem, max int) []store.ScoredItem {
	const minSeats = 2
	const maxSeats = 4

	count := map[string]int{}
	for _, it := range items {
		count[it.Source]++
	}
	sources := make([]string, 0, len(count))
	for s := range count {
		sources = append(sources, s)
	}

	bestOf := map[string][]store.ScoredItem{}
	for _, s := range sources {
		bestOf[s] = bestOf[s][:0]
	}
	for _, it := range items {
		bestOf[it.Source] = append(bestOf[it.Source], it) // already score-ordered
	}

	// Sort sources by best score (descending) with alphabetical
	// tiebreak. A tight max_items must not systematically favour
	// whichever source sorts first alphabetically — the previous
	// sort.Strings(sources) here did exactly that.
	sort.SliceStable(sources, func(i, j int) bool {
		bi, bj := bestOf[sources[i]], bestOf[sources[j]]
		if len(bi) == 0 || len(bj) == 0 {
			return len(bi) > len(bj)
		}
		if bi[0].Score.Final != bj[0].Score.Final {
			return bi[0].Score.Final > bj[0].Score.Final
		}
		return sources[i] < sources[j]
	})

	taken := map[string]int{}
	out := make([]store.ScoredItem, 0, max)

	// Phase 1: guarantee the per-source minimum, but never exceed max.
	// With many active sources, sum(minSeats) can exceed max; the
	// ceiling must win, and the last batch must be truncated.
	for _, s := range sources {
		if len(out) >= max {
			break
		}
		n := minSeats
		if len(bestOf[s]) < n {
			n = len(bestOf[s])
		}
		if len(out)+n > max {
			n = max - len(out)
		}
		if n <= 0 {
			continue
		}
		out = append(out, bestOf[s][:n]...)
		bestOf[s] = bestOf[s][n:]
		taken[s] = n
	}

	// Phase 2: top-up by global score, capping each source at maxSeats.
	for len(out) < max {
		// pick the remaining item with the highest score among
		// non-capped sources, if any
		bestSource := ""
		for _, s := range sources {
			if len(bestOf[s]) == 0 || taken[s] >= maxSeats {
				continue
			}
			if bestSource == "" || bestOf[s][0].Score.Final > bestOf[bestSource][0].Score.Final {
				bestSource = s
			}
		}
		if bestSource == "" {
			break
		}
		out = append(out, bestOf[bestSource][0])
		bestOf[bestSource] = bestOf[bestSource][1:]
		taken[bestSource]++
	}
	return out
}

// dedupeByTitle collapses items whose normalized titles are identical —
// typically the same story surfaced by two collectors, or a tweet picked
// up twice. The first occurrence wins, so the score ordering is
// preserved.
func dedupeByTitle(items []store.ScoredItem) []store.ScoredItem {
	seen := make(map[string]bool, len(items))
	out := make([]store.ScoredItem, 0, len(items))
	for _, it := range items {
		key := normTitle(it.Title)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, it)
	}
	return out
}

// detectTrends clusters items sharing >= 3 significant words in the title.
func (s *Service) detectTrends(items []store.ScoredItem) []string {
	type cluster struct {
		key    string
		titles []string
		count  int
	}
	var clusters []cluster
	for _, it := range items {
		kws := keywords(it.Title, 6)
		if len(kws) < 2 {
			continue
		}
		key := strings.Join(kws, " ")
		found := false
		for i := range clusters {
			if overlap(kws, strings.Split(clusters[i].key, " ")) >= 3 {
				clusters[i].count++
				clusters[i].titles = append(clusters[i].titles, it.Title)
				found = true
				break
			}
		}
		if !found {
			clusters = append(clusters, cluster{key: key, titles: []string{it.Title}, count: 1})
		}
	}
	sort.Slice(clusters, func(i, j int) bool { return clusters[i].count > clusters[j].count })
	var out []string
	for _, c := range clusters {
		if c.count >= 3 && len(out) < s.opts.MaxTrends {
			out = append(out, fmt.Sprintf("• %s (%d sources)", c.titles[0], c.count))
		}
	}
	return out
}

func keywords(title string, n int) []string {
	stop := map[string]bool{"the": true, "a": true, "an": true, "for": true, "and": true, "with": true, "new": true, "how": true, "to": true, "of": true, "in": true, "is": true, "it": true}
	words := strings.Fields(strings.ToLower(title))
	var out []string
	seen := map[string]bool{}
	for _, w := range words {
		w = strings.Trim(w, ".,!?:;\"'()[]")
		if len(w) < 3 || stop[w] || seen[w] {
			continue
		}
		seen[w] = true
		out = append(out, w)
		if len(out) == n {
			break
		}
	}
	return out
}

func overlap(a, b []string) int {
	m := map[string]bool{}
	for _, w := range a {
		m[w] = true
	}
	var n int
	for _, w := range b {
		if m[w] {
			n++
		}
	}
	return n
}

func (s *Service) loc() *time.Location {
	if s.opts.Location != nil {
		return s.opts.Location
	}
	return time.UTC
}

// render produces Discord-flavoured markdown: the briefing is persisted
// in the DB and posted to a Discord channel by the chat agent. Markdown
// replaces the old Telegram HTML parse mode; no escaping is needed
// beyond keeping the URL raw inside the link syntax.
func (s *Service) render(ctx context.Context, items []store.ScoredItem, trends []string) string {
	var b strings.Builder
	now := time.Now().In(s.loc())
	fmt.Fprintf(&b, "☀️ **DAILY RADAR**\n%s\n\n", now.Format("Monday 2 January 2006"))

	if len(items) == 0 {
		b.WriteString("Rien de marquant aujourd'hui — calme plat.\n")
		return b.String()
	}

	b.WriteString("🔥 **À NE PAS MANQUER**\n\n")
	for i, it := range items {
		icon := sourceIcon(it.Source)
		title := it.Title
		if it.URL != "" {
			fmt.Fprintf(&b, "%d. %s [%s](<%s>)\n", i+1, icon, title, it.URL)
		} else {
			fmt.Fprintf(&b, "%d. %s %s\n", i+1, icon, title)
		}
		// Optional LLM "why it matters" line (best-effort).
		if s.synth != nil {
			if why, err := s.synth.Rationale(ctx, it.Title, it.Content, it.Source); err == nil && why != "" {
				fmt.Fprintf(&b, "   💡 %s\n", why)
			}
		}
	}

	if len(trends) > 0 {
		b.WriteString("\n🧠 **TENDANCES**\n\n")
		for _, t := range trends {
			b.WriteString(t + "\n")
		}
	}
	return b.String()
}

// sourceIcon maps a collector source name to a recognizable emoji.
func sourceIcon(src string) string {
	switch src {
	case "github":
		return "🐙"
	case "x":
		return "🐦"
	case "reddit", "reddit-public":
		return "🔴"
	case "rss":
		return "📰"
	default:
		return "•"
	}
}

// normTitle lowercases and strips punctuation/space so near-identical
// titles collapse to the same dedup key.
func normTitle(t string) string {
	t = strings.ToLower(t)
	var b strings.Builder
	for _, r := range t {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}
