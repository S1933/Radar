package ranking

import (
	"context"
	"fmt"
	"time"

	"github.com/S1933/personal-radar/internal/store"
)

// CompareRow pairs the score already stored for an item with a fresh Jev
// verdict. Nothing is written to the database.
type CompareRow struct {
	Item   store.ScoredItem
	Stored store.Score
	Jev    jevVerdict
	Err    error
}

// Compare scores the newest already-scored items with Jev, read-only. It exists
// to decide whether to flip models.rank_engine: the stored rows show what the
// heuristic (or the previous engine) produced for the same items, so the two
// rankings can be compared item by item before anything is rewritten.
func (s *Service) Compare(ctx context.Context, since time.Duration, limit int) ([]CompareRow, error) {
	jev, ok := s.JevScorer()
	if !ok {
		return nil, fmt.Errorf("rank_engine is not jev (active tag %q)", s.scorerTag())
	}
	items, err := s.store.RecentScoredItems(ctx, since, limit)
	if err != nil {
		return nil, err
	}

	rows := make([]CompareRow, 0, len(items))
	for _, it := range items {
		v, err := jev.Verdict(ctx, it)
		rows = append(rows, CompareRow{Item: it, Stored: it.Score, Jev: v, Err: err})
	}
	return rows, nil
}
