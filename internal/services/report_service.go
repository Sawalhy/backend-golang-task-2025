package services

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/internal/repository"
)

// ReportService serves the daily sales report from two sources and keeps the
// materialised half up to date.
//
// The split is the whole idea (DESIGN_NOTES.md §5.17): a closed day is immutable,
// so it is computed once and stored; today is still moving, so it is computed on
// demand. Materialising today would be a cache, and caches need invalidation —
// this needs none, because nothing that has been materialised can change.
type ReportService struct {
	store *repository.Store
	log   *slog.Logger
}

func NewReportService(store *repository.Store, log *slog.Logger) *ReportService {
	return &ReportService{store: store, log: log}
}

// DailySales returns one row per day in [from, to).
//
// Days strictly before today come from daily_sales_rollup. Today — and any day
// the rollup job has not reached yet — is aggregated live, so a gap in the
// rollup degrades to slower rather than to wrong.
//
// Days are UTC. The spec never says which timezone "daily" means, so the
// boundary is stated in the response instead of inherited from the server's
// locale, which would silently shift twice a year.
func (s *ReportService) DailySales(ctx context.Context, from, to time.Time) ([]repository.DailyRow, error) {
	from, to = from.UTC(), to.UTC()
	todayStart := time.Now().UTC().Truncate(24 * time.Hour)

	byDay := make(map[string]repository.DailyRow)

	// 1. Materialised days: the closed portion of the requested range.
	if from.Before(todayStart) {
		// The rollup only ever covers closed days, so clamp the upper bound to
		// the start of today. (No builtin min here: time.Time is a struct and
		// is not an ordered type.)
		rollupEnd := to
		if rollupEnd.After(todayStart) {
			rollupEnd = todayStart
		}
		rows, err := s.store.Reports().ReadRollup(ctx, from, rollupEnd)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			byDay[r.DayLabel] = r
		}
	}

	// 2. Live aggregation over the whole range. It supplies today, and it fills
	//    any closed day the rollup job has not reached — so a lagging or failed
	//    rollup makes the report slower, never wrong.
	liveRows, err := s.store.Reports().AggregateLive(ctx, from, to)
	if err != nil {
		return nil, err
	}
	for _, r := range liveRows {
		isToday := !r.Day.UTC().Before(todayStart)
		if _, materialised := byDay[r.DayLabel]; isToday || !materialised {
			byDay[r.DayLabel] = r
		}
	}

	out := make([]repository.DailyRow, 0, len(byDay))
	for _, r := range byDay {
		out = append(out, r)
	}
	// Newest first, matching what the SQL and the callers expect.
	sort.Slice(out, func(i, j int) bool { return out[i].DayLabel > out[j].DayLabel })
	return out, nil
}

// RollupDay materialises a single day. Exported so a test or an operator can
// rebuild one day without waiting for the scheduler.
func (s *ReportService) RollupDay(ctx context.Context, day time.Time) error {
	return s.store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		return s.store.Reports().RollupDay(ctx, tx, day)
	})
}

// RollupClosedDays materialises every day that has closed and is not yet stored,
// oldest first, and returns how many it wrote.
//
// It resumes from the last materialised day rather than recomputing history, so
// a worker that was down for a week catches up in a few statements. maxDays
// bounds the catch-up on a database with a long gap, so the first run after a
// long outage does not become an unbounded scan.
//
// Safe to run repeatedly: RollupDay is an upsert.
func (s *ReportService) RollupClosedDays(ctx context.Context, maxDays int) (int, error) {
	if maxDays <= 0 {
		maxDays = 30
	}
	todayStart := time.Now().UTC().Truncate(24 * time.Hour)

	last, err := s.store.Reports().LastRolledUpDay(ctx)
	if err != nil {
		return 0, err
	}

	// Start the day after the last materialised one, or maxDays back on a cold
	// database.
	start := todayStart.AddDate(0, 0, -maxDays)
	if last != nil {
		next := last.UTC().Truncate(24 * time.Hour).AddDate(0, 0, 1)
		if next.After(start) {
			start = next
		}
	}

	written := 0
	for day := start; day.Before(todayStart); day = day.AddDate(0, 0, 1) {
		if err := s.RollupDay(ctx, day); err != nil {
			return written, fmt.Errorf("rolling up closed days: %w", err)
		}
		written++
		if written >= maxDays {
			break
		}
	}

	if written > 0 {
		s.log.InfoContext(ctx, "materialised closed sales days", "days", written)
	}
	return written, nil
}
