package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"gorm.io/gorm"
)

type ReportRepo struct{ *Store }

func (s *Store) Reports() *ReportRepo { return &ReportRepo{s} }

// DailyRow is one day of the sales report, from either source.
type DailyRow struct {
	Day         time.Time `json:"-"`
	DayLabel    string    `json:"day"`
	OrdersCount int       `json:"orders_count"`
	GrossCents  int64     `json:"gross_cents"`
	Currency    string    `json:"currency"`
	// Source makes the two halves of the report distinguishable rather than
	// silently blended: a closed day is a materialised fact, today is a moving
	// number that will change before the day ends.
	Source string `json:"source"` // "rollup" | "live"
}

// RollupDay materialises one closed day into daily_sales_rollup.
//
// The report has an immutable half and a mutable half. Yesterday's total can
// never change — those orders are terminal — so recomputing it on every request
// is wasted work that grows with history. Today's total is still moving, so
// materialising it would just be a cache with an invalidation problem.
// Materialise the part that is finished; compute the part that is not.
//
// The aggregation runs in Postgres. Streaming a day of orders into Go to sum a
// column moves megabytes over the wire to produce one number, and it is exactly
// the report path that would otherwise stall order processing.
//
// Idempotent by construction: ON CONFLICT overwrites, so re-running for a day
// produces the same row. That matters because the scheduler will re-run days
// after a restart, and a rollup that double-counted on replay would be worse
// than no rollup at all.
//
// A day with no sales still gets a zero row — the aggregate has no GROUP BY, so
// it always returns exactly one row. Without that, "no sales" and "never
// computed" would be indistinguishable.
func (r *ReportRepo) RollupDay(ctx context.Context, tx *gorm.DB, day time.Time) error {
	d := day.UTC().Format("2006-01-02")

	err := r.txOrDB(tx).WithContext(ctx).Exec(`
		INSERT INTO daily_sales_rollup (day, orders_count, gross_cents, currency, computed_at)
		SELECT ?::date,
		       COALESCE(count(o.id), 0),
		       COALESCE(sum(o.total_cents), 0),
		       COALESCE(max(o.currency), 'USD'),
		       now()
		  FROM orders o
		 WHERE o.status IN ('PAID','FULFILLED')
		   AND o.created_at >= ?::date
		   AND o.created_at <  (?::date + interval '1 day')
		ON CONFLICT (day) DO UPDATE
		   SET orders_count = EXCLUDED.orders_count,
		       gross_cents  = EXCLUDED.gross_cents,
		       currency     = EXCLUDED.currency,
		       computed_at  = now()`,
		d, d, d).Error
	if err != nil {
		return fmt.Errorf("rolling up %s: %w", d, err)
	}
	return nil
}

// ReadRollup returns materialised days in [from, to).
func (r *ReportRepo) ReadRollup(ctx context.Context, from, to time.Time) ([]DailyRow, error) {
	var out []DailyRow
	err := r.db.WithContext(ctx).Raw(`
		SELECT day,
		       to_char(day, 'YYYY-MM-DD') AS day_label,
		       orders_count,
		       gross_cents,
		       currency,
		       'rollup' AS source
		  FROM daily_sales_rollup
		 WHERE day >= ?::date AND day < ?::date
		 ORDER BY day DESC`,
		from.UTC().Format("2006-01-02"), to.UTC().Format("2006-01-02")).Scan(&out).Error
	if err != nil {
		return nil, fmt.Errorf("reading sales rollup: %w", err)
	}
	return out, nil
}

// AggregateLive computes days in [from, to) directly from orders.
//
// Used for today, and as the fallback for any day the rollup has not reached
// yet. The predicate matches the partial index orders_report_range
// (created_at) WHERE status IN ('PAID','FULFILLED').
func (r *ReportRepo) AggregateLive(ctx context.Context, from, to time.Time) ([]DailyRow, error) {
	var out []DailyRow
	err := r.db.WithContext(ctx).Raw(`
		SELECT date_trunc('day', created_at AT TIME ZONE 'UTC')::date AS day,
		       to_char(date_trunc('day', created_at AT TIME ZONE 'UTC'), 'YYYY-MM-DD') AS day_label,
		       count(*)                      AS orders_count,
		       COALESCE(sum(total_cents), 0) AS gross_cents,
		       COALESCE(max(currency), 'USD') AS currency,
		       'live' AS source
		  FROM orders
		 WHERE status IN ('PAID','FULFILLED')
		   AND created_at >= ? AND created_at < ?
		 GROUP BY 1, 2
		 ORDER BY 1 DESC`, from, to).Scan(&out).Error
	if err != nil {
		return nil, fmt.Errorf("aggregating daily sales: %w", err)
	}
	return out, nil
}

// LastRolledUpDay reports the most recent materialised day, so the scheduler
// knows where to resume after downtime instead of recomputing all of history.
func (r *ReportRepo) LastRolledUpDay(ctx context.Context) (*time.Time, error) {
	// max() over an empty table returns one row containing NULL, not zero rows,
	// so this has to scan into a nullable type. Scanning that NULL straight into
	// a time.Time is how "the rollup has never run" turns into an error on the
	// very first tick of a fresh deployment.
	var out sql.NullTime
	err := r.db.WithContext(ctx).
		Raw(`SELECT max(day) FROM daily_sales_rollup`).Scan(&out).Error
	if err != nil {
		return nil, fmt.Errorf("reading last rollup day: %w", err)
	}
	if !out.Valid {
		return nil, nil // never rolled up
	}
	day := out.Time
	return &day, nil
}
