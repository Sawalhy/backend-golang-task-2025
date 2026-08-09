package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/internal/repository"
	"github.com/Sawalhy/backend-golang-task-2025/internal/services"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/logger"
)

// seedOrder inserts an order directly with a chosen created_at and status.
//
// Bypassing the service is correct here: these tests are about the REPORT, and
// arranging a month of history through the real intake path would mean
// manipulating the clock and would test the wrong thing.
func seedOrder(t *testing.T, db *gorm.DB, userID uint64, createdAt time.Time, status models.OrderStatus, cents int64) uint64 {
	t.Helper()

	var id uint64
	err := db.Raw(`
		INSERT INTO orders (user_id, status, total_cents, currency, created_at, updated_at)
		VALUES (?, ?::order_status, ?, 'USD', ?, ?)
		RETURNING id`,
		userID, status, cents, createdAt, createdAt).Scan(&id).Error
	require.NoError(t, err)
	return id
}

func seedUser(t *testing.T, db *gorm.DB, email string) uint64 {
	t.Helper()

	var id uint64
	err := db.Raw(`
		INSERT INTO users (email, password_hash, name, role)
		VALUES (?, 'x', 'Test User', 'CUSTOMER')
		RETURNING id`, email).Scan(&id).Error
	require.NoError(t, err)
	return id
}

func TestRollupCountsOnlyRevenueOrders(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()
	reports := services.NewReportService(store, logger.New("error", false))

	user := seedUser(t, db, "rollup@example.com")
	yesterday := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour).Add(10 * time.Hour)

	// Revenue: these two count.
	seedOrder(t, db, user, yesterday, models.OrderPaid, 1000)
	seedOrder(t, db, user, yesterday, models.OrderFulfilled, 2500)

	// Not revenue: an order that was never paid for, one the customer cancelled,
	// one that expired, and one still in flight. Counting any of these would
	// overstate takings — the most consequential kind of bug in a sales report.
	seedOrder(t, db, user, yesterday, models.OrderPending, 9999)
	seedOrder(t, db, user, yesterday, models.OrderCancelled, 9999)
	seedOrder(t, db, user, yesterday, models.OrderExpired, 9999)
	seedOrder(t, db, user, yesterday, models.OrderFailed, 9999)

	require.NoError(t, reports.RollupDay(ctx, yesterday))

	rows, err := store.Reports().ReadRollup(ctx, yesterday, yesterday.AddDate(0, 0, 1))
	require.NoError(t, err)
	require.Len(t, rows, 1)

	assert.Equal(t, 2, rows[0].OrdersCount, "only PAID and FULFILLED are revenue")
	assert.Equal(t, int64(3500), rows[0].GrossCents)
	assert.Equal(t, "rollup", rows[0].Source)
}

// The scheduler re-runs days after a restart, and a rollup that accumulated on
// replay would silently double revenue.
func TestRollupIsIdempotent(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()
	reports := services.NewReportService(store, logger.New("error", false))

	user := seedUser(t, db, "idem@example.com")
	day := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour).Add(9 * time.Hour)
	seedOrder(t, db, user, day, models.OrderPaid, 4200)

	for i := 0; i < 3; i++ {
		require.NoError(t, reports.RollupDay(ctx, day))
	}

	rows, err := store.Reports().ReadRollup(ctx, day, day.AddDate(0, 0, 1))
	require.NoError(t, err)
	require.Len(t, rows, 1, "repeated rollups must not create extra rows")
	assert.Equal(t, 1, rows[0].OrdersCount)
	assert.Equal(t, int64(4200), rows[0].GrossCents, "three runs must not triple the total")
}

// A late transition (an order fulfilled after the day closed) must be picked up
// by a recompute rather than baked in wrong forever.
func TestRollupRecomputeReflectsLateChanges(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()
	reports := services.NewReportService(store, logger.New("error", false))

	user := seedUser(t, db, "late@example.com")
	day := time.Now().UTC().AddDate(0, 0, -3).Truncate(24 * time.Hour).Add(12 * time.Hour)
	orderID := seedOrder(t, db, user, day, models.OrderPending, 5000)

	require.NoError(t, reports.RollupDay(ctx, day))
	rows, err := store.Reports().ReadRollup(ctx, day, day.AddDate(0, 0, 1))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, 0, rows[0].OrdersCount, "pending order is not revenue yet")

	require.NoError(t, db.Exec(
		`UPDATE orders SET status = 'PAID' WHERE id = ?`, orderID).Error)

	require.NoError(t, reports.RollupDay(ctx, day))
	rows, err = store.Reports().ReadRollup(ctx, day, day.AddDate(0, 0, 1))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, 1, rows[0].OrdersCount)
	assert.Equal(t, int64(5000), rows[0].GrossCents)
}

// Without a zero row, "no sales that day" and "never computed" are
// indistinguishable, and the catch-up logic cannot tell where it got to.
func TestRollupWritesZeroRowForDayWithNoSales(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	reports := services.NewReportService(store, logger.New("error", false))

	day := time.Now().UTC().AddDate(0, 0, -5).Truncate(24 * time.Hour)
	require.NoError(t, reports.RollupDay(ctx, day))

	rows, err := store.Reports().ReadRollup(ctx, day, day.AddDate(0, 0, 1))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, 0, rows[0].OrdersCount)
	assert.Equal(t, int64(0), rows[0].GrossCents)
}

// The report's central claim: closed days are served from the materialised
// table, today is computed live because it is still moving.
func TestDailySalesMixesRollupAndLive(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()
	reports := services.NewReportService(store, logger.New("error", false))

	user := seedUser(t, db, "mixed@example.com")
	todayStart := time.Now().UTC().Truncate(24 * time.Hour)
	yesterday := todayStart.AddDate(0, 0, -1).Add(8 * time.Hour)

	seedOrder(t, db, user, yesterday, models.OrderPaid, 1500)
	seedOrder(t, db, user, todayStart.Add(time.Hour), models.OrderPaid, 700)

	// Only closed days are materialised.
	written, err := reports.RollupClosedDays(ctx, 30)
	require.NoError(t, err)
	assert.Positive(t, written)

	rows, err := reports.DailySales(ctx, todayStart.AddDate(0, 0, -7), todayStart.AddDate(0, 0, 1))
	require.NoError(t, err)

	bySource := map[string]repository.DailyRow{}
	for _, r := range rows {
		if r.OrdersCount > 0 {
			bySource[r.Source] = r
		}
	}

	require.Contains(t, bySource, "rollup", "yesterday should be served from the rollup")
	require.Contains(t, bySource, "live", "today should be aggregated live")
	assert.Equal(t, int64(1500), bySource["rollup"].GrossCents)
	assert.Equal(t, int64(700), bySource["live"].GrossCents)
}

// Materialising today would freeze a number that is still changing.
func TestRollupNeverMaterialisesToday(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()
	reports := services.NewReportService(store, logger.New("error", false))

	user := seedUser(t, db, "today@example.com")
	todayStart := time.Now().UTC().Truncate(24 * time.Hour)
	seedOrder(t, db, user, todayStart.Add(2*time.Hour), models.OrderPaid, 3300)

	_, err := reports.RollupClosedDays(ctx, 30)
	require.NoError(t, err)

	rows, err := store.Reports().ReadRollup(ctx, todayStart, todayStart.AddDate(0, 0, 1))
	require.NoError(t, err)
	assert.Empty(t, rows, "today must not be materialised while it can still change")

	// ...but the report still reports it, from the live side.
	report, err := reports.DailySales(ctx, todayStart, todayStart.AddDate(0, 0, 1))
	require.NoError(t, err)
	require.Len(t, report, 1)
	assert.Equal(t, int64(3300), report[0].GrossCents)
	assert.Equal(t, "live", report[0].Source)
}

// A worker that was down must resume from where it stopped rather than
// recomputing all of history on every tick.
func TestRollupResumesFromLastMaterialisedDay(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	reports := services.NewReportService(store, logger.New("error", false))

	first, err := reports.RollupClosedDays(ctx, 5)
	require.NoError(t, err)
	assert.Equal(t, 5, first)

	second, err := reports.RollupClosedDays(ctx, 5)
	require.NoError(t, err)
	assert.Zero(t, second, "a second run has nothing left to close")
}
