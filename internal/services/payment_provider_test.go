package services

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testLogger discards output: these tests assert on behaviour, and the demo
// notifier logs on every send.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// SimulatedProvider is what cmd/worker actually runs, and every other test
// substitutes a double for it — so without these it ships untested. No
// infrastructure needed: it is pure.

func chargeReq() ChargeRequest {
	return ChargeRequest{
		IdempotencyKey: uuid.NewString(),
		AmountCents:    4200,
		Currency:       "USD",
		OrderID:        1,
	}
}

// Rates are probabilities, so the outcomes are pinned to 0 or 1 to make each
// branch deterministic rather than hoping the dice land.
func TestSimulatedProviderOutcomes(t *testing.T) {
	ctx := context.Background()

	t.Run("succeeds", func(t *testing.T) {
		p := NewSimulatedProvider(0, 0, time.Millisecond)
		res, err := p.Charge(ctx, chargeReq())
		require.NoError(t, err)
		assert.Equal(t, OutcomeSucceeded, res.Outcome)
		assert.NotEmpty(t, res.ProviderRef, "a successful charge must be referenceable")
	})

	t.Run("declines", func(t *testing.T) {
		p := NewSimulatedProvider(1, 0, time.Millisecond)
		res, err := p.Charge(ctx, chargeReq())
		require.NoError(t, err, "a decline is an answer, not a transport failure")
		assert.Equal(t, OutcomeDeclined, res.Outcome)
		assert.Equal(t, "card_declined", res.Reason)
	})

	t.Run("times out", func(t *testing.T) {
		p := NewSimulatedProvider(0, 1, time.Millisecond)
		res, err := p.Charge(ctx, chargeReq())
		require.ErrorIs(t, err, ErrProviderTimeout)
		assert.Equal(t, OutcomeUnknown, res.Outcome)
	})
}

// The property the entire double-charge defence rests on: the same key returns
// the stored answer instead of charging again.
func TestSimulatedProviderIsIdempotentPerKey(t *testing.T) {
	ctx := context.Background()
	p := NewSimulatedProvider(0, 0, time.Millisecond)
	req := chargeReq()

	first, err := p.Charge(ctx, req)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		again, err := p.Charge(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, first, again, "a repeated key must return the original result")
	}

	// A different key is a genuinely different intent.
	other, err := p.Charge(ctx, chargeReq())
	require.NoError(t, err)
	assert.NotEqual(t, first.ProviderRef, other.ProviderRef)
}

// A timeout records NOTHING, and that is the point of UNKNOWN: the provider may
// well have charged the card despite our not hearing back, so we must not
// remember a verdict we never received.
func TestTimeoutRecordsNoResult(t *testing.T) {
	ctx := context.Background()
	p := NewSimulatedProvider(0, 1, time.Millisecond)
	req := chargeReq()

	_, err := p.Charge(ctx, req)
	require.ErrorIs(t, err, ErrProviderTimeout)

	_, found := p.Lookup(req.IdempotencyKey)
	assert.False(t, found,
		"an unanswered call must leave no verdict for reconciliation to read")
}

// Reconciliation's only honest way out of UNKNOWN is asking the provider what it
// believes happened.
func TestLookupReportsWhatTheProviderRemembers(t *testing.T) {
	ctx := context.Background()
	p := NewSimulatedProvider(0, 0, time.Millisecond)
	req := chargeReq()

	_, found := p.Lookup(req.IdempotencyKey)
	require.False(t, found, "nothing is known before the call")

	res, err := p.Charge(ctx, req)
	require.NoError(t, err)

	stored, found := p.Lookup(req.IdempotencyKey)
	require.True(t, found)
	assert.Equal(t, res, stored)
}

// Latency is a select on ctx.Done rather than a bare sleep, so a cancelled
// request returns immediately instead of pinning a worker for the full duration.
func TestChargeRespectsContextCancellation(t *testing.T) {
	p := NewSimulatedProvider(0, 0, 30*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	res, err := p.Charge(ctx, chargeReq())
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Equal(t, OutcomeUnknown, res.Outcome,
		"an abandoned call has no verdict, so it is Unknown")
	assert.Less(t, elapsed, 5*time.Second, "must not wait out the full latency")
}

func TestRefundRespectsContextCancellation(t *testing.T) {
	p := NewSimulatedProvider(0, 0, 30*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	require.Error(t, p.Refund(ctx, "ch_12345678", 100))
}

func TestRefundSucceeds(t *testing.T) {
	p := NewSimulatedProvider(0, 0, time.Millisecond)
	assert.NoError(t, p.Refund(context.Background(), "ch_12345678", 4200))
}

func TestProviderName(t *testing.T) {
	assert.Equal(t, "simulated", NewSimulatedProvider(0, 0, 0).Name())
}

func TestPaymentOutcomeString(t *testing.T) {
	assert.Equal(t, "succeeded", OutcomeSucceeded.String())
	assert.Equal(t, "declined", OutcomeDeclined.String())
	assert.Equal(t, "unknown", OutcomeUnknown.String())
	assert.Equal(t, "unknown", PaymentOutcome(99).String(), "an unrecognised outcome is not a success")
}

// The stored-results map is shared state guarded by a mutex. Run with -race:
// concurrent workers hitting the same key must not corrupt it, and must still
// see exactly one charge.
func TestSimulatedProviderUnderConcurrentUse(t *testing.T) {
	ctx := context.Background()
	p := NewSimulatedProvider(0, 0, time.Millisecond)
	req := chargeReq()

	const callers = 30
	var (
		wg      sync.WaitGroup
		release = make(chan struct{})
		results = make([]ChargeResult, callers)
	)

	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-release
			res, err := p.Charge(ctx, req)
			assert.NoError(t, err)
			results[idx] = res
		}(i)
	}
	close(release)
	wg.Wait()

	// Every caller must see the same charge — not one charge each.
	for _, res := range results {
		assert.Equal(t, results[0].ProviderRef, res.ProviderRef)
	}
}

// LoggingNotifier is the demo transport, and its failure rate is what makes the
// retry and lease paths reachable in a running system rather than only in tests.
func TestLoggingNotifier(t *testing.T) {
	ctx := context.Background()
	log := testLogger()

	always := NewLoggingNotifier("email", 1, time.Millisecond, log)
	assert.Equal(t, "email", always.Channel())
	require.Error(t, always.Send(ctx, "email", "confirmation", 1),
		"a failure rate of 1 must always fail")

	never := NewLoggingNotifier("sms", 0, time.Millisecond, log)
	assert.Equal(t, "sms", never.Channel())
	require.NoError(t, never.Send(ctx, "sms", "confirmation", 1))

	// Cancellation is honoured rather than slept through.
	slow := NewLoggingNotifier("email", 0, 30*time.Second, log)
	cancelled, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	require.Error(t, slow.Send(cancelled, "email", "confirmation", 1))
}
