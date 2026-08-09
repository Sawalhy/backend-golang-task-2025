package services

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"
)

// PaymentOutcome is deliberately a three-valued result, not a bool.
//
// Succeeded and Declined are both definite answers. Unknown is the one that
// makes this system hard: the request timed out, so the customer MAY have been
// charged and we cannot tell. Collapsing Unknown into Declined is how you refuse
// an order you already took money for; collapsing it into Succeeded is how you
// ship goods you were never paid for. It gets its own state.
type PaymentOutcome int

const (
	OutcomeSucceeded PaymentOutcome = iota
	OutcomeDeclined
	OutcomeUnknown
)

func (o PaymentOutcome) String() string {
	switch o {
	case OutcomeSucceeded:
		return "succeeded"
	case OutcomeDeclined:
		return "declined"
	default:
		return "unknown"
	}
}

type ChargeRequest struct {
	// IdempotencyKey is the payments row id, committed before this call and
	// reused across every retry of the same intent. The provider uses it to
	// recognise a repeat and return the original result instead of charging
	// again.
	IdempotencyKey string
	AmountCents    int64
	Currency       string
	OrderID        uint64
}

type ChargeResult struct {
	Outcome     PaymentOutcome
	ProviderRef string
	Reason      string
}

// PaymentProvider is the port. Keeping it an interface is what lets the
// concurrency tests drive deterministic declines and timeouts without waiting on
// a real network, and what would let a real Stripe client drop in unchanged.
type PaymentProvider interface {
	Charge(ctx context.Context, req ChargeRequest) (ChargeResult, error)
	Refund(ctx context.Context, providerRef string, amountCents int64) error
	Name() string
}

var ErrProviderTimeout = errors.New("payment provider timed out")

// SimulatedProvider stands in for Stripe. It is honest about being fake, and it
// is deliberately capable of all three outcomes plus latency, because a provider
// that always succeeds instantly exercises none of the interesting paths.
//
// It is idempotent in the way a real provider is: a repeated IdempotencyKey
// returns the stored result rather than charging again. That is what makes the
// retry path testable end to end.
type SimulatedProvider struct {
	failureRate float64
	timeoutRate float64
	latency     time.Duration

	mu      sync.Mutex
	charges map[string]ChargeResult // idempotency key -> first result
}

func NewSimulatedProvider(failureRate, timeoutRate float64, latency time.Duration) *SimulatedProvider {
	return &SimulatedProvider{
		failureRate: failureRate,
		timeoutRate: timeoutRate,
		latency:     latency,
		charges:     make(map[string]ChargeResult),
	}
}

func (p *SimulatedProvider) Name() string { return "simulated" }

func (p *SimulatedProvider) Charge(ctx context.Context, req ChargeRequest) (ChargeResult, error) {
	// A map guarded by a mutex, not a channel with a goroutine serving requests.
	// The shared thing here is state, and protecting shared state is exactly what
	// a mutex is for — building a message-passing rig around a map is the classic
	// misuse of channels.
	p.mu.Lock()
	if prev, ok := p.charges[req.IdempotencyKey]; ok {
		p.mu.Unlock()
		return prev, nil // same key, same answer: no second charge
	}
	p.mu.Unlock()

	// Latency is simulated with a select on ctx.Done, not time.Sleep, so a
	// cancelled request returns immediately instead of pinning a worker.
	select {
	case <-ctx.Done():
		return ChargeResult{Outcome: OutcomeUnknown}, ctx.Err()
	case <-time.After(p.latency):
	}

	roll := rand.Float64()
	switch {
	case roll < p.timeoutRate:
		// No result is recorded: the whole point of Unknown is that the provider
		// may well have charged the card despite our not hearing back.
		return ChargeResult{Outcome: OutcomeUnknown}, ErrProviderTimeout

	case roll < p.timeoutRate+p.failureRate:
		res := ChargeResult{Outcome: OutcomeDeclined, Reason: "card_declined"}
		p.remember(req.IdempotencyKey, res)
		return res, nil

	default:
		res := ChargeResult{
			Outcome:     OutcomeSucceeded,
			ProviderRef: "ch_" + req.IdempotencyKey[:8],
		}
		p.remember(req.IdempotencyKey, res)
		return res, nil
	}
}

func (p *SimulatedProvider) Refund(ctx context.Context, providerRef string, amountCents int64) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(p.latency):
		return nil
	}
}

func (p *SimulatedProvider) remember(key string, res ChargeResult) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.charges[key] = res
}

// Lookup reports what the provider believes happened for an idempotency key.
// A real Stripe integration would call the API; the reconciler for UNKNOWN
// payments needs exactly this shape.
func (p *SimulatedProvider) Lookup(key string) (ChargeResult, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	res, ok := p.charges[key]
	return res, ok
}
