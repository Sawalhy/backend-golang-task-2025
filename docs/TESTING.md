# Test Suite

> What every test asserts, how it is built, and — for the ones that matter — why
> it is written the way it is rather than the obvious way.

**177 tests, 83.9% statement coverage.** Integration against real Postgres,
RabbitMQ and Redis (`tests/`, plus the rate limiter), and pure unit tests where
no infrastructure is needed.

| File | Tests | Covers | Needs |
|---|---|---|---|
| `tests/api_test.go` | 32 | Every REST endpoint: auth, CRUD, RBAC, validation | Postgres |
| `tests/concurrency_test.go` | 7 | Overselling, deadlock ordering, idempotency (A, G) | Postgres |
| `tests/saga_test.go` | 12 | Double charge, cancel-vs-charge, worker death (C, D, E) | Postgres |
| `tests/notification_test.go` | 11 | Send-once, leases, retries (I) | Postgres |
| `tests/reaper_test.go` | 7 | Abandoned checkout reclamation (F) | Postgres |
| `tests/rollup_test.go` | 7 | Daily sales report correctness | Postgres |
| `tests/relay_test.go` | 6 | Confirm-before-`sent_at`, no double publish (B) | Postgres + RabbitMQ |
| `tests/sse_test.go` | 4 | Order status streaming over HTTP | Postgres |
| `tests/backplane_test.go` | 3 | RabbitMQ → hub delivery topology | RabbitMQ |
| `tests/consumer_test.go` | 8 | Delivery, ack/nack policy, bounded pool | RabbitMQ |
| `tests/supervisor_test.go` | 4 | Reconnect after connection loss | RabbitMQ + mgmt API |
| `tests/refund_test.go` | 8 | Refund execution and compensation | Postgres |
| `internal/api/middleware/ratelimit_test.go` | 7 | Token bucket atomicity, fail-open | Redis |
| `internal/config/config_test.go` | 9 | Env parsing, validation, boot refusal | — |
| `internal/services/status_hub_test.go` | 7 | SSE fan-out, no leaks, non-blocking publish | — |
| `internal/services/payment_provider_test.go` | 11 | Simulated provider idempotency, outcomes | — |
| `internal/models/models_test.go` | 9 | State machine, reachability, enum round-trips | — |
| `pkg/database/database_test.go` | 4 | Error classification through wrapping | — |
| `pkg/logger/logger_test.go` | 5 | Trace id propagation | — |
| `tests/repository_test.go` | 5 | Live-intent lookup, audit atomicity | Postgres |
| `tests/failure_paths_test.go` | 8 | Rollback under injected DB failure, retry classification | Postgres |
| `tests/sweep_test.go` | 3 | Rollback at EVERY step of intake, cancel and reap (18 subtests) | Postgres |

---

## 1. How the suite is built

### Real Postgres for anything concurrent

**A mock returns what you told it to return.** It is structurally incapable of
exhibiting a race: it cannot lose an update, cannot deadlock, cannot enforce a
`CHECK` constraint, and cannot make two transactions contend for a row. Every
interesting bug in this system is a database race, so an oversell test written
against a mocked repository passes trivially and proves nothing.

The tests therefore run the **real migration files** rather than `AutoMigrate`,
because the `CHECK` constraints and partial unique indexes *are* the invariants
under test. A schema built any other way would be testing a different system.

**That argument is about concurrency, and it does not extend to everything.**
Asserting *"what happens when the database returns an error"* needs an injected
error, not a real race — so "no mocks anywhere" would be the right conclusion
drawn from the wrong premise.

The suite is not anti-mock. It is **selectively seamed**, and the line is worth
stating plainly, because it is a design decision rather than an accident:

| Dependency | Seam | Why |
|---|---|---|
| `PaymentProvider` | interface | We do not own a card network and cannot run one |
| `Notifier` | interface | Same, for email and SMS |
| `*repository.Store` | concrete | We own Postgres and can run it in Docker in seconds |
| `*redis.Client` | concrete | Same |

Both interfaces are mocked heavily — `scriptedProvider` counts *distinct charges*
and can hold one mid-flight; `countingNotifier` counts sends and can be made to
fail. So test doubles are used wherever there is a port to substitute.

**Why the data layer has no seam.** Constructor injection is not the same as
substitutability: every service takes `*repository.Store`, a concrete struct, so
there is nothing to swap. Adding consumer-side interfaces would be cheap in Go
(each service declares only the handful of methods it calls, not all 66) — the
cost is not typing.

The cost is that the seam is a trap. Once `*repository.Store` is an interface,
the oversell test *can* be written against a mock, and it will pass. That is a
loaded gun left for whoever maintains this next, traded for coverage of branches
that are almost entirely `if err != nil { return fmt.Errorf("...: %w", err) }`,
where the assertion is that Go propagates errors.

**Those branches are covered by fault injection instead** — see §11. Because we
own Postgres in tests we can make it genuinely fail, which exercises the same
branches with real semantics and without opening the seam.

### Getting a database

`tests/support_test.go` provisions one two ways:

```
TEST_DATABASE_URL set  ->  use it            (compose stack, CI service container)
otherwise              ->  testcontainers-go starts and destroys one
```

The first path makes the suite runnable where the Go toolchain is itself
containerised (`scripts/go.ps1` cannot launch sibling containers). The second
makes plain `go test ./...` work on a machine with Go and Docker.

If neither is available, `TestMain` **skips rather than fails** — an environment
gap should not look like a broken build.

### Isolation between tests

`newStore(t)` opens a connection and `TRUNCATE`s every table with
`RESTART IDENTITY CASCADE`. Truncate rather than delete: far faster, and it
resets sequences so ids are predictable per test. `CASCADE` follows the foreign
keys, so drop order does not matter.

> **Use a separate database.** The suite truncates everything, so pointing
> `TEST_DATABASE_URL` at `orders` would wipe the running stack. Use `orders_test`.

### The barrier — why concurrency tests look like this

Every concurrency test follows one shape:

```go
release := make(chan struct{})
for i := 0; i < n; i++ {
    go func() { <-release; /* do the thing */ }()
}
time.Sleep(150 * time.Millisecond)  // let every goroutine reach the barrier
close(release)                       // release them together
```

**Starting goroutines in a plain loop is not a concurrency test.** Each one
finishes before the next begins, they never collide, and the test passes happily
against code that oversells freely. The barrier is what makes them contend for
the same row at the same instant.

The sleep before `close` is not the synchronisation — it only ensures every
goroutine has *reached* the barrier, so spawn cost does not stagger the release.

---

## 2. The REST API — `api_test.go`

Every endpoint the brief specifies, table-driven where the cases are variations
on one theme. Requests go through the engine with a recorder rather than a live
socket: these are request/response, so there is nothing to stream.

| Area | Asserts |
|---|---|
| Users | Creation, validation table (bad email, short password, missing name), duplicate email → 409, password hash never serialised |
| Auth | Login success and both failure modes returning the **same** status, so login is not an account-enumeration oracle |
| RBAC | Own profile vs someone else's (403), admin overrides, malformed tokens (401), unparseable path ids (400) |
| Products | Full CRUD, admin-only writes, validation table, pagination with a **capped** page size |
| Orders | 202 on create, validation table, list scoped to the caller, 404 (not 403) on someone else's order, cancel, `Idempotency-Key` header |
| Admin | Every admin route refused for customers and anonymous callers; order status through the state machine; daily report; low stock; restock with optimistic locking |

Three worth calling out:

**`TestSelfRegistrationCannotMintAnAdmin`** — without the server-side role check,
`"role":"ADMIN"` in a public signup body is a privilege escalation. The test
registers with that field and then asserts the resulting token is refused by an
admin route.

**`TestAdminOrderStatusGoesThroughTheStateMachine`** — admins do not get a free
hand with `orders.status`. `PENDING → FULFILLED` is refused because it is not an
edge, and `PAID` is refused outright because marking an order paid by hand means
money that was never taken.

**`TestPriceChangesDoNotRewriteHistoricalOrders`** — places an order, changes the
product price, and asserts the order still totals what it was placed at. That is
`order_items.unit_price_cents` being a snapshot rather than a join, and it is the
difference between an audit trail and a fiction.

## 3. Overselling and ordering — `concurrency_test.go`

Failure mode A: *two customers buy the last item, both succeed.*

| Test | Asserts |
|---|---|
| `TestNoOversellOnTheLastUnit` | 50 buyers, 1 unit → exactly 1 success, 49 × `ErrInsufficientStock`, `available=0 reserved=1`, exactly one order and one reservation |
| `TestConcurrentBuyersCannotExceedStock` | 60 buyers, 7 units → exactly 7 succeed |
| `TestDatabaseRefusesNegativeStock` | A direct `UPDATE` that would go negative is rejected by the `CHECK` |
| `TestOppositeOrderLineItemsDoNotDeadlock` | 40 orders, half with line items reversed → no deadlocks |
| `TestDuplicateLinesAreMergedNotRejected` | Two lines for one product collapse into one, quantities summed |
| `TestIdempotencyKeyPreventsDuplicateOrders` | A retry returns the original order and does **not** reserve stock twice |
| `TestAcceptedOrderAlwaysWritesItsEvent` | One `order.created` per accepted order — never more, never fewer |

**The oversell test also checks the ledger, not just the count.** Asserting "one
success" alone would pass against code that succeeds once and corrupts
`available`. It asserts `available=0`, `reserved=1`, and that rejected attempts
left *nothing* behind — which is what rolling the whole transaction back buys.

**The deadlock test exists to catch a future edit.** Line items are sorted by
`product_id` before touching inventory, giving a total order on resources so a
wait cycle cannot form. Half the goroutines submit their items reversed; if
someone removes the sort as a "simplification", this test starts failing with
`40P01` instead of passing.

**Two guards, tested separately.** `TestConcurrentBuyersCannotExceedStock`
covers the `WHERE` clause, which turns an oversell into a clean 409.
`TestDatabaseRefusesNegativeStock` covers `CHECK (available >= 0)`, which makes
corruption impossible even if the application logic above it is ever wrong.
Replace the conditional `UPDATE` with a read-then-write and the first test fails
on the count while the constraint holds the line — which is the division of
labour those two tests exist to pin down.

---

## 4. The payment saga — `saga_test.go`

The hardest part of the system, and where node death actually hurts.

### The scripted provider

These tests use a fake provider with two properties the real simulated one lacks:

**A gate that holds a charge mid-flight.** A cancel racing a live charge is a
timing bug. Reproducing it by hoping the Go scheduler interleaves two goroutines
helpfully would be flaky; blocking inside `Charge` until the test says so makes
it deterministic.

**Distinct charges counted separately from calls.** A repeated idempotency key
returns the stored result and increments `replays`, not `charges`. That is what
lets a test assert *"the card was charged exactly once"* rather than the much
weaker *"the code ran once"*.

### Failure mode D — cancel arrives while charging

| Test | Asserts |
|---|---|
| `TestCancelDuringChargeCompensatesWithRefund` | Cancel mid-charge → 202 not 200; stock stays **held**; charge succeeds → `CANCELLED_REFUNDED` + refund event; stock released only then; exactly 1 charge |
| `TestCancelDuringChargeWithDeclineNeedsNoRefund` | Same race, card declined → plain `CANCELLED`, **no** refund requested |
| `TestCancelBeforeChargeIsImmediate` | Nothing in flight → final immediately, and the worker must never charge a cancelled order |

The second test is the one people forget. Refunding a charge that never landed
pays the customer money they never spent, so "always refund on cancel" is a real
bug — the refund must be conditional on the charge actually succeeding.

### Failure mode C — no double charge

| Test | Asserts |
|---|---|
| `TestDuplicateDeliveryChargesOnlyOnce` | Three sequential deliveries → 1 charge, order `PAID`, stock consumed once |
| `TestConcurrentDeliveriesChargeOnlyOnce` | Eight *concurrent* workers on the same message → 1 charge |
| `TestSecondLiveIntentIsRefused` | The partial unique index rejects a second live intent |

### Failure mode E — a worker dies mid-charge

`killWorkerMidCharge` reproduces a `SIGKILL`: it performs the
`PENDING → CHARGING` transition and commits the intent, then simply stops — no
deferred cleanup, because a killed process runs none.

| Test | Asserts |
|---|---|
| `TestRedeliveryAloneCannotRecoverAHalfChargedOrder` | **The inaction.** Redelivery skips it, the reaper skips it — proving why the sweep must exist |
| `TestRecoverStuckChargeCompletesTheOrder` | The sweep re-drives it → `PAID`, still exactly 1 charge |
| `TestRecoverStuckChargeReleasesStockOnDecline` | Declined → `FAILED`, stock returned |
| `TestRecoveryLeavesRecentIntentsAlone` | A recent intent is **not** re-driven — a slow payment must not be double-driven |

The first is unusual and deliberate: it asserts that nothing happens. It
documents the gap the recovery sweep fills, so if someone later deletes the
sweep as redundant, this test explains what breaks.

The last one guards against the fix for E reintroducing C. The grace period must
exceed the provider timeout, or "recovery" starts racing charges that are merely
slow.

### Unknown outcomes

| Test | Asserts |
|---|---|
| `TestTimeoutParksPaymentInUnknown` | A timeout parks the intent in `UNKNOWN` and holds the stock — no guess in either direction |
| `TestReconciliationResolvesUnknownAsNeverCharged` | Reconciliation asks the provider, resolves to `FAILED`, releases stock |

`UNKNOWN` exists because collapsing it into `DECLINED` refuses an order you
already took money for, and collapsing it into `SUCCEEDED` ships goods you were
never paid for.

---

## 5. The reaper — `reaper_test.go`

Failure mode F: *a customer abandons checkout and holds the last unit forever.*

Reservations are aged by rewriting `expires_at` rather than sleeping — the real
TTL is 15 minutes, and a test that waits for it is a test nobody runs.

| Test | Asserts |
|---|---|
| `TestReaperReclaimsStockFromAbandonedOrders` | Order → `EXPIRED`, units back in `available`, no reservation still `HELD` |
| `TestReclaimedStockCanBeBoughtAgain` | Blocked buyer succeeds *after* the reap — the stock is genuinely sellable, not just a number that moved |
| `TestReaperWillNotExpireAnOrderBeingCharged` | A `CHARGING` order is **skipped** even with an expired reservation |
| `TestReaperIsIdempotent` | Repeated runs restore stock exactly once, never inflate it |
| `TestReaperPublishesExpiryThroughTheOutbox` | Expiry announced exactly once |
| `TestReaperLeavesUnexpiredReservationsAlone` | Live reservations untouched |
| `TestReaperReclaimsAcrossManyOrders` | 12 abandoned multi-item orders → each product gets back exactly what it lent |

**`TestReaperWillNotExpireAnOrderBeingCharged` is the important one.** A
reservation can be past expiry while its order is legitimately mid-charge: the
worker claimed it at 14:59 and the provider is still thinking at 15:01.
Expiring it would release stock for a purchase about to succeed, and the
customer would be charged for goods already given away. The `PENDING → EXPIRED`
CAS is what prevents it, and this test stops anyone "simplifying" that into an
unconditional update.

---

## 6. Notifications — `notification_test.go`

Failure mode I: *a notification is sent twice, or never.* Both halves need
different mechanisms, so both are tested.

The fake notifier **counts sends**. Checking only the row's status would miss the
bug entirely — a row marked `SENT` says nothing about how many emails left the
building.

| Test | Asserts |
|---|---|
| `TestNotificationIsSentAndRecorded` | Sent once, row `SENT` with a timestamp |
| `TestDuplicateEventsSendOneNotification` | Four deliveries → **one** email |
| `TestConcurrentWorkersSendOneNotification` | Eight concurrent workers → **one** email |
| `TestEmailAndSmsAreIndependent` | Both channels fire; neither suppresses the other |
| `TestExpiredLeaseIsReclaimed` | A dead worker's `SENDING` row becomes claimable, then delivers |
| `TestLiveLeaseIsNotReclaimed` | A live lease is not stolen |
| `TestSweepExpiredLeasesReclaimsAbandonedSends` | The timer path the worker actually runs |
| `TestFailedSendBecomesClaimableAgain` | A transport failure is retried, not dropped |
| `TestSendGivesUpAfterTheAttemptBudget` | A permanently broken transport eventually stops |
| `TestNonNotifiableEventsAreIgnored` | `order.created` creates no notification |
| `TestDifferentKindsCoexist` | Confirmation and cancellation are separate rows |

`TestEmailAndSmsAreIndependent` is the one that guards the queue topology from
the database side: they are separate rows because they are separate jobs that
must **both** happen. Collapse them and the customer gets an email *or* a text.

Exactly-once delivery to an external system is not achievable — we cannot send an
email and record that we sent it atomically. What these tests pin down is
at-least-once with a very small duplicate window, which for notifications is the
correct side to err on.

## 7. The outbox relay — `relay_test.go`

Failure mode B, from the publishing side.

| Test | Asserts |
|---|---|
| `TestRelayPublishesAndMarksSent` | Backlog drains, `sent_at` set |
| `TestRelayLeavesRowsUnsentWhenThePublishFails` | **Nothing marked sent when nothing was confirmed** |
| `TestRelayRecordsFailedAttempts` | A failed publish bumps `attempts` |
| `TestTwoRelaysDoNotDoublePublish` | Two instances, 25 events, each claimed exactly once |
| `TestRelayOnEmptyOutboxIsANoop` | The common case — polling finds nothing |
| `TestOutboxLagReflectsTheBacklog` | Depth and age track the backlog |

`TestRelayLeavesRowsUnsentWhenThePublishFails` is the one that matters. A plain
publish is a write to a socket buffer and returns long before the broker has the
message; marking `sent_at` on the back of that means a broker crash silently
loses an event **the database believes was delivered** — the outbox recording a
lie. The test closes the publisher's channel so every publish fails, then asserts
nothing was marked sent and the rows stay claimable.

`TestTwoRelaysDoNotDoublePublish` verifies that `FOR UPDATE SKIP LOCKED` is what
lets relays scale: the two instances' published counts must **sum** to the number
of events, so no row was claimed twice.

## 8. The daily report — `rollup_test.go`

| Test | Asserts |
|---|---|
| `TestRollupCountsOnlyRevenueOrders` | Only `PAID`/`FULFILLED` count; `PENDING`, `CANCELLED`, `EXPIRED`, `FAILED` excluded |
| `TestRollupIsIdempotent` | Three runs do not triple revenue |
| `TestRollupRecomputeReflectsLateChanges` | An order paid after the day closed is picked up on recompute |
| `TestRollupWritesZeroRowForDayWithNoSales` | "No sales" is distinguishable from "never computed" |
| `TestDailySalesMixesRollupAndLive` | Closed days from the rollup, today live, each row tagged with its `source` |
| `TestRollupNeverMaterialisesToday` | Today is never frozen while it can still change |
| `TestRollupResumesFromLastMaterialisedDay` | Catch-up resumes rather than recomputing history |

Idempotency is not academic here: the scheduler re-runs days after every
restart, and a rollup that accumulated on replay would **silently double
revenue** — the most consequential bug a sales report can have.

Orders are inserted directly with a chosen `created_at`. Arranging a month of
history through the real intake path would mean manipulating the clock and
would test the wrong thing — these tests are about the *report*.

---

## 9. Real-time, transport and middleware

The SSE stack in three layers (9.1–9.3), tested separately because they fail
differently, then the machinery underneath it: the rate limiter that guards the
same routes (9.4), and the broker plumbing every consumer depends on (9.6–9.7).

### 9.1 The hub (pure unit, no database, runs anywhere)

| Test | Asserts |
|---|---|
| `TestHubDeliversToSubscriber` | Basic fan-out |
| `TestHubIsolatesOrders` | One customer's stream never carries another's orders |
| `TestPublishNeverBlocksOnAFullSubscriber` | 10,000 publishes to a subscriber that stopped reading never block |
| `TestUnsubscribeClosesChannelAndReleasesMemory` | Channel closed *and* the per-order map entry dropped |
| `TestDoubleUnsubscribeIsSafe` | No panic from closing a closed channel |
| `TestPublishToNobodyIsHarmless` | Unknown and zero order ids |
| `TestHubUnderConcurrentUse` | 8 publishers + 8 subscribers churning; no leaks. **Clean under `-race`** |

`TestPublishNeverBlocksOnAFullSubscriber` is the one that matters. `Publish`
runs on the backplane consumer's goroutine, so if one stalled browser could
block it, event delivery would stop for **every** client on that instance.

`TestUnsubscribeClosesChannelAndReleasesMemory` asserts the *outer* map shrinks
too — otherwise it grows by one entry per order ever streamed and never shrinks.

### 9.2 The endpoint (`sse_test.go`, full HTTP stack)

| Test | Asserts |
|---|---|
| `TestSSESendsCurrentStateImmediately` | A late-connecting client gets current state at once, not silence |
| `TestSSEPushesTransitionsAndClosesOnTerminal` | Transition pushed with the authoritative status re-read from Postgres; stream closes on terminal |
| `TestSSERejectsOtherPeoplesOrders` | **404, not 403** — 403 confirms the order exists |
| `TestSSERequiresAuthentication` | 401 without a token |

The SSE route is authenticated but deliberately **outside** the rate limiter
([routes.go:143](../internal/api/routes/routes.go)) — a token bucket charges one
token per request, and an SSE connection is one request held open for minutes, so
a reconnecting client would be throttled for behaving correctly. The limiter is
covered separately, in §9.4.

### 9.3 The topology (`backplane_test.go`, needs RabbitMQ)

| Test | Asserts |
|---|---|
| `TestBackplaneDeliversOrderEventsToHub` | Event survives the round trip, id intact |
| `TestBackplaneQueuesDoNotCompete` | Two instances **both** receive the same event |
| `TestBackplaneBindingScopesToOrderEvents` | `payment.*` does not leak onto the order stream |

### 9.4 The rate limiter (`internal/api/middleware/ratelimit_test.go`, needs Redis)

| Test | Asserts |
|---|---|
| `TestTokenBucketAllowsBurstThenRejects` | Burst granted, then refused |
| `TestTokenBucketRefillsOverTime` | Tokens accrue continuously, not on a window boundary |
| `TestTokenBucketIsAtomicUnderConcurrency` | **200 concurrent callers, burst 20 → exactly 20 granted** |
| `TestRateLimiterFailsOpenWhenRedisIsUnreachable` | `Allow` reports the error; the middleware still serves 200 |
| `TestBucketsAreIsolatedPerKey` | One noisy client cannot exhaust another's bucket |
| `TestMiddlewareReturns429WithHeaders` | 429 plus `X-RateLimit-*` and `Retry-After` |
| `TestIdleBucketsExpire` | Abandoned keys carry a TTL and do not accumulate |

`TestTokenBucketIsAtomicUnderConcurrency` is the oversell test in a different
store. The naive limiter does `GET`, compute, `SET` from the application; with N
replicas on one key those steps interleave and the limit leaks under exactly the
load it exists to control — the same shape as `SELECT`-then-`UPDATE`. Redis runs
the Lua script to completion with nothing interleaved, so the count is exact.

It runs against a **real Redis** for the same reason the database tests do: a
fake client would execute the script in Go and test the opposite of what matters.

`TestRateLimiterFailsOpenWhenRedisIsUnreachable` asserts the behaviour claimed in
SOLUTION.md directly — a cache outage must not become an API outage.

### 9.5 Why the SSE queues must not compete

`TestBackplaneQueuesDoNotCompete` guards the subtlest topology decision in the
system. The `payments` queue has N consumers precisely so each message is handled
*once*; the SSE queues must do the opposite, because any instance might hold the
browser connection that cares. If someone "simplifies" them into one shared
durable queue, an `order.paid` reaches one replica while the customer sits on
another and is never told.

> **These tests republish until delivery rather than sleeping before the first
> publish.** A topic exchange silently discards messages matching no binding, so
> a fixed sleep means that under any slowdown the publish beats the bind and the
> event vanishes.
>
> The scoping test needs more than that: it asserts an event does *not* arrive,
> which an unready binding would satisfy **vacuously**. It therefore proves the
> backplane is live by getting a matching event through *first*, then asserts
> the non-delivery. A test that cannot fail for the right reason is worse than
> no test.

---

### 9.6 The consumer machinery (`consumer_test.go`, needs RabbitMQ)

A live broker, but nothing killed: publish, let the consumer handle it, observe
the acknowledgement. Each test gets a **scratch queue** bound to a unique routing
key, declared with a raw connection — production code has no business growing a
"make me a test queue" method.

| Test | Asserts |
|---|---|
| `TestConsumerDeliversEventsToTheHandler` | The round trip reaches the handler intact |
| `TestSuccessfulHandlingAcksTheMessage` | Handled once and **not redelivered** |
| `TestFailingHandlerRetriesOnceThenDeadLetters` | Exactly two attempts, then it stops |
| `TestUnparseableMessagesAreDiscardedImmediately` | Garbage never reaches the handler, and does not wedge the consumer for good messages |
| `TestConcurrencyIsBounded` | Ten messages, limit 2 → peak in-flight never exceeds 2 |
| `TestCancellationWaitsForInFlightWork` | A handler mid-flight when shutdown arrives **completes** |
| `TestPaymentHandlerRequiresAnOrderID` | An event with no aggregate id is refused rather than charging order zero |
| `TestRelayRunDrainsOnItsTicker` | The relay loop picks up work unprompted, and stops on cancellation |

`TestFailingHandlerRetriesOnceThenDeadLetters` is the one worth reading. A
handler error nacks with requeue on first delivery and *without* on redelivery,
so a poison message cannot loop forever — the `Redelivered` flag bounds it
without a counter. The test asserts the count stops at two rather than climbing,
which is the difference between a retry and an infinite loop that looks like a
busy worker.

Every test's cleanup asserts the consumer goroutine actually **exits** after
cancellation. A goroutine with no exit path is a leak, and one blocked on a
delivery channel that will never deliver again is indistinguishable from an idle
one.

### 9.7 Broker reconnection (`supervisor_test.go`, needs RabbitMQ + management API)

A `Channel` belongs to a `Connection` and cannot outlive it, and amqp091-go does
not reconnect. A connection acquired once at startup therefore fails permanently
and *quietly* the first time the broker drops it: the process stays alive, its
health check stays green, and every publish fails from then on. These tests
guard `workers.Supervise`, which owns the connection lifecycle instead.

Exercising that needs a connection that dies *underneath* a running session.
These tests use **RabbitMQ's management API to force-close the connection from
the broker side** — the same thing the broker does to every client when it
restarts, and both faster and more deterministic than killing a process.

Connections are dialled with a `connection_name`, which is what makes a specific
one findable and killable. That naming is not test scaffolding: without it the
management UI lists connections by `host:port`, so during an incident you cannot
tell the relay from a worker from an API replica.

| Test | Asserts |
|---|---|
| `TestSupervisorRedialsAfterTheConnectionIsKilled` | The session ends and a **new one starts**, without the process restarting |
| `TestRelayKeepsDrainingAcrossAConnectionLoss` | Events enqueued *after* the kill still reach the broker |
| `TestEventsEnqueuedWhileDisconnectedArePublishedOnRecovery` | Rows written *during* the outage are published on reconnect |
| `TestSupervisorRetriesAnUnreachableBrokerAndStopsCleanly` | An unreachable broker is retried, not fatal, and cancellation returns cleanly |

The second and third are the ones that matter. Redialling is not the point —
**work resuming** is. Against a relay that never redials, the second test fails
in the way that matters: the process stays alive and simply never publishes
again.

The third is the outbox earning its keep. Nothing is lost across a reconnect
because unpublished rows still have `sent_at IS NULL`, so the next session claims
the same batch.

## 10. The refund consumer — `refund_test.go`

Worth its own section because requesting a refund and executing one are
different claims, and only the second returns any money.

The saga tests (§4) prove a refund is **requested**: the order reaches
`CANCELLED_REFUNDED` and a `payment.refund_requested` event lands in the outbox.
These prove it is **executed**. That is the compensating action of the entire
cancel-vs-charge design, and the one path where a bug costs real money in the
direction nobody complains about — paying a customer back twice.

| Test | Asserts |
|---|---|
| `TestRefundIsExecutedAgainstTheProvider` | The provider is actually called; payment → `REFUNDED`; completion announced |
| `TestDuplicateRefundEventsRefundOnce` | Four deliveries, **one** refund |
| `TestRefundSkipsPaymentsThatNeverSucceeded` | A declined charge is never refunded — that would be inventing a payout |
| `TestRefundWithoutAProviderReferenceEscalates` | A `SUCCEEDED` payment with no reference errors rather than guessing at the card network, and stays refundable |
| `TestRefundFailureLeavesThePaymentRefundable` | A refused refund is not recorded as done |
| `TestRefundRejectsMalformedEvents` | Missing, empty, malformed and wrong-typed `paymentId` |
| `TestRefundForAnUnknownPaymentErrors` | A stale replay surfaces rather than being swallowed |
| `TestCancelDuringChargeLeadsToAnExecutedRefund` | End to end: cancel races the charge, charge wins, refund actually returns the money — charged once, refunded once, stock back |

The last one is the full compensation loop end to end, which is the only test
that shows the cancel-vs-charge design actually returning money rather than
recording an intention to.

## 11. Failure paths — `failure_paths_test.go` + `faults_test.go`

What happens when the database fails part-way through an operation. The
assertion in every case is **not** "an error came back" — that only proves Go
propagates errors. It is **"nothing was left behind."**

### How the faults are injected

GORM implements its own features as a chain of named callbacks per operation —
`gorm:begin_transaction`, `gorm:create`, `gorm:query`, `gorm:commit_or_rollback`.
That chain is an extension point, occupying the same position as EF Core's
`DbCommandInterceptor` or middleware in an HTTP pipeline. The whole plugin
interface is two methods:

```go
type Plugin interface {
    Name() string
    Initialize(*DB) error
}
```

`faultInjector` registers a callback *before* the one that actually executes,
and when armed calls `db.AddError(...)`. GORM then propagates it exactly as if
the driver had returned it.

**Why this rather than mocking a repository.** A mock replaces the repository, so
everything below the seam disappears — the SQL, GORM, the driver, the
transaction. A callback leaves all of it in place and makes the *real* thing
fail, so the error travels the production path: the repository wraps it,
`database.IsRetryable` classifies it, `InTx` decides replay-or-return, the
service branches, the handler maps a status code.

**And it changes no production code.** The plugin is installed on the test
connection only; `database.Open` never registers it. Adding repository
interfaces would change every service signature *and* open the seam that makes
the oversell test mockable — see §1.

> **Sharp edge:** the matcher is a substring, so a fault armed on `"outbox"` also
> matches `SELECT count(*) FROM outbox`. A still-armed rule fails the test's own
> verification queries rather than the code under test. Disarm before asserting.

| Test | Asserts |
|---|---|
| `TestIntakeRollsBackEntirelyWhenInventoryFails` | No order, no items, no reservations, **no event**, stock untouched |
| `TestIntakeRollsBackWhenTheOutboxWriteFails` | An order whose event cannot be written must not exist — failure mode B from the inside |
| `TestDeadlockIsRetriedAndSucceeds` | A `40P01` is replayed and the operation **succeeds**, reserving stock exactly once |
| `TestConstraintViolationIsNotRetried` | A unique violation surfaces on the **first** attempt — replaying it would hang |
| `TestRetriesAreBounded` | A fault that never clears gives up after the budget, and says so |
| `TestReaperRollsBackOnFailure` | A half-reaped order releases no stock; the reservation stays `HELD` for the next sweep |
| `TestPaymentSettlementRollsBackWhenTheEventFails` | No `PAID` order without its `order.paid` event |

`TestDeadlockIsRetriedAndSucceeds` and `TestConstraintViolationIsNotRetried` are
the pair worth reading together. They exercise the retry *classification* end to
end with errors Postgres would really produce: a serialization conflict is
transient and replaying usually works, while a constraint violation means an
invariant **held** and replaying produces the identical result. Getting that
backwards turns a lost race into an infinite loop.

### Sweeps — failing every step, not a chosen one

The tests above pick a step and prove the rollback holds *there*. A sweep runs the
same operation once per position, failing a **different** step each time, and
asserts the same invariant after every one.

```go
for n := 1; n <= 12; n++ {
    t.Run(fmt.Sprintf("fail_at_call_%d", n), func(t *testing.T) {
        faults.FailNthCall("", n, errInjected)
        _, err := orders.Create(ctx, input)
        faults.disarm()
        require.Error(t, err)
        assertIntakeLeftNothing(t, store, db, productID)
    })
}
```

Each position is a **subtest**, so a failure names the step rather than leaving
you to bisect:

```
--- FAIL: TestIntakeRollsBackAtEveryStep/fail_at_call_4
```

Positions past the operation's real call count skip, and the parent asserts a
minimum number actually fired — so the sweep cannot silently stop doing anything
if the code changes shape.

| Sweep | Positions exercised |
|---|---|
| `TestIntakeRollsBackAtEveryStep` | 6 — products, order, inventory, items, reservations, outbox |
| `TestCancelRollsBackAtEveryStep` | 6 |
| `TestReaperRollsBackAtEveryStep` | 6 |

**What this guards that the picked-step tests cannot.** Rollback itself is
Postgres's job. What is *ours* is that the work is inside a transaction at all,
that errors propagate, and that nothing escapes the transaction's reach. A sweep
catches three changes a future edit could make:

- a step moved **outside** the transaction
- an error **swallowed** — a missing `return`, a discarded `_ =`
- a **non-transactional side effect** added: a cache write, an HTTP call, a
  direct publish

That last one is the reason these earn their keep. It silently reintroduces
failure mode B — the dual write the outbox exists to eliminate — and it is
exactly the change someone makes without realising. No other test in the suite
would notice.

The coverage gain is incidental and small: each position covers one `return err`.
The invariant is the point.

### A failure that is genuinely real

Everything above injects a synthetic error — the database never actually failed,
so those tests exercise *our handling*, not Postgres's behaviour.

`TestConnectionKilledMidTransactionRollsBack` closes that gap. It reserves stock
inside a transaction, reads its own `pg_backend_pid()`, and then — **from a
second connection** — calls `pg_terminate_backend` on itself. Postgres aborts the
transaction while it holds row locks; no `COMMIT` is possible. The assertion is
that the reservation left no trace, read back through the surviving connection.

That is rollback with the exact semantics production would see, rather than an
error a test author invented.

## 12. Running it

```bash
# With Go installed — testcontainers provides Postgres
go test ./... -race
```

```powershell
# Through the containerised toolchain, against the running compose stack
docker compose exec postgres createdb -U postgres orders_test
$env:TEST_DATABASE_URL = "postgres://postgres:postgres@postgres:5432/orders_test?sslmode=disable"
$env:TEST_RABBITMQ_URL = "amqp://guest:guest@rabbitmq:5672/"
$env:TEST_REDIS_ADDR   = "redis:6379"
.\scripts\go.ps1 test ./... -count 1
.\scripts\go.ps1 test ./internal/services/... -race   # hub, no DB needed
```

Each dependency has its own variable, and **anything unset skips rather than
fails** — an environment gap should not look like a broken build. So a run
without `TEST_REDIS_ADDR` silently omits the rate limiter suite; check the
`ok`/`skip` lines rather than assuming a green run covered everything. The
RabbitMQ management URL used by §9.7 is derived from `TEST_RABBITMQ_URL`, and
only needs `TEST_RABBITMQ_MGMT` if it lives somewhere else.

`-count 1` disables the result cache, which matters for tests whose outcome
depends on external state. Write flags in the **space-separated** form when
going through `scripts/go.ps1`; PowerShell does not reliably forward the
`-flag=value` form through the wrapper, and a dropped `-coverpkg` fails silently
as a plausible-looking but wrong number.

### On `-race`

Run it, and know its limit. The race detector finds **Go memory races** — two
goroutines touching the same variable without synchronisation. It is completely
blind to **database races**, which is what almost every bug in this system is.
A clean `-race` run says nothing about whether stock can be oversold; only the
concurrency tests against real Postgres say that.

---

## 13. Coverage

**83.9% of statements**, measured across `internal/` and `pkg/`:

```powershell
.\scripts\go.ps1 test ./... -count 1 `
    -coverpkg ./internal/...,./pkg/... -coverprofile coverage.out
go tool cover -func coverage.out
```

> `-coverpkg` is mandatory here. The integration tests live in their own `tests`
> package, so a plain `go test -cover` measures only the test package itself and
> reports ~0% for everything it actually exercises.
>
> **Integration tests count.** Go instruments the packages named by `-coverpkg`
> and records which statements the binary executed — it has no notion of "unit"
> versus "integration". Almost all of this figure comes from `tests/`; the pure
> unit binary contributes about 5% on its own.

| Package | Coverage | Why |
|---|---|---|
| `internal/config` | 100.0% | Table-driven over pure functions |
| `internal/models` | 100.0% | State machine, reachability, enum round-trips |
| `pkg/logger` | 100.0% | Trace id propagation |
| `internal/api/middleware` | 95.3% | Auth via the HTTP tests, limiter directly |
| `internal/api/routes` | 91.7% | Wiring, fully walked by the HTTP fixtures |
| `pkg/database` | 91.7% | Error classification through wrapped errors |
| `internal/services` | 88.2% | Intake, saga, refunds, reaper, rollup, notifications, provider |
| `internal/api/handlers` | 84.2% | Every endpoint, table-driven |
| `internal/workers` | 81.8% | Relay, backplane, consumers, supervisor |
| `internal/repository` | 81.8% | Hot paths, outbox, notifications, leases, listings |

*(Per-package figures are the mean of per-function percentages, so they are
indicative rather than exact; the 83.9% total is statement-weighted.)*

**Past the >80% the brief asks for at `README.md:164`, and every package is
individually above it** — so the number is not one well-covered package carrying
several thin ones.

What the number does *not* capture is that the coverage is deliberately
concentrated on the parts that can lose money or corrupt data — the conditional
`UPDATE`, the saga, the CAS transitions, lease reclamation — while the untested
remainder is mostly CRUD and wiring.

That distinction is the whole argument for how this suite is built. A suite at
85% built from handler tests over mocked repositories would score better on this
metric and be blind to every defect that actually threatens this system, because
**coverage measures which lines ran, not whether the dangerous ones were run
concurrently** — and concurrently is the only way these bugs appear. The figure
is a useful prompt for where to look next. It is a poor measure of whether the
looking was worth anything.

## 14. What is not covered, and why

Three buckets, all deliberate. They are listed with what closing them would
cost, because "untested" without a reason is just an apology.

### 14.1 The residue of error branches

Fault injection (§11) reaches the error paths that matter. What remains
uncovered is the residue: `if err != nil` paths on operations no test happens to
fail, plus a few unreachable-in-practice branches like a `nil` statement guard.

Each fault-injection test covers exactly one such statement, so closing this
bucket means roughly one test per `return err` — a large number of tests whose
combined assertion is that Go propagates errors. What was worth reaching is
reached.

### 14.2 What a real `SIGKILL` would still add

The supervisor tests kill the *connection*. They do not kill a *process*, and
there is one thing only that can demonstrate:

**That RabbitMQ actually redelivers an unacked message when a consumer dies
mid-handler.** Today `killWorkerMidCharge` reproduces the database state a dead
worker leaves behind — enough to test the recovery logic, and deliberately kept
for being fast and deterministic — but the broker half is still argued from the
code rather than shown.

The shape it would take, since the pieces already exist:

1. `exec.Command` the real `cmd/worker` binary with `PAYMENT_LATENCY=30s`, which
   gives a wide, reliable window rather than a race against the scheduler.
2. Place an order and poll the database until it reaches `CHARGING` — the worker
   is now inside the provider call.
3. `Process.Kill()`. A real `SIGKILL`: no deferred cleanup, no graceful channel
   close, no ack.
4. Assert the message is redelivered **and** that `RecoverStuckCharges` completes
   the order.

The awkwardness is not the killing. It is process lifecycle — building the
binary, wiring its environment, capturing stderr so a failure is diagnosable
rather than mysterious — plus the fact that it runs in seconds and depends on
process scheduling, so it belongs behind a build tag rather than on every save.

Worth doing, but not the largest gap: the reconnect path it was mainly wanted
for is covered directly in §9.7.

### 14.3 Not worth testing

- **`cmd/*` entry points.** Wiring only. The logic they wire is covered, and a
  test would assert that the code is shaped the way it is shaped.
- **The periodic loops themselves.** Every job is tested by calling its function
  directly — `ReapOnce`, `RecoverStuckCharges`, `SweepExpiredLeases`,
  `RollupClosedDays`, `DrainOnce`. What is *not* tested is `everyTick` firing on
  its configured interval, which would mean a test that mostly waits. The
  scheduling is four lines and reviewable by eye.

### Summary

| Bucket | Verdict |
|---|---|
| Residual error branches | **Leave** — one test per `return err`, asserting that Go propagates errors |
| Real process `SIGKILL` (broker redelivery) | **Worth doing**, behind a build tag |
| Entry points and tick scheduling | **Leave** — wiring, reviewable by eye |

Closing the first would push the number higher and make the suite worse.
