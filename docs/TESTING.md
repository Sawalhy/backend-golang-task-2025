# Test Suite

> What every test asserts, how it is built, and — for the ones that matter — why
> it is written the way it is rather than the obvious way.

**113 tests.** Integration against real Postgres, RabbitMQ and Redis (`tests/`,
plus the rate limiter), and pure unit tests where no infrastructure is needed.

| File | Tests | Covers | Needs |
|---|---|---|---|
| `tests/api_test.go` | 25 | Every REST endpoint: auth, CRUD, RBAC, validation | Postgres |
| `tests/concurrency_test.go` | 7 | Overselling, deadlock ordering, idempotency (A, G) | Postgres |
| `tests/saga_test.go` | 12 | Double charge, cancel-vs-charge, worker death (C, D, E) | Postgres |
| `tests/notification_test.go` | 11 | Send-once, leases, retries (I) | Postgres |
| `tests/reaper_test.go` | 7 | Abandoned checkout reclamation (F) | Postgres |
| `tests/rollup_test.go` | 7 | Daily sales report correctness | Postgres |
| `tests/relay_test.go` | 6 | Confirm-before-`sent_at`, no double publish (B) | Postgres + RabbitMQ |
| `tests/sse_test.go` | 4 | Order status streaming over HTTP | Postgres |
| `tests/backplane_test.go` | 3 | RabbitMQ → hub delivery topology | RabbitMQ |
| `internal/api/middleware/ratelimit_test.go` | 7 | Token bucket atomicity, fail-open | Redis |
| `internal/config/config_test.go` | 10 | Env parsing, validation, boot refusal | — |
| `internal/services/status_hub_test.go` | 7 | SSE fan-out, no leaks, non-blocking publish | — |

---

## 1. How the suite is built

### Real Postgres, never a mock

Nothing here mocks the database, and that is the central decision.

**A mock returns what you told it to return.** It is structurally incapable of
exhibiting a race: it cannot lose an update, cannot deadlock, cannot enforce a
`CHECK` constraint, and cannot make two transactions contend for a row. Every
interesting bug in this system is a database race, so a mock-based suite would
pass while the system overselling stock.

The tests therefore run the **real migration files** rather than `AutoMigrate`,
because the `CHECK` constraints and partial unique indexes *are* the invariants
under test. A schema built any other way would be testing a different system.

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

### These tests were checked for teeth

A concurrency test that passes proves nothing until you have watched it fail.
`Reserve` was temporarily reverted to the naive read-then-write —
`SELECT available`, check in Go, then `UPDATE`:

```
--- FAIL: TestConcurrentBuyersCannotExceedStock
    violates check constraint "inventory_available_check" (23514)  ... × 24
    expected: 53   actual: 29
```

Twenty-four buyers passed the in-Go check simultaneously. It also showed the
**second guard** working: `available` never went negative, because the `CHECK`
refused what the application logic wrongly allowed. The `WHERE` clause turns an
oversell into a clean 409; the constraint makes corruption impossible even when
the code above it is wrong.

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

## 9. Real-time — `status_hub_test.go`, `sse_test.go`, `backplane_test.go`

Three layers, tested separately because they fail differently.

### The hub (pure unit, no database, runs anywhere)

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

### The endpoint (`sse_test.go`, full HTTP stack)

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

### The topology (`backplane_test.go`, needs RabbitMQ)

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

### 9.5 Topology

`TestBackplaneQueuesDoNotCompete` guards the subtlest topology decision in the
system. The `payments` queue has N consumers precisely so each message is handled
*once*; the SSE queues must do the opposite, because any instance might hold the
browser connection that cares. If someone "simplifies" them into one shared
durable queue, an `order.paid` reaches one replica while the customer sits on
another and is never told.

> **These tests originally used a fixed `time.Sleep` before publishing, and it
> was wrong.** A topic exchange silently discards messages matching no binding,
> so under any slowdown the publish beat the bind and the event vanished. They
> now republish until delivery.
>
> The scoping test was worse: it asserts an event does *not* arrive, so an
> unready binding made it **pass vacuously**. It now proves the backplane is live
> by getting a matching event through *first*, then asserts the non-delivery. A
> test that cannot fail for the right reason is worse than no test.

---

## 10. Running it

```bash
# With Go installed — testcontainers provides Postgres
go test ./... -race
```

```powershell
# Through the containerised toolchain
docker compose exec postgres createdb -U postgres orders_test
$env:TEST_DATABASE_URL = "postgres://postgres:postgres@postgres:5432/orders_test?sslmode=disable"
$env:TEST_RABBITMQ_URL = "amqp://guest:guest@rabbitmq:5672/"
.\scripts\go.ps1 test ./... -count=1
.\scripts\go.ps1 test ./internal/services/... -race   # hub, no DB needed
```

`-count=1` disables the result cache, which matters for tests whose outcome
depends on external state.

### On `-race`

Run it, and know its limit. The race detector finds **Go memory races** — two
goroutines touching the same variable without synchronisation. It is completely
blind to **database races**, which is what almost every bug in this system is.
A clean `-race` run says nothing about whether stock can be oversold; only the
concurrency tests against real Postgres say that.

---

## 11. Coverage

**71.6% of statements**, measured across `internal/` and `pkg/`:

```powershell
.\scripts\go.ps1 test ./tests/... ./internal/services/... -count=1 `
    -coverpkg=./internal/...,./pkg/... -coverprofile=coverage.out
go tool cover -func=coverage.out
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
| `internal/services` | 88.0% | Intake, saga, refunds, reaper, rollup, notifications, provider |
| `internal/api/handlers` | 84.2% | Every endpoint, table-driven |
| `internal/repository` | 80.7% | Hot paths, outbox, notifications, leases, listings |
| **`internal/workers`** | **37.5%** | **The only package below 80% — see §12.1** |

*(Per-package figures are the mean of per-function percentages, so they are
indicative rather than exact; the 77.1% total is statement-weighted.)*

**Just short of the >80% the brief asks for at `README.md:164`**, and the shortfall
is now concentrated in exactly one place. Every other package is at or above the
bar; `internal/workers` sits at 37.5% because its untested half is the broker
supervisor and the consumer loops, which cannot be reached without a connection
that can be severed on demand. That is the next section.

What the number does *not* capture is that the coverage is deliberately
concentrated on the parts that can lose money or corrupt data — the conditional
`UPDATE`, the saga, the CAS transitions, lease reclamation — while the untested
remainder is mostly CRUD and wiring. A suite at 85% built from handler tests over
mocked repositories would score better and catch none of the three real bugs this
one found. Coverage measures which lines ran, not whether the dangerous ones were
run *concurrently*, which is the only way this system's bugs appear.

That said, the gap is real and closable. The cheapest wins, in order:

1. **`internal/config`** — pure functions, no I/O. Table-driven tests would take
   it past 90% for ~72 statements of effort.
2. **`internal/api/handlers`** — the single largest untested block (320
   statements). The `sseFixture` pattern already provides a full HTTP stack, so
   table-driven tests over products/users/admin are mechanical.
3. **`internal/repository`** — targeted tests for the listing and admin queries
   that the hot paths never touch.

## 12. What is not covered, and why

Every remaining gap falls into one of four buckets. They are listed with what it
would actually take to close them, because "untested" without a reason is just an
apology.

### 12.1 Needs a severable broker connection — the SIGKILL bucket

**`internal/workers/supervisor.go` — 45 statements, 0% covered.** The single
largest untested unit, and the only one that genuinely resists ordinary testing.

This is the code that fixed the worst bug in the project: an AMQP connection was
acquired once at startup and never redialled, so after RabbitMQ dropped it the
relay failed **241 consecutive publishes** while its health check stayed green
and the entire async pipeline silently stopped.

Testing it requires making a live connection die *underneath* a running session —
not closing it politely, which is a different code path. Options:

| Approach | Cost |
|---|---|
| **toxiproxy** between the app and RabbitMQ | Best fidelity. Adds a container and a control API; can sever mid-publish on demand |
| **testcontainers RabbitMQ**, stopped mid-session | No extra dependency, but stop/start is slow and the timing is coarse |
| **Management API**, force-close the connection | Light, but it closes politely-ish and may not reproduce a hard drop |
| **A real `SIGKILL`** of a worker subprocess | Tests the whole thing end to end, including redelivery |

The `SIGKILL` variant is the most valuable and the most awkward, and it is worth
being precise about *why* it is awkward, because it is not the killing:

- **Determinism.** The process must be killed at a specific moment — after the
  `PENDING → CHARGING` CAS, before settlement. `PAYMENT_LATENCY` is already
  configurable, so setting it to 30s gives a wide, reliable window. That part is
  easy.
- **Process lifecycle.** The test must build the binary, start it with the right
  environment, wait for readiness (poll the database until the order reaches
  `CHARGING`), kill it, and capture its stderr so a failure is diagnosable rather
  than mysterious.
- **Speed and flakiness.** Seconds, not milliseconds, and it depends on process
  scheduling. It belongs behind a build tag or `-short` guard so it does not run
  on every save.

What it would prove that nothing currently does: **that RabbitMQ actually
redelivers the unacked message** when a consumer's connection dies, and that the
*real binary's* wiring recovers. Today both are argued from the code rather than
demonstrated. `killWorkerMidCharge` reproduces the database state a dead worker
leaves behind — which is enough to test the recovery logic, and is deliberately
kept for being fast and deterministic — but it cannot prove the broker half.

### 12.2 Needs a live consumer, but nothing killed

**`internal/workers/consumer.go` (34 statements), `broker.go` `Consume`,
`relay.go` `Run`.** These need a broker and a running loop, but no failure
injection: publish a message, let the consumer handle it, assert the ack.

The interesting cases are the ack/nack policy — a handler error nacks with
requeue on first delivery and dead-letters on the second, so a poison message
cannot loop forever. That is testable today with the existing RabbitMQ; it simply
has not been written. Roughly a day, and it would take `internal/workers` from
37.5% to most of the way.

### 12.3 Error branches — needs fault injection

**~250 statements**, spread thinly across every file as
`if err != nil { return err }`. These fire only when the database itself fails
mid-transaction: a dropped connection, a full disk, a cancelled context at an
awkward moment.

Reaching them needs a proxy that can fail queries on command, or interface seams
introduced purely so a mock can return errors — which would mean mocking the
database, the one thing this suite deliberately refuses to do.

**This is the bucket I would leave alone.** The branches are one line each and
uniform; the risk they carry is low, and the machinery to reach them would cost
more clarity than it buys confidence. Coverage percentage is the wrong reason to
add it.

### 12.4 Not worth testing

- **`cmd/*` entry points.** Wiring only. The logic they wire is covered, and a
  test would assert that the code is shaped the way it is shaped.
- **The periodic loops themselves.** Every job is tested by calling its function
  directly — `ReapOnce`, `RecoverStuckCharges`, `SweepExpiredLeases`,
  `RollupClosedDays`, `DrainOnce`. What is *not* tested is `everyTick` firing on
  its configured interval, which would mean a test that mostly waits. The
  scheduling is four lines and reviewable by eye.

### Summary

| Bucket | Statements | Verdict |
|---|---|---|
| Broker supervisor (severable connection / SIGKILL) | ~45 | **Worth doing** — guards the worst bug found |
| Consumer loops (live broker) | ~60 | **Worth doing** — ordinary integration work |
| Error branches (fault injection) | ~250 | **Leave** — cost exceeds benefit |
| Entry points and scheduling | ~30 | **Leave** — wiring |

Closing the first two would put coverage in the mid-eighties. Closing the third
would push it higher and make the suite worse.

## 13. The refund consumer — `refund_test.go`

Worth its own section because it was the most consequential gap in the suite, and
it had nothing to do with infrastructure.

The saga tests prove a refund is **requested**: the order reaches
`CANCELLED_REFUNDED` and a `payment.refund_requested` event lands in the outbox.
Nothing proved it was ever **executed**. That is the compensating action of the
entire cancel-vs-charge design, and it is the one path where a bug costs real
money in the direction nobody complains about — paying a customer back twice.

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

The last one is the full compensation loop, which previously stopped at the
outbox row.

## 14. Recently closed

Context on what moved, and why the number is not the point:

| Was | Now | What it took |
|---|---|---|
| `internal/api/handlers` 23.6% | 84.2% | Table-driven tests over every endpoint |
| `internal/services` 51.6% | 88.0% | Refund consumer, simulated provider, notifications |
| `internal/repository` 48.6% | 80.7% | Listing and admin queries, audit, live-intent lookup |
| `internal/config` 14.3% | 100% | Pure functions, table-driven |
| `internal/models` 87.0% | 100% | State machine reachability, enum round-trips |
| `pkg/logger` 58.0% | 100% | Trace id propagation |
| `pkg/database` 75.0% | 91.7% | Error classification through wrapped errors |
| **total 44.8%** | **77.1%** | |

Several of those found real defects rather than moving a percentage: the
worker-death recovery gap, the reconciliation CAS that abandoned half its work,
and the backplane tests that were passing vacuously. The refund consumer was the
largest correctness gap of the lot and needed no infrastructure at all — it had
simply never been written.

- **The broker reconnect.** `workers.Supervise` is verified by hand
  (`docker compose restart rabbitmq`, watching both services redial with jittered
  backoff), but nothing in the suite guards it. Needs a broker whose connection
  can be severed on demand — toxiproxy, or a stoppable testcontainers RabbitMQ.
  **This is the most valuable remaining test.**
- **The broker supervisor and consumer loops** (`internal/workers`, 37.5%). Both
  need a broker whose connection can be severed on demand; see the reconnect
  regression test below.
- **`cmd/*` entry points.** Wiring only, and the logic they wire is covered.
- **The periodic loops themselves.** Every job is tested by calling its function
  directly — `ReapOnce`, `RecoverStuckCharges`, `SweepExpiredLeases`,
  `RollupClosedDays`, `DrainOnce`. The `everyTick` scheduling in `cmd/worker` is
  not, so "the timer actually fires on the configured interval" rests on
  inspection rather than a test.
- **Actual process death.** `killWorkerMidCharge` reproduces the database *state*
  a dead worker leaves behind, not a real `SIGKILL`, and nothing exercises
  RabbitMQ redelivering an unacked message when a consumer's connection drops.
  The recovery *from* that state is well covered; the broker mechanics that
  produce it are not.
