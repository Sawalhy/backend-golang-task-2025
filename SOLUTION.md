# Concurrent Order Processing System — Solution

> `README.md` is the assignment brief exactly as issued and is left untouched.
> This file is the submission: how to run it, what was built, and which decisions
> were deliberate.

## Run it

```bash
docker compose up --build
```

That starts Postgres, RabbitMQ, Redis, runs migrations, seeds demo data, and
brings up the three application processes. The API is on `http://localhost:8080`;
RabbitMQ's management UI is on `http://localhost:15672` (guest / guest).

Seeded accounts:

| Email | Password | Role |
|---|---|---|
| `admin@example.com` | `admin12345` | ADMIN |
| `sarah@example.com` | `customer123` | CUSTOMER |

A five-second smoke test:

```bash
curl -s localhost:8080/api/v1/auth/login -H 'content-type: application/json' \
  -d '{"email":"sarah@example.com","password":"customer123"}'
```

Then place an order with the returned token:

```bash
curl -s localhost:8080/api/v1/orders -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' -d '{"items":[{"product_id":2,"qty":1}]}'
```

It returns **202**, not 201 — see "Why 202" below.

### Without Docker

Go is not required on the host if you have Docker: `scripts/go.ps1` runs the
toolchain in a container (`.\scripts\go.ps1 test ./...`). With Go installed
locally, use `go` directly.

## Architecture

```
                  ┌──────── one image, three entry points ────────┐
 client ─HTTP─►  cmd/api        cmd/worker            cmd/relay
                    │               │                     │
                    │               │   claims outbox rows (SKIP LOCKED),
                    │               │   publishes, marks sent
                    ▼               ▼                     ▼
                 Postgres ◄─────────┴──────────► RabbitMQ ──► consumers
```

Three processes, one binary image. They scale independently — API replicas track
request load, workers track queue depth — while a single artifact keeps
deployment to one thing to build and promote.

**Order intake is one transaction:** reserve stock with a conditional `UPDATE`,
insert the order, items and reservations, insert the outbox event. Commit. Return
202. Payment happens afterwards in a worker with **no transaction open and no
lock held**.

## The five decisions that carry the system

### 1. The conditional UPDATE (failure mode A: overselling)

```sql
UPDATE inventory SET available = available - $1, reserved = reserved + $1
 WHERE product_id = $2 AND available >= $1
```

The check is in the `WHERE` clause, so the read and the write are one statement
and cannot interleave. The naive version — `SELECT` the stock, decide in Go,
`UPDATE` — has a gap in which two transactions both see stock 1, both decide yes,
and the last unit sells twice.

`RowsAffected == 0` means insufficient stock. **That is a business outcome, not an
error**: it maps to 409 Conflict, not 500. A second, independent guard —
`CHECK (available >= 0)` in migration 001 — means the database enforces the
invariant even if the query above were ever wrong.

### 2. Reserve → pay → commit

A payment provider call takes seconds. Holding a database transaction across it
would pin a pool connection and hold row locks on the product, serialising every
other buyer behind one slow card. So stock is *reserved* in the intake
transaction, the transaction commits, and payment runs later with nothing locked.

The cost is honest: reserved stock is unavailable to other customers while an
order is unpaid, so an abandoned checkout holds inventory hostage — which is why
the reaper exists.

### 3. The outbox (failure mode B: the dual write)

Writing the order to Postgres and then publishing to RabbitMQ is a write to two
systems with no shared transaction. Crash in between and the order exists but
nothing will ever process it. Publish first instead and you may announce an order
that never committed.

The event row is written **in the same transaction as the order**, so
"committed but never queued" is unrepresentable. `cmd/relay` publishes it
afterwards, waits for a **publisher confirm**, and only then marks `sent_at`. A
plain publish is a socket write, not a delivery guarantee.

This makes delivery **at-least-once**, so every consumer dedupes. Losing events
instead would be far worse than occasionally repeating one.

### 4. Cancel versus charge (failure mode D)

The subtlest bug here. A cancel arriving while a payment is in flight cannot
simply cancel the order — the charge may be about to succeed.

`PENDING` → cancel outright, release stock.
`CHARGING` → move to **`CANCELLING`**, release nothing, and answer **202**, not
200. The payment worker finishes its call, finds the order in `CANCELLING`
rather than `CHARGING`, and settles it: `CANCELLED_REFUNDED` plus a refund event
if the charge landed, plain `CANCELLED` if it was declined.

A saga cannot roll back a committed step in another system, so the compensation
is a refund — a second forward action, not an undo.

### 5. Every state change is a CAS

Nothing writes `orders.status` directly. `Transition(ctx, tx, id, from, to)`
checks the edge against the state machine, then does the update with the expected
state in the `WHERE`, and returns **whether this caller performed it**.

That bool is the entire mechanism that makes at-least-once delivery survivable:
two workers handling a duplicate event both attempt `PENDING → CHARGING` and
exactly one wins. `RowsAffected == 0` means *you lost a race*, which is a branch,
not an error to retry.

## Deliberate decisions you should ask about

The spec leaves these open. Each was decided rather than guessed at.

| Gap | Decision |
|---|---|
| **Partial fulfilment** — 3 items, 1 out of stock | **Reject the whole order.** Matches normal checkout, and it is free: one transaction rolling back releases every reservation already taken in it. Partial fulfilment would mean splitting orders, partial payments and partial refunds — a materially larger design. |
| **No auth endpoints exist** in the spec, yet JWT is mandatory | Added `POST /api/v1/auth/login`. |
| **Report timezone** — "daily" in which zone? | **UTC**, stated in the response body. A report that silently uses server-local time is wrong twice a year. |
| **`PUT /orders/{id}/cancel`** is not RESTful | Implemented exactly as specified (`README.md:86`). Noting that it was noticed: a cancellation is a resource, so `POST /orders/{id}/cancellation` would be better. |
| **Job state visibility** | RabbitMQ is transport, not storage — once a message is acked the broker cannot say what became of it. `GET /orders/{id}/status` reads payment attempt history from Postgres. |
| **Real-time inventory** | Not pushed. See "Not built" below. |

## Indexing strategy

Three ideas, all in `migrations/000001_init.up.sql`:

- **Foreign keys are indexed explicitly.** Postgres does not do this for you, and
  it is the most common miss — joins and cascades seq-scan without them.
- **Partial indexes for queue-shaped tables.** `outbox_unsent` indexes only rows
  `WHERE sent_at IS NULL`. After a year the table holds tens of millions of rows
  and the index holds ~50: *its size tracks the backlog, not the table*. Same for
  expiring reservations and claimable notifications.
- **Composites are equality-then-sort**, e.g. `(user_id, created_at DESC)` — not
  "most selective first", which is a rule about competing equality columns and
  says nothing about a sort column.

**`inventory.available` is deliberately left unindexed.** It is the most-updated
column in the system; indexing it taxes every order — and blocks heap-only-tuple
updates — to accelerate an occasional admin query over ~10k rows. Take the seq
scan, protect the write path.

Unique indexes that are **invariants rather than performance**:

```sql
-- One live payment INTENT per order. Not per attempt (a retry reuses the row
-- AND its idempotency key), not per order (a new card is a new intent).
-- UNKNOWN must block: it means "may or may not have been charged".
CREATE UNIQUE INDEX ON payments (order_id)
  WHERE status IN ('INITIATED','UNKNOWN','SUCCEEDED');

CREATE UNIQUE INDEX ON notifications (order_id, channel, kind);   -- send-once
```

## Why 202 on order creation

`POST /orders` returns **202 Accepted**. When it returns, the stock is reserved
and the order exists; the payment has *not* happened. 201 Created would imply a
completed resource and invite clients to treat the order as paid.

Clients learn the outcome either by polling `GET /orders/{id}/status` or by
subscribing to `GET /orders/{id}/events` (SSE) — see below.

## Concurrency inventory

| Mechanism | Where | Why that one |
|---|---|---|
| `errgroup` with `SetLimit` | consumers, entry points | Bounded pool; unbounded fan-out exhausts the DB pool the moment a backlog arrives |
| `sync.Mutex` | simulated provider's charge map | Protecting shared state is a mutex's job. Building a channel rig around a map is the classic misuse |
| Channels | delivery streams, shutdown | Moving work between goroutines |
| `FOR UPDATE SKIP LOCKED` | outbox, reaper, notifications | The clause that makes a table usable as a work queue: workers step over locked rows instead of queueing behind one |
| Conditional `UPDATE` + rowcount | every state change | No read-write gap |
| `context.Context` threaded everywhere | all I/O | Cancellation and timeouts propagate; shutdown drains rather than severs |
| Jittered retry on `40P01` | `Store.InTx` | Deadlock is prevented by product_id ordering; this covers the residue. Un-jittered retries deadlock again in lockstep |

**Deadlock prevention (failure mode G):** line items are merged and **sorted by
`product_id`** before touching inventory. A total order on resources makes a wait
cycle impossible. Every path that settles reservations reads them back in the
same order.

## API documentation

Swagger UI is at **`http://localhost:8080/swagger/index.html`** (or `/docs`),
served in development only — in production it publishes a complete map of every
route, field and validation rule to anyone who asks. The raw spec lives at
`docs/swagger/swagger.json`.

Regenerate after changing any annotation:

```bash
go run github.com/swaggo/swag/cmd/swag@latest init -g cmd/api/main.go -o docs/swagger --parseDependency --parseInternal
```

## Load testing

`cmd/loadtest` drives concurrent orders and reports latency percentiles,
throughput and the outcome mix.

```bash
go run ./cmd/loadtest -n 1000 -c 200 -product 2 -settle 10s
go run ./cmd/loadtest -n 500 -c 500 -product 5 -mode burst   # oversell check
```

Through the containerised toolchain, target the service name — `scripts/go.ps1`
joins the compose network:

```bash
.\scripts\go.ps1 run ./cmd/loadtest -url http://api:8080 -n 1000 -c 200 -product 2
```

**Raise `RATE_LIMIT_RPS` before measuring**, or the token bucket becomes the
bottleneck and every number describes the limiter rather than the pipeline.

`-mode burst` blocks every goroutine on one channel and releases them together.
That distinction matters: requests trickled out never collide, so a load test
without a barrier proves nothing about contention.

The point is not a single throughput number — it is **varying one thing at a
time** and seeing what moves.

### Measured: the pool is the bottleneck

1000 orders, 200 concurrent, everything else held constant. The only change
between runs is `DB_MAX_OPEN_CONNS`:

| | pool 12 | pool 40 | change |
|---|---|---|---|
| throughput | 39 req/s | **147 req/s** | **3.8×** |
| wall clock | 25.8s | 6.8s | 3.8× faster |
| p50 | 3.304s | **1.061s** | 3.1× |
| p95 | 14.064s | 2.999s | 4.7× |
| p99 | 20.157s | **4.710s** | 4.3× |
| accepted | 1000/1000 | 1000/1000 | — |

Both runs accepted every order with zero transport errors, so this is pure
queueing, not failure. At 200 concurrent requests against 12 connections, ~188 of
them are waiting on the pool rather than on Postgres — which is what a p50 of 3.3s
against a p50 of 1.0s is measuring.

**Do not read this as "bigger pools are better."** It says the pool was *this*
system's constraint at *this* concurrency. Past the point where connections
exceed what the database's cores can serve, added connections contend rather than
help and throughput falls — which is why the default stays conservative and why
`replicas × poolSize` is the number that matters on Kubernetes.

Settlement after the pool-12 run: 48 `PAID`, 2 `FAILED` from a sample of 50 —
the simulated provider's decline rate showing up end to end.

Still worth running, same method:

| Change | Expected | What it would prove |
|---|---|---|
| `PAYMENT_LATENCY` 200ms → 2s | ~none on intake | payment is off the request path — what reserve→pay→commit buys |
| `--scale worker=1 → 4` | moves settlement only | intake and processing are genuinely decoupled |

`-settle` re-reads a sample of accepted orders after the pipeline drains,
because **202 throughput and end-to-end completion are different numbers** and
reporting the first as if it were the second is how an async system gets claimed
as faster than it is.

## Benchmarks

`README.md:247` asks for performance benchmarks for concurrent operations. The
load test above is the outside view — what a client sees through HTTP, and it
cannot say where the time went. `tests/bench_test.go` is the inside view: the
statements the design rests on, measured directly against real Postgres with the
goroutines genuinely colliding.

```bash
go test ./tests -bench . -run NONE -benchmem
```

Most are paired, and the pair is the point:

```
contended     every goroutine hits the SAME inventory row
uncontended   every goroutine gets its OWN row
```

Same code, same transaction, same pool. The only variable is whether the workers
collide, so the ratio between the arms is the price of contention on the hot
path — which is the number reserve → pay → commit was decided on. No single
figure can show it, which is why nothing here reports one.

### Measured

8 goroutines against a pool of 25, Postgres 16, `-benchtime 2s -count 3`, median
of the three:

| | ms/op | ops/s | allocs/op |
|---|---|---|---|
| `ReserveInventory/uncontended` | 1.23 | 814 | 29 |
| `ReserveInventory/contended` | 5.13 | 195 | 29 |
| `OrderIntake/uncontended` | 1.57 | 636 | 512 |
| `OrderIntake/contended` | 7.63 | 131 | 507 |
| `OrderIntakeMultiLine` — two contended rows | 12.72 | 79 | 593 |
| `TransitionLostRace` | **0.037** | **27,335** | 40 |

**Contention costs 4–5×, and the intake pays more than the statement does.**
Reserving alone degrades 4.2× when every worker wants the same row; the full
intake transaction degrades 4.9×. The gap between those two figures is the
interesting part: the reserving `UPDATE` takes the row lock partway through the
transaction, and Postgres holds it until `COMMIT`. Everything the transaction
does *after* reserving — the items, the reservations, the outbox row, the commit
itself — is time the next buyer of that product spends queued. That is the whole
argument for keeping the payment call outside the transaction stated as a
measurement: a 2.4s provider call inside this transaction would extend a ~7.6ms
lock hold by roughly three orders of magnitude.

**The losing CAS is ~43× cheaper than the intake it deduplicates** — 37µs against
1.57ms. A duplicate delivery costs about 2% of the work of the order it would
have duplicated, because the `WHERE status = from` clause matches no row, so the
statement writes no tuple and forces no commit. Dedupe by CAS rather than by
read-then-decide is usually argued for on correctness; this is the throughput
half of the same argument.

**Allocations separate the two layers cleanly.** 29 allocs for the bare
statement against ~510 for the intake transaction, and contention moves neither —
which is the expected shape. Contention is spent waiting, not allocating, so an
allocation count that moved with it would mean something was retrying.

> **Absolute figures are host-bound; the ratios travel.** These were taken on
> Docker Desktop for Windows, where Postgres commit `fsync` dominates every write
> path and adds noise besides — `OrderIntakeMultiLine` spread 9.5–39ms across
> three runs. The paired arms are measured minutes apart under identical
> conditions, so the ratio between them is far more trustworthy than either
> number alone. Re-run with `-count 5` and read the median.

Two knobs are exposed for varying one thing at a time, which is the same method
the load test uses: `-cpu` sets the number of parallel goroutines,
`BENCH_POOL_CONNS` sets the pool they share. Driving the second while holding the
first is the `DB_MAX_OPEN_CONNS` experiment above, run from the inside — the
table above is the end-to-end version of it, and the figures in this section were
all taken at the defaults.

```bash
go test ./tests -bench . -run NONE -cpu 1,8,32
BENCH_POOL_CONNS=12 go test ./tests -bench Intake -run NONE -cpu 32
```

> Starve the pool hard enough and the bottleneck stops being the code under
> measurement. `-cpu 32` against `BENCH_POOL_CONNS=8` saturated Docker Desktop on
> this machine badly enough to wedge the daemon; on a host where Postgres is not
> behind a VM, it is an ordinary run.

## Daily sales report, and the rollup

`GET /admin/reports/daily` is served from **two sources**, and each row says
which one it came from:

| Source | Days | Why |
|---|---|---|
| `rollup` | closed days | Yesterday's total can never change — those orders are terminal. Compute it once, store it. |
| `live` | today | Still moving. Materialising it would be a cache, and caches need invalidation. |

`daily_sales_rollup` was **not** required by the spec — `README.md:54` mandates
eight entities and this is not one of them. It comes from `DESIGN_NOTES.md`
§5.17, where the argument is that the immutable half of a report wants
materialising rather than caching. It is one of three tables the design adds
beyond the brief, alongside `reservations` and `outbox`.

Three properties make it safe to run on a timer:

- **Idempotent.** `ON CONFLICT DO UPDATE` overwrites, so re-running a day
  produces the same row. The scheduler re-runs days after a restart, and a
  rollup that accumulated on replay would silently double revenue.
- **Resumes.** It starts from the last materialised day, so a worker down for a
  week catches up in a few statements instead of recomputing all history.
- **Degrades to slow, never to wrong.** Any closed day the job has not reached
  falls through to live aggregation, so a lagging rollup costs time, not
  accuracy.

Only `PAID` and `FULFILLED` count as revenue. Counting a `PENDING` or `EXPIRED`
order would overstate takings, which is the most consequential kind of bug a
sales report can have — and it is the first thing the tests check.

Like the reaper, the rollup is a **timer, not a consumer**: nothing happens at
midnight to announce that a day ended, so only a clock can notice.

## Real-time order status (SSE)

```
GET /api/v1/orders/{id}/events     # text/event-stream
```

SSE rather than WebSocket: the client never sends anything on this channel, so a
bidirectional protocol solves a problem that does not exist. SSE is plain HTTP —
it survives proxies, needs no upgrade handshake, and browsers reconnect on their
own.

**How an event reaches a browser.** The relay publishes to the `orders` exchange
as usual. Each API instance declares its own **exclusive, auto-delete** queue
`sse.<instance-id>` bound to `order.#`, so every instance receives every event
and pushes to whichever connections it happens to be holding. No sticky
sessions, no shared state between replicas, no coordination.

That the per-instance queues do **not** compete is the whole trick. `payments`
has N consumers precisely so each message is handled *once*; here every instance
must receive *every* event, because any of them might hold the connection that
cares. Same exchange, same messages — different topology.

**Two details that are easy to get wrong:**

- **Subscribe before reading current state.** The handler registers with the hub
  *first*, then reads the order and emits it. Reading first leaves a window in
  which a transition fires with nobody listening, and the client sits on a stale
  status forever. Subscribing first can only produce a duplicate.
- **Publishing never blocks.** The hub sends non-blocking into buffered channels
  and drops on overflow, because `Publish` runs on the backplane consumer's
  goroutine — one stalled browser must not stop delivery for everyone else on
  the instance. Dropping is safe because events are doorbells: the handler
  re-reads authoritative state from Postgres on every one it does receive.

Streams close when the order reaches a terminal state, rather than holding a
connection open for an event that can never arrive.

**Inventory is still not pushed** — that half of `README.md:110-114` is
deliberately not built, and the reasoning is in "What is not built".

## Testing

With Go installed locally:

```bash
go test ./... -race        # testcontainers starts a throwaway Postgres
```

Through the containerised toolchain, point the tests at a database rather than
letting testcontainers start one — `scripts/go.ps1` does not mount the Docker
socket, so the container cannot launch sibling containers:

```powershell
docker compose exec postgres createdb -U postgres orders_test
$env:TEST_DATABASE_URL = "postgres://postgres:postgres@postgres:5432/orders_test?sslmode=disable"
.\scripts\go.ps1 test ./... -count=1
.\scripts\go.ps1 test ./internal/services/... -race    # hub concurrency, no DB needed
```

Use a **separate database**: the suite truncates every table between tests, so
pointing it at `orders` would wipe the running stack.

Integration tests run the real migration files, not `AutoMigrate`, because the
CHECK constraints and partial unique indexes *are* the invariants under test.

Current state: **47 passing** — 12 payment saga (failure modes C, D, E),
7 oversell/concurrency, 7 reaper, 7 rollup, 4 SSE, 3 backplane, plus 7 hub unit
tests (the hub set clean under `-race`).

The saga tests cover what happens when things die. A cancel racing a live charge
is a timing bug, so the fake provider has a **gate** that holds the charge
mid-flight — that makes the race deterministic instead of hoping the scheduler
interleaves two goroutines helpfully. It also counts *distinct charges*
separately from *calls*, which is what lets a test assert "the card was charged
exactly once" rather than merely "the code ran once".

### The tests were checked for teeth

A concurrency test that passes proves nothing until you have watched it fail.
`Reserve` was temporarily reverted to the naive read-then-write —
`SELECT available`, check it in Go, then `UPDATE` — and the suite was re-run:

```
--- FAIL: TestConcurrentBuyersCannotExceedStock
    unexpected error: reserving 1 of product 1: ERROR: new row for relation
    "inventory" violates check constraint "inventory_available_check" (23514)
    ... × 24
    expected: 53   actual: 29
```

Twenty-four buyers passed the in-Go check simultaneously, exactly as the design
predicts. It also demonstrates the second guard doing its job: `available` never
went negative, because `CHECK (available >= 0)` refused the writes the
application logic wrongly allowed. The `WHERE` clause turns an oversell into a
clean 409; the constraint is what makes corruption impossible even when the code
above it is wrong.

The mutation was reverted and the suite is green again.

**Concurrency tests use a barrier**: every goroutine blocks on one channel and is
released together. Started in a plain loop they never collide, and the test
passes against code that oversells freely.

**Nothing mocks the database, on purpose.** A mock returns what you told it to,
so it is structurally incapable of exhibiting a race — it cannot lose an update,
deadlock, or enforce a constraint. Every interesting bug in this system is a
database race, so a mock-based suite would pass while the system oversold stock.

## What is not built

Stated plainly, because silence reads as unfinished.

- **Regression test for the broker reconnect.** The wedged-relay bug below is
  fixed and verified by hand (`docker compose restart rabbitmq`, watching both
  services redial), but nothing in the suite would catch a regression. It needs a
  broker whose connection can be severed on demand — toxiproxy, or a
  testcontainers RabbitMQ that can be stopped mid-session — then asserts the
  relay redials, re-declares topology and drains the backlog. This is the most
  valuable remaining test.
- **Real-time inventory push** — order status streams; stock levels do not.
  Broadcasting every decrement to every browsing customer is a firehose that
  serves almost nobody, and the number is stale the instant it is rendered
  anyway. The conditional `UPDATE` at order time is what actually decides who
  gets the last unit, which is why the API never invites "check stock, then
  order" as two steps.
- **Bonus items** — Prometheus, tracing, Kubernetes manifests, WebSocket.
- **No WebSockets, and no push for inventory.** Deliberate: a bidirectional
  protocol solves a problem order status does not have. This forfeits a bonus
  tick; the justification is worth more.

## Two bugs the failure-mode tests found

Writing tests for node death turned up two defects that reading the code did not.

**A worker dying mid-charge stranded the order forever.** `ProcessOrder` skips
any order that is not `PENDING`, so a process SIGKILLed between the
`PENDING → CHARGING` transition and settlement left an order in `CHARGING` with
an `INITIATED` intent that nothing could recover: redelivery skipped it (not
`PENDING`), the reaper skipped it (only expires `PENDING`, and rightly so — a
live charge may be in flight), and reconciliation skipped it (only looks at
`UNKNOWN`). The order held its stock forever while the customer may already have
been charged. The code comment even claimed "the retry finds this row" — but no
retry ever came.

Fixed with `RecoverStuckCharges`, a timer that re-drives abandoned intents **with
the same idempotency key**, so the provider either replays the original result or
performs the charge for the first time — never twice. That is precisely why the
key lives on the `payments` row rather than being generated per call. The grace
period must exceed the provider timeout, or the fix for E would reintroduce C by
"recovering" a payment that is merely slow.

**Reconciliation silently abandoned half its work.** `resolveUnknown` moved a
payment out of `UNKNOWN`, then called `settleDecline`, which unconditionally
CASed `INITIATED → DECLINED`. That CAS found the row already `DECLINED`, returned
false, and returned early — so the *order* half never ran and the order stayed
in `CHARGING` with its stock held. The success path worked only because
`settleSuccess` happened to guard its CAS. Both paths now guard symmetrically.

Both were caught by a test asserting the end state, not the call sequence —
which is the argument for testing what the system *is* rather than what it *did*.

## A bug that only running it would find

Worth reading, because it is the one defect here that no unit test would have
caught and every design review would have missed.

An AMQP `Connection` and its `Channel`s were acquired **once at startup**.
amqp091-go does not reconnect. When RabbitMQ dropped the connection under load,
every subsequent publish returned:

```
Exception (504) Reason: "channel/connection is not open"
```

...and kept returning it. The relay stayed alive, its process-level health check
stayed green, and it never published another event — 241 identical failures in a
row before it was noticed. The outbox grew without bound and **the entire async
pipeline silently stopped**: no payments, no notifications, no order ever leaving
`PENDING`.

The failure mode is nastier than a crash. A crashed relay restarts and recovers;
a wedged one looks healthy forever.

The fix is conceptual rather than mechanical: **a broker connection is a session
that ends, not a resource acquired once.** `workers.Supervise` owns the lifecycle
— it dials, runs the session, and redials with capped jittered backoff when the
session ends. Everything the session owns (channels, publishers, consumers, queue
declarations) is rebuilt on reconnect, because a Channel belongs to a Connection
and cannot outlive it. A `NotifyClose` watcher turns a silently dead connection
into a session that ends, which matters most for consumers: one parked on a
delivery channel that will never deliver again is indistinguishable from an idle
one.

Nothing is lost across a reconnect, and that is the outbox earning its keep:
unpublished rows still have `sent_at IS NULL`, so the next session claims exactly
the same batch. Unacked deliveries are redelivered by the broker.

Two details worth defending: the backoff is **jittered**, or every service
reconnects in lockstep the moment the broker returns and knocks it straight over
again; and outbox lag reporting runs on the **database** connection, not the
broker session, so it keeps reporting while the broker is down — precisely when a
growing backlog matters most. A rising `oldest_age_seconds` is what this bug
looks like from the outside, and it is the thing to alert on.

## Operational notes

- **Pod churn is already survivable**, and not by luck. Kill a worker mid-charge:
  the delivery is unacked and redelivered, and the idempotency key means the
  provider charges once. Kill the relay mid-publish: the outbox row is still
  unsent. Kill an API pod: clients re-read current state. A rolling deploy is
  exactly the failure this design was built for.
- **`stop_grace_period` must exceed the longest in-flight job**, or a rolling
  deploy SIGKILLs a worker mid-payment.
- **Scaling the API multiplies the connection pool.** The constraint is
  `replicas × poolSize < max_connections − headroom`; the real fix is PgBouncer.
  This is the most common way a working app falls over on Kubernetes.
- The rate limiter **fails open**. If Redis is unreachable, rejecting every
  request would turn a cache outage into a full outage.
