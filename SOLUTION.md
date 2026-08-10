# Concurrent Order Processing System — Solution

> `README.md` is the assignment brief exactly as issued and is left untouched.
> This file is the submission: how to run it, what was built, and which
> decisions were deliberate. The test suite has its own reference in
> [`docs/TESTING.md`](docs/TESTING.md).

## Run it

```bash
docker compose up --build
```

That starts Postgres, RabbitMQ and Redis, runs migrations, seeds demo data, and
brings up the three application processes. Startup ordering uses healthchecks
rather than `depends_on` alone, so a clean clone works with no manual steps.

| | |
|---|---|
| API | `http://localhost:8080` |
| Swagger UI | `http://localhost:8080/swagger/index.html` (or `/docs`) |
| RabbitMQ management | `http://localhost:15672` — guest / guest |
| Health | `GET /healthz` (liveness), `GET /readyz` (readiness) |

Seeded accounts:

| Email | Password | Role |
|---|---|---|
| `admin@example.com` | `admin12345` | ADMIN |
| `sarah@example.com` | `customer123` | CUSTOMER |

A five-second smoke test:

```bash
curl -s localhost:8080/api/v1/auth/login -H 'content-type: application/json' -d '{"email":"sarah@example.com","password":"customer123"}'
```

Then place an order with the returned token:

```bash
curl -s localhost:8080/api/v1/orders -H "authorization: Bearer $TOKEN" -H 'content-type: application/json' -d '{"items":[{"product_id":2,"qty":1}]}'
```

It returns **202**, not 201 — see [Why 202](#why-202-on-order-creation).

Useful while watching it work:

```bash
docker compose logs -f worker
```

```bash
docker compose exec postgres psql -U postgres -d orders
```

```bash
docker compose up --scale worker=4
```

### Without Go installed

Go is not required on the host if you have Docker: `scripts/go.ps1` runs the
toolchain in a container and takes the same arguments as `go`.

```powershell
.\scripts\go.ps1 build ./...
```

With Go installed locally, use `go` directly and the wrapper is unnecessary.

## What is implemented

Everything the brief lists as mandatory, plus two of the bonus items.

**Entities** — all eight required (`README.md:54`), and three the design adds:
`reservations` (stock held for an unpaid order), `outbox` (transactional event
log), `daily_sales_rollup` (materialised closed days).

**Endpoints** — every route in `README.md:65-94`, plus two additions:

| Added | Why |
|---|---|
| `POST /api/v1/auth/login` | JWT is mandatory and the spec defines no way to obtain one |
| `GET /api/v1/orders/{id}/events` | SSE stream for live order status — `README.md:113` |

**Cross-cutting** — request-id and structured `log/slog` logging, JWT auth with
role checks, Redis token-bucket rate limiting, panic recovery, input validation,
Swagger/OpenAPI, golang-migrate migrations, connection pooling, graceful
shutdown on SIGTERM.

**Bonus taken:** RabbitMQ, and load testing (`cmd/loadtest`). **Bonus not
taken:** Prometheus, tracing, Kubernetes manifests, WebSocket — see
[What is not built](#what-is-not-built).

**One deliberate deviation from the brief.** `README.md:30` says "Database
migrations using GORM". Migrations run through **golang-migrate** instead, and
`AutoMigrate` runs nowhere outside tests. The invariants in this system are
constraints — `CHECK (available >= 0)`, the partial unique index on `payments`,
the send-once unique on `notifications` — and `AutoMigrate` expresses none of
them, never drops anything, and has no down path. GORM is still the ORM
everywhere else. The tests run the real migration files for the same reason:
those constraints *are* what is under test.

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

Three processes, one binary image. They scale independently — API replicas
track request load, workers track queue depth — while a single artifact keeps
deployment to one thing to build and promote.

| Process | Runs |
|---|---|
| `cmd/api` | HTTP, plus an SSE backplane consumer for its own connections |
| `cmd/worker` | Four queue consumers (payments, email, SMS, refunds) and four timers |
| `cmd/relay` | Drains the outbox to RabbitMQ |

Plus `cmd/migrate`, `cmd/seed` and `cmd/loadtest`.

The worker's two kinds of work are genuinely different. **Consumers react to
events**; **timers react to the passage of time**, which is the only way to
notice that something did *not* happen:

| Timer | Interval | Reclaims |
|---|---|---|
| Reservation reaper | 30s | Stock held by abandoned checkouts |
| Notification lease sweep | 60s | Sends abandoned by a dead worker |
| Stuck-charge recovery + unknown reconciliation | 1m | Payments abandoned mid-flight |
| Sales rollup | 1h (and once at startup) | Closed days, materialised |

**Order intake is one transaction:** reserve stock with a conditional `UPDATE`,
insert the order, items and reservations, insert the outbox event. Commit.
Return 202. Payment happens afterwards in a worker with **no transaction open
and no lock held**.

## The decisions that carry the system

### 1. The conditional UPDATE

```sql
UPDATE inventory SET available = available - $1, reserved = reserved + $1
 WHERE product_id = $2 AND available >= $1
```

The check is in the `WHERE` clause, so the read and the write are one statement
and cannot interleave. The naive version — `SELECT` the stock, decide in Go,
`UPDATE` — has a gap in which two transactions both see stock 1, both decide
yes, and the last unit sells twice.

`RowsAffected == 0` means insufficient stock. **That is a business outcome, not
an error**: it maps to 409 Conflict, not 500. A second, independent guard —
`CHECK (available >= 0)` in migration 001 — means the database enforces the
invariant even if the query above were ever wrong.

### 2. Reserve → pay → commit

A payment provider call takes seconds. Holding a database transaction across it
would pin a pool connection and hold row locks on the product, serialising every
other buyer behind one slow card. So stock is *reserved* in the intake
transaction, the transaction commits, and payment runs later with nothing
locked.

The cost is honest: reserved stock is unavailable to other customers while an
order is unpaid, so an abandoned checkout holds inventory hostage — which is why
the reaper exists.

### 3. The outbox

Writing the order to Postgres and then publishing to RabbitMQ is a write to two
systems with no shared transaction. Crash in between and the order exists but
nothing will ever process it. Publish first instead and you may announce an
order that never committed.

The event row is written **in the same transaction as the order**, so
"committed but never queued" is unrepresentable. `cmd/relay` publishes it
afterwards, waits for a **publisher confirm**, and only then marks `sent_at`. A
plain publish is a socket write, not a delivery guarantee.

This makes delivery **at-least-once**, so every consumer dedupes. Losing events
instead would be far worse than occasionally repeating one.

### 4. Cancel versus charge

The subtlest part of the system. A cancel arriving while a payment is in flight
cannot simply cancel the order — the charge may be about to succeed.

`PENDING` → cancel outright, release stock.
`CHARGING` → move to **`CANCELLING`**, release nothing, and answer **202**, not
200. The payment worker finishes its call, finds the order in `CANCELLING`
rather than `CHARGING`, and settles it: `CANCELLED_REFUNDED` plus a refund event
if the charge landed, plain `CANCELLED` if it was declined.

A saga cannot roll back a committed step in another system, so the compensation
is a refund — a second forward action, not an undo. The refund itself is a
consumer, so the compensation completes asynchronously and retries like any
other job.

### 5. Every state change is a CAS

Nothing writes `orders.status` directly. `Transition(ctx, tx, id, from, to)`
checks the edge against the state machine, then does the update with the
expected state in the `WHERE`, and returns **whether this caller performed it**.

That bool is the entire mechanism that makes at-least-once delivery survivable:
two workers handling a duplicate event both attempt `PENDING → CHARGING` and
exactly one wins. `RowsAffected == 0` means *you lost a race*, which is a
branch, not an error to retry.

### 6. Idempotency keys live on the `payments` row

The key is committed **before** the provider call and never changes across
retries of the same intent. A fresh key per attempt would mean a charge per
attempt.

A partial unique index enforces **one live intent per order** — not per attempt,
since a retry reuses the row *and* its key, and not per order, since a new card
is legitimately a new intent:

```sql
CREATE UNIQUE INDEX ON payments (order_id)
  WHERE status IN ('INITIATED','UNKNOWN','SUCCEEDED');
```

`UNKNOWN` blocks precisely because it means "may or may not have been charged".
Collapsing it into `DECLINED` refuses an order you already took money for;
collapsing it into `SUCCEEDED` ships goods you were never paid for. A
reconciliation timer asks the provider and resolves it.

### 7. Recovery is a timer, because nothing else can see the gap

A worker killed between the `PENDING → CHARGING` transition and settlement
leaves an order in `CHARGING` with an `INITIATED` intent, holding stock, and
possibly a charged customer. Nothing else in the system can act on it:
redelivery skips it (not `PENDING`), the reaper skips it (only expires
`PENDING`, and rightly so — a live charge may be in flight), and reconciliation
skips it (only looks at `UNKNOWN`).

`RecoverStuckCharges` re-drives those intents **with the same idempotency key**,
so the provider either replays the original result or performs the charge for
the first time — never twice. The grace period is `4 × PAYMENT_TIMEOUT`, and it
must exceed the provider timeout, or "recovery" starts racing charges that are
merely slow.

### 8. A broker connection is a session that ends, not a resource acquired once

An AMQP `Channel` belongs to a `Connection` and cannot outlive it, and
amqp091-go does not reconnect. A connection acquired once at startup therefore
fails *permanently* the first time the broker drops it, and fails quietly: the
process stays alive, its health check stays green, and every publish returns
`Exception (504) channel/connection is not open` forever. A crashed relay
restarts and recovers; a wedged one looks healthy indefinitely.

`workers.Supervise` owns the lifecycle instead. It dials, runs the session, and
redials with capped **jittered** backoff when the session ends — jittered
because otherwise every service reconnects in lockstep the moment the broker
returns and knocks it straight over again. Everything the session owns
(channels, publishers, consumers, queue declarations) is rebuilt on reconnect. A
`NotifyClose` watcher turns a silently dead connection into a session that ends,
which matters most for consumers: one parked on a delivery channel that will
never deliver again is indistinguishable from an idle one.

Nothing is lost across a reconnect, and that is the outbox earning its keep:
unpublished rows still have `sent_at IS NULL`, so the next session claims
exactly the same batch, and unacked deliveries are redelivered by the broker.

Outbox lag reporting deliberately runs on the **database** connection rather
than the broker session, so it keeps reporting while the broker is down —
precisely when a growing backlog matters most. A rising `oldest_age_seconds` is
the thing to alert on.

## Deliberate decisions where the spec is open

Each of these was decided rather than guessed at.

| Gap | Decision |
|---|---|
| **Partial fulfilment** — 3 items, 1 out of stock | **Reject the whole order.** Matches normal checkout, and it is free: one transaction rolling back releases every reservation already taken in it. Partial fulfilment would mean splitting orders, partial payments and partial refunds — a materially larger design. |
| **No auth endpoints exist** in the spec, yet JWT is mandatory | Added `POST /api/v1/auth/login`. |
| **Report timezone** — "daily" in which zone? | **UTC**, stated in the response body. A report that silently uses server-local time is wrong twice a year. |
| **`PUT /orders/{id}/cancel`** is not RESTful | Implemented exactly as specified (`README.md:86`). Noting that it was noticed: a cancellation is a resource, so `POST /orders/{id}/cancellation` would be better. |
| **Job state visibility** | RabbitMQ is transport, not storage — once a message is acked the broker cannot say what became of it. `GET /orders/{id}/status` reads payment attempt history from Postgres. |
| **Admin status updates** | Admins do not get a free hand with `orders.status`: the same state machine applies, and `PAID` is refused outright, because marking an order paid by hand means money that was never taken. |
| **Historical order totals** | `order_items.unit_price_cents` is a snapshot, not a join to the current price. A price change must not rewrite what a customer was charged last month. |
| **Real-time inventory** | Not pushed — see [What is not built](#what-is-not-built). |

## Why 202 on order creation

`POST /orders` returns **202 Accepted**. When it returns, the stock is reserved
and the order exists; the payment has *not* happened. 201 Created would imply a
completed resource and invite clients to treat the order as paid.

Clients learn the outcome either by polling `GET /orders/{id}/status` or by
subscribing to `GET /orders/{id}/events`.

## Indexing strategy

Three ideas, all in `migrations/000001_init.up.sql`:

- **Foreign keys are indexed explicitly.** Postgres does not do this for you,
  and it is the most common miss — joins and cascades seq-scan without them.
- **Partial indexes for queue-shaped tables.** `outbox_unsent` indexes only rows
  `WHERE sent_at IS NULL`. After a year the table holds tens of millions of rows
  and the index holds ~50: *its size tracks the backlog, not the table*. Same
  for expiring reservations and claimable notifications.
- **Composites are equality-then-sort**, e.g. `(user_id, created_at DESC)` — not
  "most selective first", which is a rule about competing equality columns and
  says nothing about a sort column.

**`inventory.available` is deliberately left unindexed.** It is the most-updated
column in the system; indexing it taxes every order — and blocks heap-only-tuple
updates — to accelerate an occasional admin query over ~10k rows. Take the seq
scan, protect the write path.

Two unique indexes are **invariants rather than performance**: the partial
unique on `payments` above, and send-once on notifications:

```sql
CREATE UNIQUE INDEX ON notifications (order_id, channel, kind);
```

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
| Redis Lua token bucket | rate limiter | `GET`/compute/`SET` from N replicas interleaves and leaks the limit — the same shape as `SELECT`-then-`UPDATE` |

**Deadlock prevention:** line items are merged and **sorted by `product_id`**
before touching inventory. A total order on resources makes a wait cycle
impossible. Every path that settles reservations reads them back in the same
order.

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

The route is authenticated but deliberately **outside** the rate limiter. A
token bucket charges one token per request, and an SSE connection is one request
held open for minutes, so a reconnecting client would be throttled for behaving
correctly. Long-lived connections want a concurrent-connection cap, which is a
different mechanism.

## Daily sales report, and the rollup

`GET /admin/reports/daily` is served from **two sources**, and each row says
which one it came from:

| Source | Days | Why |
|---|---|---|
| `rollup` | closed days | Yesterday's total can never change — those orders are terminal. Compute it once, store it. |
| `live` | today | Still moving. Materialising it would be a cache, and caches need invalidation. |

`daily_sales_rollup` is **not** one of the eight entities the brief mandates
(`README.md:54`). It is one of three tables the design adds, on the argument
that the immutable half of a report wants materialising rather than caching.

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
sales report can have.

Like the reaper, the rollup is a **timer, not a consumer**: nothing happens at
midnight to announce that a day ended, so only a clock can notice.

## Configuration

Every value in `.env.example` has a working default in `internal/config`, so an
empty `.env` boots in development. The service **refuses to boot** without
`JWT_SECRET` in production rather than falling back to a known value.

The ones that change behaviour under load:

| Variable | Default | Note |
|---|---|---|
| `DB_MAX_OPEN_CONNS` | 12 | The most load-bearing number here — see the load test below. Per process, so replicas multiply it |
| `RATE_LIMIT_RPS` / `_BURST` | 50 / 100 | Raise before benchmarking, or you measure the limiter |
| `WORKER_CONCURRENCY` | 10 | Bound on the consumer pool |
| `RABBITMQ_PREFETCH` | 16 | Without it one consumer grabs the queue and its peers idle |
| `RESERVATION_TTL` | 15m | Too short fails slow payments; too long starves buyers |
| `PAYMENT_FAILURE_RATE` / `_TIMEOUT_RATE` | 0.10 / 0.02 | Deliberately non-zero so declines and the `UNKNOWN` path are reachable in a demo |
| `WORKER_SHUTDOWN_TIMEOUT` | 30s | Must exceed the longest in-flight job |

## API documentation

Swagger UI at **`http://localhost:8080/swagger/index.html`** (or `/docs`),
served in **development only** — in production it publishes a complete map of
every route, field and validation rule to anyone who asks. The raw spec lives at
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
```

```bash
go run ./cmd/loadtest -n 500 -c 500 -product 5 -mode burst
```

`-mode burst` blocks every goroutine on one channel and releases them together.
That distinction matters: requests trickled out never collide, so a load test
without a barrier proves nothing about contention.

**Raise `RATE_LIMIT_RPS` before measuring**, or the token bucket becomes the
bottleneck and every number describes the limiter rather than the pipeline.

`-settle` re-reads a sample of accepted orders after the pipeline drains,
because **202 throughput and end-to-end completion are different numbers**, and
reporting the first as if it were the second is how an async system gets claimed
as faster than it is.

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
queueing, not failure. At 200 concurrent requests against 12 connections, ~188
of them are waiting on the pool rather than on Postgres — which is what a p50 of
3.3s against a p50 of 1.0s is measuring. Settlement after the pool-12 run: 48
`PAID`, 2 `FAILED` from a sample of 50, the simulated provider's decline rate
showing up end to end.

**Do not read this as "bigger pools are better."** It says the pool was *this*
system's constraint at *this* concurrency. Past the point where connections
exceed what the database's cores can serve, added connections contend rather
than help and throughput falls — which is why the default stays conservative and
why `replicas × poolSize` is the number that matters on Kubernetes.

The point is not a single throughput number — it is **varying one thing at a
time** and seeing what moves.

## Testing

**177 tests, 83.9% statement coverage** across `internal/` and `pkg/`, past the
>80% the brief asks for at `README.md:164` and above 80% in every package
individually. [`docs/TESTING.md`](docs/TESTING.md) is the full reference: what
each test asserts and why it is written the way it is.

With Go installed locally:

```bash
go test ./... -race
```

testcontainers starts a throwaway Postgres. Through the containerised toolchain,
point the tests at a database instead — `scripts/go.ps1` does not mount the
Docker socket, so it cannot launch sibling containers:

```powershell
docker compose exec postgres createdb -U postgres orders_test
```

```powershell
$env:TEST_DATABASE_URL = "postgres://postgres:postgres@postgres:5432/orders_test?sslmode=disable"
```

```powershell
.\scripts\go.ps1 test ./... -count 1
```

Use a **separate database**: the suite truncates every table between tests, so
pointing it at `orders` would wipe the running stack.

Three things worth knowing about the approach:

- **Nothing mocks the database, on purpose.** A mock returns what you told it
  to, so it is structurally incapable of exhibiting a race — it cannot lose an
  update, deadlock, or enforce a constraint. Every interesting bug in this
  system is a database race. Ports we do not own (the payment provider, the
  notifier) *are* substituted; the data layer is not.
- **Integration tests run the real migration files**, not `AutoMigrate`, because
  the CHECK constraints and partial unique indexes *are* the invariants under
  test.
- **Concurrency tests use a barrier**: every goroutine blocks on one channel and
  is released together. Started in a plain loop they never collide, and the test
  passes against code that oversells freely.

`-race` finds Go memory races and is completely blind to database races, which
is what almost every bug here would be. A clean `-race` run says nothing about
whether stock can be oversold; only the concurrency tests against real Postgres
say that.

## What is not built

Stated plainly, because silence reads as unfinished.

- **Real-time inventory push** — order status streams; stock levels do not.
  Broadcasting every decrement to every browsing customer is a firehose that
  serves almost nobody, and the number is stale the instant it is rendered
  anyway. The conditional `UPDATE` at order time is what actually decides who
  gets the last unit, which is why the API never invites "check stock, then
  order" as two steps. This forfeits half of `README.md:110-114` deliberately.
- **WebSocket** — a bidirectional protocol solves a problem order status does
  not have. This forfeits a bonus tick; the justification is worth more.
- **Prometheus, distributed tracing, Kubernetes manifests** — the remaining
  bonus items.
- **A real `SIGKILL` test.** Process death is reproduced at the database level
  rather than by killing a subprocess, and connection loss is reproduced by
  force-closing the connection from the broker side. What neither shows directly
  is RabbitMQ redelivering an unacked message when a consumer process dies
  mid-handler; that half is argued from the code. `docs/TESTING.md` §14.2 has
  the shape it would take.

## Operational notes

- **Pod churn is already survivable**, and not by luck. Kill a worker
  mid-charge: the delivery is unacked and redelivered, and the idempotency key
  means the provider charges once. Kill the relay mid-publish: the outbox row is
  still unsent. Kill an API pod: clients re-read current state. A rolling deploy
  is exactly the failure this design was built for.
- **`stop_grace_period` must exceed the longest in-flight job**, or a rolling
  deploy SIGKILLs a worker mid-payment.
- **Scaling the API multiplies the connection pool.** The constraint is
  `replicas × poolSize < max_connections − headroom`; the real fix is PgBouncer.
  This is the most common way a working app falls over on Kubernetes.
- **The rate limiter fails open.** If Redis is unreachable, rejecting every
  request would turn a cache outage into a full outage.
- **Both scale horizontally without code changes.** Workers are competing
  consumers on shared queues; relays claim outbox rows with
  `FOR UPDATE SKIP LOCKED`, so two never publish the same event.
