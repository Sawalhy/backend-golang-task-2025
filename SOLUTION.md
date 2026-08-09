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
completed resource and invite clients to treat the order as paid. Clients poll
`GET /orders/{id}/status`.

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

## What is not built

Stated plainly, because silence reads as unfinished.

- **Concurrency test suite (Testcontainers)** — the highest-value remaining item.
  The design calls for N goroutines released simultaneously against real
  Postgres, asserting exactly one success on a single-unit product. Not yet
  written.
- **Swagger UI** — handlers carry `@Summary`/`@Router` annotations ready for
  `swag init`; the generated spec and served endpoint are not wired up.
- **SSE order status** — the transport decision is made (SSE over WebSocket: the
  client never sends, and `Last-Event-ID` reconnection comes free) but the
  endpoint and backplane are not implemented.
- **Load test and benchmarks** — the plan is to vary one thing at a time (pool
  size, lock hold time, process count) to demonstrate which is actually the
  bottleneck.
- **`daily_sales_rollup`** — the table exists; the report currently aggregates
  live rather than reading the rollup.
- **Bonus items** — Prometheus, tracing, Kubernetes manifests, WebSocket.
- **No WebSockets, and no push for inventory.** Deliberate: a bidirectional
  protocol solves a problem order status does not have. This forfeits a bonus
  tick; the justification is worth more.

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
