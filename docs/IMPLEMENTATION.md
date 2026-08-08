# Implementation Digest — Go

> Everything decided across the design sessions, translated to Go and compressed to what you need
> while writing code. `DESIGN_NOTES.md` holds the reasoning; this holds the conclusions.
>
> **Language: Go** (reversed from TypeScript on 2026-08-09). ~85% of the design was
> language-independent — invariants, CAS, outbox, saga, leases, lock ordering, messaging topology,
> token buckets are all Postgres and RabbitMQ problems.

## Contents

1. [Stack](#1-stack)
2. [Architecture](#2-architecture)
3. [The rules](#3-the-rules--for-claudemd)
4. [Go translations of the patterns](#4-go-translations-of-the-patterns)
5. [Schema sketch](#5-schema-sketch)
6. [Messaging topology](#6-messaging-topology)
7. [Package layout](#7-package-layout)
8. [Scope triage](#8-scope-triage)
9. [Still open](#9-still-open)

---

## 1. Stack

| Concern | Choice | Note |
|---|---|---|
| Language | Go 1.21+ | spec-mandated |
| HTTP | **Gin** | spec-recommended; Echo also allowed |
| ORM | **GORM v2** | spec-mandated, and graded explicitly |
| Migrations | **golang-migrate** | *not* `AutoMigrate` — it won't drop columns and is the weakest part of GORM. Keep `AutoMigrate` out of the prod path entirely |
| Driver | pgx v5 (via GORM's postgres driver) | |
| Broker | **RabbitMQ** + `rabbitmq/amqp091-go` | `README.md:259` bonus |
| Cache/limits | **Redis** + `redis/go-redis/v9` | rate limiting only — see §5.17 |
| Testing | `testify` + **`testcontainers-go`** | race tests are meaningless without real Postgres |
| Logging | `log/slog` | stdlib since 1.21, structured, no dependency |
| Config | `envconfig` or Viper | `.env.example` is a deliverable |
| Tracing | OpenTelemetry | bonus; see roadmap topic 9 |

## 2. Architecture

```
                         ┌──────────── one binary, three entry points ────────────┐
  client ──HTTP──► cmd/api      cmd/worker            cmd/relay
                     │              │                     │
                     │              │  claims outbox rows (SKIP LOCKED),
                     │              │  publishes, marks sent
                     ▼              ▼                     ▼
                  Postgres ◄────────┴──────────────► RabbitMQ ──► consumers
                     ▲                                   │
                     └────────── SSE backplane ◄─────────┘
```

**Order intake is one transaction:** reserve stock (conditional `UPDATE`), insert order + items,
insert outbox row. Commit. Return `202`. Payment happens in a worker with **no transaction open** and
**no lock held** — that is the reserve→pay→commit decision, worth ~500× throughput on a hot product.

**Scheduled work is not a consumer.** The reservation reaper is a periodic loop in `cmd/worker`.
A queue delivers *"something happened"*; the reaper's trigger is *"Sarah still hasn't paid"*, and
**the absence of an event is not an event.**

## 3. The rules — for CLAUDE.md

Each of these looks like arbitrary style until you know the failure it prevents.

1. **Never write `orders.status` directly.** Use `transition()` and check its result.
2. **Every state change is a CAS.** Check `RowsAffected`. **Rows affected 0 means you lost the race,
   not that the operation failed.**
3. **Sort line items by `product_id`** before touching `inventory`. Merge duplicate lines first.
4. **Never do network I/O inside a transaction.** Not Stripe, not the broker, not email.
5. **Idempotency keys come from the `payments` row** and never change across retries.
6. **Thread `context.Context` as the first parameter** through every function that does I/O.
7. **`defer rows.Close()`** immediately after any `Query` that returns `*sql.Rows`.
8. Relay uses a **confirm channel** and awaits the broker ack *before* marking `sent_at`.

## 4. Go translations of the patterns

**Conditional update + rowcount** — the single most-used guard in the codebase:

```go
res := tx.WithContext(ctx).Exec(`
    UPDATE inventory SET available = available - ?, reserved = reserved + ?
     WHERE product_id = ? AND available >= ?`, qty, qty, productID, qty)
if res.Error != nil { return res.Error }
if res.RowsAffected == 0 { return ErrInsufficientStock }   // 409, not a failure
```

**State transitions** — Go can't do TypeScript's compile-time `Next<S>`, so it's a runtime map plus a
test that no illegal edge is reachable:

```go
var orderTransitions = map[OrderStatus][]OrderStatus{
    StatusPending:    {StatusCharging, StatusCancelled, StatusExpired},
    StatusCharging:   {StatusPaid, StatusFailed, StatusCancelling},
    StatusCancelling: {StatusCancelled, StatusCancelledRefunded},
    StatusPaid:       {StatusFulfilled, StatusRefunded},
}

// Returns true if THIS caller performed the transition.
func transition(ctx context.Context, tx *gorm.DB, id uint64, from, to OrderStatus) (bool, error) {
    if !slices.Contains(orderTransitions[from], to) {
        return false, fmt.Errorf("illegal transition %s → %s", from, to)
    }
    res := tx.WithContext(ctx).Exec(
        `UPDATE orders SET status = ?, updated_at = now() WHERE id = ? AND status = ?`,
        to, id, from)
    return res.RowsAffected == 1, res.Error
}
```

**Claiming a job / outbox row:**

```go
err := tx.WithContext(ctx).
    Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
    Where("sent_at IS NULL").Order("id").Limit(100).
    Find(&rows).Error
```

**Worker pool** — the whole `worker_threads` apparatus collapses to:

```go
g, ctx := errgroup.WithContext(ctx)
g.SetLimit(10)
for d := range deliveries {          // amqp091 delivery channel
    d := d
    g.Go(func() error { return handle(ctx, d) })
}
return g.Wait()
```

**SSE** — no library needed:

```go
c.Header("Content-Type", "text/event-stream")
c.Header("Cache-Control", "no-cache")
flusher, _ := c.Writer.(http.Flusher)
// subscribe (buffering) → read current state → emit → flush buffer → stream live
```

## 5. Schema sketch

Eight entities are mandated at `README.md:54`. Lifecycle tables (`orders`, `payments`,
`reservations`, `outbox`, `notifications`) carry a status column and a state machine — see §5.14.
`inventory` deliberately has **no** states; it is a counter with an invariant.

```sql
CREATE TABLE inventory (
  product_id bigint PRIMARY KEY REFERENCES products(id),
  available  integer NOT NULL CHECK (available >= 0),   -- the invariant, enforced by the DB
  reserved   integer NOT NULL CHECK (reserved  >= 0),
  version    integer NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX outbox_unsent ON outbox (id) WHERE sent_at IS NULL;      -- partial: stays tiny
CREATE INDEX res_expiring  ON reservations (expires_at) WHERE status = 'HELD';
```

Partial indexes matter here — an index over *only* unsent rows stays small no matter how large the
table grows. Full indexing strategy is roadmap topic 7.

## 6. Messaging topology

Exchange `orders`, type `topic`, durable. Full detail in §5.15.

| Queue | Binding | Kind |
|---|---|---|
| `payments` | `order.created` | durable, competing consumers |
| `notifications.email` | `order.paid`, `order.cancelled`, `order.failed` | durable, competing |
| `notifications.sms` | `order.paid`, `order.cancelled` | durable, competing |
| `refunds` | `payment.refund_requested` | durable, competing |
| `sse.<instance>` | `order.#` | **exclusive, auto-delete** — must *not* compete |

**Email and SMS must not share a queue** — one `order.paid` would reach a single consumer and the
customer would get one or the other, never both. One queue per *job that must happen*; N consumers
per queue *for throughput*.

## 7. Package layout

Spec-mandated at `README.md:120`. Follow it — deviating loses marks for no gain.

```
cmd/{api,worker,relay}/main.go
internal/{api/{handlers,middleware,routes},models,services,repository,config,workers}
pkg/{database,logger,utils}
migrations/  tests/  docker/  docs/
```

Keep `internal/services` free of Gin types and `internal/repository` free of business rules.
Java-style `AbstractOrderServiceFactory` layering is one of the five tells — see roadmap topic 6.

## 8. Scope triage

Build in this order; each row is independently demonstrable if the clock runs out.

| # | | Why this order |
|---|---|---|
| 1 | Schema, migrations, seed | everything depends on it |
| 2 | Auth + CRUD + validation + Swagger | mandatory, low risk, gets the API real |
| 3 | **Order intake with conditional `UPDATE`** | the graded core — `README.md:189` names it |
| 4 | Outbox + relay + payment consumer | the pipeline |
| 5 | Reaper, cancel path, refund compensation | F and D |
| 6 | Notifications with per-channel policy | I |
| 7 | **Concurrency tests (Testcontainers)** | proves 3–6 actually work. Do not cut this |
| 8 | Rate limiting (Redis, Lua token bucket) | mandatory middleware |
| 9 | SSE order status | real-time requirement |
| 10 | Load test + benchmarks | `README.md:247` deliverable |
| 11 | Reports + rollup table | |
| 12 | Bonus: Prometheus, tracing, k8s | only if 1–11 are done |

If time runs short, cut from the bottom and **say so in the README**. Stating what you deliberately
didn't build, and why, reads as senior; silence reads as unfinished.

## 9. Still open

- **Pipeline shape** — async (202 + worker) strongly favoured; hybrid is safe given CAS. §5.4
- **Job state visibility** — RabbitMQ is transport, not storage, so `GET /orders/{id}/status`
  (`README.md:87`) needs a table. On `orders` itself, or a separate `payment_attempts`?
- **Partial fulfilment** — 3-item order, 1 out of stock: reject wholesale or fulfil partially? §7
- **Report timezone** — "daily" in UTC or Cairo? Not specified. §5.17
- **How many days you actually have.** Drives §8 more than anything else here.
