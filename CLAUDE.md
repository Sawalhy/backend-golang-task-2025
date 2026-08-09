# CLAUDE.md

Senior backend take-home: a **concurrent order processing system**. Go 1.21+, Postgres, RabbitMQ,
Redis, Gin, GORM v2.

`README.md` is the assignment brief as given by the employer. **Do not edit it.** Cite it by line
number (`README.md:189`) when a decision traces back to a requirement.

## Read these before writing code

| Doc | When |
|---|---|
| **`docs/IMPLEMENTATION.md`** | Before every task. §3 is the rules, §8 is the build order, §4 has the Go form of every pattern used here |
| **`docs/DESIGN_NOTES.md`** | When something looks arbitrary — it probably isn't. §4 lists the ten failure modes that every design choice traces back to; §2 lists locked decisions and §3 the open ones |

Two sessions of design work sit behind this code. **Do not re-derive decisions.** If a choice looks
wrong, read the relevant `DESIGN_NOTES` section first, then say so — don't quietly do it differently.

`DESIGN_NOTES.md` §5.1, §5.2 and §5.8 are marked obsolete (they assumed a TypeScript submission).
Everything from §5.10 on is current.

## Non-negotiable rules

Each looks like arbitrary style until you know the failure it prevents.

1. **Never write `orders.status` directly.** Use `transition(ctx, tx, id, from, to)` and check the
   returned bool. Illegal transitions must be unreachable, not merely unwritten.
2. **Every state change is a CAS** — expected state in the `WHERE`, then check `RowsAffected`.
   **`RowsAffected == 0` means you lost a race, not that the operation failed.** It is a legitimate
   outcome with its own branch, never an error to blindly retry.
3. **Sort line items by `product_id`** and merge duplicate lines before touching `inventory`.
   Prevents deadlock (failure mode G). Wrap order transactions in a bounded, jittered retry on
   `40P01`.
4. **Never do network I/O inside a transaction.** Not the payment provider, not the broker, not
   email. A row lock held across a 2.4s call costs ~500× throughput and burns a pool connection.
5. **Idempotency keys come from the `payments` row**, are committed *before* the provider call, and
   never change across retries of the same intent. A fresh key per attempt means a charge per attempt.
6. **`context.Context` is the first parameter of every function that does I/O**, and it is threaded
   through. Never `context.Background()` mid-call-stack. Cancellation, timeouts and trace spans all
   ride on it.
7. **`defer rows.Close()` immediately** after any query returning `*sql.Rows`. Same discipline for
   files, locks, and AMQP channels.
8. **The relay uses a confirm channel** and awaits the broker ack before marking `sent_at`. Plain
   `publish()` is a socket write, not a delivery guarantee — marking sent on the back of it voids the
   entire outbox.

## Concurrency

- **Protecting shared state → `sync.Mutex`/`RWMutex`. Moving work between goroutines → channels.**
  Don't build a message-passing system to guard a map.
- **Every goroutine needs a guaranteed exit path.** Prefer `errgroup.Group` with `SetLimit` over bare
  `go`. An unbuffered send blocks forever if nobody receives, and a blocked goroutine is never
  collected.
- Worker pools are N goroutines consuming a delivery channel, bounded by `SetLimit`.

## Database

- **Money is integer cents.** Never float.
- Migrations via **golang-migrate**. GORM `AutoMigrate` must not run in any path outside tests.
- **Invariant-enforcing constraints go in the migration that creates the table** — `CHECK
  (available >= 0)`, the partial unique on `payments`, the unique on `notifications`. Adding them
  later means writing data cleanup.
- Partial indexes for queue-shaped tables (`WHERE sent_at IS NULL`), so the index tracks the backlog
  rather than the table.
- `inventory.available` is **deliberately unindexed** — see `DESIGN_NOTES.md` §5.18.

## Layout

Follow the structure at `README.md:120` exactly; deviating loses marks for no gain.

- `internal/services` must not import Gin types.
- `internal/repository` must not contain business rules.
- Three entry points: `cmd/api`, `cmd/worker`, `cmd/relay`.

## Style

- **`log/slog`**, structured, key-value. Never `fmt.Println`. Every log line inside a request carries
  the trace id.
- Wrap errors with context: `fmt.Errorf("charging order %d: %w", id, err)`.
- Domain outcomes are typed errors (`ErrInsufficientStock`), never string matching.

## Testing

- **Never mock the database** in anything exercising concurrency — a mock returns what you told it to
  and is blind to every race in this system. Real Postgres via **testcontainers-go**.
- Concurrency tests spawn N goroutines blocked on a channel, then release together. Trickling them
  out means they never collide and the test proves nothing.
- Run with `-race`. Know its limit: it finds Go memory races and is completely blind to database
  races, which is what almost every bug here is.

## Commands

No code yet. Add build/test/lint commands here as they exist.

## Do not

- Do not edit `README.md`.
- Do not add a dependency without saying why.
- Do not implement bonus items (Kubernetes, Prometheus, tracing, WebSocket) before build-order rows
  1–11 are done — see `docs/IMPLEMENTATION.md` §8.
