# Concurrent Order Processing System — Design Plan (TypeScript)

> **Status: DESIGN IN PROGRESS.** Nothing is being built. Working through failure modes
> pedagogically before locking architecture. Three structural decisions remain open.
>
> **Companion file:** `design-dialogue-transcript.md` — the chronological back-and-forth, with
> Ahmed's questions verbatim and the corrections made along the way. This file organizes
> conclusions by topic; that one preserves the order they were reached in.
>
> Citations of the form `README.md:212` refer to the assignment brief at the repository root
> (`../README.md`), by line number.

## Contents

1. [Context](#1-context)
2. [Decisions locked](#2-decisions-locked)
3. [Decisions open](#3-decisions-open)
4. [The ten failure modes](#4-the-ten-failure-modes)
5. [Session log](#5-session-log)
6. [Roadmap — remaining discussions](#6-roadmap--remaining-discussions)
7. [Spec gaps to decide deliberately](#7-spec-gaps-to-decide-deliberately)
8. [Verification plan](#8-verification-plan)
9. [Owed](#9-owed)

---

## 1. Context

`README.md` specifies a Go/GORM/Gin take-home: a concurrent order processing system with
inventory management, payment processing, notifications, and reporting. Ahmed has employer
permission to submit in TypeScript instead.

Stripped of Go vocabulary, the assignment reduces to four hard problems:

1. **Don't oversell** under concurrent purchase of the last unit
2. **Don't double-charge** when payments time out ambiguously
3. **Don't block** throughput on slow payment calls or heavy reports
4. **Stay consistent** when 1000 orders land at once

### The rubric problem

`README.md:212` grades Technical Implementation at 40%, and two of its four sub-bullets are
*"Proper use of Go idioms and patterns"* and *"GORM usage"*. Concurrency at 25% says
*"Effective use of goroutines and channels"* verbatim. Roughly a quarter of the rubric is
written in vocabulary that cannot be satisfied literally.

Equivalence must therefore be **argued and measured**, not assumed. A `DESIGN_DECISIONS.md`
with a Go↔TS mapping table and real benchmark numbers is load-bearing for ~25% of the grade,
not polish.

### The argument that defuses it

> The concurrency primitive was never the bottleneck. The database was.

1000 concurrent orders in Go spawn 1000 cheap goroutines — which immediately queue on a
~20-connection pool and serialize on a row lock for the hot product. Node hits that same wall
at the same place, with less memory per in-flight request. Goroutines buy nothing the pool
takes straight back.

Say it, then **prove it with a load test**. That benchmark is worth more than any prose.

### The rule that dictates the architecture

**Never hold a row lock across network I/O.** A payment call is 100ms–3s. Hold an inventory
lock across it and throughput on a hot product is 0.3–10 orders/sec in *any* language.

---

## 2. Decisions locked

| Decision | Choice | Rationale |
|---|---|---|
| Language | TypeScript | Permission granted; Go fluency was the limiting factor |
| Inventory strategy | Reserve → pay → commit | Never holds a row lock across the payment network call |
| Go-framing stance | Idiomatic Node + measured proof | Win on benchmarks, not on resemblance |
| Process model | Two entry points, one image | `dist/api.js` + `dist/worker.js`, scaled independently |
| State transitions | CAS updates everywhere | Mandatory — makes at-least-once delivery survivable |
| Inventory write | Atomic conditional `UPDATE` + rowcount, `CHECK (available >= 0)` | One statement, no read-write gap, no retry loop — §5.10 |
| Deadlock avoidance | Sort line items by `product_id`; bounded retry on `40P01` | Total order on resources makes wait-cycles impossible — §5.10 |

---

## 3. Decisions open

- **Pipeline shape** — sync / async (202 + worker) / hybrid. Leaning async: the spec lists
  `GET /orders/{id}/status` as separate from `GET /orders/{id}`, which only makes sense if state
  changes after creation returns. Also required to satisfy the mandatory "Background Jobs /
  job queue" bullet. Hybrid is *also* safe given CAS — see §5.4.
- **Queue substrate** — Postgres `SKIP LOCKED` / outbox→RabbitMQ / split by path. Ahmed favours
  the outbox pattern.
- **CPU offload depth (failure mode H)** — SQL aggregation is settled. Open only on the residual
  CSV/PDF serialization: `worker_threads` with a before/after p99 benchmark, vs. simply running
  exports in the worker process. §5.8 shrank this considerably.
- Framework, ORM, real-time transport, Redis role, scope triage — all downstream of the above.

---

## 4. The ten failure modes

The territory is finite. Everything this system can get wrong:

| # | Failure | Cluster |
|---|---|---|
| A | Two customers buy the last item, both succeed → oversold | Inventory |
| B | Order saved, crash before job queued → stranded forever | Distributed |
| C | Payment times out, outcome unknown, retry → double charge | Distributed |
| D | Cancel arrives while payment in flight → charged a cancelled order | Distributed |
| E | Worker dies mid-job → stuck, or runs twice | Distributed |
| F | Customer abandons checkout → stock held hostage | Inventory |
| G | Two multi-item orders lock same products in opposite order → deadlock | Inventory |
| H | Report generation freezes the event loop → every order stalls | Node-specific |
| I | Notification sent twice, or never | Distributed |
| J | 1000 concurrent orders exhaust the DB connection pool | Node-specific |

---

## 5. Session log

### 5.1 Language choice — Go vs TypeScript

**Where Go is genuinely easier:** report generation (`go generateReport()` vs. a whole
`worker_threads` apparatus); structured concurrency (`errgroup.Group` with `SetLimit`,
`context.Context` threading deadlines through the call tree); rubric literalism.

**Where the Go advantage is a mirage:** `go test -race` finds *memory* races. Every race in this
assignment is a *database* race. The race detector is inert against overselling and
double-charging — the two things it gets cited for.

**Where TypeScript is genuinely easier:** validation + OpenAPI from one Zod/TypeBox declaration
(vs. struct tags + validator + swaggo comments, three sources of truth that drift); migrations
(GORM `AutoMigrate` is the weakest part of GORM — won't drop columns, usually paired with
`golang-migrate` anyway); error-handling volume.

**Effort split, holding skill constant:**

| Chunk | Share | Easier in |
|---|---|---|
| Schema, migrations, seed | 10% | TS |
| CRUD, auth, middleware, validation, docs | 25% | **TS, clearly** |
| Pipeline, worker, state machine | 20% | Wash (except report offload → Go) |
| Inventory concurrency correctness | 15% | **Identical** |
| Payment idempotency + retries | 10% | **Identical** |
| Tests incl. concurrency | 15% | Slight TS edge (Testcontainers) |
| Docker, README, benchmarks | 5% | Go (no translation doc needed) |

~60% of the genuine difficulty is language-independent. Fluency dominates everything else:
non-native Go reads as junior even when correct, damaging precisely the "Go idioms"
sub-criterion the switch was meant to capture.

**Action:** state the permission grant (who approved, when) in the README's opening paragraph —
the grader may not be the person who approved it.

### 5.2 Topic 1 — How Node handles 1000 concurrent orders

**"Single-threaded" means one thread runs *your JavaScript*.** It does not mean one thing
happens at a time. Waiting for Postgres is not running JavaScript.

*Waiter analogy:* the naive assumption is a waiter who takes your order, walks to the kitchen,
stands watching the food cook, then greets the next table. Real waiters hand the order off and
move on. They're busy while *talking*, never while *cooking*. Your JS thread is the waiter;
Postgres and Stripe are the kitchen.

**Where the time goes in one order request:**

| Step | Type | Time |
|---|---|---|
| Parse JSON, validate | **CPU** | 0.4ms |
| Wait for inventory `SELECT` | waiting | 3ms |
| Wait for order `INSERT` | waiting | 2ms |
| Wait for Stripe | waiting | 2400ms |
| Serialize response | **CPU** | 0.2ms |

0.6ms of thread time; 2405ms of waiting. One thread at 0.6ms/request sustains ~1,600 req/sec.

```
ONE THREAD, THREE ORDERS, ALL AT ONCE

Order A   ▓▓░░░░░░░░░░░░░░░░░░░░░░░░░▓▓
Order B    ▓▓░░░░░░░░░░░░░░░░░░░░░░░░░▓▓
Order C     ▓▓░░░░░░░░░░░░░░░░░░░░░░░░░▓▓

          ▓ = thread running your JS     ░ = waiting, thread is FREE
```

**Three real ceilings — only one is Node-specific:**

1. **Connection pool (J).** 1000 orders queue on ~20 connections. *Identical in Go.*
2. **Row locks.** Throughput = 1 / lock hold time. 5ms hold → 200 orders/sec; 2400ms hold →
   0.4 orders/sec. A 500× difference. *Identical in Go.*
3. **CPU on the event loop (H).** A 500ms synchronous report build stalls *every* in-flight
   order. **Genuinely Node-specific.**

```
Order A   ▓▓░░░░░░░░░░
Order B    ▓▓░░░░░░░░░░
REPORT      ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓   ← 500ms pure CPU
Order C                           ▓▓░░░░
                                  ↑ waited 500ms to even BEGIN
```

**Naive → industry, J (connections):** per-request `new Client()` (TCP + auth handshake each
time; Postgres spawns an OS *process* per connection and dies at a few hundred) → a pool →
pool sized to ~2-4× *DB* cores, acquired as late and released as early as possible, PgBouncer
once multiple app instances run.

**Naive → industry, H (CPU):** JS loop over 100k rows (the expensive part isn't your loop —
it's the driver parsing 100k rows off the wire and allocating 100k objects, 200-500ms) →
aggregate in SQL (`GROUP BY` returns 30 rows, done on Postgres's cores) → `worker_threads` only
for the residual CSV/PDF serialization Postgres can't do.

**Multi-core:** one Node process = one core. Run N processes (`cluster`, PM2, `docker compose
--scale`). Go uses all cores in one process; Node uses all cores across N processes. Same
result, different packaging — and irrelevant once you're scaling containers horizontally anyway.

### 5.3 Connection pool sizing

Postgres spawns a separate OS **process** per connection (~5-10MB each); default
`max_connections` is 100. More connections does *not* mean more throughput — past ~2-4× DB
cores, Postgres spends more time context-switching and contending on internal locks than
working, and throughput **drops**.

Starting point: `pool ≈ DB cores × 2` (up to ×4 on fast SSD). A 4-core Postgres wants **8-16**,
not 100. Divide across app instances; add PgBouncer once several run; reserve headroom for
migrations and monitoring.

**Operational rule:** pool starvation is almost always fixed by **shorter transactions**, not a
bigger pool. Throughput = concurrency ÷ latency.

### 5.4 Pipeline shapes — sync / async / hybrid

**Synchronous** (2.4s customer wait): one code path, trivial tests, no eventual consistency.
Killer objection is rubric coverage — `README.md:40` makes "Background Jobs: implement job
queue" mandatory and Scenario 1 names the worker pool pattern outright.

**Async (202 + worker):**

```
t=0ms      Customer clicks Buy
t=1ms      BEGIN
t=3ms      reserve stock
t=5ms      INSERT order (status='PENDING')
t=6ms      INSERT job  ('charge_payment', order 123)
t=8ms      COMMIT
t=9ms      → 202 Accepted {orderId:123, status:'PENDING'}
           ← browser DONE. "Order placed, processing payment…"

           ── separate process ──
t=50ms     Worker claims the job
t=2450ms   Stripe approves
t=2460ms   UPDATE orders SET status='PAID'
```

`202 Accepted` = "I got your request, I haven't finished it yet" (vs `201 Created` = "done").
The worker is not magic — it's a second program running a loop:

```ts
while (true) {
  const job = await claimOneJob()
  if (!job) { await sleep(200); continue }
  await chargePayment(job.orderId)
  await markJobDone(job.id)
}
```

A "worker pool" is that loop running 10 times concurrently.

**Hybrid** — sync fast path, detach past ~500ms. **Initially dismissed; that was wrong.**
Ahmed's correction holds: with idempotent operations both writers reach the same conclusion and
the only cost is wasted work. The required guard is CAS (§5.5), which is mandatory anyway for
failure mode D — so hybrid's marginal cost is only detach plumbing, not a new class of
correctness risk.

**Two arguments that async is what the spec assumes:**
1. *Spec archaeology.* `README.md:87` lists `GET /orders/{id}/status` as separate from
   `GET /orders/{id}`. That endpoint only earns its existence if state changes after creation
   returns.
2. *Reserve→pay→commit presupposes it.* That pattern exists so payment runs with no locks held.
   If payment runs inside the request anyway, the reservations table and reaper are pure cost.

Note what async still gets right for the customer: inventory is reserved **synchronously**, so
the response is immediately authoritative about stock. Only payment is eventual.

### 5.5 CAS — compare-and-swap

**Not related to RLS.** Row-Level Security is a Postgres *authorization* feature answering "who
may see this row." CAS answers "who wins a race." (RLS is legitimately usable here for
"customers see only their own orders" — just unrelated.)

> Change X from A to B — but only if it's currently A.

Not a special SQL feature. An ordinary `UPDATE` with expected state in the `WHERE`, plus a
**rowcount check**:

```sql
UPDATE orders SET status = 'PAID', paid_at = now()
WHERE id = $1 AND status = 'PENDING'
```

- rowcount **1** → you won; perform side effects (commit reservation, queue notification)
- rowcount **0** → someone already transitioned it; do nothing

**Why it works:** Postgres row-locks during update. Two concurrent attempts serialize; the
second re-evaluates its `WHERE` against the *new* value, matches nothing.

**The bug it replaces:**

```ts
const order = await db.query('SELECT status FROM orders WHERE id=42')
if (order.status === 'PENDING') {        // ← BOTH requests see PENDING
  await db.query("UPDATE orders SET status='PAID' WHERE id=42")
}                                         // ← BOTH send the confirmation email
```

That gap between read and write is where essentially every race in this assignment lives.

### 5.6 The dual-write problem and the five patterns

**What "backing the queue" means:** where job records physically live and what hands them to
workers — a Postgres table, Redis, RabbitMQ, Kafka, or memory.

**The dual-write problem.** You need two things to happen together: the order exists, and a
worker knows to process it. With any broker that's two systems:

```
BEGIN;
  INSERT INTO orders (status) VALUES ('PENDING');
  INSERT INTO reservations ...;
COMMIT;                        -- ✅ committed
publishToRabbit({orderId});    -- 💥 process dies here
```

Order stranded in `PENDING` forever, stock held, nothing knows. Reversing the order is worse.
**No sequencing fixes it** — two systems, no shared transaction. AMQP transactions cover the
broker only; they can't enroll Postgres. Applies identically to Redis, Kafka, SQS.

**The five patterns**, distinguished by what gets written first — walked through concretely on
*Sarah buys 1 Wireless Mouse, product 7, stock 3, order #1001*:

**① Queue inside the DB (`SKIP LOCKED`)**
```
POST /orders
  BEGIN
    INSERT orders     (id=1001, status='PENDING')
    UPDATE inventory  SET available=2, reserved=1 WHERE product_id=7 AND available>=1
    INSERT jobs       (type='charge', order_id=1001)    ← the "message" is a row
  COMMIT        ← all three, or none
→ 202

Worker:
  Txn A (5ms)  UPDATE jobs SET status='processing', locked_until=now()+'5 min'
               WHERE id = (SELECT id FROM jobs WHERE status='pending'
                           ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1)
               RETURNING *
  [no txn]     Stripe, idempotency-key 'order-1001'   ... 2400ms
  Txn B (5ms)  UPDATE orders SET status='PAID' WHERE id=1001 AND status='PENDING'   ← CAS
               UPDATE inventory SET reserved=reserved-1 WHERE product_id=7
               INSERT jobs (type='email', order_id=1001)
```
`SKIP LOCKED` is what makes ten workers safe — worker 2 sees worker 1's locked row and skips
past it rather than blocking. Note the job claim is a *short* transaction; the Stripe call
happens with no transaction open, so no pool connection is held for 2400ms.

**② Outbox** — DB first; message row in the same txn; a relay publishes it and marks `sent`.
Crash between publish and mark → delivered twice. At-least-once.

**③ CDC / WAL tailing** — app only writes the DB; Debezium reads Postgres's own replication log
and publishes every change. Zero application code; heavy infra; your *table schema* becomes your
event contract.

**④ Write-behind / command queue** — publish first, consumer does the INSERT. *Ahmed's
suggestion.* Correctly eliminates the dual write (one write per side). Costs: no synchronous
validation (product missing / out of stock → already returned 202), can't promise the stock,
broker becomes system of record for intake, read-your-writes breaks.

**⑤ Sweeper** — accept the gap; periodic job re-publishes orders stuck in `PENDING` past a
threshold. Cannot distinguish "stranded" from "still processing", so it re-publishes healthy
work too.

| Pattern | Worst-case crash | Sarah's order |
|---|---|---|
| ① SKIP LOCKED | anywhere | Safe — resumes immediately |
| ② Outbox | between publish and mark-sent | Safe — possibly delivered twice |
| ③ CDC | anywhere | Safe — resumes from offset |
| ④ Write-behind | broker drops message | **Gone without a trace** |
| ⑤ Sweeper | between commit and publish | Safe — ~2 min late |

①②③ share one idea: **make the "someone must act on this" record durable inside the same
transaction as the data.** They differ only in where it lives — your jobs table, your outbox
table, or Postgres's own WAL.

**No pattern delivers exactly-once.** Every one degrades to at-least-once, which is why CAS
updates and idempotency keys are structural requirements, not polish.

### 5.7 The cost of reserve→pay→commit (state honestly in the README)

**Violating version:**
```sql
BEGIN;
  SELECT * FROM inventory WHERE product_id=7 FOR UPDATE;   -- 🔒 acquired
  -- call Stripe ... 2400ms ...                            -- 🔒 STILL HELD
  UPDATE inventory SET available = available - 1;
COMMIT;                                                     -- 🔓
```
→ **0.4 orders/sec**

**Ours:** two ~5ms transactions with the Stripe call between them, no lock held →
**~200 orders/sec**. Same work, 500×.

**What we pay for it.** Holding the lock across payment would delete: the reservations table,
the `reserved` column, the expiry reaper (failure mode F *vanishes*), the two-phase state
machine, and most of the case for an async pipeline — roughly 30-40% of the order-domain code.

The trade is worth it because `README.md:189` names this exact scenario as a graded challenge.
Being able to state precisely what the complexity bought is the senior part.

### 5.8 Concurrency mechanics — threads, processes, isolates

**"JavaScript is single-threaded" is missing a qualifier: single-threaded *per isolate*.** An
isolate is one independent V8 instance — own heap, own GC, own event loop, own variables.

**What the guarantee buys:** run-to-completion. Your function finishes before any other JS runs.
`count = count + 1` is a data race in Go/Java/C#; in JS it cannot be. That's why JS has no
`mutex`, no `synchronized`, no `volatile`.

**How `worker_threads` doesn't break it:** each worker gets a real OS thread *and a brand new
V8 isolate*. Two JavaScript worlds, genuinely parallel, sharing zero variables. Like two people
writing in their own notebooks in the same room — to share, one photocopies a page. That
photocopy is `postMessage`, which **copies** (structured clone), never shares.

**The escape hatch:** `SharedArrayBuffer` gives real shared memory — and hands you Go's problems
back, which is why `Atomics.add` / `Atomics.wait` / `Atomics.notify` exist. Opt-in, rarely used.

**The reveal:** Node has always been multi-threaded. libuv keeps a **4-thread pool** handling
`fs.*`, `dns.lookup`, `crypto.pbkdf2`/`scrypt`/`randomBytes`, and `zlib`. Network I/O doesn't
even use it — that's `epoll`/`kqueue` directly. Those threads run C++, never JavaScript, so the
guarantee was never threatened.

**Four ways to get work off the main thread:**

| | `worker_threads` | `child_process.fork()` | `cluster` | Separate service |
|---|---|---|---|---|
| What | Thread, same process, own isolate | Separate Node process | N forks sharing a socket | Independent container |
| Memory each | a few MB | 30–50MB | 30–50MB | 30–50MB |
| Startup | 10–30ms | 50–200ms | 50–200ms | deploy-time |
| Comms | `postMessage` / `SharedArrayBuffer` | IPC (JSON over pipe) | IPC | DB / queue / HTTP |
| Crash | **can kill the process** | isolated | isolated | isolated |
| Across machines | no | no | no | **yes** |

**Rule:** **process** for "go do this, I'll check later"; **thread** for "compute this and hand
it straight back into this request." Only the second justifies `worker_threads`, because IPC
serialization can cost more than the work.

**.NET comparison** (Ahmed's question): `await` ↔ `await`; `worker_threads` ↔ `Task.Run`.
Three differences —
1. *Continuation affinity.* .NET may resume on a different pool thread (hence
   `ConfigureAwait(false)`, `SynchronizationContext` deadlocks, `[ThreadStatic]` traps). Node
   always resumes on the same thread; that whole bug class doesn't exist.
2. *Shared memory.* .NET pool threads share the heap — parallelism free, `lock`/`Interlocked`
   required. Node workers share nothing — safety free, serialization required.
3. *Ergonomics.* `await Task.Run(() => BuildCsv(rows))` is one line, no copying. Node needs a
   separate file plus a round-trip copy. Honest gap; .NET and Go are on the same side of it.

Shared misconception in both: **`await` does not create parallelism.** `await buildCsv(rows)`
still blocks in either language. Async is about not blocking on *waiting*, not working faster.
.NET thread-pool starvation is the same shape of bug as blocking Node's event loop.

**Applying the process/thread rule shrinks failure mode H:**
- `GET /admin/reports/daily` — SQL aggregation returns ~30 rows in ~50ms. Synchronous is fine.
- Large CSV/PDF export — nobody expects it in one HTTP response. Background job in the worker
  process, admin polls and downloads. Blocking there harms no customer.

So `worker_threads` is now an optional showpiece, not a structural necessity.

### 5.9 Async work products — two rules

**Claim-check pattern.** Queues carry pointers, never payloads. A consumer building a 50MB CSV
uploads it to object storage (S3/MinIO) and publishes only `{jobId, fileKey}`. The client
downloads **directly from storage via a presigned URL** — never streamed back through the API,
which would reintroduce the blocking the offload was meant to solve.

**Queues are transport, not storage.** Job status lives in Postgres (`report_jobs`), never in
the broker. You cannot query a queue for "status of job abc" — once consumed the message is
gone. The broker only carries the nudge that says *go look at the database*.

**Getting the result to the user:** polling (simplest, fine here) / SSE (one-directional
server→client, plain HTTP, auto-reconnect — the natural fit) / WebSocket (overkill, needs
sticky sessions or a backplane) / email-webhook (for minutes-long reports).

The report pipeline is **structurally identical** to the order pipeline: row in a table, async
worker, status endpoint, client polls or subscribes. One machine, two job types — worth stating
explicitly in the README.

**Caveat:** reports have exactly one consumer doing one thing. RabbitMQ earns its place on
*fanout-shaped* work (one event → email + SMS + analytics). The jobs table would serve reports
just as well.

### 5.10 Topic 2 — Two orders, one item (failure modes A, F, G)

*Product 7, stock 1. Sarah and Tom both click Buy in the same millisecond.*

**The naive code and why it oversells (A):**

```ts
const inv = await db.query('SELECT available FROM inventory WHERE product_id=7')
if (inv.available >= 1) {
  await db.query('UPDATE inventory SET available = available - 1 WHERE product_id=7')
  await createOrder(...)
}
```

```
t=0ms  Sarah  SELECT → 1
t=1ms  Tom    SELECT → 1        ← reads before Sarah writes
t=2ms  Sarah  if (1 >= 1) ✓
t=3ms  Tom    if (1 >= 1) ✓
t=4ms  Sarah  UPDATE → available = 0
t=5ms  Tom    UPDATE → available = -1     ← oversold
```

Same shape as the CAS bug in §5.5: **the decision is made on a value that is already stale by the
time it is acted on.**

**Three fixes that look right and aren't:**

| Attempt | Why it fails |
|---|---|
| Wrap it in `BEGIN`/`COMMIT` | Transactions give atomicity and isolation from *uncommitted* data — not mutual exclusion. At READ COMMITTED both txns still read `1`. The `UPDATE` arithmetic serializes correctly; the `if` was already wrong. Result is still `-1`. |
| A mutex / in-process flag in Node | Correct for exactly one process. Dies at two replicas — and §2 already locked two entry points and horizontal scaling. |
| `SERIALIZABLE` isolation | Genuinely correct, but converts contention into `40001` serialization failures the app must catch and retry. On one hot row that is a retry storm. Right tool for complex read-write invariants, wrong tool for a counter. |

**Fix 1 — pessimistic: `SELECT … FOR UPDATE`**

```sql
BEGIN;
  SELECT available FROM inventory WHERE product_id=7 FOR UPDATE;   -- 🔒 row locked
  -- app checks available >= 1
  UPDATE inventory SET available = available-1, reserved = reserved+1 WHERE product_id=7;
COMMIT;                                                             -- 🔓
```

Tom's `SELECT … FOR UPDATE` **blocks** at t=1ms until Sarah commits, then reads `0` and correctly
fails. Correct, readable, and the shape to reach for when the decision needs several rows or
app-side logic between read and write. Cost: every buyer of the hot product serializes, so
throughput = 1 / lock hold time — which is precisely why §5.7 forbids holding it across Stripe.

**Fix 2 — atomic conditional `UPDATE` + rowcount (what we use)**

```sql
UPDATE inventory
   SET available = available - $qty,
       reserved  = reserved  + $qty
 WHERE product_id = $pid
   AND available >= $qty
RETURNING available;
```

- rowcount **1** → reserved, proceed
- rowcount **0** → insufficient stock → `409 Conflict`

There is no separate read, so there is no gap to lose. Tom's `UPDATE` blocks on Sarah's row lock;
when it unblocks, Postgres re-reads the newly committed row and **re-evaluates the `WHERE` against
the new value** (`EvalPlanQual`) — `available >= 1` is now false, rowcount 0.

**Isolation-level caveat worth knowing:** that re-evaluation is READ COMMITTED behaviour. At
REPEATABLE READ or higher Postgres raises `could not serialize access due to concurrent update`
instead. The pattern is correct at both, but only the default gives it silently.

**Belt and braces — make the invariant the database's job:**

```sql
CREATE TABLE inventory (
  product_id bigint PRIMARY KEY REFERENCES products(id),
  available  integer NOT NULL CHECK (available >= 0),
  reserved   integer NOT NULL CHECK (reserved  >= 0),
  version    integer NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now()
);
```

The `WHERE` clause is the graceful path; the `CHECK` constraint is the proof that no code path can
oversell. Say that distinction out loud in the README.

**Fix 3 — optimistic versioning (the alternative, and why not here)**

```sql
UPDATE inventory SET available = $new, version = version + 1
 WHERE product_id = $pid AND version = $readVersion;
-- rowcount 0 → someone moved it → re-read and retry, bounded
```

Wins when contention is low, when there is long think-time between read and write, or when the new
value isn't a simple delta of the old. Loses on hot rows: the last unit of a flash-sale item is
maximum contention, so it retry-storms exactly where correctness matters most.

**Taxonomy note:** Fix 2 *is* optimistic concurrency collapsed into a single statement — which is
why it cannot fail spuriously and needs no retry loop. Keep the `version` column anyway for
admin-side edits (§ product update endpoints), where think-time is real.

| | `FOR UPDATE` | Conditional `UPDATE` | Optimistic `version` |
|---|---|---|---|
| Round trips | 2 | **1** | 2 + retries |
| Behaviour under contention | blocks | blocks briefly, then decides | retries |
| Fails spuriously | no | **no** | yes |
| Needs app-side retry | no | **no** | yes |
| Good for | multi-row decisions | **hot counters** | low-contention edits |

**Failure mode F — the abandoned reservation**

Reserve→pay→commit means stock leaves `available` at reserve time. If payment never resolves
(customer abandons, worker dies), the unit is stranded in `reserved` forever. Requires:

- `reservations(id, order_id, product_id, qty, status, expires_at)` — `status ∈ HELD|COMMITTED|EXPIRED`
- a **reaper** in the worker process, returning expired holds to `available`

The reaper races the payment worker, so both sides CAS the *reservation*, not the inventory:

```sql
-- reaper                                    -- payment worker on success
UPDATE reservations SET status='EXPIRED'     UPDATE reservations SET status='COMMITTED'
 WHERE id=$1 AND status='HELD';               WHERE id=$1 AND status='HELD';
```

Exactly one gets rowcount 1. **Honest consequence:** if the reaper wins and the charge later
succeeds, the correct behaviour is a refund, not a crash — so `PAYMENT_CAPTURED` +
`RESERVATION_EXPIRED` is a real state the machine must model. Set the TTL well above the payment
timeout (15 min vs 30 s) so this is rare, not routine.

**Failure mode G — deadlock on multi-item orders**

```
Order A: [mouse(7), keyboard(3)]        Order B: [keyboard(3), mouse(7)]

t=0  A  UPDATE product 7   🔒7
t=1  B  UPDATE product 3   🔒3
t=2  A  UPDATE product 3   ⏳ waits on B
t=3  B  UPDATE product 7   ⏳ waits on A     → cycle
```

Postgres detects it after `deadlock_timeout` (default **1 s**) and kills one victim with SQLSTATE
`40P01`. Not a hang — a one-second stall plus an error, which under load is arguably worse.

**Fix: a global lock order.** Sort line items by `product_id` ascending before touching inventory,
and merge duplicate lines first. Both transactions then take 3 before 7, so B simply waits for A —
no cycle. A deadlock needs a cycle in the wait-for graph, and a total order on resources makes
cycles impossible (dining philosophers, unchanged since 1965).

Still wrap the order transaction in a **bounded retry on `40P01`** with jitter: sorting covers the
order path, but the reaper, admin edits, and restock jobs touch the same rows without going through
it.

**Verification (extends §8):** stock = 1, fire 50 concurrent `POST /orders` → exactly 1 success, 49
× `409`, `available = 0`, `reserved = 1`, no row ever negative. Real Postgres via Testcontainers;
this test is meaningless against a mock or SQLite.

---

## 6. Roadmap — remaining discussions

| # | Topic | Covers | Status |
|---|---|---|---|
| 1 | Node concurrency & 1000 orders | H, J | ✅ done (§5.2) |
| 2 | Two orders, one item | A, F, G | ✅ done (§5.10) |
| **3** | **Payment & cancellation races** | **B, C, D, E, I** | **← next** |
| 4 | WebSockets / SSE — where real-time is standard vs cargo-cult | — | queued |
| 5 | Redis — where caching helps vs. creates a consistency bug | — | queued |

**Topic 3 outline (next up):** the ambiguous payment timeout — you sent the charge, the connection
died, you do not know if the customer was charged (C); idempotency keys and why the key must be
derived from the order, not generated per attempt; cancel arriving mid-charge (D); worker dying
mid-job and at-least-once redelivery (E); the order state machine drawn as a diagram with every
legal transition CAS-guarded; notification exactly-once being impossible and what to do instead (I);
and mode B — order committed, job never queued — which §5.6 already answers but should be closed
formally alongside the rest.

**After the five topics:**
- Resolve the two open structural decisions (pipeline shape, queue substrate)
- Stack choices — framework (Fastify vs NestJS), ORM (Drizzle vs Prisma vs TypeORM), testing
  (Vitest + Testcontainers)
- Schema design — the 8 entities, indexing strategy, constraints
- Scope triage against the 5-7 day clock; which bonus items to attempt
- Final implementation plan

---

## 7. Spec gaps to decide deliberately

Calling these out in the README reads as senior; silently guessing reads as junior.

- **No auth endpoints exist** in the spec, yet JWT is mandatory — must add `/auth/login` + refresh.
- **Cancel-vs-charge race** (failure D) is unspecified and is the subtlest correctness bug here.
- **Partial fulfillment** — 3-item order, 1 item out of stock: reject wholesale or fulfill partially?
- **"Real-time inventory updates"** — pushed to admins only, or to every browsing customer?
- `PUT /orders/{id}/cancel` is not RESTful. Implement as specified; note that it was noticed.

---

## 8. Verification plan

**Concurrency correctness** — N concurrent purchases of a product with stock 1 → exactly 1
success, 0 oversells. Must run against **real Postgres via Testcontainers**; race conditions
cannot be tested against a mock or SQLite.

**The load test that carries the submission's argument** — 1000 concurrent orders, varying one
thing at a time:

| Change | Expected effect | Proves |
|---|---|---|
| Pool 10 → 40 | large | pool is the bottleneck |
| Lock hold 5ms → 50ms | large | lock contention is the bottleneck |
| Node processes 1 → 4 | negligible | **the runtime was never the bottleneck** |

**Event-loop test** — p99 order latency during report generation, with and without offload.

**Crash-recovery test** — kill the process between order commit and job pickup; assert the order
still completes.

---

## 9. Owed

Walkthrough of five non-native-Go tells: goroutine leaks, channels where a mutex belonged,
unthreaded `context.Context`, unclosed `rows`, Java-style package layout. Deferred to a natural
pause; do not drop.

*(Partially touched in §5.8 — Go's constant "which synchronization primitive?" question simply
doesn't arise in JS, which is the root of the channels-vs-mutex confusion.)*
