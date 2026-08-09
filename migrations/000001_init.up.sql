-- 000001_init — the whole schema, including every invariant-enforcing constraint.
--
-- Constraints that encode invariants live in the migration that creates the table
-- (CLAUDE.md). Adding CHECK (available >= 0) later means writing data cleanup first.
--
-- Eleven tables. README.md:54 mandates eight; reservations, outbox and
-- daily_sales_rollup were added by the design — see DESIGN_NOTES.md §5.18.

BEGIN;

-- Case-insensitive email comparison without lower() on every lookup, which would
-- also defeat the unique index. gen_random_uuid() is built into Postgres 13+, so
-- pgcrypto is not needed.
CREATE EXTENSION IF NOT EXISTS citext;

-- ---------------------------------------------------------------------------
-- Enums. Postgres enums are cheap and make illegal states unrepresentable at
-- the storage layer, which is the same argument as the transition map in Go.
-- ---------------------------------------------------------------------------

CREATE TYPE user_role AS ENUM ('CUSTOMER', 'ADMIN');

CREATE TYPE order_status AS ENUM (
  'PENDING',            -- stock reserved, payment not yet attempted
  'CHARGING',           -- a payment intent is in flight
  'PAID',               -- provider confirmed
  'FAILED',             -- provider declined; reservations released
  'CANCELLING',         -- cancel arrived while charging (failure mode D)
  'CANCELLED',          -- cancelled before any charge landed
  'CANCELLED_REFUNDED', -- cancelled after a charge landed; compensated
  'EXPIRED',            -- reaper reclaimed the stock (failure mode F)
  'FULFILLED',
  'REFUNDED'
);

CREATE TYPE reservation_status AS ENUM ('HELD', 'COMMITTED', 'RELEASED', 'EXPIRED');

CREATE TYPE payment_status AS ENUM (
  'INITIATED',
  'SUCCEEDED',
  'DECLINED',
  'UNKNOWN',    -- provider call timed out: the customer MAY have been charged
  'REFUNDED'
);

CREATE TYPE notification_status AS ENUM ('UNCLAIMED', 'SENDING', 'SENT', 'FAILED');

-- ---------------------------------------------------------------------------
-- Reference data
-- ---------------------------------------------------------------------------

CREATE TABLE users (
  id            bigserial PRIMARY KEY,
  email         citext    NOT NULL UNIQUE,
  password_hash text      NOT NULL,
  name          text      NOT NULL,
  role          user_role NOT NULL DEFAULT 'CUSTOMER',
  created_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE products (
  id           bigserial PRIMARY KEY,
  sku          text   NOT NULL UNIQUE,
  name         text   NOT NULL,
  description  text   NOT NULL DEFAULT '',
  price_cents  bigint NOT NULL CHECK (price_cents >= 0),  -- integer cents, never float
  currency     char(3) NOT NULL DEFAULT 'USD',
  active       boolean NOT NULL DEFAULT true,
  created_at   timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now()
);

-- No status column: inventory is a counter with an invariant, not a lifecycle.
-- CHECK (available >= 0) is the oversell guarantee (failure mode A) enforced by
-- the database itself, so it holds even if application code is wrong.
CREATE TABLE inventory (
  product_id  bigint PRIMARY KEY REFERENCES products(id) ON DELETE CASCADE,
  available   integer NOT NULL CHECK (available >= 0),
  reserved    integer NOT NULL DEFAULT 0 CHECK (reserved >= 0),
  version     integer NOT NULL DEFAULT 0,   -- admin edits, not the hot path (§5.10)
  updated_at  timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Order lifecycle
-- ---------------------------------------------------------------------------

CREATE TABLE orders (
  id              bigserial PRIMARY KEY,
  user_id         bigint NOT NULL REFERENCES users(id),
  status          order_status NOT NULL DEFAULT 'PENDING',
  total_cents     bigint NOT NULL CHECK (total_cents >= 0),
  currency        char(3) NOT NULL DEFAULT 'USD',
  idempotency_key text,                     -- client retry dedupe on POST /orders
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),
  paid_at         timestamptz,
  cancelled_at    timestamptz
);

CREATE TABLE order_items (
  id               bigserial PRIMARY KEY,
  order_id         bigint  NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  product_id       bigint  NOT NULL REFERENCES products(id),
  qty              integer NOT NULL CHECK (qty > 0),
  -- SNAPSHOT, not a join to products.price: an admin raising a price must not
  -- retroactively rewrite historical orders or past reports.
  unit_price_cents bigint  NOT NULL CHECK (unit_price_cents >= 0),
  UNIQUE (order_id, product_id)   -- forces line merging, which deadlock avoidance needs anyway
);

CREATE TABLE reservations (
  id         bigserial PRIMARY KEY,
  order_id   bigint  NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  product_id bigint  NOT NULL REFERENCES products(id),
  qty        integer NOT NULL CHECK (qty > 0),
  status     reservation_status NOT NULL DEFAULT 'HELD',
  expires_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE payments (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),  -- IS the provider idempotency key
  order_id     bigint NOT NULL REFERENCES orders(id),
  status       payment_status NOT NULL DEFAULT 'INITIATED',
  amount_cents bigint NOT NULL CHECK (amount_cents >= 0),
  provider     text   NOT NULL,
  provider_ref text,                                        -- provider charge id, once known
  attempts     integer NOT NULL DEFAULT 0,
  created_at   timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Async machinery
-- ---------------------------------------------------------------------------

-- The outbox exists because of failure mode B: an order committed but its event
-- never queued is stranded forever. Writing the event in the SAME transaction as
-- the order removes the dual write (§5.12).
CREATE TABLE outbox (
  id          bigserial PRIMARY KEY,
  event_id    uuid  NOT NULL UNIQUE,   -- consumer dedupe key
  routing_key text  NOT NULL,
  payload     jsonb NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  sent_at     timestamptz,
  attempts    integer NOT NULL DEFAULT 0
);

CREATE TABLE notifications (
  id          bigserial PRIMARY KEY,
  order_id    bigint NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  channel     text   NOT NULL CHECK (channel IN ('email', 'sms')),
  kind        text   NOT NULL CHECK (kind IN ('confirmation', 'cancellation', 'refund')),
  status      notification_status NOT NULL DEFAULT 'UNCLAIMED',
  lease_until timestamptz,             -- per-channel policy, §5.14
  attempts    integer NOT NULL DEFAULT 0,
  last_error  text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  sent_at     timestamptz
);

CREATE TABLE audit_logs (
  id            bigserial PRIMARY KEY,
  actor_user_id bigint REFERENCES users(id),
  entity_type   text NOT NULL,
  entity_id     text NOT NULL,
  action        text NOT NULL,
  before        jsonb,
  after         jsonb,
  created_at    timestamptz NOT NULL DEFAULT now()
);

-- The immutable half of the daily report wants materialising, not caching (§5.17).
CREATE TABLE daily_sales_rollup (
  day          date PRIMARY KEY,
  orders_count integer NOT NULL DEFAULT 0,
  gross_cents  bigint  NOT NULL DEFAULT 0,
  currency     char(3) NOT NULL DEFAULT 'USD',
  computed_at  timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Unique indexes that are INVARIANTS, not performance (§5.11)
-- ---------------------------------------------------------------------------

-- P1: one live payment INTENT per order.
--   Not per attempt  — retrying the same intent reuses the row and the key,
--                      which is the entire double-charge defence (mode C).
--   Not per order    — a declined card followed by a different card is a new intent.
-- So DECLINED, DECLINED, SUCCEEDED is a legal history; two live intents are not.
-- UNKNOWN must block: it means "may or may not have been charged", and opening a
-- second intent in that state can double-charge for real. REFUNDED is excluded —
-- a refunded order may legitimately be paid again.
CREATE UNIQUE INDEX payments_one_live_intent
  ON payments (order_id)
  WHERE status IN ('INITIATED', 'UNKNOWN', 'SUCCEEDED');

-- N1: a given order gets at most one notification per channel per kind (mode I).
CREATE UNIQUE INDEX notifications_once
  ON notifications (order_id, channel, kind);

-- Client retry of POST /orders must not create a second order.
CREATE UNIQUE INDEX orders_idempotency_key
  ON orders (idempotency_key)
  WHERE idempotency_key IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Foreign key indexes. Postgres does NOT create these for you — the most
-- common indexing miss, and it makes joins and cascades seq-scan.
-- ---------------------------------------------------------------------------

CREATE INDEX order_items_order_id   ON order_items   (order_id);
CREATE INDEX order_items_product_id ON order_items   (product_id);
CREATE INDEX reservations_order_id  ON reservations  (order_id);
CREATE INDEX reservations_product   ON reservations  (product_id);
CREATE INDEX payments_order_id      ON payments      (order_id);
CREATE INDEX notifications_order_id ON notifications (order_id);
CREATE INDEX audit_logs_entity      ON audit_logs    (entity_type, entity_id);

-- ---------------------------------------------------------------------------
-- Partial indexes for the claim queries.
--
-- The index size tracks the size of the BACKLOG, not the size of the table:
-- 40M rows in outbox, ~50 in the index. A few kilobytes, permanently cached.
-- Indexing sent_at or status outright would maintain a 40M-entry structure to
-- find dozens of rows.
--
-- Key column follows the ORDER BY; the filter selecting which rows participate
-- belongs in the index's WHERE, not in the key.
-- ---------------------------------------------------------------------------

CREATE INDEX outbox_unsent   ON outbox        (id)         WHERE sent_at IS NULL;
CREATE INDEX res_expiring    ON reservations  (expires_at) WHERE status = 'HELD';
CREATE INDEX notif_claimable ON notifications (id)         WHERE status IN ('UNCLAIMED', 'SENDING');

-- ---------------------------------------------------------------------------
-- Composites — equality column first, then the sort column.
-- A btree is sorted lexicographically, so with (user_id, created_at) one user's
-- rows are contiguous AND already ordered: seek once, walk 20, stop.
-- ---------------------------------------------------------------------------

CREATE INDEX orders_user_created   ON orders   (user_id, created_at DESC);  -- GET /orders
CREATE INDEX orders_status_created ON orders   (status,  created_at DESC);  -- GET /admin/orders
CREATE INDEX products_active_created ON products (active, created_at DESC); -- GET /products
CREATE INDEX orders_report_range   ON orders   (created_at)
  WHERE status IN ('PAID', 'FULFILLED');                                    -- daily report

-- NOTE: inventory.available is DELIBERATELY left unindexed (§5.18). It is the
-- most-updated column in the system; indexing it taxes every order to speed up
-- GET /admin/inventory/low-stock, an occasional admin query over ~10k rows.
-- Take the seq scan, protect the write path.

COMMIT;
