-- Reverse of 000001_init. Drop order is child-before-parent; indexes and
-- constraints go with their tables.

BEGIN;

DROP TABLE IF EXISTS daily_sales_rollup;
DROP TABLE IF EXISTS audit_logs;
DROP TABLE IF EXISTS notifications;
DROP TABLE IF EXISTS outbox;
DROP TABLE IF EXISTS payments;
DROP TABLE IF EXISTS reservations;
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS inventory;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS users;

DROP TYPE IF EXISTS notification_status;
DROP TYPE IF EXISTS payment_status;
DROP TYPE IF EXISTS reservation_status;
DROP TYPE IF EXISTS order_status;
DROP TYPE IF EXISTS user_role;

-- citext is left installed: other schemas in the same database may rely on it,
-- and dropping an extension is not this migration's business to undo.

COMMIT;
