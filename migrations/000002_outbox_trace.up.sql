-- Carries the W3C traceparent of the request that produced the event.
--
-- This column is what makes tracing across the async boundary possible at all.
-- The handler that writes the order and the consumer that charges it are
-- separate processes running minutes apart with no call stack between them, so
-- the trace context has to be written down and travel with the work — the relay
-- reads it back out and puts it on the AMQP message. Same reason the payload
-- lives here rather than being published directly: the network cannot carry
-- causality across a crash, but a committed row can.
--
-- NOT NULL DEFAULT '' rather than nullable: '' already means "no trace" — which
-- is what tracing-disabled runs and every pre-existing row produce — so a
-- nullable column would add a pointer and a nil check to every read for a second
-- spelling of the same thing.
ALTER TABLE outbox ADD COLUMN trace_id text NOT NULL DEFAULT '';

-- Deliberately unindexed. It is written once and read only by the relay on rows
-- it has already claimed by primary key; nothing queries BY trace id. An index
-- here would be write amplification on the hottest insert path in the system for
-- a lookup that never happens.
