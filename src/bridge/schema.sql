CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE IF NOT EXISTS production_orders (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id text NOT NULL,
    lot_id text NOT NULL,
    asset_id text NOT NULL CHECK (asset_id IN ('P101','TK101')),
    product_id text NOT NULL,
    valid_from timestamptz NOT NULL,
    valid_to timestamptz NOT NULL,
    CHECK (valid_from < valid_to),
    EXCLUDE USING gist (asset_id WITH =, tstzrange(valid_from, valid_to, '[)') WITH &&)
);

CREATE TABLE IF NOT EXISTS events (
    event_id uuid PRIMARY KEY,
    digest text NOT NULL,
    body jsonb NOT NULL,
    asset_id text NOT NULL,
    tag_id text NOT NULL,
    event_time timestamptz NOT NULL,
    observed_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    write_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    order_context jsonb,
    context_rule text NOT NULL
);
CREATE INDEX IF NOT EXISTS events_history ON events(tag_id,event_time DESC,event_id);
CREATE INDEX IF NOT EXISTS events_lot ON events((order_context->>'lot_id'),event_time);

CREATE TABLE IF NOT EXISTS delivery_errors (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id uuid,
    digest text NOT NULL,
    kind text NOT NULL,
    detail text NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(digest, kind)
);

CREATE TABLE IF NOT EXISTS delivery_counters (kind text PRIMARY KEY, total bigint NOT NULL);
