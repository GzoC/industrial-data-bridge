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
ALTER TABLE production_orders DROP CONSTRAINT IF EXISTS production_orders_asset_id_check;
ALTER TABLE production_orders ADD COLUMN IF NOT EXISTS site_id text NOT NULL DEFAULT 'aurora-demo';
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='orders_site_asset_period'
                   AND conrelid='production_orders'::regclass) THEN
        ALTER TABLE production_orders DROP CONSTRAINT IF EXISTS production_orders_asset_id_tstzrange_excl;
        ALTER TABLE production_orders ADD CONSTRAINT orders_site_asset_period
            EXCLUDE USING gist (site_id WITH =, asset_id WITH =, tstzrange(valid_from, valid_to, '[)') WITH &&);
    END IF;
END $$;

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
CREATE INDEX IF NOT EXISTS events_scoped_history ON events(
    (body->>'site_id'), (body->>'gateway_id'), tag_id, event_time DESC, observed_at DESC, event_id
);

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
CREATE TABLE IF NOT EXISTS scoped_delivery_counters (
    site_id text NOT NULL, gateway_id text NOT NULL, kind text NOT NULL, total bigint NOT NULL,
    PRIMARY KEY (site_id, gateway_id, kind)
);
CREATE TABLE IF NOT EXISTS consumer_status (
    scope text PRIMARY KEY, heartbeat timestamptz NOT NULL, subscribed boolean NOT NULL
);
