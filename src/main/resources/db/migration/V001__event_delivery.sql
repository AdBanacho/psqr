BEGIN;

CREATE TABLE accepted_event (
    id uuid PRIMARY KEY,
    payload text NOT NULL,
    accepted_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE tracked_item (
    item_id text COLLATE "C" PRIMARY KEY,
    latest_event_id uuid REFERENCES accepted_event(id)
);

CREATE TABLE event_outbox (
    event_id uuid NOT NULL REFERENCES accepted_event(id),
    destination text NOT NULL CHECK (destination IN ('statistics', 'subscription')),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    failure_count integer NOT NULL DEFAULT 0,
    last_error text,
    confirmed_at timestamptz,
    PRIMARY KEY (event_id, destination)
);

CREATE INDEX event_outbox_due ON event_outbox (next_attempt_at, event_id, destination)
    WHERE confirmed_at IS NULL;

CREATE TABLE statistics_processed (
    event_id uuid PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE daily_statistics (
    epoch_day bigint PRIMARY KEY,
    item_count bigint NOT NULL CHECK (item_count >= 0)
);

COMMIT;
