-- Persist GTFS transfers.txt per source import. Stop IDs are source-local.
-- Keep route/trip selectors even though the first Gentian transfer compiler
-- only supports stop-scoped rules; do not silently discard source semantics.
CREATE TABLE IF NOT EXISTS gtfs.transfers (
    id bigserial PRIMARY KEY,
    onestop_feed_id text NOT NULL,
    attempt_id text NOT NULL,
    from_stop_id text NOT NULL,
    to_stop_id text NOT NULL,
    transfer_type smallint NOT NULL CHECK (transfer_type BETWEEN 0 AND 5),
    min_transfer_seconds integer CHECK (min_transfer_seconds >= 0),
    from_route_id text,
    to_route_id text,
    from_trip_id text,
    to_trip_id text
);
CREATE INDEX IF NOT EXISTS gtfs_transfers_source
    ON gtfs.transfers (onestop_feed_id, attempt_id);
