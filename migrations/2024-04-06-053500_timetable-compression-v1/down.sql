-- This file should undo anything in `up.sql`
CREATE TABLE IF NOT EXISTS gtfs.stoptimes (
    onestop_feed_id text NOT NULL,
    attempt_id text NOT NULL,
    trip_id text NOT NULL,
    stop_sequence int NOT NULL,
    arrival_time OID,
    departure_time OID,
    stop_id text NOT NULL,
    stop_headsign text,
    stop_headsign_translations jsonb,
    pickup_type smallint NOT NULL,
    drop_off_type smallint NOT NULL,
    shape_dist_traveled float4,
    -- true is 1, false is 0
    timepoint bool NOT NULL,
    continuous_pickup smallint NOT NULL,
    continuous_drop_off smallint NOT NULL,
  --  point GEOMETRY(POINT, 4326),
    route_id text NOT NULL,
    chateau text NOT NULL,
    PRIMARY KEY (onestop_feed_id, attempt_id, trip_id, stop_sequence)
);

CREATE INDEX IF NOT EXISTS stoptimes_chateau_idx ON gtfs.stoptimes (chateau);

CREATE TABLE IF NOT EXISTS gtfs.trips (
    onestop_feed_id text NOT NULL,
    trip_id text NOT NULL,
    attempt_id text NOT NULL,
    route_id text NOT NULL,
    service_id text NOT NULL,
    trip_headsign text,
    trip_headsign_translations jsonb,
    has_stop_headsigns boolean NOT NULL,
    stop_headsigns text[],
    trip_short_name text,
    direction_id smallint,
    block_id text,
    shape_id text,
    wheelchair_accessible smallint NOT NULL,
    bikes_allowed smallint NOT NULL,
    chateau text NOT NULL,
    frequencies trip_frequency[],
    has_frequencies boolean NOT NULL,
    PRIMARY KEY (onestop_feed_id, attempt_id, trip_id)
);

CREATE TABLE IF NOT EXISTS gtfs.trip_frequencies (
    onestop_feed_id text NOT NULL,
    trip_id text NOT NULL,
    attempt_id text NOT NULL,
    index smallint NOT NULL,
    start_time OID NOT NULL,
    end_time OID NOT NULL,
    headway_secs OID NOT NULL,
    -- a false means 0 or FrequencyBased, true means ScheduleBased or 1
    exact_times boolean NOT NULL,
    PRIMARY KEY (onestop_feed_id, attempt_id, trip_id, index)
);

CREATE INDEX IF NOT EXISTS trips_chateau ON gtfs.trips (chateau);

DROP TABLE gtfs.itinerary_pattern CASCADE;
DROP TABLE gtfs.itinerary_pattern_meta CASCADE;
DROP TABLE gtfs.trips_compressed CASCADE;

DROP INDEX IF EXISTS itinerary_pattern_chateau_idx;
DROP INDEX IF EXISTS trips_compressed_chateau_idx;

ALTER TABLE gtfs.routes
DROP COLUMN IF EXISTS stops;

DROP TABLE IF EXISTS gtfs.stopsforroute;

ALTER TABLE gtfs.realtime_passwords DROP COLUMN IF EXISTS last_updated_ms;
ALTER TABLE gtfs.static_passwords DROP COLUMN IF EXISTS last_updated_ms;

DROP TABLE IF EXISTS gtfs.credentials;
DROP TABLE IF EXISTS gtfs.admin_credentials;
