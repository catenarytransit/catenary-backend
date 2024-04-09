DROP TABLE IF EXISTS gtfs.stoptimes CASCADE;
DROP TABLE IF EXISTS gtfs.trips CASCADE;
DROP TABLE IF EXISTS gtfs.frequencies CASCADE;

-- row by row
CREATE TABLE gtfs.itinerary_pattern (
    onestop_feed_id text NOT NULL,
    attempt_id text NOT NULL,
    itinerary_pattern_id text NOT NULL,
    stop_sequence int NOT NULL,
    arrival_time_since_start int,
    departure_time_since_start int,
    stop_id text NOT NULL,
    chateau text NOT NULL,
    PRIMARY KEY (onestop_feed_id, attempt_id, itinerary_pattern_id, stop_sequence)
);

CREATE INDEX itinerary_pattern_chateau_idx ON gtfs.itinerary_pattern (chateau);

CREATE TABLE gtfs.itinerary_pattern_meta (
    onestop_feed_id text NOT NULL,
    route_id text NOT NULL,
    attempt_id text NOT NULL,
    trip_ids text[] NOT NULL,
    itinerary_pattern_id text NOT NULL,
    chateau text NOT NULL,
    trip_headsign text,
    trip_headsign_translations jsonb,
    shape_id text,
    timezone text NOT NULL,
    PRIMARY KEY (onestop_feed_id, attempt_id, itinerary_pattern_id)
);

CREATE TABLE gtfs.trips_compressed (
    onestop_feed_id text NOT NULL,
    trip_id text NOT NULL,
    attempt_id text NOT NULL,
    service_id text NOT NULL,
    trip_short_name text,
    direction_id boolean,
    block_id text,
    wheelchair_accessible smallint NOT NULL,
    bikes_allowed smallint NOT NULL,
    chateau text NOT NULL,
    frequencies bytea,
    has_frequencies boolean NOT NULL,
    itinerary_pattern_id text NOT NULL,
    compressed_trip_frequencies text,
    PRIMARY KEY (onestop_feed_id, attempt_id, trip_id)
);

CREATE INDEX trips_compressed_chateau_idx ON gtfs.trips_compressed (chateau);

CREATE TABLE gtfs.stopsforroute (
    onestop_feed_id text NOT NULL,
    attempt_id text NOT NULL,
    route_id text NOT NULL,
    stops bytea,
    chateau text NOT NULL,
    PRIMARY KEY (onestop_feed_id, attempt_id, route_id)
);