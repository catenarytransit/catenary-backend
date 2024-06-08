-- Your SQL goes here
CREATE TABLE gtfs.direction_pattern (
    chateau text NOT NULL,
    direction_pattern_id text NOT NULL,
    stop_id text NOT NULL,
    stop_sequence OID NOT NULL,
    arrival_time_since_start int,
    departure_time_since_start int,
    interpolated_time_since_start int,
    onestop_feed_id text NOT NULL,
    attempt_id text NOT NULL,
    PRIMARY KEY (onestop_feed_id, attempt_id, direction_pattern_id, stop_sequence)
);

CREATE INDEX direction_pattern_chateau_idx ON gtfs.direction_pattern (chateau);
CREATE INDEX direction_pattern_chateau_and_stop ON gtfs.direction_pattern (chateau, stop_id);
CREATE INDEX direction_pattern_chateau_and_id ON gtfs.direction_pattern (chateau, direction_pattern_id);

CLUSTER gtfs.direction_pattern USING direction_pattern_chateau_idx;

CREATE TABLE gtfs.direction_pattern_meta (
    chateau text NOT NULL,
    direction_pattern_id text NOT NULL,
    headsign_or_destination text NOT NULL,
    gtfs_shape_id text,
    fake_shape boolean NOT NULL,
    onestop_feed_id text NOT NULL,
    attempt_id text NOT NULL,
    PRIMARY KEY (onestop_feed_id, attempt_id, direction_pattern_id)
);

CREATE INDEX direction_pattern_meta_chateau_idx ON gtfs.direction_pattern_meta (chateau);
CREATE INDEX direction_pattern_meta_chateau_and_id ON gtfs.direction_pattern_meta (chateau, direction_pattern_id);

CLUSTER gtfs.direction_pattern_meta USING direction_pattern_meta_chateau_idx;

ALTER TABLE gtfs.itinerary_pattern_meta ADD COLUMN direction_pattern_id text;
ALTER TABLE gtfs.itinerary_pattern ADD COLUMN interpolated_time_since_start int;

CREATE INDEX itinerary_pattern_direction_pattern_idx ON gtfs.itinerary_pattern_meta (chateau, direction_pattern_id);