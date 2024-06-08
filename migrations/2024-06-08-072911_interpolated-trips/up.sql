-- Your SQL goes here
CREATE TABLE gtfs.direction_pattern (
    chateau_id text NOT NULL,
    direction_pattern_id text NOT NULL,
    stop_id text NOT NULL,
    stop_sequence OID NOT NULL,
    arrival_time_since_start int,
    departure_time_since_start int,
    interpolated_time_since_start int,
    onestop_feed_id text NOT NULL,
    attempt_id text NOT NULL,
    PRIMARY KEY (onestop_feed_id, attempt_id, direction_pattern_id, stop_sequence)
)

CREATE INDEX direction_pattern_chateau_idx ON gtfs.direction_pattern (chateau_id);
CREATE INDEX direction_pattern_chateau_and_stop ON gtfs.direction_pattern (chateau_id, stop_id);
CREATE INDEX direction_pattern_chateau_and_id ON gtfs.direction_pattern (chateau_id, direction_pattern_id);

CLUSTER gtfs.direction_pattern USING direction_pattern_chateau_idx;

CREATE TABLE gtfs.direction_pattern_meta (
    chateau_id text NOT NULL,
    direction_pattern_id text NOT NULL,
    headsign_or_destination text NOT NULL,
    gtfs_shape_id text,
    fake_shape boolean NOT NULL,
    onestop_feed_id text NOT NULL,
    attempt_id text NOT NULL,
    PRIMARY KEY (onestop_feed_id, attempt_id, direction_pattern_id)
)