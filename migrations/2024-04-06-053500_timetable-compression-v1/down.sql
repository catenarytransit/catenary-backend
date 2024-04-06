-- This file should undo anything in `up.sql`
CREATE TABLE gtfs.stoptimes (
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

CREATE INDEX stoptimes_chateau_idx ON gtfs.stoptimes (chateau);