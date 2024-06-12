-- Your SQL goes here
ALTER TABLE gtfs.direction_pattern_meta ADD route_id text;
ALTER TABLE gtfs.shapes ADD stop_to_stop_generated boolean;