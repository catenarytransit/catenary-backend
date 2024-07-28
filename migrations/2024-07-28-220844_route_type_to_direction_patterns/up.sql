-- Your SQL goes here
ALTER TABLE gtfs.direction_pattern_meta ADD route_type smallint;
CREATE INDEX direction_route_type ON gtfs.direction_pattern_meta (route_type);
CREATE INDEX direction_chateau_route_type ON gtfs.direction_pattern_meta (chateau, route_type);
CREATE INDEX direction_chateau_route_id ON gtfs.direction_pattern_meta (chateau, route_id);
CREATE INDEX direction_onestop_feed_id_route_id ON gtfs.direction_pattern_meta (onestop_feed_id, route_id);