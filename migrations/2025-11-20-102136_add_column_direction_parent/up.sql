-- Your SQL goes here
ALTER TABLE gtfs.direction_pattern_meta ADD direction_pattern_id_parents TEXT;
CREATE INDEX direction_pattern_id_parents_idx ON gtfs.direction_pattern_meta (chateau, direction_pattern_id_parents);