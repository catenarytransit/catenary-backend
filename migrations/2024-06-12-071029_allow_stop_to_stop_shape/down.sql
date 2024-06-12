-- This file should undo anything in `up.sql`
ALTER TABLE gtfs.direction_pattern_meta DROP COLUMN route_id;
ALTER TABLE gtfs.shapes DROP COLUMN stop_to_stop_generated;