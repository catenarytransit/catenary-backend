-- This file should undo anything in `up.sql`
ALTER TABLE gtfs.direction_pattern_meta DROP COLUMN route_type;
DROP INDEX IF EXISTS direction_route_type;
DROP INDEX IF EXISTS direction_chateau_route_type;