-- This file should undo anything in `up.sql`
DROP TABLE IF EXISTS gtfs.direction_pattern;
DROP TABLE IF EXISTS gtfs.direction_pattern_meta;

DROP INDEX IF EXISTS gtfs.direction_pattern_chateau_idx;
DROP INDEX IF EXISTS gtfs.direction_pattern_chateau_and_stop;
DROP INDEX IF EXISTS gtfs.direction_pattern_chateau_and_id;

DROP INDEX IF EXISTS gtfs.direction_pattern_meta_chateau_idx;
DROP INDEX IF EXISTS gtfs.direction_pattern_meta_chateau_and_id;

ALTER TABLE gtfs.itinerary_pattern_meta DROP COLUMN IF EXISTS direction_pattern_id;

DROP INDEX IF EXISTS itinerary_pattern_direction_pattern_idx;