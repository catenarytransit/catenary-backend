-- This file should undo anything in `up.sql`
ALTER TABLE gtfs.itinerary_pattern DROP COLUMN stop_headsign_idx;
ALTER TABLE gtfs.direction_pattern DROP COLUMN stop_headsign_idx;
ALTER TABLE gtfs.direction_pattern_meta DROP COLUMN stop_headsigns_unique_list;