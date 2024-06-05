-- This file should undo anything in `up.sql`
DROP INDEX IF EXISTS shapes_not_bus_feed_id_index;
DROP INDEX IF EXISTS shapes_feed_id_index;
DROP INDEX IF EXISTS itinerary_pattern_feed_id_idx;
DROP INDEX IF EXISTS itinerary_pattern_meta_feed_id_idx;
DROP INDEX IF EXISTS routes_feed_id_idx;