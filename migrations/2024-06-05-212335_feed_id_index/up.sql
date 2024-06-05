-- Your SQL goes here
CREATE INDEX shapes_not_bus_feed_id_index ON gtfs.shapes_not_bus (onestop_feed_id);
CREATE INDEX shapes_feed_id_index ON gtfs.shapes (onestop_feed_id);
CREATE INDEX itinerary_pattern_feed_id_idx ON gtfs.itinerary_pattern (onestop_feed_id);
CREATE INDEX itinerary_pattern_meta_feed_id_idx ON gtfs.itinerary_pattern_meta (onestop_feed_id);
CREATE INDEX routes_feed_id_idx ON gtfs.routes (onestop_feed_id);