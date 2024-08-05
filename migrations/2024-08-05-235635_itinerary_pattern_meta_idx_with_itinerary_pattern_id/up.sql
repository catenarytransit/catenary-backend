-- Your SQL goes here
CREATE INDEX itinerary_pattern_meta_with_chateau_idx ON gtfs.itinerary_pattern_meta (chateau, itinerary_pattern_id);