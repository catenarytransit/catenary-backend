-- Your SQL goes here
CREATE INDEX chateau_itin_pattern_idx ON gtfs.itinerary_pattern (chateau,itinerary_pattern_id);
CLUSTER gtfs.itinerary_pattern USING chateau_itin_pattern_idx;