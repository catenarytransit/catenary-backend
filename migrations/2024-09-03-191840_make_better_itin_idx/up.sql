-- Your SQL goes here
DROP INDEX IF EXISTS itinerary_pattern_chateau_idx;
DROP INDEX IF EXISTS  chateau_itin_pattern_idx;
CREATE INDEX chateau_itin_pattern_stop_number_idx ON gtfs.itinerary_pattern USING btree (chateau, itinerary_pattern_id, stop_sequence);