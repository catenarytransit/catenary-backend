-- This file should undo anything in `up.sql`
CREATE INDEX chateau_itin_pattern_idx ON gtfs.itinerary_pattern USING btree (chateau, itinerary_pattern_id);
 CREATE INDEX chateau_itin_pattern_idx ON gtfs.itinerary_pattern USING btree (chateau, itinerary_pattern_id);
 DROP INDEX chateau_itin_pattern_stop_number_idx;