ALTER TABLE gtfs.itinerary_pattern_meta ADD COLUMN row_count INT4 NOT NULL DEFAULT 0;
ALTER TABLE gtfs.direction_pattern_meta ADD COLUMN row_count INT4 NOT NULL DEFAULT 0;
