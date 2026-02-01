-- Index to optimize queries filtering by chateau, itinerary_pattern_id, and service_id
CREATE INDEX trips_compressed_chateau_itin_service_idx 
ON gtfs.trips_compressed (chateau, itinerary_pattern_id, service_id);
