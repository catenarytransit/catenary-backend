CREATE INDEX IF NOT EXISTS idx_trips_compressed_trip_short_name ON gtfs.trips_compressed (trip_short_name) WHERE trip_short_name IS NOT NULL;
