-- Your SQL goes here
CREATE INDEX trips_compressed_idx_chateau_id_trip_id ON gtfs.trips_compressed (chateau, trip_id);
CLUSTER gtfs.trips_compressed USING trips_compressed_idx_chateau_id_trip_id;