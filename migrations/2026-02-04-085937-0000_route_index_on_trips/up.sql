-- Your SQL goes here
CREATE INDEX IF NOT EXISTS idx_trips_route_id ON gtfs.trips_compressed(route_id);