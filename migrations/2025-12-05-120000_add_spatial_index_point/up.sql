CREATE INDEX idx_stations_point ON gtfs.stations USING GIST (point);
