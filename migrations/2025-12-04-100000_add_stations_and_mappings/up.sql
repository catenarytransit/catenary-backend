CREATE TABLE gtfs.stations (
    station_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    point Geometry(Point, 4326) NOT NULL,
    is_manual BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE gtfs.stop_mappings (
    feed_id TEXT NOT NULL,
    stop_id TEXT NOT NULL,
    station_id TEXT NOT NULL REFERENCES gtfs.stations(station_id),
    match_score FLOAT8 NOT NULL,
    match_method TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (feed_id, stop_id)
);

CREATE INDEX idx_stop_mappings_station_id ON gtfs.stop_mappings(station_id);
