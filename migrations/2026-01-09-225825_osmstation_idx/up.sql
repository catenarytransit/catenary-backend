-- Add index for efficient lookup of GTFS stops by OSM station ID
CREATE INDEX idx_stops_osm_station_id ON gtfs.stops (osm_station_id) 
    WHERE osm_station_id IS NOT NULL;

-- Add index for efficient OSM station lookup by osm_id
CREATE INDEX idx_osm_stations_osm_id ON gtfs.osm_stations (osm_id);
