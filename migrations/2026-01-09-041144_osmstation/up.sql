-- OSM Station Import Files: Track imported files with versioned hashes
CREATE TABLE gtfs.osm_station_imports (
    import_id SERIAL PRIMARY KEY,
    file_name TEXT NOT NULL,
    file_hash TEXT NOT NULL,      -- SHA256 hash of file contents
    imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    station_count INTEGER NOT NULL DEFAULT 0,
    UNIQUE(file_name, file_hash)
);

-- OSM Stations: Rail/Tram/Subway stations extracted from OSM
CREATE TABLE gtfs.osm_stations (
    osm_id BIGINT NOT NULL,           -- OSM node/way/relation ID
    osm_type TEXT NOT NULL,           -- 'node', 'way', 'relation'
    import_id INTEGER NOT NULL REFERENCES gtfs.osm_station_imports(import_id) ON DELETE CASCADE,
    
    -- Location
    point Geometry(Point, 4326) NOT NULL,
    
    -- Names (multilingual)
    name TEXT,                         -- Default name (name tag)
    name_translations JSONB,           -- {"en": "...", "de": "...", "fr": "..."}
    
    -- Classification
    station_type TEXT,                 -- 'station', 'halt', 'stop_position', 'platform'
    railway_tag TEXT,                  -- Original railway tag value
    mode_type TEXT NOT NULL,           -- 'rail', 'tram', 'subway' (for matching)
    
    -- References
    uic_ref TEXT,                      -- UIC reference code
    ref TEXT,                          -- General reference
    wikidata TEXT,                     -- Wikidata ID (Q...)
    
    -- Metadata
    operator TEXT,
    network TEXT,
    
    PRIMARY KEY (osm_id, osm_type, import_id)
);

-- Spatial index for proximity queries
CREATE INDEX idx_osm_stations_point ON gtfs.osm_stations USING GIST (point);

-- Index for import lookup
CREATE INDEX idx_osm_stations_import ON gtfs.osm_stations (import_id);

-- Index for mode-specific queries during matching
CREATE INDEX idx_osm_stations_mode ON gtfs.osm_stations (mode_type);

-- Index for UIC reference lookup
CREATE INDEX idx_osm_stations_uic ON gtfs.osm_stations (uic_ref) WHERE uic_ref IS NOT NULL;

-- Add OSM station reference directly to stops table
ALTER TABLE gtfs.stops 
    ADD COLUMN osm_station_id BIGINT;
