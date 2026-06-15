-- Your SQL goes here
CREATE TABLE gtfs.osm_stations_ranked (
    osm_id BIGINT NOT NULL,           -- OSM node/way/relation ID
    osm_type TEXT NOT NULL,           -- 'node', 'way', 'relation'
    
    run_id INTEGER NOT NULL,
    
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
    wikidata TEXT,                     -- Wikidata ID (Q...)
    
    -- Metadata
    operator TEXT,
    network TEXT,

    tram BOOLEAN NOT NULL,
    subway BOOLEAN NOT NULL,
    rail BOOLEAN NOT NULL,

    number_of_associated_stops INTEGER,

    platform_count INTEGER, -- we can omit this when importing trams or subways or other stuff
    terminal_route_count INTEGER NOT NULL,
    route_span_log INTEGER NOT NULL,
    degree_centrality INTEGER NOT NULL,
    --is_intermodal BOOLEAN NOT NULL,

    importance_level_station SMALLINT NOT NULL,
    -- There are 6 levels total for rail stations
    -- 1 for the highest level station (Toulouse, Marseille, Lyon, Paris, Munchen, Zurich, Bern, Praha, Frankfurt, Kassel, Koln, Bruxelles, Amsterdam, Hamburg, Stuttgart). 
    -- Level 2 is like Sion, Brig, Neuchatel, Ulm, Ingolstadt, Regensburg, Magdeburg
    -- Level 3 is like Poitiers, Geltendorf, 's-Hertogenbosch, Southampton
    -- The levels above it are for the in between stations like Derby, Swindon, Reading, Bellinzona
    
    -- then levels 7 to 9 are reserved for subway and tram stations. Subway and tram and light rail stations should NEVER recieve a ranking below 7.

    -- the other flags such as parent osm id are ignored because we are only picking parent stations for display purposes

    admin_hierarchy JSONB,

    -- level is dropped since it corrosponds to the location of the platform 
    -- parent osm id is dropped since everything here should be a parent station
    -- so are ref and local ref since they represent platform numbers

    -- Phase 2: Typographical Visibility & Overshadowing (The New Dimension)
    -- Dictates the minimum zoom level required to render the text label
    label_min_zoom SMALLINT NOT NULL DEFAULT 15,
    
    -- Dictates the minimum zoom level required to render the dot/icon
    icon_min_zoom SMALLINT NOT NULL DEFAULT 15,

    -- Tracks which primary station is suppressing this station's label
    overshadowed_by_osm_id BIGINT,
    overshadowed_by_osm_type TEXT,

    allowed_spatial_query BOOLEAN NOT NULL,

    PRIMARY KEY (osm_id, osm_type, run_id)
);

-- Spatial index for proximity queries
CREATE INDEX idx_osm_stations_ranked_point ON gtfs.osm_stations_ranked USING GIST (point);

CREATE INDEX run_id_osm_stations_ranked_point ON gtfs.osm_stations_ranked USING BTREE (run_id);

CREATE TABLE gtfs.osm_stations_ranking_runs (
    run_id INTEGER PRIMARY KEY,
    time_start TIMESTAMPTZ NOT NULL,
    time_end TIMESTAMPTZ,
    allowed_spatial_query BOOLEAN NOT NULL
);