-- Your SQL goes here
CREATE TABLE gtfs.unified_agency (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    name_translations JSONB,
    primary_level_0 TEXT,
    primary_level_1 TEXT,
    has_rail BOOLEAN NOT NULL,
    has_tram BOOLEAN NOT NULL,
    has_metro BOOLEAN NOT NULL,
    has_ferry BOOLEAN NOT NULL,
    has_bus BOOLEAN NOT NULL,
    is_national_railway_operator BOOLEAN NOT NULL,
    no_home_country_europe BOOLEAN NOT NULL,
    chateaux TEXT[] NOT NULL,
    level_0s TEXT[],
    level_1s TEXT[]
);

ALTER TABLE gtfs.agencies ADD COLUMN unified_agency_id TEXT;

CREATE INDEX idx_agencies_unified_agency_id ON gtfs.agencies (unified_agency_id);
