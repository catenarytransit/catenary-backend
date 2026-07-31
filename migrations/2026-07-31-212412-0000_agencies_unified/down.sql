-- This file should undo anything in `up.sql`
ALTER TABLE gtfs.agencies DROP COLUMN unified_agency_id ;

DROP INDEX IF EXISTS gtfs.idx_agencies_unified_agency_id;

ALTER TABLE gtfs.agencies
    DROP COLUMN IF EXISTS unified_agency_id,
    DROP COLUMN IF EXISTS level_0s,
    DROP COLUMN IF EXISTS level_1s,
    DROP COLUMN IF EXISTS has_rail,
    DROP COLUMN IF EXISTS has_tram,
    DROP COLUMN IF EXISTS has_metro,
    DROP COLUMN IF EXISTS has_ferry,
    DROP COLUMN IF EXISTS has_bus;

DROP TABLE IF EXISTS gtfs.unified_agency;