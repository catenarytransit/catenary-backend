-- This file should undo anything in `up.sql`
DROP TABLE IF EXISTS gtfs.unified_agency;
ALTER TABLE gtfs.agencies DROP COLUMN unified_agency_id ;

DROP INDEX IF EXISTS idx_agencies_unified_agency_id ;