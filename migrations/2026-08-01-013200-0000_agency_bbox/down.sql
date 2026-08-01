DROP INDEX IF EXISTS gtfs.idx_unified_agency_bbox;
DROP INDEX IF EXISTS gtfs.idx_agencies_bbox;

ALTER TABLE gtfs.unified_agency
    DROP COLUMN IF EXISTS bbox;

ALTER TABLE gtfs.agencies
    DROP COLUMN IF EXISTS bbox;
