ALTER TABLE gtfs.agencies
    ADD COLUMN bbox geometry(Polygon, 4326);

ALTER TABLE gtfs.unified_agency
    ADD COLUMN bbox geometry(Polygon, 4326);

CREATE INDEX idx_agencies_bbox
    ON gtfs.agencies
    USING GIST (bbox);

CREATE INDEX idx_unified_agency_bbox
    ON gtfs.unified_agency
    USING GIST (bbox);
