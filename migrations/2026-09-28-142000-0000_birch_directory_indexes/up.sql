-- Index the predicates used by Birch's /directory/v1 region and agency pages.
--
-- gtfs.unified_agency.id is already the PRIMARY KEY, so agency-by-id lookups
-- need no additional index.

CREATE INDEX idx_unified_agency_directory_primary_level_0
    ON gtfs.unified_agency (primary_level_0);

CREATE INDEX idx_unified_agency_directory_primary_level_1
    ON gtfs.unified_agency (primary_level_1);

-- The directory query uses @> for country membership and && for level-1
-- membership. GIN array_ops supports both operators.
CREATE INDEX idx_unified_agency_directory_level_0s_gin
    ON gtfs.unified_agency
    USING GIN (level_0s);

CREATE INDEX idx_unified_agency_directory_level_1s_gin
    ON gtfs.unified_agency
    USING GIN (level_1s);

-- National railway operators are intentionally included in the broad country
-- candidate query. Keep that OR branch cheap without indexing every FALSE row.
CREATE INDEX idx_unified_agency_directory_national_operator
    ON gtfs.unified_agency (is_national_railway_operator)
    WHERE is_national_railway_operator = TRUE;

-- Agency pages first resolve all feed agencies for a unified agency, then use
-- chateau/static feed pairs to count agencies in each feed. These covering
-- indexes avoid extra heap/table scans for those two access paths.
CREATE INDEX idx_agencies_directory_unified
    ON gtfs.agencies (unified_agency_id)
    INCLUDE (chateau, static_onestop_id, agency_id);

CREATE INDEX idx_agencies_directory_feed
    ON gtfs.agencies (chateau, static_onestop_id)
    INCLUDE (agency_id, unified_agency_id);
