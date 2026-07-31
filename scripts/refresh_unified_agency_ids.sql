UPDATE gtfs.agencies
SET unified_agency_id = replace(agency_name, ' ', '_')
WHERE unified_agency_id IS DISTINCT FROM replace(agency_name, ' ', '_');
