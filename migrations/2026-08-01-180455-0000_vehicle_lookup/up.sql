-- Your SQL goes here
CREATE INDEX vehicle_label_search_history_idx ON gtfs.basic_vehicle_history (unified_agency_id, vehicle_label, operation_date);