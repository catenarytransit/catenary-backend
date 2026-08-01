-- This file should undo anything in `up.sql`
DROP TABLE IF EXISTS gtfs.basic_vehicle_history;
DROP TABLE IF EXISTS gtfs.basic_vehicles;
ALTER TABLE gtfs.unified_agency DROP COLUMN has_vehicle_histories;