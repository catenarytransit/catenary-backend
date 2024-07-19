-- This file should undo anything in `up.sql`
DROP TABLE IF EXISTS gtfs.ip_geo_addr CASCADE;
DROP TABLE IF EXISTS gtfs.ip_addr_to_geo CASCADE;
DROP TABLE IF EXISTS gtfs.ip_to_geo_addr CASCADE;
DROP INDEX IF EXISTS gtfs.ip_addr_to_geo;
DROP INDEX IF EXISTS gtfs.ip_addr_to_geo;