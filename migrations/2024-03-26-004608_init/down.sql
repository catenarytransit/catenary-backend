-- This file should undo anything in `up.sql`
DROP SCHEMA IF EXISTS gtfs CASCADE;
DROP TYPE IF EXISTS trip_frequency CASCADE;
DROP TYPE IF EXISTS trip_frequency_pre CASCADE;
DROP DOMAIN IF EXISTS trip_frequency CASCADE;
DROP DOMAIN IF EXISTS trip_frequency_pre CASCADE;