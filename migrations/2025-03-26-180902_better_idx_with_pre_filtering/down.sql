-- This file should undo anything in `up.sql`
DROP INDEX shapes_linestring_index_intercity_rail IF EXISTS;
DROP INDEX shapes_linestring_index_local_rail IF EXISTS;
DROP INDEX shapes_linestring_index_ferry IF EXISTS;
DROP INDEX shapes_linestring_not_bus IF EXISTS;
DROP INDEX shapes_linestring_bus IF EXISTS;