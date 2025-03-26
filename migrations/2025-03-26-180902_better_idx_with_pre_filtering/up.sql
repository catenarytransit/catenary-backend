-- Your SQL goes here
DROP INDEX  IF EXISTS gtfs.shapes_linestring_index_intercity_rail ;
DROP INDEX  IF EXISTS gtfs.shapes_linestring_index_local_rail ;
DROP INDEX IF EXISTS gtfs.shapes_linestring_index_ferry;
DROP INDEX IF EXISTS  gtfs.shapes_linestring_not_bus ;
DROP INDEX  IF EXISTS gtfs.shapes_linestring_bus;
CREATE INDEX shapes_linestring_index_intercity_rail ON gtfs.shapes USING GIST (linestring) WHERE route_type = 2 and allowed_spatial_query = true;
CREATE INDEX shapes_linestring_index_local_rail ON gtfs.shapes USING GIST (linestring) WHERE route_type IN (0,1,5,7,11,12) and allowed_spatial_query = true;
CREATE INDEX shapes_linestring_index_ferry ON gtfs.shapes USING GIST (linestring) WHERE route_type = 4 AND allowed_spatial_query = true;
CREATE INDEX shapes_linestring_bus ON gtfs.shapes USING GIST (linestring) WHERE route_type IN (3,11,200) AND allowed_spatial_query = true;