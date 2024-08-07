-- Your SQL goes here
DROP TABLE IF EXISTS gtfs.shapes_not_bus;
CREATE INDEX shapes_linestring_index_intercity_rail ON gtfs.shapes USING GIST (linestring) WHERE route_type = 2;
CREATE INDEX shapes_linestring_index_local_rail ON gtfs.shapes USING GIST (linestring) WHERE route_type IN (0,1,5,7,11,12);
CREATE INDEX shapes_linestring_index_ferry ON gtfs.shapes USING GIST (linestring) WHERE route_type = 4;
CREATE INDEX shapes_linestring_not_bus ON gtfs.shapes USING GIST (linestring) WHERE route_type IN (0,1,2,4,5,7,11,12);