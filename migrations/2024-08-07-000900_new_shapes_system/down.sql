-- This file should undo anything in `up.sql`
CREATE TABLE IF NOT EXISTS gtfs.shapes (
    onestop_feed_id text NOT NULL,
    attempt_id text NOT NULL,
    shape_id text NOT NULL,
    linestring geometry(Linestring,4326) NOT NULL,
    color text,
    --remains null in case of no routes
    routes text[],
    route_type smallint NOT NULL,
    route_label text,
    route_label_translations jsonb,
    text_color text,
    chateau text NOT NULL,
    allowed_spatial_query boolean NOT NULL,
    PRIMARY KEY (onestop_feed_id, attempt_id, shape_id)
);

DROP INDEX shapes_linestring_index_intercity_rail;
DROP INDEX shapes_linestring_index_local_rail;
DROP INDEX shapes_linestring_index_ferry;