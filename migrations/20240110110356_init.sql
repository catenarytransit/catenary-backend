-- Add migration script here
-- Initial version 3 of ingest: Kyler Chin
-- This was heavily inspired and copied from Emma Alexia, thank you Emma!

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS gtfs;
CREATE EXTENSION IF NOT EXISTS hstore;

CREATE TABLE IF NOT EXISTS gtfs.static_download_attempts (
   onestop_feed_id text NOT NULL,
   file_hash bigint NOT NULL,
   downloaded_unix_time_ms bigint NOT NULL,
   ingested boolean,
   failed boolean,
   PRIMARY KEY (onestop_feed_id, downloaded_unix_time_ms)
);

CREATE INDEX IF NOT EXISTS gtfs_static_download_attempts_file_hash ON gtfs.static_download_attempts (file_hash);

CREATE TABLE gtfs.static_feeds (
    onestop_feed_id text PRIMARY KEY,
    only_realtime_ref text,
    operators text[],
    operators_to_gtfs_ids JSONB,
    realtime_onestop_ids text[],
    realtime_onestop_ids_to_gtfs_ids JSONB,
    max_lat double precision NOT NULL,
    max_lon double precision NOT NULL,
    min_lat double precision NOT NULL,
    min_lon double precision NOT NULL,
    hull GEOMETRY(POLYGON,4326) NOT NULL
);

-- this dataset may be missing
-- if the feed start end date or end date is missing, replace the file
-- switch data asap ASAP if the start date is before the current date
-- time enable of new data when the current feed expires
CREATE TABLE gtfs.feed_info (
    onestop_feed_id text,
    feed_publisher_name text,
    feed_publisher_url text,
    feed_lang text,
    default_lang text,
    feed_start_date DATE,
    feed_end_date DATE,
    feed_version text,
    feed_contact_email text,
    feed_contact_url text,
    attempt_id text,
    PRIMARY KEY (onestop_feed_id, attempt_id, feed_publisher_name)
);

CREATE TABLE gtfs.operators (
    onestop_operator_id text PRIMARY KEY,
    name text,
    gtfs_static_feeds text[],
    gtfs_realtime_feeds text[],
    static_onestop_feeds_to_gtfs_ids JSONB,
    realtime_onestop_feeds_to_gtfs_ids JSONB
);

CREATE TABLE gtfs.realtime_feeds (
    onestop_feed_id text PRIMARY KEY,
    name text,
    operators text[],
    operators_to_gtfs_ids JSONB,
    max_lat double precision,
    max_lon double precision,
    min_lat double precision,
    min_lon double precision
);

CREATE TABLE gtfs.ingested (
    static_onestop_id text NOT NULL,
    file_hash bigint NOT NULL,
    attempt_id text NOT NULL,
    ingest_start_unix_time_ms bigint,
    ingesting_in_progress boolean,
    production boolean,
    deleted boolean,
    feed_expiration_date date,
    feed_start_date date,
    PRIMARY KEY (static_onestop_id, ingest_start_unix_time_ms)
);

CREATE TABLE gtfs.agencies (
    static_onestop_id text NOT NULL,
    -- fill with word "default" if this is empty
    agency_id text NOT NULL,
    attempt_id text NOT NULL,
    agency_name text NOT NULL,
    agency_name_lang hstore,
    agency_url text NOT NULL,
    agency_url_lang hstore,
    agency_timezone text NOT NULL,
    agency_lang text,
    agency_phone text,
    agency_fare_url	text,
    agency_fare_url_lang hstore,
    PRIMARY KEY (static_onestop_id, attempt_id, agency_id)
);

CREATE TABLE gtfs.routes (
    onestop_feed_id text NOT NULL,
    attempt_id text NOT NULL,
    route_id text NOT NULL,
    short_name text NOT NULL,
    short_name_lang hstore,
    long_name text NOT NULL,
    long_name_lang hstore,
    gtfs_desc text,
    route_type smallint NOT NULL,
    url text,
    url_lang hstore,
    agency_id text,
    gtfs_order int,
    color text,
    text_color text,
    continuous_pickup smallint,
    continuous_drop_off smallint,
    shapes_list text[],
    PRIMARY KEY (onestop_feed_id, attempt_id, route_id)
);

CREATE TABLE IF NOT EXISTS gtfs.shapes (
    onestop_feed_id text NOT NULL,
    attempt_id text NOT NULL,
    shape_id text NOT NULL,
    linestring GEOMETRY(LINESTRING, 4326) NOT NULL,
    color text,
    routes text[],
    route_type smallint NOT NULL,
    route_label text,
    text_color text,
    PRIMARY KEY (onestop_feed_id, attempt_id, shape_id)
);

CREATE TABLE gtfs.trips (
    trip_id text NOT NULL,
    onestop_feed_id text NOT NULL,
    attempt_id text NOT NULL,
    route_id text NOT NULL,
    service_id text NOT NULL,
    trip_headsign text,
    trip_headsign_lang hstore,
    has_stop_headsign boolean,
    stop_headsigns text[],
    trip_short_name text,
    direction_id int,
    block_id text,
    shape_id text,
    wheelchair_accessible int,
    bikes_allowed int,
    PRIMARY KEY (onestop_feed_id, attempt_id, trip_id)
);

CREATE TABLE gtfs.stops (
    onestop_feed_id text NOT NULL,
    attempt_id text NOT NULL,
    gtfs_id text NOT NULL,
    name text NOT NULL,
    name_lang hstore,
    displayname text NOT NULL,
    code text,
    gtfs_desc text,
    gtfs_desc_lang hstore,
    location_type smallint,
    parent_station text,
    zone_id text,
    url text,
    point GEOMETRY(POINT, 4326) NOT NULL,
    timezone text,
    wheelchair_boarding int,
    primary_route_type text,
    level_id text,
    platform_code text,
    platform_code_lang hstore,
    routes text[],
    route_types smallint[],
    children_ids text[],
    children_route_types smallint[],
    station_feature boolean,
    hidden boolean,
    location_alias text[],
    tts_stop_lang hstore,
    PRIMARY KEY (onestop_feed_id, attempt_id, gtfs_id)
);

CREATE TABLE gtfs.stoptimes (
    onestop_feed_id text NOT NULL,
    attempt_id text NOT NULL,
    trip_id text NOT NULL,
    stop_sequence int NOT NULL,
    arrival_time bigint,
    departure_time bigint,
    stop_id text NOT NULL,
    stop_headsign text,
    stop_headsign_lang text,
    pickup_type int,
    drop_off_type int,
    shape_dist_traveled double precision,
    timepoint int,
    continuous_pickup smallint,
    continuous_drop_off smallint,
    point GEOMETRY(POINT, 4326) NOT NULL,
    route_id text,
    PRIMARY KEY (onestop_feed_id, attempt_id, trip_id, stop_sequence)
);

CREATE INDEX gtfs_static_geom_idx ON gtfs.shapes USING GIST (linestring);
CREATE INDEX gtfs_static_stops_geom_idx ON gtfs.stops USING GIST (point);
CREATE INDEX gtfs_static_feed_id ON gtfs.shapes (onestop_feed_id);
CREATE INDEX gtfs_static_feed ON gtfs.routes (onestop_feed_id);
CREATE INDEX gtfs_static_route_type ON gtfs.routes (route_type);
CREATE INDEX static_hulls ON gtfs.static_feeds USING GIST (hull);

CREATE FUNCTION gtfs.busonly(z integer, x integer, y integer)
    RETURNS bytea AS $$
    DECLARE
    mvt bytea;
    BEGIN
    SELECT INTO mvt ST_AsMVT(tile, 'busonly', 4096, 'geom') FROM (
        SELECT
        ST_AsMVTGeom(
            ST_Transform(linestring, 3857),
            ST_TileEnvelope(z, x, y),
            4096, 64, true) AS geom,
            onestop_feed_id, shape_id, color, routes, route_type, route_label, text_color
        FROM gtfs.shapes
        WHERE (linestring && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND (route_type = 3 OR route_type = 11 OR route_type = 200)
    ) as tile WHERE geom IS NOT NULL;

    RETURN mvt;
    END
    $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION gtfs.notbus(z integer, x integer, y integer)
    RETURNS bytea AS $$
    DECLARE
    mvt bytea;
    BEGIN
    SELECT INTO mvt ST_AsMVT(tile, 'notbus', 4096, 'geom') FROM (
    SELECT
    ST_AsMVTGeom(
        ST_Transform(linestring, 3857),
        ST_TileEnvelope(z, x, y),
        4096, 64, true) AS geom,
        onestop_feed_id, shape_id, color, routes, route_type, route_label, text_color
    FROM gtfs.shapes
    WHERE (linestring && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND route_type != 3 AND route_type != 11
    ) as tile WHERE geom IS NOT NULL;

    RETURN mvt;
    END
    $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION gtfs.localrail(z integer, x integer, y integer)
    RETURNS bytea AS $$
    DECLARE
    mvt bytea;
    BEGIN
    SELECT INTO mvt ST_AsMVT(tile, 'localrail', 4096, 'geom') FROM (
    SELECT
    ST_AsMVTGeom(
        ST_Transform(linestring, 3857),
        ST_TileEnvelope(z, x, y),
        4096, 64, true) AS geom,
        onestop_feed_id, shape_id, color, routes, route_type, route_label, text_color
    FROM gtfs.shapes
    WHERE (linestring && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND (route_type = 0 OR route_type = 1 OR route_type = 5 OR route_type = 12)
    ) as tile WHERE geom IS NOT NULL;

    RETURN mvt;
    END
    $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION gtfs.intercityrail(z integer, x integer, y integer)
    RETURNS bytea AS $$
    DECLARE
    mvt bytea;
    BEGIN
    SELECT INTO mvt ST_AsMVT(tile, 'intercityrail', 4096, 'geom') FROM (
    SELECT
    ST_AsMVTGeom(
        ST_Transform(linestring, 3857),
        ST_TileEnvelope(z, x, y),
        4096, 64, true) AS geom,
        onestop_feed_id, shape_id, color, routes, route_type, route_label, text_color
    FROM gtfs.shapes
    WHERE (linestring && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND (route_type = 2)
    ) as tile WHERE geom IS NOT NULL;

    RETURN mvt;
    END
    $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION gtfs.other(z integer, x integer, y integer)
    RETURNS bytea AS $$
    DECLARE
    mvt bytea;
    BEGIN
    SELECT INTO mvt ST_AsMVT(tile, 'intercityrail', 4096, 'geom') FROM (
    SELECT
    ST_AsMVTGeom(
        ST_Transform(linestring, 3857),
        ST_TileEnvelope(z, x, y),
        4096, 64, true) AS geom,
        onestop_feed_id, shape_id, color, routes, route_type, route_label, text_color
    FROM gtfs.shapes
    WHERE (linestring && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND (route_type = 4 OR route_type = 6 OR route_type = 7)
    ) as tile WHERE geom IS NOT NULL;

    RETURN mvt;
    END
    $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION gtfs.stationfeatures(z integer, x integer, y integer)
    RETURNS bytea AS $$
    DECLARE
    mvt bytea;
    BEGIN
    SELECT INTO mvt ST_AsMVT(tile, 'stationfeatures', 4096, 'geom') FROM (
    SELECT
    ST_AsMVTGeom(
        ST_Transform(point, 3857),
        ST_TileEnvelope(z, x, y),
        4096, 64, true) AS geom,
        onestop_feed_id, name, displayname, code, gtfs_desc, location_type, parent_station, zone_id, url, timezone, wheelchair_boarding, level_id, platform_code, routes, route_types, children_ids, children_route_types
    FROM gtfs.stops
    WHERE (point && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND (location_type=2 OR location_type=3 OR location_type=4)
    ) as tile WHERE geom IS NOT NULL;

    RETURN mvt;
    END
    $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION gtfs.busstops(z integer, x integer, y integer)
    RETURNS bytea AS $$
    DECLARE
    mvt bytea;
    BEGIN
    SELECT INTO mvt ST_AsMVT(tile, 'busstops', 4096, 'geom') FROM (
    SELECT
    ST_AsMVTGeom(
        ST_Transform(point, 3857),
        ST_TileEnvelope(z, x, y),
        4096, 64, true) AS geom,
        onestop_feed_id,  REPLACE (name, 'Station','') as name, displayname, code, gtfs_desc, location_type, parent_station, zone_id, url, timezone, wheelchair_boarding, level_id, platform_code, routes, route_types, children_ids, children_route_types, hidden
    FROM gtfs.stops
    WHERE (point && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND (ARRAY[3,11,200,1700,1500,1702]::smallint[] && route_types::smallint[] OR ARRAY[3,11,200,1700,1500,1702]::smallint[] && children_route_types::smallint[]) AND hidden = false
    ) as tile WHERE geom IS NOT NULL;

    RETURN mvt;
    END
    $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION gtfs.railstops(z integer, x integer, y integer)
    RETURNS bytea AS $$
    DECLARE
    mvt bytea;
    BEGIN
    SELECT INTO mvt ST_AsMVT(tile, 'railstops', 4096, 'geom') FROM (
    SELECT
    ST_AsMVTGeom(
        ST_Transform(point, 3857),
        ST_TileEnvelope(z, x, y),
        4096, 64, true) AS geom,
        onestop_feed_id, REPLACE (name, 'Station','') as name, displayname, code, gtfs_desc, location_type, parent_station, zone_id, url, timezone, wheelchair_boarding, level_id, platform_code, routes, route_types, children_ids, children_route_types, hidden
    FROM gtfs.stops
    WHERE (point && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND (ARRAY[0,1,2,5,12]::smallint[] && route_types::smallint[] OR ARRAY[0,1,2,5,12]::smallint[] && children_route_types::smallint[]) AND hidden = false
    ) as tile WHERE geom IS NOT NULL;

    RETURN mvt;
    END
    $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION gtfs.otherstops(z integer, x integer, y integer)
    RETURNS bytea AS $$
    DECLARE
    mvt bytea;
    BEGIN
    SELECT INTO mvt ST_AsMVT(tile, 'otherstops', 4096, 'geom') FROM (
    SELECT
    ST_AsMVTGeom(
        ST_Transform(point, 3857),
        ST_TileEnvelope(z, x, y),
        4096, 64, true) AS geom,
        onestop_feed_id, REPLACE (name, 'Station','') as name, displayname, code, gtfs_desc, location_type, parent_station, zone_id, url, timezone, wheelchair_boarding, level_id, platform_code, routes, route_types, children_ids, children_route_types
    FROM gtfs.stops
    WHERE (point && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND (ARRAY[4,6,7]::smallint[] && route_types::smallint[] OR ARRAY[4,6,7]::smallint[] && children_route_types::smallint[])
    ) as tile WHERE geom IS NOT NULL;

    RETURN mvt;
    END
    $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;