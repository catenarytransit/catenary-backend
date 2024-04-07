-- Your SQL goes here
-- Add migration script here
-- Initial version 3 of ingest: Kyler Chin
-- This was heavily inspired and copied from Emma Alexia, thank you Emma!

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS gtfs;
CREATE EXTENSION IF NOT EXISTS hstore;

CREATE TABLE IF NOT EXISTS gtfs.static_download_attempts (
   onestop_feed_id text NOT NULL,
   file_hash text,
   downloaded_unix_time_ms bigint NOT NULL,
   ingested boolean NOT NULL,
   url text NOT NULL,
   failed boolean NOT NULL,
   ingestion_version integer NOT NULL,
   mark_for_redo boolean NOT NULL,
   http_response_code text,
   PRIMARY KEY (onestop_feed_id, downloaded_unix_time_ms)
);

CREATE TABLE gtfs.ingested_static (
    onestop_feed_id text NOT NULL,
    -- hash of the zip file
    file_hash text NOT NULL,
    attempt_id text NOT NULL,
    ingest_start_unix_time_ms bigint NOT NULL,
    ingest_end_unix_time_ms bigint NOT NULL,
    ingest_duration_ms int NOT NULL,
    ingesting_in_progress boolean NOT NULL,
    ingestion_successfully_finished boolean NOT NULL,
    ingestion_errored boolean NOT NULL,
    production boolean NOT NULL,
    deleted boolean NOT NULL,
    feed_expiration_date date,
    feed_start_date date,
    default_lang text,
    languages_avaliable text[] NOT NULL,
    ingestion_version integer NOT NULL,
    PRIMARY KEY (onestop_feed_id, attempt_id)
);

CREATE TABLE gtfs.in_progress_static_ingests (
    onestop_feed_id text NOT NULL,
    file_hash text NOT NULL,
    attempt_id text NOT NULL,
    ingest_start_unix_time_ms bigint NOT NULL,
    PRIMARY KEY (onestop_feed_id, attempt_id)
);

CREATE INDEX IF NOT EXISTS gtfs_static_download_attempts_file_hash ON gtfs.static_download_attempts (file_hash);

CREATE TABLE IF NOT EXISTS gtfs.static_feeds (
    onestop_feed_id text NOT NULL PRIMARY KEY,
    chateau text NOT NULL,
    default_lang text,
    languages_avaliable text[] NOT NULL,
    previous_chateau_name text NOT NULL,
    hull GEOMETRY(POLYGON,4326)
);

CREATE INDEX IF NOT EXISTS static_hulls ON gtfs.static_feeds USING GIST (hull);

CREATE TABLE gtfs.chateaus (
    chateau text NOT NULL PRIMARY KEY,
    static_feeds text[] NOT NULL,
    realtime_feeds text[] NOT NULL,
    languages_avaliable text[] NOT NULL,
    hull GEOMETRY(MultiPolygon,4326)
);

-- this dataset may be missing
-- if the feed start end date or end date is missing, replace the file
-- switch data asap ASAP if the start date is before the current date
-- time enable of new data when the current feed expires
CREATE TABLE  gtfs.feed_info (
    onestop_feed_id text NOT NULL,
    feed_publisher_name text NOT NULL,
    feed_publisher_url text NOT NULL,
    feed_lang text NOT NULL,
    default_lang text,
    feed_start_date DATE,
    feed_end_date DATE,
    feed_version text,
    feed_contact_email text,
    feed_contact_url text,
    attempt_id text NOT NULL,
    chateau text NOT NULL,
    PRIMARY KEY (onestop_feed_id, attempt_id, feed_publisher_name)
);

CREATE INDEX  chateau_feed_info ON gtfs.feed_info (chateau);

--CREATE TABLE gtfs.operators (
--    onestop_operator_id text PRIMARY KEY,
--    name text,
--    gtfs_static_feeds text[],
--    gtfs_realtime_feeds text[],
--    static_onestop_feeds_to_gtfs_ids jsonb,
--    realtime_onestop_feeds_to_gtfs_ids hstore
--);

CREATE TABLE gtfs.realtime_feeds (
    onestop_feed_id text PRIMARY KEY,
    name text,
    -- operators text[],
    -- operators_to_gtfs_ids jsonb,
    --max_lat double precision,
    --max_lon double precision,
    --min_lat double precision,
    --min_lon double precision,
    previous_chateau_name text NOT NULL,
    chateau text NOT NULL,
    fetch_interval_ms integer
);

CREATE TABLE gtfs.agencies (
    static_onestop_id text NOT NULL,
    -- Option<String> where None is a valid key
    agency_id text,
    attempt_id text NOT NULL,
    agency_name text NOT NULL,
    agency_name_translations jsonb,
    agency_url text NOT NULL,
    agency_url_translations jsonb,
    agency_timezone text NOT NULL,
    agency_lang text,
    agency_phone text,
    agency_fare_url	text,
    agency_fare_url_translations jsonb,
    chateau text NOT NULL,
    PRIMARY KEY (static_onestop_id, attempt_id, agency_id)
);

CREATE INDEX IF NOT EXISTS agencies_chateau ON gtfs.agencies (chateau);

CREATE TABLE gtfs.routes (
    onestop_feed_id text NOT NULL,
    attempt_id text NOT NULL,
    route_id text NOT NULL,
    short_name text,
    short_name_translations jsonb,
    long_name text,
    long_name_translations jsonb,
    gtfs_desc text,
    gtfs_desc_translations jsonb,
    route_type smallint NOT NULL,
    url text,
    url_translations jsonb,
    agency_id text,
    gtfs_order OID,
    color text,
    text_color text,
    continuous_pickup smallint NOT NULL,
    continuous_drop_off smallint NOT NULL,
    shapes_list text[],
    chateau text NOT NULL,
    PRIMARY KEY (onestop_feed_id, attempt_id, route_id)
);

CREATE INDEX gtfs_routes_chateau_index ON gtfs.routes (chateau);
CREATE INDEX gtfs_routes_type_index ON gtfs.routes (route_type);

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


CREATE INDEX IF NOT EXISTS shapes_chateau ON gtfs.shapes (chateau);
CREATE INDEX shapes_linestring_index ON gtfs.shapes USING GIST (linestring);
CREATE INDEX shapes_route_type ON gtfs.shapes (route_type);

CREATE TABLE IF NOT EXISTS gtfs.shapes_not_bus (
    onestop_feed_id text NOT NULL,
    attempt_id text NOT NULL,
    shape_id text NOT NULL,
    linestring geometry(Linestring,4326) NOT NULL,
    color text,
    --remain null in case of no shape
    routes text[],
    route_type smallint NOT NULL,
    route_label text,
    route_label_translations jsonb,
    text_color text, 
    chateau text NOT NULL,
    allowed_spatial_query boolean NOT NULL,
    PRIMARY KEY (onestop_feed_id, attempt_id, shape_id)
);

CREATE INDEX shapes_not_bus_linestring_index ON gtfs.shapes_not_bus USING GIST (linestring);
CREATE INDEX IF NOT EXISTS shapes_not_bus_chateau ON gtfs.shapes_not_bus (chateau);
CREATE INDEX shapes_not_bus_route_type ON gtfs.shapes (route_type);

-- no nulls so just contrain and unwrap ngl
CREATE TYPE trip_frequency_pre AS (
    start_time integer,
    end_time integer,
    headway_secs integer,
    -- false is zero [frequency based trips], true is schedule based
    -- None should return false
    exact_times boolean
);

create domain trip_frequency as trip_frequency_pre
check (
  (value).start_time is not null and 
  (value).start_time >=0 and 
  (value).end_time is not null and
  (value).end_time >= 0 and
  (value).headway_secs is not null and
  (value).headway_secs >= 0 and
  (value).exact_times is not null
);

CREATE TABLE gtfs.trips (
    onestop_feed_id text NOT NULL,
    trip_id text NOT NULL,
    attempt_id text NOT NULL,
    route_id text NOT NULL,
    service_id text NOT NULL,
    trip_headsign text,
    trip_headsign_translations jsonb,
    has_stop_headsigns boolean NOT NULL,
    stop_headsigns text[],
    trip_short_name text,
    direction_id smallint,
    block_id text,
    shape_id text,
    wheelchair_accessible smallint NOT NULL,
    bikes_allowed smallint NOT NULL,
    chateau text NOT NULL,
    frequencies trip_frequency[],
    has_frequencies boolean NOT NULL,
    PRIMARY KEY (onestop_feed_id, attempt_id, trip_id)
);

CREATE TABLE gtfs.trip_frequencies (
    onestop_feed_id text NOT NULL,
    trip_id text NOT NULL,
    attempt_id text NOT NULL,
    index smallint NOT NULL,
    start_time OID NOT NULL,
    end_time OID NOT NULL,
    headway_secs OID NOT NULL,
    -- a false means 0 or FrequencyBased, true means ScheduleBased or 1
    exact_times boolean NOT NULL,
    PRIMARY KEY (onestop_feed_id, attempt_id, trip_id, index)
);

CREATE TABLE gtfs.f_test (
trip_id text NOT NULL PRIMARY KEY,
f trip_frequency[]
);

CREATE INDEX IF NOT EXISTS trips_chateau ON gtfs.trips (chateau);

CREATE TABLE gtfs.stops (
    onestop_feed_id text NOT NULL,
    attempt_id text NOT NULL,
    gtfs_id text NOT NULL,
    name text,
    name_translations jsonb,
    displayname text,
    code text,
    gtfs_desc text,
    gtfs_desc_translations jsonb,
    location_type smallint NOT NULL,
    parent_station text,
    zone_id text,
    url text,
    point GEOMETRY(POINT, 4326),
    timezone text,
    wheelchair_boarding smallint NOT NULL,
    primary_route_type smallint,
    level_id text,
    platform_code text,
    platform_code_translations jsonb,
    routes text[] NOT NULL,
    route_types smallint[] NOT NULL,
    children_ids text[] NOT NULL,
    children_route_types smallint[] NOT NULL,
    station_feature boolean NOT NULL,
    hidden boolean NOT NULL,
    chateau text NOT NULL,
    location_alias text[],
    tts_name text,
    tts_name_translations jsonb,
    allowed_spatial_query boolean NOT NULL,
    PRIMARY KEY (onestop_feed_id, attempt_id, gtfs_id)
);

CREATE INDEX gtfs_static_stops_geom_idx ON gtfs.stops USING GIST (point);
CREATE INDEX stops_chateau_idx ON gtfs.stops (chateau);

CREATE TABLE gtfs.stoptimes (
    onestop_feed_id text NOT NULL,
    attempt_id text NOT NULL,
    trip_id text NOT NULL,
    stop_sequence int NOT NULL,
    arrival_time OID,
    departure_time OID,
    stop_id text NOT NULL,
    stop_headsign text,
    stop_headsign_translations jsonb,
    pickup_type smallint NOT NULL,
    drop_off_type smallint NOT NULL,
    shape_dist_traveled float4,
    -- true is 1, false is 0
    timepoint bool NOT NULL,
    continuous_pickup smallint NOT NULL,
    continuous_drop_off smallint NOT NULL,
  --  point GEOMETRY(POINT, 4326),
    route_id text NOT NULL,
    chateau text NOT NULL,
    PRIMARY KEY (onestop_feed_id, attempt_id, trip_id, stop_sequence)
);

CREATE INDEX stoptimes_chateau_idx ON gtfs.stoptimes (chateau);

CREATE TABLE gtfs.gtfs_errors (
onestop_feed_id text NOT NULL,
error text NOT NULL,
attempt_id text,
file_hash text,
chateau text NOT NULL,
PRIMARY KEY (onestop_feed_id, attempt_id)
);

CREATE TABLE gtfs.realtime_passwords (
    onestop_feed_id text NOT NULL PRIMARY KEY,
    passwords jsonb
);

CREATE TABLE gtfs.static_passwords (
    onestop_feed_id text NOT NULL PRIMARY KEY,
    passwords jsonb
);

CREATE TABLE gtfs.calendar_dates (
    onestop_feed_id text NOT NULL,
    attempt_id text NOT NULL,
    service_id text NOT NULL,
    gtfs_date date NOT NULL,
    exception_type smallint NOT NULL,
    chateau text NOT NULL,
    PRIMARY KEY (onestop_feed_id, service_id, gtfs_date)
);

CREATE INDEX calendar_dates_chateau ON gtfs.calendar_dates (chateau); 

CREATE TABLE gtfs.calendar (
    onestop_feed_id text NOT NULL,
    attempt_id text NOT NULL,
    service_id text NOT NULL,
    monday boolean NOT NULL,
    tuesday boolean NOT NULL,
    wednesday boolean NOT NULL,
    thursday boolean NOT NULL,
    friday boolean NOT NULL,
    saturday boolean NOT NULL,
    sunday boolean NOT NULL,
    gtfs_start_date date NOT NULL,
    gtfs_end_date date NOT NULL,
    chateau text NOT NULL,
    PRIMARY KEY (onestop_feed_id, attempt_id, service_id)
);

CREATE INDEX calendar_chateau ON gtfs.calendar (chateau); 

CREATE TABLE gtfs.chateau_metadata_last_updated_time (
    -- value should only be zero
    catenary smallint PRIMARY KEY,
    last_updated_ms bigint NOT NULL
);

-- translations does not need a table, values should be directly inserted into the data structure