\set ON_ERROR_STOP on
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS gtfs;
CREATE TABLE gtfs.chateaus (chateau text PRIMARY KEY, hull geometry(MultiPolygon,4326));
CREATE INDEX chateaus_test_hull_gist ON gtfs.chateaus USING gist (hull);
CREATE TABLE gtfs.static_feeds (onestop_feed_id text PRIMARY KEY, chateau text NOT NULL);
CREATE TABLE gtfs.ingested_static (
 onestop_feed_id text NOT NULL, attempt_id text NOT NULL,
 production boolean NOT NULL, ingestion_successfully_finished boolean NOT NULL,
 ingesting_in_progress boolean NOT NULL, ingestion_errored boolean NOT NULL,
 deleted boolean NOT NULL, ingest_end_unix_time_ms bigint NOT NULL,
 PRIMARY KEY(onestop_feed_id,attempt_id)
);
-- The geometry literal below is replaced by the Python fixture generator.
CREATE TEMP TABLE region AS SELECT
 ST_SetSRID(ST_GeomFromGeoJSON('__GEOJSON_GEOMETRY__'),4326) AS geom;
DO $$
DECLARE g geometry; p geometry; q geometry;
BEGIN
 SELECT ST_UnaryUnion(ST_CollectionExtract(geom,3)) INTO g FROM region;
 IF NOT ST_IsValid(g) OR ST_IsEmpty(g) THEN
   RAISE EXCEPTION 'invalid Geofabrik test geometry';
 END IF;
 p := ST_PointOnSurface(g);
 q := ST_PointOnSurface(ST_Difference(ST_Envelope(g),g));
 IF ST_IsEmpty(q) THEN RAISE EXCEPTION 'test region has no exterior bbox false positive'; END IF;
 INSERT INTO gtfs.chateaus(chateau,hull) VALUES
   ('test-inside',ST_Multi(ST_Buffer(p,0.001))),
   ('test-edge-overlap',ST_Multi(ST_Buffer(p,0.05))),
   ('test-hole',ST_Multi(ST_Buffer(q,0.00001))),
   ('test-outside',ST_Multi(ST_Buffer(ST_SetSRID(ST_MakePoint(0,0),4326),0.01)));
END $$;
INSERT INTO gtfs.static_feeds VALUES
  ('f-test-inside','test-inside'),('f-test-edge','test-edge-overlap');
INSERT INTO gtfs.ingested_static VALUES
  ('f-test-inside','active',true,true,false,false,false,100),
  ('f-test-inside','old',false,true,false,false,false,90),
  ('f-test-edge','active',true,true,false,false,false,100),
  ('f-test-edge','incomplete',true,false,false,false,false,200);
