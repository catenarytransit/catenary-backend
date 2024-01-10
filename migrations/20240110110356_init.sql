-- Add migration script here
-- Initial version 3 of ingest: Kyler Chin
-- This was heavily inspired and copied from Emma Alexia, thank you Emma!

CREATE SCHEMA IF NOT EXISTS gtfs;

CREATE TABLE IF NOT EXISTS gtfs.static_download_attempts (
   static_onestop_id text,
   file_hash text,
   downloaded_unix_time_ms bigint,
   ingested boolean,
   PRIMARY KEY (static_onestop_id, downloaded_unix_time_ms)
);

CREATE TABLE IF NOT EXISTS gtfs.ingested (
    static_onestop_id text,
    file_hash text,
    attempt_id text,
    ingest_start_unix_time_ms bigint,
    ingesting_in_progress boolean,
    production boolean,
    deleted boolean,
    PRIMARY KEY (static_onestop_id, ingest_start_unix_time_ms)
);