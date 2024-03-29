// @generated automatically by Diesel CLI.

pub mod gtfs {
    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.agencies (static_onestop_id, attempt_id, agency_id) {
            static_onestop_id -> Text,
            agency_id -> Text,
            attempt_id -> Text,
            agency_name -> Text,
            agency_name_translations -> Nullable<Jsonb>,
            agency_url -> Text,
            agency_url_translations -> Nullable<Jsonb>,
            agency_timezone -> Text,
            agency_lang -> Nullable<Text>,
            agency_phone -> Nullable<Text>,
            agency_fare_url -> Nullable<Text>,
            agency_fare_url_translations -> Nullable<Jsonb>,
            chateau -> Text,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.calendar (onestop_feed_id, attempt_id, service_id) {
            onestop_feed_id -> Text,
            attempt_id -> Text,
            service_id -> Text,
            monday -> Bool,
            tuesday -> Bool,
            wednesday -> Bool,
            thursday -> Bool,
            friday -> Bool,
            saturday -> Bool,
            sunday -> Bool,
            gtfs_start_date -> Date,
            gtfs_end_date -> Date,
            chateau -> Text,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.calendar_dates (onestop_feed_id, service_id, gtfs_date) {
            onestop_feed_id -> Text,
            attempt_id -> Text,
            service_id -> Text,
            gtfs_date -> Date,
            exception_type -> Int2,
            chateau -> Text,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.chateaus (chateau) {
            chateau -> Text,
            static_feeds -> Array<Nullable<Text>>,
            realtime_feeds -> Array<Nullable<Text>>,
            languages_avaliable -> Array<Nullable<Text>>,
            hull -> Nullable<Geometry>,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.f_test (trip_id) {
            trip_id -> Text,
            f -> Nullable<Array<Nullable<TripFrequency>>>,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.feed_info (onestop_feed_id, attempt_id, feed_publisher_name) {
            onestop_feed_id -> Text,
            feed_publisher_name -> Text,
            feed_publisher_url -> Nullable<Text>,
            feed_lang -> Nullable<Text>,
            default_lang -> Nullable<Text>,
            feed_start_date -> Nullable<Date>,
            feed_end_date -> Nullable<Date>,
            feed_version -> Nullable<Text>,
            feed_contact_email -> Nullable<Text>,
            feed_contact_url -> Nullable<Text>,
            attempt_id -> Text,
            chateau -> Nullable<Text>,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.gtfs_errors (onestop_feed_id, attempt_id) {
            onestop_feed_id -> Text,
            error -> Text,
            attempt_id -> Text,
            file_hash -> Nullable<Text>,
            chateau -> Text,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.in_progress_static_ingests (onestop_feed_id, attempt_id) {
            onestop_feed_id -> Text,
            file_hash -> Text,
            attempt_id -> Text,
            ingest_start_unix_time_ms -> Int8,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.ingested_static (onestop_feed_id, attempt_id) {
            onestop_feed_id -> Text,
            file_hash -> Text,
            attempt_id -> Text,
            ingest_start_unix_time_ms -> Int8,
            ingest_end_unix_time_ms -> Int8,
            ingest_duration_ms -> Int4,
            ingesting_in_progress -> Bool,
            ingestion_successfully_finished -> Bool,
            ingestion_errored -> Bool,
            production -> Bool,
            deleted -> Bool,
            feed_expiration_date -> Nullable<Date>,
            feed_start_date -> Nullable<Date>,
            default_lang -> Nullable<Text>,
            languages_avaliable -> Array<Nullable<Text>>,
            ingestion_version -> Int4,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.realtime_feeds (onestop_feed_id) {
            onestop_feed_id -> Text,
            name -> Nullable<Text>,
            previous_chateau_name -> Text,
            chateau -> Text,
            fetch_interval_ms -> Nullable<Int4>,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.realtime_passwords (onestop_feed_id) {
            onestop_feed_id -> Text,
            passwords -> Nullable<Array<Nullable<Text>>>,
            header_auth_key -> Nullable<Text>,
            header_auth_value_prefix -> Nullable<Text>,
            url_auth_key -> Nullable<Text>,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.routes (onestop_feed_id, attempt_id, route_id) {
            onestop_feed_id -> Text,
            attempt_id -> Text,
            route_id -> Text,
            short_name -> Text,
            short_name_translations -> Nullable<Jsonb>,
            long_name -> Text,
            long_name_translations -> Nullable<Jsonb>,
            gtfs_desc -> Nullable<Text>,
            route_type -> Int2,
            url -> Nullable<Text>,
            url_translations -> Nullable<Jsonb>,
            agency_id -> Nullable<Text>,
            gtfs_order -> Nullable<Int4>,
            color -> Nullable<Text>,
            text_color -> Nullable<Text>,
            continuous_pickup -> Nullable<Int2>,
            continuous_drop_off -> Nullable<Int2>,
            shapes_list -> Nullable<Array<Nullable<Text>>>,
            chateau -> Text,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.shapes (onestop_feed_id, attempt_id, shape_id) {
            onestop_feed_id -> Text,
            attempt_id -> Text,
            shape_id -> Text,
            linestring -> Geometry,
            color -> Nullable<Text>,
            routes -> Nullable<Array<Nullable<Text>>>,
            route_type -> Int2,
            route_label -> Nullable<Text>,
            route_label_translations -> Nullable<Jsonb>,
            text_color -> Nullable<Text>,
            chateau -> Text,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.static_download_attempts (onestop_feed_id, downloaded_unix_time_ms) {
            onestop_feed_id -> Text,
            file_hash -> Nullable<Text>,
            downloaded_unix_time_ms -> Int8,
            ingested -> Bool,
            url -> Text,
            failed -> Bool,
            ingestion_version -> Int4,
            mark_for_redo -> Bool,
            http_response_code -> Nullable<Text>,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.static_feeds (onestop_feed_id) {
            onestop_feed_id -> Text,
            chateau -> Text,
            default_lang -> Nullable<Text>,
            languages_avaliable -> Array<Nullable<Text>>,
            previous_chateau_name -> Text,
            hull -> Nullable<Geometry>,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.static_passwords (onestop_feed_id) {
            onestop_feed_id -> Text,
            passwords -> Nullable<Array<Nullable<Text>>>,
            header_auth_key -> Nullable<Text>,
            header_auth_value_prefix -> Nullable<Text>,
            url_auth_key -> Nullable<Text>,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.stops (onestop_feed_id, attempt_id, gtfs_id) {
            onestop_feed_id -> Text,
            attempt_id -> Text,
            gtfs_id -> Text,
            name -> Text,
            name_translations -> Nullable<Jsonb>,
            displayname -> Text,
            code -> Nullable<Text>,
            gtfs_desc -> Nullable<Text>,
            gtfs_desc_translations -> Nullable<Jsonb>,
            location_type -> Nullable<Int2>,
            parent_station -> Nullable<Text>,
            zone_id -> Nullable<Text>,
            url -> Nullable<Text>,
            point -> Nullable<Geometry>,
            timezone -> Nullable<Text>,
            wheelchair_boarding -> Nullable<Int4>,
            primary_route_type -> Nullable<Text>,
            level_id -> Nullable<Text>,
            platform_code -> Nullable<Text>,
            platform_code_translations -> Nullable<Jsonb>,
            routes -> Nullable<Array<Nullable<Text>>>,
            route_types -> Nullable<Array<Nullable<Int2>>>,
            children_ids -> Nullable<Array<Nullable<Text>>>,
            children_route_types -> Nullable<Array<Nullable<Int2>>>,
            station_feature -> Nullable<Bool>,
            hidden -> Nullable<Bool>,
            chateau -> Text,
            location_alias -> Nullable<Array<Nullable<Text>>>,
            tts_stop_translations -> Nullable<Jsonb>,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.stoptimes (onestop_feed_id, attempt_id, trip_id, stop_sequence) {
            onestop_feed_id -> Text,
            attempt_id -> Text,
            trip_id -> Text,
            stop_sequence -> Int4,
            arrival_time -> Nullable<Oid>,
            departure_time -> Nullable<Oid>,
            stop_id -> Text,
            stop_headsign -> Nullable<Text>,
            stop_headsign_translations -> Nullable<Jsonb>,
            pickup_type -> Int2,
            drop_off_type -> Int2,
            shape_dist_traveled -> Nullable<Float4>,
            timepoint -> Bool,
            continuous_pickup -> Int2,
            continuous_drop_off -> Int2,
            point -> Nullable<Geometry>,
            route_id -> Text,
            chateau -> Text,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.trip_frequencies (onestop_feed_id, attempt_id, trip_id, index) {
            onestop_feed_id -> Text,
            trip_id -> Text,
            attempt_id -> Text,
            index -> Int2,
            start_time -> Oid,
            end_time -> Oid,
            headway_secs -> Oid,
            exact_times -> Bool,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.trips (onestop_feed_id, attempt_id, trip_id) {
            onestop_feed_id -> Text,
            trip_id -> Text,
            attempt_id -> Text,
            route_id -> Text,
            service_id -> Text,
            trip_headsign -> Nullable<Text>,
            trip_headsign_translations -> Nullable<Jsonb>,
            has_stop_headsigns -> Bool,
            stop_headsigns -> Nullable<Array<Nullable<Text>>>,
            trip_short_name -> Nullable<Text>,
            direction_id -> Nullable<Int2>,
            block_id -> Nullable<Text>,
            shape_id -> Nullable<Text>,
            wheelchair_accessible -> Nullable<Int2>,
            bikes_allowed -> Int2,
            chateau -> Text,
            frequencies -> Nullable<Array<Nullable<TripFrequency>>>,
            has_frequencies -> Bool,
        }
    }

    diesel::allow_tables_to_appear_in_same_query!(
        agencies,
        calendar,
        calendar_dates,
        chateaus,
        f_test,
        feed_info,
        gtfs_errors,
        in_progress_static_ingests,
        ingested_static,
        realtime_feeds,
        realtime_passwords,
        routes,
        shapes,
        static_download_attempts,
        static_feeds,
        static_passwords,
        stops,
        stoptimes,
        trip_frequencies,
        trips,
    );
}
