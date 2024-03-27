// @generated automatically by Diesel CLI.

pub mod gtfs {
    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;

        gtfs.agencies (static_onestop_id, attempt_id) {
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

        gtfs.ingested_static (static_onestop_id, ingest_start_unix_time_ms) {
            static_onestop_id -> Text,
            file_hash -> Int8,
            attempt_id -> Text,
            ingest_start_unix_time_ms -> Int8,
            ingesting_in_progress -> Nullable<Bool>,
            production -> Nullable<Bool>,
            deleted -> Nullable<Bool>,
            feed_expiration_date -> Nullable<Date>,
            feed_start_date -> Nullable<Date>,
            languages_avaliable -> Nullable<Array<Nullable<Text>>>,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;

        gtfs.realtime_feeds (onestop_feed_id) {
            onestop_feed_id -> Text,
            name -> Nullable<Text>,
            chateau -> Text,
            fetch_interval_ms -> Nullable<Int4>,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;

        gtfs.realtime_passwords (onestop_feed_id) {
            onestop_feed_id -> Text,
            passwords -> Nullable<Array<Nullable<Text>>>,
            header_auth_key -> Nullable<Text>,
            header_auth_value_prefix -> Nullable<Text>,
            url_auth_key -> Nullable<Text>,
            interval_ms -> Nullable<Int4>,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;

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

        gtfs.static_feeds (onestop_feed_id) {
            onestop_feed_id -> Text,
            chateau -> Text,
            previous_chateau_name -> Text,
            hull -> Nullable<Geometry>,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;

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
            point -> Geometry,
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

        gtfs.stoptimes (onestop_feed_id, attempt_id, trip_id, stop_sequence) {
            onestop_feed_id -> Text,
            attempt_id -> Text,
            trip_id -> Text,
            stop_sequence -> Int4,
            arrival_time -> Nullable<Int8>,
            departure_time -> Nullable<Int8>,
            stop_id -> Text,
            stop_headsign -> Nullable<Text>,
            stop_headsign_translations -> Nullable<Jsonb>,
            pickup_type -> Nullable<Int4>,
            drop_off_type -> Nullable<Int4>,
            shape_dist_traveled -> Nullable<Float8>,
            timepoint -> Nullable<Int4>,
            continuous_pickup -> Nullable<Int2>,
            continuous_drop_off -> Nullable<Int2>,
            point -> Geometry,
            route_id -> Nullable<Text>,
            chateau -> Text,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;

        gtfs.trips (onestop_feed_id, attempt_id, trip_id) {
            trip_id -> Text,
            onestop_feed_id -> Text,
            attempt_id -> Text,
            route_id -> Text,
            service_id -> Text,
            trip_headsign -> Nullable<Text>,
            trip_headsign_translations -> Nullable<Jsonb>,
            has_stop_headsign -> Nullable<Bool>,
            stop_headsigns -> Nullable<Array<Nullable<Text>>>,
            trip_short_name -> Nullable<Text>,
            direction_id -> Nullable<Int4>,
            block_id -> Nullable<Text>,
            shape_id -> Nullable<Text>,
            wheelchair_accessible -> Nullable<Int4>,
            bikes_allowed -> Nullable<Int4>,
            chateau -> Text,
        }
    }

    diesel::allow_tables_to_appear_in_same_query!(
        agencies,
        chateaus,
        feed_info,
        gtfs_errors,
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
        trips,
    );
}
