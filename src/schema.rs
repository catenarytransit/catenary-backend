// @generated automatically by Diesel CLI.

pub mod gtfs {
    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.admin_credentials (email) {
            email -> Text,
            hash -> Text,
            salt -> Text,
            last_updated_ms -> Int8,
        }
    }

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

        gtfs.calendar_dates (onestop_feed_id, attempt_id, service_id, gtfs_date) {
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

        gtfs.chateau_metadata_last_updated_time (catenary) {
            catenary -> Int2,
            last_updated_ms -> Int8,
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

        gtfs.direction_pattern (onestop_feed_id, attempt_id, direction_pattern_id, stop_sequence) {
            chateau -> Text,
            direction_pattern_id -> Text,
            stop_id -> Text,
            stop_sequence -> Oid,
            arrival_time_since_start -> Nullable<Int4>,
            departure_time_since_start -> Nullable<Int4>,
            interpolated_time_since_start -> Nullable<Int4>,
            onestop_feed_id -> Text,
            attempt_id -> Text,
            stop_headsign_idx -> Nullable<Int2>,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.direction_pattern_meta (onestop_feed_id, attempt_id, direction_pattern_id) {
            chateau -> Text,
            direction_pattern_id -> Text,
            headsign_or_destination -> Text,
            gtfs_shape_id -> Nullable<Text>,
            fake_shape -> Bool,
            onestop_feed_id -> Text,
            attempt_id -> Text,
            route_id -> Nullable<Text>,
            route_type -> Nullable<Int2>,
            direction_id -> Nullable<Bool>,
            stop_headsigns_unique_list -> Nullable<Array<Nullable<Text>>>,
            direction_pattern_id_parents -> Nullable<Text>,
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
            feed_publisher_url -> Text,
            feed_lang -> Text,
            default_lang -> Nullable<Text>,
            feed_start_date -> Nullable<Date>,
            feed_end_date -> Nullable<Date>,
            feed_version -> Nullable<Text>,
            feed_contact_email -> Nullable<Text>,
            feed_contact_url -> Nullable<Text>,
            attempt_id -> Text,
            chateau -> Text,
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
            hash_of_file_contents -> Nullable<Text>,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.ip_addr_to_geo (range_start, range_end) {
            is_ipv6 -> Bool,
            range_start -> Inet,
            range_end -> Inet,
            country_code -> Nullable<Text>,
            geo_state -> Nullable<Text>,
            geo_state2 -> Nullable<Text>,
            city -> Nullable<Text>,
            postcode -> Nullable<Text>,
            latitude -> Float8,
            longitude -> Float8,
            timezone -> Nullable<Text>,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.itinerary_pattern (onestop_feed_id, attempt_id, itinerary_pattern_id, stop_sequence) {
            onestop_feed_id -> Text,
            attempt_id -> Text,
            itinerary_pattern_id -> Text,
            stop_sequence -> Int4,
            arrival_time_since_start -> Nullable<Int4>,
            departure_time_since_start -> Nullable<Int4>,
            stop_id -> Text,
            chateau -> Text,
            gtfs_stop_sequence -> Oid,
            interpolated_time_since_start -> Nullable<Int4>,
            timepoint -> Nullable<Bool>,
            stop_headsign_idx -> Nullable<Int2>,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.itinerary_pattern_meta (onestop_feed_id, attempt_id, itinerary_pattern_id) {
            onestop_feed_id -> Text,
            route_id -> Text,
            attempt_id -> Text,
            trip_ids -> Array<Nullable<Text>>,
            itinerary_pattern_id -> Text,
            chateau -> Text,
            trip_headsign -> Nullable<Text>,
            trip_headsign_translations -> Nullable<Jsonb>,
            shape_id -> Nullable<Text>,
            timezone -> Text,
            direction_pattern_id -> Nullable<Text>,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.osm_station_imports (import_id) {
            import_id -> Int4,
            file_name -> Text,
            file_hash -> Text,
            imported_at -> Timestamptz,
            station_count -> Int4,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.osm_stations (osm_id, osm_type, import_id) {
            osm_id -> Int8,
            osm_type -> Text,
            import_id -> Int4,
            point -> Geometry,
            name -> Nullable<Text>,
            name_translations -> Nullable<Jsonb>,
            station_type -> Nullable<Text>,
            railway_tag -> Nullable<Text>,
            mode_type -> Text,
            uic_ref -> Nullable<Text>,
            #[sql_name = "ref"]
            ref_ -> Nullable<Text>,
            wikidata -> Nullable<Text>,
            operator -> Nullable<Text>,
            network -> Nullable<Text>,
            level -> Nullable<Text>,
            local_ref -> Nullable<Text>,
            parent_osm_id -> Nullable<Int8>,
            is_derivative -> Bool,
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
            realtime_vehicle_positions -> Nullable<Text>,
            realtime_trip_updates -> Nullable<Text>,
            realtime_alerts -> Nullable<Text>,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.realtime_passwords (onestop_feed_id) {
            onestop_feed_id -> Text,
            passwords -> Nullable<Jsonb>,
            last_updated_ms -> Int8,
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
            short_name -> Nullable<Text>,
            short_name_translations -> Nullable<Jsonb>,
            long_name -> Nullable<Text>,
            long_name_translations -> Nullable<Jsonb>,
            gtfs_desc -> Nullable<Text>,
            gtfs_desc_translations -> Nullable<Jsonb>,
            route_type -> Int2,
            url -> Nullable<Text>,
            url_translations -> Nullable<Jsonb>,
            agency_id -> Nullable<Text>,
            gtfs_order -> Nullable<Oid>,
            color -> Nullable<Text>,
            text_color -> Nullable<Text>,
            continuous_pickup -> Int2,
            continuous_drop_off -> Int2,
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
            allowed_spatial_query -> Bool,
            stop_to_stop_generated -> Nullable<Bool>,
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
            passwords -> Nullable<Jsonb>,
            last_updated_ms -> Int8,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.stations (station_id) {
            station_id -> Text,
            name -> Text,
            point -> Geometry,
            is_manual -> Bool,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.stop_mappings (feed_id, stop_id) {
            feed_id -> Text,
            stop_id -> Text,
            station_id -> Text,
            match_score -> Float8,
            match_method -> Text,
            active -> Bool,
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
            name -> Nullable<Text>,
            name_translations -> Nullable<Jsonb>,
            displayname -> Nullable<Text>,
            code -> Nullable<Text>,
            gtfs_desc -> Nullable<Text>,
            gtfs_desc_translations -> Nullable<Jsonb>,
            location_type -> Int2,
            parent_station -> Nullable<Text>,
            zone_id -> Nullable<Text>,
            url -> Nullable<Text>,
            point -> Nullable<Geometry>,
            timezone -> Nullable<Text>,
            wheelchair_boarding -> Int2,
            primary_route_type -> Nullable<Int2>,
            level_id -> Nullable<Text>,
            platform_code -> Nullable<Text>,
            platform_code_translations -> Nullable<Jsonb>,
            routes -> Array<Nullable<Text>>,
            route_types -> Array<Nullable<Int2>>,
            children_ids -> Array<Nullable<Text>>,
            children_route_types -> Array<Nullable<Int2>>,
            station_feature -> Bool,
            hidden -> Bool,
            chateau -> Text,
            location_alias -> Nullable<Array<Nullable<Text>>>,
            tts_name -> Nullable<Text>,
            tts_name_translations -> Nullable<Jsonb>,
            allowed_spatial_query -> Bool,
            osm_station_id -> Nullable<Int8>,
            osm_platform_id -> Nullable<Int8>,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.stopsforroute (onestop_feed_id, attempt_id, route_id) {
            onestop_feed_id -> Text,
            attempt_id -> Text,
            route_id -> Text,
            stops -> Nullable<Bytea>,
            chateau -> Text,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.tile_storage (category, z, x, y) {
            category -> Int2,
            z -> Int2,
            x -> Int4,
            y -> Int4,
            mvt_data -> Bytea,
            added_time -> Timestamptz,
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

        gtfs.trips_compressed (onestop_feed_id, attempt_id, trip_id) {
            onestop_feed_id -> Text,
            trip_id -> Text,
            attempt_id -> Text,
            service_id -> Text,
            trip_short_name -> Nullable<Text>,
            direction_id -> Nullable<Bool>,
            block_id -> Nullable<Text>,
            wheelchair_accessible -> Int2,
            bikes_allowed -> Int2,
            chateau -> Text,
            frequencies -> Nullable<Bytea>,
            has_frequencies -> Bool,
            itinerary_pattern_id -> Text,
            route_id -> Text,
            start_time -> Oid,
        }
    }

    diesel::table! {
        use postgis_diesel::sql_types::*;
        use diesel::sql_types::*;
        use crate::custom_pg_types::*;

        gtfs.vehicles (file_path, key_str) {
            file_path -> Text,
            starting_range -> Nullable<Int4>,
            ending_range -> Nullable<Int4>,
            starting_text -> Nullable<Text>,
            ending_text -> Nullable<Text>,
            use_numeric_sorting -> Nullable<Bool>,
            manufacturer -> Nullable<Text>,
            model -> Nullable<Text>,
            years -> Nullable<Array<Nullable<Text>>>,
            engine -> Nullable<Text>,
            transmission -> Nullable<Text>,
            notes -> Nullable<Text>,
            key_str -> Text,
        }
    }

    diesel::joinable!(osm_stations -> osm_station_imports (import_id));
    diesel::joinable!(stop_mappings -> stations (station_id));

    diesel::allow_tables_to_appear_in_same_query!(
        admin_credentials,
        agencies,
        calendar,
        calendar_dates,
        chateau_metadata_last_updated_time,
        chateaus,
        direction_pattern,
        direction_pattern_meta,
        f_test,
        feed_info,
        gtfs_errors,
        in_progress_static_ingests,
        ingested_static,
        ip_addr_to_geo,
        itinerary_pattern,
        itinerary_pattern_meta,
        osm_station_imports,
        osm_stations,
        realtime_feeds,
        realtime_passwords,
        routes,
        shapes,
        static_download_attempts,
        static_feeds,
        static_passwords,
        stations,
        stop_mappings,
        stops,
        stopsforroute,
        tile_storage,
        trip_frequencies,
        trips_compressed,
        vehicles,
    );
}
