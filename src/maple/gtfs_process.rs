// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Removal of the attribution is not allowed, as covered under the AGPL license

// Initial version 3 of ingest written by Kyler Chin
use crate::DownloadedFeedsInformation;
#[cfg(not(target_env = "msvc"))]
use tikv_jemalloc_ctl::{epoch, thread};

use crate::gtfs_handlers::colour_correction;
use crate::gtfs_handlers::colour_correction::fix_background_colour_rgb_feed_route;
use crate::gtfs_handlers::colour_correction::fix_foreground_colour_rgb_feed;
use crate::gtfs_handlers::shape_colour_calculator::ShapeToColourResponse;
use crate::gtfs_handlers::shape_colour_calculator::shape_to_colour;
use crate::gtfs_handlers::stops_associated_items::*;
use crate::gtfs_ingestion_sequence::calendar_into_postgres::calendar_into_postgres;
use crate::gtfs_ingestion_sequence::extra_stop_to_stop_shapes_into_postgres::insert_stop_to_stop_geometry;
use crate::gtfs_ingestion_sequence::shapes_into_postgres::shapes_into_postgres;
use crate::gtfs_ingestion_sequence::stops_into_postgres::stops_into_postgres_and_elastic;
use crate::shapes_reader::*;
use anyhow::Context;
use catenary::enum_to_int::*;
use catenary::gtfs_schedule_protobuf::frequencies_to_protobuf;
use catenary::maple_syrup;
use catenary::models::{
    DirectionPatternMeta, DirectionPatternRow, ItineraryPatternMeta, ItineraryPatternRow,
    Route as RoutePgModel,
};
use catenary::name_shortening_hash_insert_elastic;
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::route_id_transform;
use catenary::schedule_filtering::include_only_route_types;
use catenary::schedule_filtering::minimum_day_filter;
use chrono::NaiveDate;
use compact_str::CompactString;
use diesel::ExpressionMethods;
use diesel_async::RunQueryDsl;
use geo::BoundingRect;
use gtfs_structures::ContinuousPickupDropOff;
use gtfs_structures::FeedInfo;
use gtfs_structures::Gtfs;
use gtfs_translations::TranslationResult;
use gtfs_translations::translation_csv_text_to_translations;
use itertools::Itertools;
use language_tags::LanguageTag;
use prost::Message;
use regex::Regex;
use rgb::RGB;
use rgb::Rgb;
use serde_json::json;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;
use std::io::{self, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug)]
pub struct GtfsSummary {
    pub feed_start_date: Option<NaiveDate>,
    pub feed_end_date: Option<NaiveDate>,
    pub languages_avaliable: HashSet<String>,
    pub default_lang: Option<String>,
    pub general_timezone: String,
    pub bbox: Option<geo::Rect>,
}

const METRA_MINI_IDS: &[&str] = &["UW", "BN", "SW", "RI", "ME", "HC", "MW", "UNW", "UN"];

fn execute_pfaedle_rs(
    gtfs_path: &str,
    osm_path: &str,
    mots: Option<Vec<String>>,
    drop_shapes: bool,
    write_colours: bool,
) -> Result<(), Box<dyn Error + Sync + Send>> {
    let mut command_pfaedle = Command::new("pfaedle-rs");

    let mut run = command_pfaedle
        .arg("--gtfs-dir")
        .arg(&gtfs_path)
        .arg("--osm-file")
        .arg(&osm_path);

    if drop_shapes {
        run = run.arg("--wipe-shapes");
    }

    //run = run.arg("--skip-small-roads");
    run = run.arg("--low-priority");

    if write_colours {
        run = run.arg("--write-colours");
    }

    if let Some(mots) = mots {
        run = run.arg("--mots").arg(mots.iter().join(","));
    }

    // Use spawn() with inherited stdio to stream output in real-time
    run = run.stdout(Stdio::inherit()).stderr(Stdio::inherit());

    let mut child = run.spawn()?;

    println!("ran pfaedle for {}", gtfs_path);

    let status = child.wait()?;

    if !status.success() {
        return Err(format!("pfaedle-rs exited with status: {}", status).into());
    }

    Ok(())
}

// take a feed id and throw it into postgres
pub async fn gtfs_process_feed(
    gtfs_unzipped_path: &str,
    feed_id: &str,
    arc_conn_pool: Arc<CatenaryPostgresPool>,
    chateau_id: &str,
    attempt_id: &str,
    this_download_data: &DownloadedFeedsInformation,
    elasticclient: Option<&elasticsearch::Elasticsearch>,
) -> Result<GtfsSummary, Box<dyn Error + Send + Sync>> {
    let regex_train_starting = regex::RegexBuilder::new(r"^(train)")
        .case_insensitive(true)
        .build()
        .unwrap();

    println!("Begin feed {} processing", feed_id);
    let start = Instant::now();
    let conn_pool = arc_conn_pool.as_ref();

    //read the GTFS zip file
    let path = format!("{}/{}", gtfs_unzipped_path, feed_id);

    // Fix route colors before processing
    let _ = crate::gtfs_handlers::route_file_fixer::fix_gtfs_route_colors(&path);

    match feed_id {
        "f-gtfs~de" => {
            // Remove banned agencies (duplicates from other feeds) before processing
            crate::raw_file_agency_remover::remove_banned_agencies(
                path.as_str(),
                &[
                    "SNCF",
                    "SNCB",
                    "FlixBus-de",
                    "FlixTrain-de",
                    "SBB",
                    // f-münchner~verkehrsgesellschaft~mvg
                    "U-Bahn München",
                    "Stadtwerke München",
                    "Straßenbahn München",
                    "Österreichische Bundesbahnen",
                    "ÖBB",
                    "Kölner VB",
                    // f-u281z9-mvv
                    "Bus München",
                    "MVV-Regionalbus",
                    // f-münchner~verkehrsgesellschaft~mvg
                    "Straßenbahn München",
                    // covered in f-u0z-vgn
                    "VGN",
                    "Bus Nürnberg",
                    "U-Bahn Nürnberg",
                    "Berliner Verkehrsbetriebe",
                    "Karlsruher Verkehrsverbund",
                    "Hochbahn U-Bahn",
                    "RNV LU-MA (Strab)",
                    "Nachtverkehr Ausstieg",
                    // VBB
                    "S-Bahn Berlin GmbH",
                ],
            )?;

            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./railonly-europe-latest.osm.pbf",
                Some(vec![
                    String::from("rail"),
                    String::from("subway"),
                    String::from("tram"),
                ]),
                true,
                true,
            )?;

            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./pfaedle-filtered-germany-latest.osm.pbf",
                Some(vec![
                    String::from("bus"),
                    String::from("coach"),
                    String::from("trolleybus"),
                ]),
                true,
                true,
            )?;
        }
        "f-u3h-koleje~dolnoslaskie" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./railonly-europe-latest.osm.pbf",
                Some(vec![String::from("rail")]),
                true,
                false,
            )?;
        }
        "f-münchner~verkehrsgesellschaft~mvg" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./pfaedle-filtered-germany-latest.osm.pbf",
                None,
                false,
                true,
            )?;
        }
        "f-sf~bay~area~rg" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./railonly-north-america-latest.osm.pbf",
                Some(vec![String::from("rail")]),
                true,
                false,
            )?;
        }
        "f-seoulmetro" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./south-korea-latest.osm.pbf",
                None,
                true,
                false,
            )?;
        }
        "f-amtrak~gold~runner"
        | "f-northern~indiana~commuter~transportation~district"
        | "f-dp3-metra"
        | "f-dr5-mtanyclirr"
        | "f-dr7-mtanyc~metro~north"
        | "f-dr5r-mtasubway" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./railonly-north-america-latest.osm.pbf",
                None,
                true,
                false,
            )?;
        }
        "f-dp3-cta" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./cta-rail-filtered-illinois-latest.osm.pbf",
                Some(vec!["metro".to_string()]),
                true,
                false,
            )?;
        }
        "f-u05-tcl~systral" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./pfaedle-filtered-france-latest.osm.pbf",
                None,
                true,
                true,
            )?;
        }
        "f-bus~dft~gov~uk~england" | "f-bus~dft~gov~uk~scotland" | "f-bus~dft~gov~uk~wales" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./pfaedle-filtered-great-britain-latest.osm.pbf",
                None,
                true,
                true,
            )?;
        }
        "f-hauts~de~france~du~nord~1"
        | "f-dunkerque~fr"
        | "f-hauts~de~france~du~nord~2"
        | "f-rhdf62com~seau~interurbain~fr"
        | "f-rhdf80com~seau~interurbain~fr"
        | "f-rhdf80sco~seau~scolaire~fr"
        | "f-auvergne~rhône~alpes~oùra"
        | "f-hauts~de~france~loise~1" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./pfaedle-filtered-france-latest.osm.pbf",
                None,
                true,
                true,
            )?;
        }
        "f-hauts~de~france~pas~de~calais" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./pfaedle-filtered-france-latest.osm.pbf",
                None,
                false,
                true,
            )?;
        }
        "f-mobigo~nièvre" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./pfaedle-filtered-france-latest.osm.pbf",
                None,
                false,
                true,
            )?;
        }
        "f-mobigo~yonne" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./pfaedle-filtered-france-latest.osm.pbf",
                None,
                false,
                true,
            )?;
        }
        "f-hauts~de~france~somme" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./pfaedle-filtered-france-latest.osm.pbf",
                None,
                false,
                true,
            )?;
        }
        "f-ametis" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./pfaedle-filtered-france-latest.osm.pbf",
                None,
                false,
                true,
            )?;
        }
        "f-artis~arras" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./pfaedle-filtered-france-latest.osm.pbf",
                None,
                true,
                true,
            )?;
        }
        "f-hauts~de~france~laisne" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./pfaedle-filtered-france-latest.osm.pbf",
                None,
                false,
                true,
            )?;
        }
        "f-keolis~dijon~fr" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./pfaedle-filtered-france-latest.osm.pbf",
                None,
                true,
                true,
            )?;
        }
        "f-mobigo~côte~dor" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./pfaedle-filtered-france-latest.osm.pbf",
                None,
                true,
                true,
            )?;
        }
        //japan
        "f-jr~east" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./railway-filtered-japan-latest.osm.pbf",
                None,
                true,
                true,
            )?;
        }
        "f-mir~tsukuba" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./railway-filtered-japan-latest.osm.pbf",
                None,
                true,
                true,
            )?;
        }
        "f-tobu" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./railway-filtered-japan-latest.osm.pbf",
                None,
                true,
                true,
            )?;
        }
        "f-tokyo~metro" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./railway-filtered-japan-latest.osm.pbf",
                None,
                true,
                true,
            )?;
        }
        "f-toei~metro" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./railway-filtered-japan-latest.osm.pbf",
                None,
                true,
                true,
            )?;
        }
        "f-ouigo" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./railonly-europe-latest.osm.pbf",
                None,
                true,
                true,
            )?;
        }
        "f-thello" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./railonly-europe-latest.osm.pbf",
                None,
                true,
                true,
            )?;
        }
        "f-mavcsoport" | "f-u2e-idsjmk" | "f-pol~regio~pl" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./railonly-europe-latest.osm.pbf",
                None,
                true,
                true,
            )?;
        }
        "f-u3j-lodzka~kolej~aglomeracyjna" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./railonly-europe-latest.osm.pbf",
                None,
                true,
                true,
            )?;
        }
        "f-hrvatske~željeznice" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./railonly-europe-latest.osm.pbf",
                None,
                true,
                true,
            )?;
        }
        "f-pkp~intercity~pl" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./railonly-europe-latest.osm.pbf",
                None,
                true,
                true,
            )?;
        }
        "f-networkrail"
        | "f-czptt"
        | "f-oebb~at"
        | "f-u1j-kvbkölnerverkehrs~betriebeag~wupsiwupsigmbh~dbdeutschebahn" => {
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./railonly-europe-latest.osm.pbf",
                None,
                true,
                true,
            )?;
        }
        "f-u0-switzerland" => {
            // 2 passes requried
            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./railonly-europe-latest.osm.pbf",
                Some(vec![String::from("rail")]),
                false,
                true,
            )?;

            let _ = execute_pfaedle_rs(
                path.as_str(),
                "./switzerland-gtfs-pfaedle.osm.pbf",
                Some(
                    "subway,metro,bus,ferry,cablecar,funicular,coach,trolley,monorail,tram"
                        .split(",")
                        .map(String::from)
                        .collect::<Vec<String>>(),
                ),
                false,
                true,
            )?;
        }
        _ => {
            //no pfaedle needed
        }
    }

    let gtfs = gtfs_structures::GtfsReader::default()
        .read_shapes(false)
        .read(path.as_str())
        .context("Failed to read GTFS via gtfs_structures")?;

    let shapes_txt_path = format!("{}/{}/shapes.txt", gtfs_unzipped_path, feed_id);

    let gtfs_shapes_minimised = match std::path::Path::new(&shapes_txt_path).exists() {
        true => {
            let shapes_read = faster_shape_reader(Path::new(&shapes_txt_path).to_path_buf());

            shapes_read.ok()
        }
        false => None,
    };

    let gtfs: Gtfs = match feed_id {
        "f-dpz8-ttc" => {
            let mut gtfs = gtfs;

            let route_ids_to_keep: BTreeSet<String> = gtfs
                .routes
                .iter()
                .filter(|(_route_id, route)| {
                    route.route_type == gtfs_structures::RouteType::Subway
                    || route.id == "6" // Finch West LRT
                    || route.id == "5" // Eglinton LRT
                })
                .map(|(route_id, _route)| route_id)
                .cloned()
                .collect();

            println!(
                "keeping {} routes for TTC (Subway + LRT)",
                route_ids_to_keep.len()
            );

            let trips_to_keep: BTreeSet<String> = gtfs
                .trips
                .iter()
                .filter(|(_trip_id, trip)| route_ids_to_keep.contains(&trip.route_id))
                .map(|(trip_id, _trip)| trip_id)
                .cloned()
                .collect();

            let mut keep_stop_ids: BTreeSet<String> = BTreeSet::new();
            let mut keep_shapes: BTreeSet<String> = BTreeSet::new();

            for trip_id in &trips_to_keep {
                if let Some(trip) = gtfs.trips.get(trip_id) {
                    for stop_time in &trip.stop_times {
                        keep_stop_ids.insert(stop_time.stop.id.clone());
                    }

                    if let Some(shape_id) = &trip.shape_id {
                        keep_shapes.insert(shape_id.clone());
                    }
                }
            }

            let mut stop_ids_to_add = BTreeSet::new();

            for stop_id in &keep_stop_ids {
                if let Some(stop) = gtfs.stops.get(stop_id) {
                    if let Some(parent_station_id) = &stop.parent_station {
                        stop_ids_to_add.insert(parent_station_id.clone());

                        if let Some(parent_stop) = gtfs.stops.get(parent_station_id) {
                            if let Some(grandparent_station_id) = &parent_stop.parent_station {
                                stop_ids_to_add.insert(grandparent_station_id.clone());
                            }
                        }
                    }
                }
            }

            keep_stop_ids.extend(stop_ids_to_add);

            gtfs.routes
                .retain(|route_id, _| route_ids_to_keep.contains(route_id));

            gtfs.trips
                .retain(|trip_id, _| trips_to_keep.contains(trip_id));

            gtfs.stops
                .retain(|stop_id, _| keep_stop_ids.contains(stop_id));

            gtfs.shapes
                .retain(|shape_id, _| keep_shapes.contains(shape_id));

            println!("Filtered TTC Subway + Finch West LRT");
            gtfs.print_stats();
            gtfs
        }
        "f-dr5-nj~transit~rail~no~realtime" => {
            let route_types = vec![gtfs_structures::RouteType::Tramway];

            let gtfs = include_only_route_types(gtfs, route_types, true);

            println!("Filtered NJ Light Rail");
            gtfs.print_stats();
            gtfs
        }
        "f-dr5-nj~transit~bus" => {
            let route_types = vec![gtfs_structures::RouteType::Bus];

            let gtfs = include_only_route_types(gtfs, route_types, true);

            let mut gtfs = gtfs;

            let route_ids_to_delete = vec!["HBLR", "NLR", "PRIB", "RvLN"];

            gtfs.routes
                .retain(|route_id, _| !route_ids_to_delete.contains(&route_id.as_str()));

            gtfs.trips
                .retain(|trip_id, trip| !route_ids_to_delete.contains(&trip.route_id.as_str()));

            println!("Filtered NJ Light Rail Out");
            gtfs.print_stats();
            gtfs
        }
        "f-dpz-gotransit" | "f-dpz2-upexpress" => {
            let mut gtfs = gtfs;

            for trip in gtfs.trips.values_mut() {
                // remove stop headsigns such as "Kipling GO 08:42 - Union Station GO 09:03"
                for stoptime in trip.stop_times.iter_mut() {
                    stoptime.stop_headsign = None;
                }

                //trip id uses "YYYYMMDD-ROUTE-TRIP" format, "20260424-MI-2722" so extracting the last element is the trip number
                let trip_id_split: Vec<&str> = trip.id.split('-').collect();

                let trip_short_name = trip_id_split[trip_id_split.len() - 1];

                trip.trip_short_name = Some(trip_short_name.to_string());

                //forces recomputation downstream in Maple, so "MI - Union Station GO" will be replaced with "Union Station"
                trip.trip_headsign = None;
            }
            gtfs
        }
        "f-dr4-septa~rail" => {
            let mut gtfs = gtfs;

            for trip in gtfs.trips.values_mut() {
                // Trip ID uses "[route][train number]_[yyyymmdd]_[unknown]" format
                // Example: CYN1052_20260105_SID185785
                let trip_id_split: Vec<&str> = trip.id.split('_').collect();

                let first_component = trip_id_split[0];

                if first_component.chars().count() < 4 {
                    continue;
                }

                let trip_short_name: String = first_component.chars().skip(3).collect();

                trip.trip_short_name = Some(trip_short_name.to_string());
            }
            gtfs
        }
        "f-dp3-metra" => {
            let mut gtfs = gtfs;

            for trip in gtfs.trips.values_mut() {
                // Trip ID uses "[route ID]_[route mini-ID][train number]_[version numeric]_[version alphabetic]" format
                // Examples:
                // UP-W_UW50_V2_C
                // BNSF_BN1258_V2_C
                // SWS_SW809_V2_C
                // RI_RI413_V8_C
                // ME_ME621_V2_C
                // ME_ME321_V7_C
                // SWS_SW809_V2_C
                // HC_HC915_V1_C
                // MD-W_MW2217_V1_C
                // UP-NW_UNW648_V2_B
                // UP-N_UN327_V2_B

                let mini_and_number = match trip.id.split('_').nth(1) {
                    Some(x) => x,
                    None => continue, // malformed
                };

                let mut best_prefix: Option<&str> = None;

                for &prefix in METRA_MINI_IDS {
                    if mini_and_number.starts_with(prefix) {
                        best_prefix = match best_prefix {
                            None => Some(prefix),
                            Some(prev) if prefix.len() > prev.len() => Some(prefix),
                            Some(prev) => Some(prev),
                        };
                    }
                }

                if let Some(prefix) = best_prefix {
                    if let Some(rest) = mini_and_number.strip_prefix(prefix) {
                        trip.trip_short_name = Some(rest.to_string());
                    }
                }
            }
            gtfs
        }
        "f-sf~bay~area~rg" => {
            let mut gtfs = gtfs;

            for trip in gtfs.trips.values_mut() {
                if trip.route_id == "CE:ACE" || trip.route_id == "AM:CC" {
                    // remove stop headsigns
                    for stoptime in trip.stop_times.iter_mut() {
                        stoptime.stop_headsign = None;
                    }

                    // Trip ID uses "[operator]:[train number]" format
                    // Example: AM:521, CE:ACE01
                    let trip_id_split: Vec<&str> = trip.id.split(':').collect();

                    let trip_short_name = trip_id_split[trip_id_split.len() - 1];

                    trip.trip_short_name = Some(trip_short_name.to_string());

                    // forces headsign recomputation downstream in Maple
                    trip.trip_headsign = None;
                }
            }
            gtfs
        }
        "f-dq-mtamaryland~marc" => {
            let mut gtfs = gtfs;

            for route in gtfs.routes.values_mut() {
                if let Some(long_name) = route.long_name.as_deref() {
                    match long_name {
                        "PENN - WASHINGTON" => {
                            route.short_name = Some("Penn".to_string());
                            route.long_name = Some("MARC Penn Line".to_string());
                            route.color = Some(Rgb {
                                r: 0xC6,
                                g: 0x21,
                                b: 0x41,
                            });
                        }
                        "CAMDEN - WASHINGTON" => {
                            route.short_name = Some("Camden".to_string());
                            route.long_name = Some("MARC Camden Line".to_string());
                            route.color = Some(Rgb {
                                r: 0xEF,
                                g: 0x5B,
                                b: 0x2A,
                            });
                        }
                        "BRUNSWICK - WASHINGTON" => {
                            route.short_name = Some("Brunswick".to_string());
                            route.long_name = Some("MARC Brunswick Line".to_string());
                            route.color = Some(Rgb {
                                r: 0xEF,
                                g: 0xAC,
                                b: 0x1F,
                            });
                        }
                        _ => {}
                    }
                }
            }

            gtfs
        }
        "f-hongkong~justusewheels" => {
            let route_types = vec![gtfs_structures::RouteType::Subway];

            let gtfs = include_only_route_types(gtfs, route_types, true);

            let mut gtfs = gtfs;

            gtfs
        }
        "f-amtrak~gold~runner" => {
            let mut gtfs = gtfs;

            for (trip_id, trip) in gtfs.trips.iter_mut() {
                let last_st = trip.stop_times.last();

                if let Some(last_st) = last_st {
                    trip.trip_headsign = Some(last_st.stop.name.as_ref().unwrap().clone());
                }

                for stoptime in trip.stop_times.iter_mut() {
                    stoptime.stop_headsign = None;
                }
            }

            gtfs
        }
        "f-viarail~traindecharlevoix" => {
            let mut gtfs = gtfs;

            gtfs.routes.retain(|route_id, route| route_id != "119-120");

            gtfs.trips
                .retain(|trip_id, trip| trip.route_id != "119-120");

            gtfs
        }
        "f-sncf~fr" => {
            let mut gtfs = gtfs;

            for (trip_id, trip) in gtfs.trips.iter_mut() {
                if trip.trip_short_name.is_none() && trip.trip_headsign.is_some() {
                    trip.trip_short_name = trip.trip_headsign.clone();
                    trip.trip_headsign = None;
                }
            }

            gtfs
        }
        "f-r6-nswtrainlink~sydneytrains~buswayswesternsydney~interlinebus" => {
            //there's 8184 school buses in the feed. I'm removing them lmfao.
            let mut gtfs = gtfs;

            let route_ids_to_keep = gtfs
                .routes
                .iter()
                .filter_map(|(route_id, route)| match route.desc {
                    Some(ref desc) => {
                        if desc.as_str().contains("School") {
                            None
                        } else {
                            Some(route_id)
                        }
                    }
                    _ => Some(route_id),
                })
                .cloned()
                .collect::<BTreeSet<_>>();

            let trips_to_keep = gtfs
                .trips
                .iter()
                .filter_map(|(trip_id, trip)| {
                    route_ids_to_keep
                        .contains(&trip.route_id)
                        .then_some(trip_id)
                })
                .cloned()
                .collect::<BTreeSet<_>>();

            gtfs.trips
                .retain(|trip_id, _| trips_to_keep.contains(trip_id));

            gtfs.routes
                .retain(|route_id, _| route_ids_to_keep.contains(route_id));

            println!("Filtered NSW, removed school buses");
            gtfs.print_stats();
            gtfs
        }
        "f-r7h-translink" => {
            let mut gtfs = gtfs;

            let mut cut_route_ids: HashSet<String> = HashSet::new();

            let mut gtfs_routes_new: HashMap<String, gtfs_structures::Route> = HashMap::new();

            for (route_id, route) in gtfs.routes.iter() {
                if route_id.contains("-") {
                    cut_route_ids.insert(route_id.to_string());

                    let new_route_id = route_id.split('-').collect::<Vec<&str>>()[0].to_string();

                    let mut route = route.clone();

                    route.id = new_route_id.clone();
                    gtfs_routes_new.insert(new_route_id, route);
                } else {
                    gtfs_routes_new.insert(route_id.to_string(), route.clone());
                }
            }

            gtfs.routes = gtfs_routes_new;

            for (trip_id, trip) in gtfs.trips.iter_mut() {
                if cut_route_ids.contains(&trip.route_id) {
                    let new_route_id =
                        trip.route_id.split('-').collect::<Vec<&str>>()[0].to_string();
                    trip.route_id = new_route_id;
                }
            }

            gtfs
        }
        "f-los~angeles~international~airport~shuttle" => {
            let mut gtfs = gtfs;

            gtfs.routes.iter_mut().for_each(|(route_id, route)| {
                route.long_name = route.long_name.as_ref().map(|x| {
                    x.replace("Metro Connector GL", "Metro Connector C")
                        .to_string()
                });
            });

            gtfs
        }
        "f-gtfs~de" => crate::gtfs_handlers::gtfs_de_cleanup::gtfs_de_cleanup(gtfs),
        "f-u0-switzerland" => crate::gtfs_handlers::gtfs_de_cleanup::gtfs_ch_cleanup(gtfs),
        "f-nvbw" => crate::gtfs_handlers::remove_agencies::remove_agencies(
            gtfs,
            &Vec::from([
                String::from("DB AG"),
                String::from("FlixTrain-de"),
                String::from("SNCF"),
                String::from("Schweizerische Bundesbahnen SBB"),
                String::from("Schweiz. Schifffahrtsgesellschaft Untersee und Rhein AG"),
                String::from("FlixBus-de"),
                String::from("NVBW"),
                String::from("OEBB Personenverkehr AG Kundenservice"),
                String::from("DB Regio AG Bayern"),
                String::from("DB Fernverkehr AG"),
                String::from("SNCF Voyages Deutschland"),
                String::from("MVV-Regionalbus"),
                String::from("U-Bahn München"),
                String::from("Straßenbahn München"),
                String::from("Regionalverkehr Oberbayer (überregional)"),
                String::from("Bus München"),
                String::from("NachtTram München"),
                String::from("NachtBus München"),
                String::from("Nahreisezug"),
            ]),
        ),
        "f-ahverkehrsverbund~schleswig~holstein~nah" => {
            crate::gtfs_handlers::remove_agencies::remove_agencies(
                gtfs,
                &Vec::from([String::from("DB Fernverkehr (Codesharing)")]),
            )
        }
        "f-u4-ruter~flybussen~stfoldkollektivtrafikk~hedmarktrafikk~oppla" => {
            crate::gtfs_handlers::remove_agencies::remove_agencies(
                gtfs,
                &Vec::from([String::from("Avinor")]),
            )
        }
        "f-sp9x-normandie" => crate::gtfs_handlers::remove_agencies::remove_agencies(
            gtfs,
            &Vec::from([String::from("Nomad Train (SNCF, Région Normandie)")]),
        ),
        "f-gbwc-mobibreizh" => crate::gtfs_handlers::remove_agencies::remove_agencies(
            gtfs,
            &Vec::from([String::from("TER BreizhGo")]),
        ),
        "f-Ostösterreich~Österreich" => crate::gtfs_handlers::remove_agencies::remove_agencies(
            gtfs,
            &Vec::from([String::from("Wiener Linien GmbH & Co KG")]),
        ),
        "f-lax~flyaway" => {
            let mut gtfs = gtfs;

            for (trip_id, trip) in gtfs.trips.iter_mut() {
                for stop_time in trip.stop_times.iter_mut() {
                    stop_time.stop_headsign = None;
                }
            }

            gtfs
        }
        "f-dr5r-mtasubway" => {
            //https://www.mta.info/document/134521
            //Align the GTFS schedule schedule ids for the New York City Subway to the realtime ids by dropping the first underscore
            let mut gtfs = gtfs;

            let mut new_trips = HashMap::new();

            for (trip_id, trip) in gtfs.trips {
                let new_trip_id_split = trip_id.split_once('_');

                let matched_new_trip_id = match new_trip_id_split {
                    None => trip_id,
                    Some(new_trip_id_split) => new_trip_id_split.1.to_string(),
                };

                let mut trip = trip;

                trip.id = matched_new_trip_id.clone();

                new_trips.insert(matched_new_trip_id, trip);
            }

            gtfs.trips = new_trips;

            gtfs
        }
        _ => gtfs,
    };

    let mut gtfs = gtfs;

    if feed_id == "f-genentech" {
        let allowed_routes = [
            "1glenparki",
            "1glenparko",
            "2SSFI",
            "2SSFO",
            "3oysterpointferryo",
            "3OysterPointFerryi",
        ];

        gtfs.routes
            .retain(|route_id, _| allowed_routes.contains(&route_id.as_str()));

        gtfs.trips
            .retain(|_, trip| allowed_routes.contains(&trip.route_id.as_str()));
    }

    if feed_id == "f-u0-sncf~tgv" || feed_id == "f-u0-sncf~ter" || feed_id == "f-u0-sncf~intercites"
    {
        gtfs.trips = gtfs
            .trips
            .into_iter()
            .map(|(trip_id, trip)| {
                let mut trip = trip;
                if trip.trip_short_name == None {
                    trip.trip_short_name = trip.trip_headsign.clone();
                    trip.trip_headsign = None;
                }
                (trip_id, trip)
            })
            .collect();
    }

    if feed_id == "f-uc~irvine~anteater~express" {
        gtfs.routes
            .retain(|route_id, route| route.long_name.as_deref() != Some("Emergency Management"));
    }

    if feed_id == "f-9q5-metro~losangeles~rail" {
        for (route_id, route) in gtfs.routes.iter_mut() {
            if (route.long_name.as_deref() == Some("Metro C Line")) {
                //      route.long_name = Some("Metro Chin Line".to_string())
            }

            if (route.long_name.as_deref() == Some("Metro K Line")) {
                //       route.long_name = Some("Metro Kyler Line".to_string())
            }
        }
    }

    if feed_id == "f-9mu-orangecountytransportationauthority" {
        gtfs.agencies = gtfs
            .agencies
            .into_iter()
            .map(|agency| {
                let mut agency = agency;
                //      agency.name = "Metro OC Bus, a subdivision of LA Metro".to_string();
                agency
            })
            .collect();

        gtfs.routes.remove("400");
        gtfs.routes.remove("401");
        gtfs.routes.remove("403");

        let mut trips_to_remove: Vec<String> = vec![];

        for (trip_id, trip) in gtfs.trips.iter() {
            if trip.route_id.as_str() == "401"
                || trip.route_id.as_str() == "400"
                || trip.route_id.as_str() == "403"
            {
                trips_to_remove.push(trip_id.clone());
            }
        }

        for trip_to_remove in trips_to_remove {
            gtfs.trips.remove(&trip_to_remove);
        }
    }

    if feed_id == "f-9q5-metro~losangeles" {
        for (route_id, route) in gtfs.routes.iter_mut() {
            if route.long_name.as_deref() == Some("Metro G Line (Orange) 901") {
                route.long_name = Some("G 901".to_string())
            }

            if route.long_name.as_deref() == Some("Metro J Line (Silver) 910/950") {
                route.long_name = Some("J 910/950".to_string())
            }
        }

        gtfs.routes = gtfs
            .routes
            .into_iter()
            //remove anything from the route id that is the hyphen - or after
            .map(|(route_id, mut route)| {
                if let Some(hyphen_index) = route_id.find("-") {
                    route.id = route_id[0..hyphen_index].to_string();
                }
                (route.id.clone(), route)
            })
            .collect();

        //apply the same to the route id in the trip

        gtfs.trips = gtfs
            .trips
            .into_iter()
            //remove anything from the route id that is the hyphen - or after
            .map(|(trip_id, mut trip)| {
                if let Some(hyphen_index) = trip.route_id.find("-") {
                    trip.route_id = trip.route_id[0..hyphen_index].to_string();
                }
                (trip_id, trip)
            })
            .collect();
    }

    if feed_id == "f-9q5-metro~losangeles~rail" {
        for (trip_id, trip) in gtfs.trips.iter_mut() {
            for stop_t in trip.stop_times.iter_mut() {
                if let Some(headsign) = &mut stop_t.stop_headsign {
                    if headsign.contains("-") {
                        let split = headsign.split("-").collect::<Vec<&str>>();

                        *headsign = split[1].to_string();
                    }
                }
            }
        }
    }

    let today = chrono::Utc::now().naive_utc().date();

    let number_of_days = match feed_id {
        "f-gtfs~de" => 7,
        _ => 20,
    };

    let mut gtfs = minimum_day_filter(gtfs, today - chrono::Duration::days(number_of_days));

    println!(
        "Finished reading GTFS for {}, took {:?}",
        feed_id, gtfs.read_duration
    );

    // Read Translations.txt, don't fail if it doesn't exist
    let translation_path = format!("{}/{}/translations.txt", gtfs_unzipped_path, feed_id);
    let translation_data = std::fs::read_to_string(translation_path);

    let gtfs_translations: Option<TranslationResult> = match translation_data {
        Ok(data) => match translation_csv_text_to_translations(data.as_str()) {
            Ok(translations) => Some(translations),
            Err(_) => None,
        },
        Err(_) => None,
    };

    let default_lang = match gtfs.feed_info.len() {
        0 => match gtfs.agencies.len() {
            0 => Some(String::from("en")),
            _ => gtfs.agencies[0].lang.clone(),
        },
        _ => gtfs.feed_info[0].default_lang.clone(),
    };

    let mut gtfs_summary = GtfsSummary {
        feed_start_date: None,
        feed_end_date: None,
        languages_avaliable: HashSet::new(),
        default_lang: default_lang.clone(),
        general_timezone: match gtfs.agencies.len() {
            0 => String::from("Etc/UTC"),
            _ => gtfs.agencies[0].timezone.clone(),
        },
        bbox: None,
    };

    let start_optimization_timer = Instant::now();
    maple_syrup::service_optimisation::optimise_services(&mut gtfs);
    println!(
        "Optimized services for {} in {:?}",
        feed_id,
        start_optimization_timer.elapsed()
    );

    let start_reduction_timer = Instant::now();
    let reduction = maple_syrup::reduce(&gtfs);
    println!(
        "Reduced schedule for {} in {:?}",
        feed_id,
        start_reduction_timer.elapsed()
    );
    println!(
        "{} itineraries, {} trips, {:.2} ratio",
        reduction.itineraries.len(),
        reduction.trips_to_itineraries.len(),
        reduction.trips_to_itineraries.len() as f64 / reduction.itineraries.len() as f64
    );

    let mut route_to_direction_patterns: HashMap<String, Vec<&maple_syrup::DirectionPattern>> =
        HashMap::new();
    for (_, direction_pattern) in &reduction.direction_patterns {
        route_to_direction_patterns
            .entry(direction_pattern.route_id.to_string())
            .or_default()
            .push(direction_pattern);
    }

    let feed_info: Option<FeedInfo> = match !gtfs.feed_info.is_empty() {
        true => Some(gtfs.feed_info[0].clone()),
        false => None,
    };

    if let Some(feed_info) = &feed_info {
        gtfs_summary.feed_start_date = feed_info.start_date;
        gtfs_summary.feed_end_date = feed_info.end_date;

        if let Some(default_lang) = &feed_info.default_lang {
            gtfs_summary.default_lang = Some(default_lang.clone());
            gtfs_summary
                .languages_avaliable
                .insert(default_lang.clone());
        }
    }

    //copy the avaliable languages from the translations.txt file over
    if let Some(gtfs_translations) = &gtfs_translations {
        for avaliable_language in &gtfs_translations.avaliable_languages {
            gtfs_summary
                .languages_avaliable
                .insert(avaliable_language.as_str().to_string());
        }
    }

    println!(
        "Making stop to route type and route id hashmaps for {}",
        feed_id
    );
    let timer_stop_id_table = Instant::now();
    let (stop_ids_to_route_types, stop_ids_to_route_ids) =
        make_hashmap_stops_to_route_types_and_ids(&gtfs);

    let (stop_id_to_children_ids, stop_ids_to_children_route_types) =
        make_hashmaps_of_children_stop_info(&gtfs, &stop_ids_to_route_types);

    println!(
        "Finished making stop to route type and route id hashmaps in {:?} for {}",
        timer_stop_id_table.elapsed(),
        feed_id
    );

    //identify colours of shapes based on trip id's route id
    // also make reverse lookup for route ids to shape ids
    let ShapeToColourResponse {
        shape_to_color_lookup,
        shape_to_text_color_lookup,
        shape_id_to_route_ids_lookup,
        route_ids_to_shape_ids,
    } = shape_to_colour(feed_id, &gtfs);

    // Free memory from trips as they are no longer needed
    // The data has been extracted into reduction and shape_to_colour results
    println!("Dropping trips for {} to save memory...", feed_id);
    gtfs.trips.clear();
    gtfs.trips.shrink_to_fit();

    // Force jemalloc garbage collection
    #[cfg(not(target_env = "msvc"))]
    {
        epoch::advance().unwrap();
    }

    let conn_pool = arc_conn_pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let mut conn = conn_pre?;

    //insert agencies
    let mut agency_id_already_done: HashSet<Option<&String>> = HashSet::new();

    for agency in &gtfs.agencies {
        use catenary::schema::gtfs::agencies::dsl::agencies;

        if !agency_id_already_done.contains(&agency.id.as_ref()) {
            let agency_row = catenary::models::Agency {
                static_onestop_id: feed_id.to_string(),
                agency_id: agency.id.clone().unwrap_or_else(|| "".to_string()),
                attempt_id: attempt_id.to_string(),
                agency_name: agency.name.clone(),
                agency_name_translations: None,
                agency_url_translations: None,
                agency_url: agency.url.clone(),
                agency_fare_url: agency.fare_url.clone(),
                agency_fare_url_translations: None,
                chateau: chateau_id.to_string(),
                agency_lang: agency.lang.clone(),
                agency_phone: agency.phone.clone(),
                agency_timezone: agency.timezone.clone(),
            };

            diesel::insert_into(agencies)
                .values(agency_row)
                .execute(&mut conn)
                .await?;

            agency_id_already_done.insert(agency.id.as_ref());
        } else {
            eprintln!("Warning! Duplicate agency id found: \n{:?}", agency);
        }
    }

    println!("Agency insertion done for {}", feed_id);

    drop(agency_id_already_done);

    println!("Inserting shapes for {}", feed_id);

    //shove raw geometry into postgresql

    use catenary::postgres_tools::check_is_active;
    if !check_is_active(&mut conn).await {
        conn = conn_pool.get().await?;
    }

    shapes_into_postgres(
        &gtfs,
        &shape_to_color_lookup,
        &shape_to_text_color_lookup,
        feed_id,
        Arc::clone(&arc_conn_pool),
        chateau_id,
        attempt_id,
        &shape_id_to_route_ids_lookup,
        &gtfs_shapes_minimised,
    )
    .await?;

    println!("Shapes inserted for {}", feed_id);

    //insert calendar

    println!("Inserting calendar for {}", feed_id);

    // check_is_active check not needed for pool based functions unless explicitly desired, relying on internal pool behavior
    // but we can ensure our held connection is good if we used it, but here we call a function using pool.

    calendar_into_postgres(
        &gtfs,
        feed_id,
        Arc::clone(&arc_conn_pool),
        chateau_id,
        attempt_id,
    )
    .await?;

    println!("Calendar inserted for {}", feed_id);
    println!("Inserting stops for {}", feed_id);

    //insert stops
    // check_postgres_alive(&conn_pool).await?; // relying on pool
    stops_into_postgres_and_elastic(
        &gtfs,
        feed_id,
        Arc::clone(&arc_conn_pool),
        chateau_id,
        attempt_id,
        &stop_ids_to_route_types,
        &stop_ids_to_route_ids,
        &stop_id_to_children_ids,
        &stop_ids_to_children_route_types,
        gtfs_translations.as_ref(),
        &default_lang,
        elasticclient,
    )
    .await?;

    println!("Stops inserted for {}", feed_id);

    // insert trip and itineraries

    println!("Inserting directions for {}", feed_id);

    for group in &reduction.direction_patterns.iter().chunks(20000) {
        let mut d_final: Vec<DirectionPatternMeta> = vec![];

        let mut d_rows: Vec<Vec<DirectionPatternRow>> = vec![];

        for (direction_pattern_id, direction_pattern) in group {
            let gtfs_shape_id = match &direction_pattern.gtfs_shape_id {
                Some(gtfs_shape_id) => gtfs_shape_id.clone(),
                None => direction_pattern_id.to_string(),
            };

            let first_itin_id = reduction
                .direction_pattern_id_to_itineraries
                .get(direction_pattern_id)
                .unwrap()
                .iter()
                .next()
                .expect("Expected Itin for direction id");

            let itin_pattern = reduction
                .itineraries
                .get(first_itin_id)
                .expect("Did not find itin pattern, crashing....");

            if direction_pattern.gtfs_shape_id.is_none() {
                //: postgis_diesel::types::LineString<postgis_diesel::types::Point>

                let stop_points = direction_pattern
                    .stop_sequence
                    .iter()
                    .filter_map(|stop_id| gtfs.stops.get(stop_id.as_str()))
                    .filter(|stop| {
                        stop.latitude.is_some() && stop.longitude.is_some()
                            &&
                            //not within 1 degree of null is_null_island
                            !(
                                stop.latitude.unwrap() > -1.0
                                    && stop.latitude.unwrap() < 1.0
                                    && stop.longitude.unwrap() > -1.0
                                    && stop.longitude.unwrap() < 1.0
                            )
                    })
                    .filter_map(|stop| match (stop.latitude, stop.longitude) {
                        (Some(latitude), Some(longitude)) => Some(postgis_diesel::types::Point {
                            y: latitude,
                            x: longitude,
                            srid: Some(4326),
                        }),
                        _ => None,
                    })
                    .collect::<Vec<postgis_diesel::types::Point>>();

                if stop_points.len() > 2 {
                    let linestring: postgis_diesel::types::LineString<
                        postgis_diesel::types::Point,
                    > = postgis_diesel::types::LineString {
                        points: stop_points,
                        srid: Some(4326),
                    };

                    //insert into shapes and shapes_not_bus

                    let route = gtfs.routes.get(direction_pattern.route_id.as_str());

                    if let Some(route) = route {
                        let _ = insert_stop_to_stop_geometry(
                            feed_id,
                            attempt_id,
                            chateau_id,
                            route,
                            *direction_pattern_id,
                            &linestring,
                            Arc::clone(&arc_conn_pool),
                        )
                        .await;
                    }
                }
            }

            let direction_pattern_meta = DirectionPatternMeta {
                chateau: chateau_id.to_string(),
                direction_pattern_id: direction_pattern_id.to_string(),
                headsign_or_destination: match chateau_id {
                    "santacruzmetro" => match &direction_pattern.stop_headsigns_unique_list {
                        Some(list) => list.join(","),
                        _ => itin_pattern
                            .stop_sequences
                            .last()
                            .and_then(|x| x.stop_headsign.clone())
                            .unwrap_or_else(|| "".to_string()),
                    },
                    "montebellobuslines" => gtfs
                        .stops
                        .get(itin_pattern.stop_sequences.last().unwrap().stop_id.as_str())
                        .as_deref()
                        .unwrap()
                        .name
                        .clone()
                        .unwrap_or_default(),
                    _ => direction_pattern
                        .headsign_or_destination
                        .clone()
                        .unwrap_or_else(|| "".to_string()),
                },
                gtfs_shape_id: Some(gtfs_shape_id.clone()),
                fake_shape: direction_pattern.gtfs_shape_id.is_none(),
                onestop_feed_id: feed_id.to_string(),
                attempt_id: attempt_id.to_string(),
                route_id: Some(itin_pattern.route_id.clone()),
                route_type: Some(itin_pattern.route_type),
                direction_id: itin_pattern.direction_id,
                direction_pattern_id_parents: Some(
                    direction_pattern
                        .direction_pattern_id_with_parents
                        .clone()
                        .to_string(),
                ),
                stop_headsigns_unique_list: itin_pattern.stop_headsigns_unique_list.as_ref().map(
                    |x| {
                        x.into_iter()
                            .map(|x| Some(x.to_string()))
                            .collect::<Vec<Option<String>>>()
                    },
                ),
            };

            d_final.push(direction_pattern_meta);

            //insert stop list into DirectionPatternRow

            let direction_pattern_rows: Vec<DirectionPatternRow> = itin_pattern
                .stop_sequences
                .iter()
                .enumerate()
                .map(|(stop_idx, stop_time)| DirectionPatternRow {
                    attempt_id: attempt_id.to_string(),
                    chateau: chateau_id.to_string(),
                    direction_pattern_id: direction_pattern_id.to_string(),
                    stop_id: stop_time.stop_id.clone(),
                    stop_sequence: stop_idx as u32,
                    onestop_feed_id: feed_id.to_string(),
                    arrival_time_since_start: stop_time.arrival_time_since_start,
                    departure_time_since_start: stop_time.departure_time_since_start,
                    interpolated_time_since_start: stop_time.interpolated_time_since_start,
                    stop_headsign_idx: match &stop_time.stop_headsign {
                        Some(this_stop_headsign) => direction_pattern
                            .stop_headsigns_unique_list
                            .as_ref()
                            .map(|direction_headsigns| {
                                direction_headsigns
                                    .iter()
                                    .position(|x| x == this_stop_headsign)
                                    .map(|x| x as i16)
                            })
                            .flatten(),
                        None => None,
                    },
                })
                .collect();

            for dir_chunk in direction_pattern_rows.chunks(1000) {
                d_rows.push(dir_chunk.to_vec());
            }
        }

        if !check_is_active(&mut conn).await {
            conn = conn_pool.get().await?;
        }

        conn.build_transaction()
            .run::<(), diesel::result::Error, _>(|conn| {
                Box::pin(async move {
                    for dir_chunk in d_final.chunks(100) {
                        diesel::insert_into(
                            catenary::schema::gtfs::direction_pattern_meta::dsl::direction_pattern_meta,
                        )
                        .values(dir_chunk)
                        .execute(conn)
                        .await?;
                    }
                    for dir_chunk in d_rows {
                        diesel::insert_into(
                            catenary::schema::gtfs::direction_pattern::dsl::direction_pattern,
                        )
                        .values(dir_chunk)
                        .execute(conn)
                        .await?;
                    }
                    Ok(())
                })
            })
            .await?;
    }

    println!("Directions inserted for {}", feed_id);
    println!("Inserting itineraries for {}", feed_id);
    for group in &reduction.itineraries.iter().chunks(100000) {
        let mut t_final: Vec<catenary::models::ItineraryPatternMeta> = vec![];
        let mut t_rows: Vec<Vec<catenary::models::ItineraryPatternRow>> = vec![];

        for (itinerary_id, itinerary) in group {
            let itinerary_pg_meta = ItineraryPatternMeta {
                onestop_feed_id: feed_id.to_string(),
                chateau: chateau_id.to_string(),
                attempt_id: attempt_id.to_string(),
                timezone: itinerary.timezone.clone(),
                trip_headsign: match chateau_id {
                    "santacruzmetro" => match &itinerary.stop_headsigns_unique_list {
                        None => itinerary
                            .stop_sequences
                            .last()
                            .and_then(|x| x.stop_headsign.clone()),
                        Some(list) => Some(list.join(",")),
                    },
                    "montebellobuslines" => gtfs
                        .stops
                        .get(itinerary.stop_sequences.last().unwrap().stop_id.as_str())
                        .as_deref()
                        .unwrap()
                        .name
                        .clone(),
                    _ => itinerary
                        .trip_headsign
                        .clone()
                        .map(|x| x.replace(" - Funded in part by/SB County Measure A", "")),
                },
                trip_headsign_translations: None,
                itinerary_pattern_id: itinerary_id.to_string(),
                trip_ids: reduction
                    .itineraries_to_trips
                    .get(itinerary_id)
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|trip_under_itin| Some(trip_under_itin.trip_id.to_string()))
                    .collect::<Vec<Option<String>>>(),
                shape_id: itinerary.shape_id.clone(),
                route_id: itinerary.route_id.clone(),
                direction_pattern_id: Some(itinerary.direction_pattern_id.to_string()),
            };

            t_final.push(itinerary_pg_meta);

            let itinerary_pg = itinerary
                .stop_sequences
                .iter()
                .enumerate()
                .map(|(stop_index, stop_sequence)| ItineraryPatternRow {
                    onestop_feed_id: feed_id.to_string(),
                    chateau: chateau_id.to_string(),
                    attempt_id: attempt_id.to_string(),
                    itinerary_pattern_id: itinerary_id.to_string(),
                    stop_sequence: stop_index as i32,
                    stop_id: stop_sequence.stop_id.clone(),
                    gtfs_stop_sequence: stop_sequence.gtfs_stop_sequence as u32,
                    arrival_time_since_start: stop_sequence.arrival_time_since_start,
                    departure_time_since_start: stop_sequence.departure_time_since_start,
                    interpolated_time_since_start: stop_sequence.interpolated_time_since_start,
                    timepoint: stop_sequence.timepoint.into(),
                    stop_headsign_idx: match stop_sequence.stop_headsign {
                        Some(ref stop_headsign) => {
                            itinerary.stop_headsigns_unique_list.as_ref().and_then(|x| {
                                x.iter().position(|x| x == stop_headsign).map(|x| x as i16)
                            })
                        }
                        None => None,
                    },
                })
                .collect::<Vec<_>>();

            for itinerary_chunk in itinerary_pg.chunks(1000) {
                t_rows.push(itinerary_chunk.to_vec());
            }
        }

        if !check_is_active(&mut conn).await {
            conn = conn_pool.get().await?;
        }

        conn.build_transaction()
            .run::<(), diesel::result::Error, _>(|conn| {
                Box::pin(async move {
                    for itinerary_chunk in t_final.chunks(1000) {
                        diesel::insert_into(
                            catenary::schema::gtfs::itinerary_pattern_meta::dsl::itinerary_pattern_meta,
                        )
                        .values(itinerary_chunk)
                        .execute(conn)
                        .await?;
                    }

                    for itinerary_chunk in t_rows {
                        diesel::insert_into(
                            catenary::schema::gtfs::itinerary_pattern::dsl::itinerary_pattern,
                        )
                        .values(&itinerary_chunk)
                        .execute(conn)
                        .await?;
                    }

                    Ok(())
                })
            })
            .await?;
    }

    println!("Itineraries inserted for {}", feed_id);

    //insert trips

    println!("Inserting trips for {}", feed_id);

    for group in &reduction.itineraries_to_trips.iter().chunks(30000) {
        let mut t_final: Vec<Vec<catenary::models::CompressedTrip>> = vec![];
        for (itinerary_id, compressed_trip_list) in group {
            let trip_pg = compressed_trip_list
                .iter()
                .map(|compressed_trip_raw| catenary::models::CompressedTrip {
                    onestop_feed_id: feed_id.to_string(),
                    chateau: chateau_id.to_string(),
                    attempt_id: attempt_id.to_string(),
                    itinerary_pattern_id: itinerary_id.to_string(),
                    trip_id: compressed_trip_raw.trip_id.to_string(),
                    service_id: compressed_trip_raw.service_id.clone(),
                    direction_id: reduction
                        .itineraries
                        .get(itinerary_id)
                        .unwrap()
                        .direction_id,
                    start_time: compressed_trip_raw.start_time,
                    trip_short_name: compressed_trip_raw.trip_short_name.clone().map(|x| {
                        CompactString::from(
                            regex_train_starting.replace(x.as_str(), "").to_string(),
                        )
                    }),
                    block_id: compressed_trip_raw.block_id.clone(),
                    wheelchair_accessible: compressed_trip_raw.wheelchair_accessible,
                    bikes_allowed: compressed_trip_raw.bikes_allowed,
                    has_frequencies: !compressed_trip_raw.frequencies.is_empty(),
                    route_id: route_id_transform(feed_id, compressed_trip_raw.route_id.clone()),
                    frequencies: match !compressed_trip_raw.frequencies.is_empty() {
                        true => {
                            let prost_message =
                                frequencies_to_protobuf(&compressed_trip_raw.frequencies);
                            Some(prost_message.encode_to_vec())
                        }
                        false => None,
                    },
                })
                .collect::<Vec<_>>();

            for trip_chunk in trip_pg.chunks(2000) {
                t_final.push(trip_chunk.to_vec());
            }
        }

        if !check_is_active(&mut conn).await {
            conn = conn_pool.get().await?;
        }

        conn.build_transaction()
            .run::<(), diesel::result::Error, _>(|conn| {
                Box::pin(async move {
                    for trip_chunk in t_final {
                        diesel::insert_into(
                            catenary::schema::gtfs::trips_compressed::dsl::trips_compressed,
                        )
                        .values(trip_chunk)
                        .execute(conn)
                        .await?;
                    }

                    Ok(())
                })
            })
            .await?;
    }

    //insert routes

    println!("Inserting routes for {}", feed_id);

    let routes_pg: Vec<RoutePgModel> = gtfs
        .routes
        .iter()
        .map(|(route_id, route)| {
            let colour = fix_background_colour_rgb_feed_route(feed_id, route.color, route);
            let text_colour =
                fix_foreground_colour_rgb_feed(feed_id, route.color, route.text_color);

            let colour_pg = format!("#{:02x}{:02x}{:02x}", colour.r, colour.g, colour.b);
            let text_colour_pg = format!(
                "#{:02x}{:02x}{:02x}",
                text_colour.r, text_colour.g, text_colour.b
            );

            let route_pg = RoutePgModel {
                onestop_feed_id: feed_id.to_string(),
                route_id: route_id_transform(feed_id, route_id.to_string()),
                attempt_id: attempt_id.to_string(),
                agency_id: route.agency_id.clone(),
                short_name: route.short_name.clone(),
                long_name: route.long_name.clone(),
                chateau: chateau_id.to_string(),
                color: Some(colour_pg),
                text_color: Some(text_colour_pg),
                short_name_translations: None,
                long_name_translations: None,
                gtfs_desc: route.desc.clone(),
                gtfs_desc_translations: None,
                route_type: route_type_to_int(&route.route_type),
                url: route.url.clone(),
                url_translations: None,
                shapes_list: route_ids_to_shape_ids
                    .get(&route_id.clone())
                    .map(|shapes_list| {
                        shapes_list
                            .iter()
                            .map(|x| Some(x.clone()))
                            .collect::<Vec<Option<String>>>()
                    }),
                gtfs_order: route.order,
                continuous_drop_off: continuous_pickup_drop_off_to_i16(&route.continuous_drop_off),
                continuous_pickup: continuous_pickup_drop_off_to_i16(&route.continuous_pickup),
            };
            route_pg
        })
        .collect();

    let mut finished_route_chunks_elasticsearch: Vec<
        Vec<elasticsearch::http::request::JsonBody<_>>,
    > = Vec::new();

    //insert routes into elastic search with seperate columns for both names
    for route_chunk in &gtfs.routes.iter().chunks(256) {
        let mut insertable_elastic: Vec<elasticsearch::http::request::JsonBody<_>> = Vec::new();
        for (route_id, route) in route_chunk {
            let shape_points_combined =
                route_ids_to_shape_ids
                    .get(&route_id.clone())
                    .and_then(|shape_ids| {
                        Some(
                            shape_ids
                                .iter()
                                .filter_map(|shape_id| {
                                    gtfs_shapes_minimised
                                        .as_ref()
                                        .map(|gtfs_shapes_minimised| {
                                            gtfs_shapes_minimised.get(shape_id.as_str())
                                        })
                                        .flatten()
                                })
                                .cloned()
                                .flatten()
                                .collect::<Vec<_>>(),
                        )
                    });

            let envelope = match shape_points_combined {
                Some(shape_points) => match shape_points.len() {
                    0 => None,
                    _ => Some(compute_shape_envelope(route, &shape_points)),
                },
                None => None,
            };

            let mut short_name_translations_shortened_locales_elastic: HashMap<String, String> =
                HashMap::new();

            let mut long_name_translations_shortened_locales_elastic: HashMap<String, String> =
                HashMap::new();

            let mut important_points: Vec<[f64; 2]> = Vec::new();
            let mut important_points_set: HashSet<(i32, i32)> = HashSet::new();

            if let Some(direction_patterns) = route_to_direction_patterns.get(route_id) {
                // First, collect all start and end points
                for direction_pattern in &*direction_patterns {
                    let stops: Vec<_> = direction_pattern
                        .stop_sequence
                        .iter()
                        .filter_map(|stop_id| gtfs.stops.get(stop_id.as_str()))
                        .collect();

                    if let Some(start_stop) = stops.iter().next() {
                        if let (Some(lat), Some(lon)) = (start_stop.latitude, start_stop.longitude)
                        {
                            let lat_i = (lat * 1_000_000.0) as i32;
                            let lon_i = (lon * 1_000_000.0) as i32;
                            if important_points_set.insert((lat_i, lon_i)) {
                                important_points.push([lon, lat]);
                            }
                        }
                    }

                    if let Some(end_stop) = stops.iter().last() {
                        if let (Some(lat), Some(lon)) = (end_stop.latitude, end_stop.longitude) {
                            let lat_i = (lat * 1_000_000.0) as i32;
                            let lon_i = (lon * 1_000_000.0) as i32;
                            if important_points_set.insert((lat_i, lon_i)) {
                                important_points.push([lon, lat]);
                            }
                        }
                    }
                }

                // Then, iterate through all stops to find intermediate points
                for direction_pattern in &*direction_patterns {
                    let stops: Vec<_> = direction_pattern
                        .stop_sequence
                        .iter()
                        .filter_map(|stop_id| gtfs.stops.get(stop_id.as_str()))
                        .collect();

                    for stop in stops {
                        if let (Some(lat), Some(lon)) = (stop.latitude, stop.longitude) {
                            let lat_i = (lat * 1_000_000.0) as i32;
                            let lon_i = (lon * 1_000_000.0) as i32;

                            if important_points_set.contains(&(lat_i, lon_i)) {
                                continue;
                            }

                            let is_far_enough = important_points
                                .iter()
                                .all(|p| haversine_distance(lat, lon, p[1], p[0]) >= 50.0);

                            if is_far_enough {
                                important_points.push([lon, lat]);
                                important_points_set.insert((lat_i, lon_i));
                            }
                        }
                    }
                }
            }

            let route_id = route_id_transform(feed_id, route_id.to_string());

            let elastic_id = format!("{}_{}_{}", feed_id, attempt_id, route_id);

            let route_type = route_type_to_int(&route.route_type);

            let agency_name = match gtfs.agencies.len() {
                0 => String::new(),
                1 => gtfs.agencies[0].name.clone(),
                _ => match gtfs
                    .agencies
                    .iter()
                    .find(|agency| agency.id == route.agency_id)
                {
                    Some(agency) => agency.name.clone(),
                    None => String::new(),
                },
            };

            if let Some(default_lang) = &default_lang {
                if let Some(short_name) = &route.short_name {
                    if let Ok(lang_tag) = LanguageTag::parse(default_lang.as_str()) {
                        name_shortening_hash_insert_elastic(
                            &mut short_name_translations_shortened_locales_elastic,
                            &lang_tag,
                            short_name.as_str(),
                        );
                    }
                }

                if let Some(long_name) = &route.long_name {
                    if let Ok(lang_tag) = LanguageTag::parse(default_lang.as_str()) {
                        name_shortening_hash_insert_elastic(
                            &mut long_name_translations_shortened_locales_elastic,
                            &lang_tag,
                            long_name.as_str(),
                        );
                    }
                }
            } else {
                if let Some(short_name) = &route.short_name {
                    short_name_translations_shortened_locales_elastic
                        .insert("en".to_string(), short_name.clone());
                }

                if let Some(long_name) = &route.long_name {
                    long_name_translations_shortened_locales_elastic
                        .insert("en".to_string(), long_name.clone());
                }
            }

            if important_points.len() > 1 {
                insertable_elastic
                    .push(json!({"index": {"_index": "routes", "_id": elastic_id}}).into());
                insertable_elastic.push(
                    json!({
                        "chateau": chateau_id.to_string(),
                        "attempt_id": attempt_id.to_string(),
                        "route_id": route_id,
                        "route_type": route_type,
                        "agency_name_search": agency_name,
                        "route_short_name": serde_json::to_value(&short_name_translations_shortened_locales_elastic).unwrap(),
                        "route_long_name": serde_json::to_value(&long_name_translations_shortened_locales_elastic).unwrap(),
                        "bbox": envelope,
                        "important_points": important_points
                    })
                    .into(),
                );
            }
        }

        finished_route_chunks_elasticsearch.push(insertable_elastic);
    }

    if !check_is_active(&mut conn).await {
        conn = conn_pool.get().await?;
    }

    conn.build_transaction()
        .run::<(), diesel::result::Error, _>(|conn| {
            Box::pin(async move {
                for route_chunk in routes_pg.chunks(50) {
                    diesel::insert_into(catenary::schema::gtfs::routes::dsl::routes)
                        .values(route_chunk)
                        .execute(conn)
                        .await?;
                }
                Ok(())
            })
        })
        .await?;

    if let Some(elasticclient) = elasticclient {
        for chunk in finished_route_chunks_elasticsearch {
            let response = elasticclient
                .bulk(elasticsearch::BulkParts::Index("routes"))
                .body(chunk)
                .send()
                .await?;

            let response_body = response.json::<serde_json::Value>().await?;

            let mut print_err = true;

            if response_body.get("errors").map(|x| x.as_bool()).flatten() == Some(false) {
                print_err = false;
            }

            if print_err {
                println!("elastic routes response: {:#?}", response_body);
            }
        }
    }

    println!("Routes inserted for {}", feed_id);

    //calculate concave hull
    let hull = crate::gtfs_handlers::hull_from_gtfs::hull_from_gtfs(&gtfs, &feed_id);

    let bbox = hull.as_ref().map(|hull| hull.bounding_rect()).flatten();

    gtfs_summary.bbox = bbox;

    // insert feed info
    if let Some(feed_info) = &feed_info {
        if !check_is_active(&mut conn).await {
            conn = conn_pool.get().await?;
        }

        use catenary::schema::gtfs::feed_info::dsl::feed_info as feed_table;

        let feed_info_pg = catenary::models::FeedInfo {
            onestop_feed_id: feed_id.to_string(),
            feed_publisher_name: feed_info.name.clone(),
            feed_publisher_url: feed_info.url.clone(),
            feed_lang: feed_info.lang.clone(),
            feed_start_date: feed_info.start_date,
            feed_end_date: feed_info.end_date,
            feed_version: feed_info.version.clone(),
            feed_contact_email: feed_info.contact_email.clone(),
            feed_contact_url: feed_info.contact_url.clone(),
            attempt_id: attempt_id.to_string(),
            default_lang: feed_info.default_lang.clone(),
            chateau: chateau_id.to_string(),
        };

        diesel::insert_into(feed_table)
            .values(feed_info_pg)
            .execute(&mut conn)
            .await?;
    }
    //submit hull

    println!("Insert hull for {}", feed_id);

    let hull_pg: Option<postgis_diesel::types::Polygon<postgis_diesel::types::Point>> =
        hull.map(|polygon_geo| postgis_diesel::types::Polygon {
            rings: vec![
                polygon_geo
                    .into_inner()
                    .0
                    .into_iter()
                    .map(|coord| {
                        postgis_diesel::types::Point::new(
                            coord.x,
                            coord.y,
                            Some(catenary::WGS_84_SRID),
                        )
                    })
                    .collect(),
            ],
            srid: Some(catenary::WGS_84_SRID),
        });

    let languages_avaliable_pg = gtfs_summary
        .languages_avaliable
        .iter()
        .map(|x| Some(x.clone()))
        .collect::<Vec<Option<String>>>();

    let static_feed_pg = catenary::models::StaticFeed {
        onestop_feed_id: feed_id.to_string(),
        chateau: chateau_id.to_string(),
        default_lang: match feed_info {
            Some(feed_info) => Some(feed_info.lang.clone()),
            None => None,
        },
        previous_chateau_name: chateau_id.to_string(),
        languages_avaliable: languages_avaliable_pg.clone(),
        hull: hull_pg.clone(),
    };

    //create the static feed entry
    if !check_is_active(&mut conn).await {
        conn = conn_pool.get().await?;
    }

    let _ = diesel::insert_into(catenary::schema::gtfs::static_feeds::dsl::static_feeds)
        .values(&static_feed_pg)
        .on_conflict(catenary::schema::gtfs::static_feeds::dsl::onestop_feed_id)
        .do_update()
        .set((
            catenary::schema::gtfs::static_feeds::dsl::languages_avaliable
                .eq(languages_avaliable_pg),
            catenary::schema::gtfs::static_feeds::dsl::hull
                .eq(hull_pg.map(postgis_diesel::types::GeometryContainer::Polygon)),
        ))
        .execute(&mut conn)
        .await?;

    println!("matching stops to osm stations");

    // Match stops to OSM stations (for rail/tram/subway routes)
    // This runs after stops are inserted and associates them with imported OSM stations
    if let Err(e) = crate::osm_station_matching::match_stops_for_feed(
        &mut conn, feed_id, attempt_id, chateau_id,
    )
    .await
    {
        // Log but don't fail - OSM matching is optional enhancement
        eprintln!(
            "Warning: OSM station matching failed for {}: {:?}",
            feed_id, e
        );
    }

    let ingest_duration = start.elapsed();
    println!(
        "Finished {}, took {:.3}s",
        feed_id,
        ingest_duration.as_secs_f32()
    );

    Ok(gtfs_summary)
}

pub fn pickup_dropoff_to_i16(x: &gtfs_structures::PickupDropOffType) -> i16 {
    match x {
        gtfs_structures::PickupDropOffType::Regular => 0,
        gtfs_structures::PickupDropOffType::NotAvailable => 1,
        gtfs_structures::PickupDropOffType::ArrangeByPhone => 2,
        gtfs_structures::PickupDropOffType::CoordinateWithDriver => 3,
        gtfs_structures::PickupDropOffType::Unknown(x) => *x,
    }
}

pub fn continuous_pickup_drop_off_to_i16(x: &ContinuousPickupDropOff) -> i16 {
    match x {
        ContinuousPickupDropOff::Continuous => 0,
        ContinuousPickupDropOff::NotAvailable => 1,
        ContinuousPickupDropOff::ArrangeByPhone => 2,
        ContinuousPickupDropOff::CoordinateWithDriver => 3,
        ContinuousPickupDropOff::Unknown(x) => *x,
    }
}

fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const R: f64 = 6371.0; // Earth radius in kilometers
    let d_lat = (lat2 - lat1).to_radians();
    let d_lon = (lon2 - lon1).to_radians();
    let a = (d_lat / 2.0).sin() * (d_lat / 2.0).sin()
        + lat1.to_radians().cos()
            * lat2.to_radians().cos()
            * (d_lon / 2.0).sin()
            * (d_lon / 2.0).sin();
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    R * c
}

fn compute_shape_envelope(
    route: &gtfs_structures::Route,
    shapes: &Vec<ShapePoint>,
) -> serde_json::Value {
    let mut min_latitude: Option<f64> = None;
    let mut max_latitude: Option<f64> = None;

    let mut min_longitude: Option<f64> = None;
    let mut max_longitude: Option<f64> = None;

    for shape in shapes {
        match min_latitude {
            None => min_latitude = Some(shape.geometry.y()),
            Some(current_min) => {
                if shape.geometry.y() < current_min {
                    min_latitude = Some(shape.geometry.y());
                }
            }
        }

        match max_latitude {
            None => max_latitude = Some(shape.geometry.y()),
            Some(current_max) => {
                if shape.geometry.y() > current_max {
                    max_latitude = Some(shape.geometry.y());
                }
            }
        }

        match min_longitude {
            None => min_longitude = Some(shape.geometry.x()),
            Some(current_min) => {
                if shape.geometry.x() < current_min {
                    min_longitude = Some(shape.geometry.x());
                }
            }
        }

        match max_longitude {
            None => max_longitude = Some(shape.geometry.x()),
            Some(current_max) => {
                if shape.geometry.x() > current_max {
                    max_longitude = Some(shape.geometry.x());
                }
            }
        }
    }

    //[[minLon, maxLat], [maxLon, minLat]] as defined in https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/geo-shape
    return json! ({
      "type" : "envelope",
      "coordinates" : [ [min_longitude.unwrap(), max_latitude.unwrap()], [max_longitude.unwrap(), min_latitude.unwrap()] ]
    });
}
