use clap::Parser;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use rstar::{AABB, RTree};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use catenary::models::Stop;
use catenary::postgres_tools::make_async_pool;
use catenary::routing_common::api::{Place, RoutingRequest, TravelMode};
use catenary::routing_common::graph_loader::GraphManager;
use catenary::routing_common::osm_router::OsmRouter;
use catenary::routing_common::timetable::{
    Coordinate, Footpath, LocationIdx, PointObj, RouteData, StopFootpaths, StringOffset,
    TimetableHeader, ZeroCopyTimetable,
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to export the compiled routing binary data.
    #[arg(short, long)]
    output: PathBuf,

    /// Path to the Avens graph directories (import paths) for dynamic loading.
    #[arg(short, long)]
    avens_graphs: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().init();

    let args = Args::parse();

    println!("Initializing Gentian Timetable Compiler");
    println!("Output Path: {:?}", args.output);
    println!("Avens Graphs Directory: {:?}", args.avens_graphs);

    let graph_manager = Arc::new(GraphManager::new(&args.avens_graphs));
    let mut pool = make_async_pool().await.map_err(|e| anyhow::anyhow!(e))?;
    let mut conn = pool.get().await?;

    println!("Loading transit stops from PostgreSQL...");
    use catenary::schema::gtfs::stops::dsl::*;
    let all_stops: Vec<Stop> = stops
        .filter(hidden.eq(false))
        .select(Stop::as_select())
        .load(&mut conn)
        .await?;

    println!(
        "Loaded {} stops. Computing transfer footpaths via Avens/OsmRouter...",
        all_stops.len()
    );

    let mut footpaths_out: Vec<Footpath> = Vec::new();
    let mut coordinates: Vec<Coordinate> = Vec::with_capacity(all_stops.len());
    let mut original_chateau_ids: Vec<String> = Vec::with_capacity(all_stops.len());
    let mut original_stop_ids: Vec<String> = Vec::with_capacity(all_stops.len());

    // In a real topology extraction step, these IDs would map to exact offsets
    for (idx, stop_row) in all_stops.iter().enumerate() {
        if let Some(pt) = &stop_row.point {
            coordinates.push(Coordinate {
                lat: pt.y as f32, // Note: Postgis point uses (x=lon, y=lat)
                lon: pt.x as f32,
            });
        } else {
            coordinates.push(Coordinate { lat: 0.0, lon: 0.0 });
        }
        original_chateau_ids.push(stop_row.chateau.clone());
        original_stop_ids.push(stop_row.gtfs_id.clone());
    }

    // R-Tree for finding nearby bounds
    let mut rtree = RTree::new();
    for (i, coord) in coordinates.iter().enumerate() {
        if coord.lat != 0.0 {
            // Using small array as rstar point
            rtree.insert(catenary::routing_common::timetable::PointObj {
                idx: i as u32,
                point: [coord.lat as f64, coord.lon as f64],
            });
        }
    }

    for (from_idx, from_coord) in coordinates.iter().enumerate() {
        if from_coord.lat == 0.0 {
            continue;
        }

        let nearby_stops =
            rtree.locate_within_distance([from_coord.lat as f64, from_coord.lon as f64], 0.01);

        let region_opt =
            graph_manager.get_region_for_point(from_coord.lat as f64, from_coord.lon as f64);
        if region_opt.is_none() {
            continue;
        }

        let loaded_graph = graph_manager.get_or_load_region(&region_opt.unwrap());
        if loaded_graph.is_err() {
            continue;
        }
        let router = OsmRouter::new(loaded_graph.unwrap());

        for nearby_stop in nearby_stops {
            let to_idx = nearby_stop.idx as usize;
            if to_idx == from_idx {
                continue;
            }
            let to_coord = &coordinates[to_idx];

            let req = RoutingRequest {
                origin: Place::Coordinate {
                    lat: from_coord.lat as f64,
                    lon: from_coord.lon as f64,
                },
                destination: Place::Coordinate {
                    lat: to_coord.lat as f64,
                    lon: to_coord.lon as f64,
                },
                time: 0,
                is_departure_time: true,
                max_travel_time: Some(15 * 60), // Motis max 15m transfer
                mode: TravelMode::Walk,
                wheelchair_accessible: false,
                speed_mps: 1.2,
                max_transfers: None,
                min_transfer_time: None,
                additional_transfer_time: None,
                transit_modes: None,
                pedestrian_profile: None,
                elevation_costs: None,
                direct_modes: None,
                pre_transit_modes: None,
                post_transit_modes: None,
                num_itineraries: None,
                max_itineraries: None,
                search_windows: None,
                timetable_view: None,
            };

            // OSM Request
            if let Ok(mut result) = router.route(&req) {
                if let Some(itin) = result.itineraries.pop() {
                    footpaths_out.push(Footpath {
                        target: catenary::routing_common::timetable::LocationIdx(to_idx as u32),
                        duration_seconds: itin.duration_seconds as u16,
                        padding: 0,
                    });
                }
            }
        }
    }

    println!("Computed {} transfer edges.", footpaths_out.len());

    println!("Constructing RAPTOR sequence arrays...");
    // Memory arrays for zero-copy mapping (scaffolded layout per Motis)
    let mut raptor_routes: Vec<RouteData> = Vec::new();
    let mut route_stops: Vec<LocationIdx> = Vec::new();
    let mut route_stop_times: Vec<u32> = Vec::new();
    let mut stop_footpaths: Vec<StopFootpaths> = Vec::new();
    let mut route_id_offsets: Vec<StringOffset> = Vec::new();
    let mut trip_id_offsets: Vec<StringOffset> = Vec::new();

    // Map `footpaths_out` into `stop_footpaths` (flattened representation)
    // Note: The Motis implementation uses a pre-calculated conflict graph and route-stop maps.
    // We mock/scaffold this with empty arrays, but they are fully integrated into memory mapping.
    
    // Build the sequential zero-copy raw buffer
    let mut buffer: Vec<u8> = Vec::new();

    // We reserve space for the header, then copy arrays

    buffer.extend(vec![0u8; std::mem::size_of::<TimetableHeader>()]);

    let mut header = TimetableHeader {
        magic: 0xC0DE_0001,
        version: 1,
        ..Default::default()
    };

    header.location_coords_offset = buffer.len() as u32;
    header.location_coords_count = coordinates.len() as u32;
    buffer.extend_from_slice(bytemuck::cast_slice(&coordinates));

    header.footpath_out_offset = buffer.len() as u32;
    header.footpath_out_count = footpaths_out.len() as u32;
    buffer.extend_from_slice(bytemuck::cast_slice(&footpaths_out));

    header.routes_offset = buffer.len() as u32;
    header.routes_count = raptor_routes.len() as u32;
    buffer.extend_from_slice(bytemuck::cast_slice(&raptor_routes));

    header.route_stops_offset = buffer.len() as u32;
    header.route_stops_count = route_stops.len() as u32;
    buffer.extend_from_slice(bytemuck::cast_slice(&route_stops));

    header.route_stop_times_offset = buffer.len() as u32;
    header.route_stop_times_count = route_stop_times.len() as u32;
    buffer.extend_from_slice(bytemuck::cast_slice(&route_stop_times));

    header.stop_footpaths_offset = buffer.len() as u32;
    header.stop_footpaths_count = stop_footpaths.len() as u32;
    buffer.extend_from_slice(bytemuck::cast_slice(&stop_footpaths));

    header.route_id_pool_offset = buffer.len() as u32;
    header.route_id_pool_count = route_id_offsets.len() as u32;
    buffer.extend_from_slice(bytemuck::cast_slice(&route_id_offsets));
    
    header.trip_id_pool_offset = buffer.len() as u32;
    header.trip_id_pool_count = trip_id_offsets.len() as u32;
    buffer.extend_from_slice(bytemuck::cast_slice(&trip_id_offsets));

    // String pool writing
    let mut string_pool = Vec::new();
    // Implementation of string pooling logic for CHATEAU / IDS mapping goes here
    // ...

    header.string_pool_offset = buffer.len() as u32;
    header.string_pool_length = string_pool.len() as u32;
    buffer.extend_from_slice(&string_pool);

    // Overwrite the header at origin byte 0
    let header_bytes = bytemuck::bytes_of(&header);
    buffer[0..std::mem::size_of::<TimetableHeader>()].copy_from_slice(header_bytes);

    println!(
        "Constructed raw mapping ({} bytes). Writing uncompressed raw memory page...",
        buffer.len()
    );

    let mut out_file = File::create(&args.output).await?;
    out_file.write_all(&buffer).await?;

    println!(
        "Successfully compiled and wrote zero-copy timetable to {:?}",
        args.output
    );

    Ok(())
}
