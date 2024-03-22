use postgis::ewkb;
use rgb::RGB;
use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;
use std::sync::Arc;

use crate::gtfs_handlers::colour_correction;
use crate::gtfs_handlers::enum_to_int::route_type_to_int;
use crate::gtfs_handlers::shape_colour_calculator::shape_to_colour;
use crate::gtfs_handlers::stops_associated_items::*;

// Initial version 3 of ingest written by Kyler Chin
// Removal of the attribution is not allowed, as covered under the AGPL license

// take a feed id and throw it into postgres
pub async fn gtfs_process_feed(
    feed_id: &str,
    pool: &Arc<sqlx::Pool<sqlx::Postgres>>,
) -> Result<(), Box<dyn Error>> {
    let path = format!("gtfs_uncompressed/{}", feed_id);

    let gtfs = gtfs_structures::Gtfs::new(path.as_str())?;

    let (stop_ids_to_route_types, stop_ids_to_route_ids) =
        make_hashmap_stops_to_route_types_and_ids(&gtfs);

    let (stop_id_to_children_ids, stop_ids_to_children_route_types) =
        make_hashmaps_of_children_stop_info(&gtfs, &stop_ids_to_route_types);

    //identify colours of shapes based on trip id's route id
    let (shape_to_color_lookup, shape_to_text_color_lookup) = shape_to_colour(&feed_id, &gtfs);

    for (shape_id, shape) in gtfs.shapes.iter() {
        let mut route_ids: HashSet<String> = gtfs
            .trips
            .iter()
            .filter(|(trip_id, trip)| {
                trip.shape_id.is_some() && trip.shape_id.as_ref().unwrap() == shape_id
            })
            .map(|(trip_id, trip)| trip.route_id.to_owned())
            .collect::<HashSet<String>>();

        if feed_id == "f-9qh-metrolinktrains" {
            let cleanedline = shape_id.to_owned().replace("in", "").replace("out", "");

            println!("cleanedline: {}", &cleanedline);
            let value = match cleanedline.as_str() {
                "91" => "91 Line",
                "IEOC" => "Inland Emp.-Orange Co. Line",
                "AV" => "Antelope Valley Line",
                "OC" => "Orange County Line",
                "RIVER" => "Riverside Line",
                "SB" => "San Bernardino Line",
                "VT" => "Ventura County Line",
                _ => "",
            };
            println!("real metrolink line {}", &value);
            if value != "" {
                route_ids.insert(value.to_string());
            }
        }

        let mut route_type_number = 3;
        if route_ids.len() > 0 {
            let route = gtfs.routes.get(route_ids.iter().nth(0).unwrap());

            if route.is_some() {
                route_type_number = route_type_to_int(&route.unwrap().route_type);
            }
        }

        let color_to_upload = match shape_to_color_lookup.get(shape_id) {
            Some(color) => format!("{:02x}{:02x}{:02x}", color.r, color.g, color.b),
            None => String::from("3a3a3a"),
        };

        let preshape: Vec<gtfs_structures::Shape> = match feed_id {
            "f-9q5-metro~losangeles~rail" => shape
                .clone()
                .into_iter()
                .filter(|point| match color_to_upload.as_str() {
                    "eb131b" => point.longitude < -118.2335698,
                    "a05da5" => point.longitude < -118.2335698,
                    "e470ab" => point.latitude > 33.961543,
                    _ => true,
                })
                .collect::<Vec<gtfs_structures::Shape>>(),
            _ => shape.clone(),
        };

        //Lines are only valid in postgres if they contain 2 or more points
        if preshape.len() >= 2 {
            let linestring = ewkb::LineStringT {
                srid: Some(4326),
                points: preshape
                    .iter()
                    .map(|point| ewkb::Point {
                        x: point.longitude,
                        y: point.latitude,
                        srid: Some(4326),
                    })
                    .collect(),
            };
        }

        let text_color = match shape_to_text_color_lookup.get(shape_id) {
            Some(color) => format!("{:02x}{:02x}{:02x}", color.r, color.g, color.b),
            None => String::from("000000"),
        };
    }

    Ok(())
}
