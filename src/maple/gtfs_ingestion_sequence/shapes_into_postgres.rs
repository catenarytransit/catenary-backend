use postgis::ewkb;
use rgb::RGB;
use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;
use std::sync::Arc;

use crate::gtfs_handlers::colour_correction;
use crate::gtfs_handlers::enum_to_int::route_type_to_int;
use crate::gtfs_handlers::rename_route_labels::*;
use crate::gtfs_handlers::shape_colour_calculator::shape_to_colour;
use crate::gtfs_handlers::stops_associated_items::*;

pub async fn shapes_into_postgres(
    gtfs: &gtfs_structures::Gtfs,
    shape_to_color_lookup: &HashMap<std::string::String, RGB<u8>>,
    shape_to_text_color_lookup: &HashMap<std::string::String, RGB<u8>>,
    feed_id: &str,
    pool: &Arc<sqlx::Pool<sqlx::Postgres>>,
    chateau_id: &str,
    attempt_id: &str
) -> Result<(), Box<dyn Error>>  {
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

        // Metro Los Angeles often has geometry that includes sections that are part of the railyard or are currently in construction
        let preshape: Vec<gtfs_structures::Shape> = match feed_id {
            "f-9q5-metro~losangeles~rail" => shape
                .clone()
                .into_iter()
                .filter(|point| match color_to_upload.as_str() {
                    //remove points from Metro Los Angeles B/D that are east of Los Angeles Union Station
                    "eb131b" => point.longitude < -118.2335698,
                    "a05da5" => point.longitude < -118.2335698,
                    //Remove the under construction segment of the Metro K Line in Los Angeles
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

        //creates a text label for the shape to be displayed with on the map
        //todo! change this with i18n
        let route_label: String = route_ids
            .iter()
            .map(|route_id| {
                let route = gtfs.routes.get(route_id);
                match route {
                    Some(route) => match route.short_name.is_some() {
                        true => route.short_name.to_owned(),
                        false => match route.long_name.is_some() {
                            true => route.long_name.to_owned(),
                            false => None,
                        },
                    },
                    _ => None,
                }
            })
            .filter(|route_label| route_label.is_some())
            .map(|route_label| rename_route_string(route_label.as_ref().unwrap().to_owned()))
            .collect::<Vec<String>>()
            .join(",")
            .as_str()
            .replace("Orange County", "OC")
            .replace("Inland Empire", "IE")
            .to_string();

            /* 
        let sql_shape_insert = sqlx::query!("INSERT INTO gtfs.shapes
        (onestop_feed_id, attempt_id, shape_id, linestring, color, routes, route_type, route_label, route_label_translations, text_colour, chateau) 
        VALUES ($1, $2, $3, $4, $5, $6,$7,$8, $9, $10, $11)",
        feed_id,
        attempt_id,
        shape_id,
        linestring,
        color_to_upload,
        );*/
    }

    Ok(())
}
