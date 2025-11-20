// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

use diesel_async::RunQueryDsl;
use rgb::RGB;
use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;
use std::sync::Arc;

use crate::gtfs_handlers::colour_correction;
use crate::gtfs_handlers::rename_route_labels::*;
use crate::shapes_reader::ShapePoint;
use ahash::AHashMap;
use catenary::enum_to_int::route_type_to_int;
use catenary::postgres_tools::CatenaryPostgresPool;

use itertools::Itertools;

pub async fn shapes_into_postgres(
    gtfs: &gtfs_structures::Gtfs,
    shape_to_color_lookup: &HashMap<std::string::String, RGB<u8>>,
    shape_to_text_color_lookup: &HashMap<std::string::String, RGB<u8>>,
    feed_id: &str,
    arc_conn_pool: Arc<CatenaryPostgresPool>,
    chateau_id: &str,
    attempt_id: &str,
    shape_id_to_route_ids_lookup: &HashMap<String, HashSet<String>>,
    gtfs_shapes_minimised: &Option<AHashMap<String, Vec<ShapePoint>>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    //establish a connection to the database
    let conn_pool = arc_conn_pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre?;

    if let Some(gtfs_shapes_minimised) = &gtfs_shapes_minimised {
        for group in &gtfs_shapes_minimised.iter().chunks(100) {
            let mut batch_of_shapes: Vec<catenary::models::Shape> = vec![];

            for (shape_id, shape) in group {
                let mut route_type_number = 3;

                let route_ids = shape_id_to_route_ids_lookup.get(shape_id);

                if let Some(route_ids) = route_ids {
                    let route = gtfs.routes.get(route_ids.iter().next().unwrap());

                    if route.is_some() {
                        route_type_number = route_type_to_int(&route.unwrap().route_type);
                    }
                }

                //backround colour to use
                let route = match route_ids {
                    Some(route_ids) => match route_ids.iter().next() {
                        Some(route_id) => gtfs.routes.get(route_id),
                        None => None,
                    },
                    None => None,
                };

                let bg_color = match shape_to_color_lookup.get(shape_id) {
                    Some(color) => match route {
                        Some(route) => colour_correction::fix_background_colour_rgb_feed_route(
                            feed_id,
                            Some(*color),
                            route,
                        ),
                        None => *color,
                    },
                    None => RGB::new(0, 0, 0),
                };

                let bg_color_string =
                    format!("{:02x}{:02x}{:02x}", bg_color.r, bg_color.g, bg_color.b);

                // Metro Los Angeles often has geometry that includes sections that are part of the railyard or are currently in construction
                let preshape: Vec<ShapePoint> = match feed_id {
                    "f-9q5-metro~losangeles~rail" => shape
                        .clone()
                        .into_iter()
                        .filter(|point| match bg_color_string.as_str() {
                            //remove points from Metro Los Angeles B/D that are east of Los Angeles Union Station
                            "eb131b" => point.geometry.x() < -118.2335698,
                            "a05da5" => point.geometry.x() < -118.2335698,
                            _ => true,
                        })
                        .collect::<Vec<ShapePoint>>(),
                    _ => shape.clone(),
                };

                let mut is_line_too_stupidly_broken = false;

                let threshold_degree_broken = match route_type_number {
                    4 => 5.,
                    _ => 1.,
                };

                //Lines are only valid in postgres if they contain 2 or more points
                if preshape.len() >= 2 {
                    if feed_id != "f-9-amtrak~amtrakcalifornia~amtrakcharteredvehicle" {
                        for (idx, point) in preshape.iter().enumerate().skip(1) {
                            if (preshape[idx - 1].geometry.x() - point.geometry.x()).abs()
                                > threshold_degree_broken
                                || (preshape[idx - 1].geometry.y() - point.geometry.y()).abs()
                                    > threshold_degree_broken
                            {
                                is_line_too_stupidly_broken = true;
                                break;
                            }
                        }
                    }

                    if is_line_too_stupidly_broken {
                        println!(
                            "Deleted feed id {} shape id {} for being too long",
                            &feed_id, &shape_id
                        );
                    }

                    if !is_line_too_stupidly_broken {
                        let linestring: postgis_diesel::types::LineString<
                            postgis_diesel::types::Point,
                        > = postgis_diesel::types::LineString {
                            srid: Some(4326),
                            points: preshape
                                .into_iter()
                                .filter(|point| {
                                    point.geometry.y() != 0.0 && point.geometry.x() != 0.0
                                })
                                .map(|point| postgis_diesel::types::Point {
                                    x: point.geometry.x(),
                                    y: point.geometry.y(),
                                    srid: Some(4326),
                                })
                                .collect(),
                        };

                        let text_color = match shape_to_text_color_lookup.get(shape_id) {
                            Some(color) => format!("{:02x}{:02x}{:02x}", color.r, color.g, color.b),
                            None => String::from("000000"),
                        };

                        //creates a text label for the shape to be displayed with on the map
                        //todo! change this with i18n
                        let route_label: String = match route_ids {
                            Some(route_ids) => route_ids
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
                                .map(|route_label| {
                                    rename_route_string(route_label.as_ref().unwrap().to_owned())
                                })
                                .collect::<Vec<String>>()
                                .join(",")
                                .as_str()
                                .to_string(),
                            None => String::from(""),
                        };
                        //run insertion

                        //Create structure to insert
                        let shape_value: catenary::models::Shape = catenary::models::Shape {
                            onestop_feed_id: feed_id.to_string(),
                            attempt_id: attempt_id.to_string(),
                            shape_id: shape_id.clone(),
                            chateau: chateau_id.to_string(),
                            linestring,
                            color: Some(bg_color_string),
                            routes: route_ids.map(|route_ids| {
                                route_ids
                                    .iter()
                                    .map(|route_id| Some(route_id.to_string()))
                                    .collect()
                            }),
                            route_type: route_type_number,
                            route_label: Some(route_label),
                            route_label_translations: None,
                            text_color: Some(text_color),
                            allowed_spatial_query: false,
                            stop_to_stop_generated: Some(false),
                        };

                        batch_of_shapes.push(shape_value);
                    }
                }
            }

            {
                use catenary::schema::gtfs::shapes::dsl::*;

                diesel::insert_into(shapes)
                    .values(batch_of_shapes)
                    .execute(conn)
                    .await?;
            }
        }
    }

    Ok(())
}
