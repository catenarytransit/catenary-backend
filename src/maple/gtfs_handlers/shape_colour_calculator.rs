use crate::gtfs_handlers::colour_correction;
use rgb::RGB;
use std::collections::{HashSet,HashMap};

pub struct ShapeToColourResponse {
    pub shape_to_color_lookup: HashMap<String, RGB<u8>>,
    pub shape_to_text_color_lookup: HashMap<String, RGB<u8>>,
    pub shape_id_to_route_ids_lookup: HashMap<String, HashSet<String>>,
    pub route_ids_to_shape_ids: HashMap<String, HashSet<String>>
}

pub fn shape_to_colour(
    feed_id: &str,
    gtfs: &gtfs_structures::Gtfs,
) -> ShapeToColourResponse {
    let mut shape_to_color_lookup: HashMap<String, RGB<u8>> = HashMap::new();
    let mut shape_to_text_color_lookup: HashMap<String, RGB<u8>> = HashMap::new();
    let mut shape_id_to_route_ids_lookup: HashMap<String, HashSet<String>> = HashMap::new();
    let mut route_ids_to_shape_ids: HashMap<String, HashSet<String>> = HashMap::new();

    for (trip_id, trip) in &gtfs.trips {
       
        
        if let Some(shape_id) = &trip.shape_id {
            if let Some(route) = gtfs.routes.get(&trip.route_id) {
                if !shape_to_color_lookup.contains_key(shape_id) {
                    //colour not yet assigned to shape, assign it!

                    let color = colour_correction::fix_background_colour_rgb_feed_route(
                        feed_id,
                        route.color,
                        route,
                    );

                    shape_to_color_lookup.insert(trip.shape_id.as_ref().unwrap().to_owned(), color);
                    shape_to_text_color_lookup
                        .insert(shape_id.clone(), route.text_color);
                }

                //assign route id to this shape id
                 shape_id_to_route_ids_lookup.entry(shape_id.clone())
                 .and_modify(|existing_route_ids| {
                    //if it does not contain the route id already
                    if !existing_route_ids.contains(&route.id) {
                        existing_route_ids.insert(route.id.clone());
                    }
                 }).or_insert(HashSet::from_iter([route.id.clone()]));

                 //assign shape to a route id
                 route_ids_to_shape_ids.entry(route.id.clone())
                 .and_modify(|existing_shape_ids| {
                    if !existing_shape_ids.contains(shape_id) {
                        existing_shape_ids.insert(shape_id.clone());
                    }
                 }).or_insert(
                    HashSet::from_iter([shape_id.clone()])
                 );

            }
        }   

    }

    ShapeToColourResponse {
        shape_to_color_lookup,
        shape_to_text_color_lookup,
        shape_id_to_route_ids_lookup,
        route_ids_to_shape_ids
    }    
}
