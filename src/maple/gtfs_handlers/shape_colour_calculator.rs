use crate::gtfs_handlers::colour_correction;
use rgb::RGB;
use std::collections::HashMap;

pub fn shape_to_colour(feed_id: &str, gtfs: &gtfs_structures::Gtfs) -> (HashMap<String, RGB<u8>>, HashMap<String, RGB<u8>>) {
    let mut shape_to_color_lookup: HashMap<String, RGB<u8>> = HashMap::new();
    let mut shape_to_text_color_lookup: HashMap<String, RGB<u8>> = HashMap::new();

        for (trip_id, trip) in &gtfs.trips {
            if trip.shape_id.is_some() {
                if !shape_to_color_lookup.contains_key(&trip.shape_id.as_ref().unwrap().to_owned()) {
                    if gtfs.routes.contains_key(&trip.route_id) {
                        let route = gtfs
                            .routes
                            .get(&trip.route_id)
                            .unwrap();

                        let color = colour_correction::fix_background_colour_rgb_feed_route(feed_id,route.color,route);

                        shape_to_color_lookup.insert(
                        trip.shape_id.as_ref().unwrap().to_owned(),
                            color,
                        );
                        shape_to_text_color_lookup.insert(
                            trip.shape_id.as_ref().unwrap().to_owned(),
                            route.text_color,
                        );
                    }
                }
            }
        }

    (shape_to_color_lookup,shape_to_text_color_lookup)
}