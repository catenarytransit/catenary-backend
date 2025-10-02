use crate::gtfs_handlers::colour_correction;
use catenary::enum_to_int::route_type_to_int;
use catenary::postgres_tools::CatenaryPostgresPool;
use diesel_async::RunQueryDsl;
use std::sync::Arc;

pub async fn insert_stop_to_stop_geometry(
    feed_id: &str,
    attempt_id: &str,
    chateau_id: &str,
    route: &gtfs_structures::Route,
    direction_id: u64,
    linestring: &postgis_diesel::types::LineString<postgis_diesel::types::Point>,
    arc_conn_pool: Arc<CatenaryPostgresPool>,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let conn_pool = arc_conn_pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre?;

    let route_type_number = route_type_to_int(&route.route_type);

    let route_label: String = match &route.short_name {
        Some(short_name) => short_name.to_owned(),
        None => match &route.long_name {
            Some(long_name) => long_name.to_owned(),
            _ => route.id.clone(),
        },
    };

    let bg_color =
        colour_correction::fix_background_colour_rgb_feed_route(feed_id, route.color, route);

    let bg_color_string = format!("{:02x}{:02x}{:02x}", bg_color.r, bg_color.g, bg_color.b);

    let text_color = match route.text_color {
        Some(text_color) => {
            format!(
                "{:02x}{:02x}{:02x}",
                text_color.r, text_color.g, text_color.b
            )
        }
        None => String::from("000000"),
    };

    let shape_value: catenary::models::Shape = catenary::models::Shape {
        onestop_feed_id: feed_id.to_string(),
        attempt_id: attempt_id.to_string(),
        shape_id: direction_id.to_string(),
        chateau: chateau_id.to_string(),
        linestring: linestring.clone(),
        color: Some(bg_color_string.clone()),
        routes: Some(vec![Some(route.id.clone())]),
        route_type: route_type_number,
        route_label: Some(route_label.clone()),
        route_label_translations: None,
        text_color: Some(text_color.clone()),
        allowed_spatial_query: false,
        stop_to_stop_generated: Some(true),
    };

    use catenary::schema::gtfs::shapes::dsl::shapes as shapes_table;

    diesel::insert_into(shapes_table)
        .values(shape_value)
        .execute(conn)
        .await?;

    Ok(())
}
