use crate::CatenaryPostgresPool;
use actix_web::http::{StatusCode, header};
use actix_web::{HttpResponse, web};
use catenary::models::{DirectionPatternMeta, DirectionPatternRow, Route, Shape, Stop};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[derive(Deserialize)]
pub struct ExportQuery {
    pub chateau: String,
    pub route_id: String,
    pub format: Option<String>,
    #[serde(default, alias = "stops", alias = "with_stops")]
    pub include_stops: bool,
}

#[derive(Clone, Copy)]
enum ExportFormat {
    GeoJson,
    Kml,
    Gpx,
}

impl ExportFormat {
    fn parse(value: Option<&str>) -> Result<Self, &'static str> {
        let normalized = value.unwrap_or("geojson").trim().to_ascii_lowercase();

        match normalized.as_str() {
            "geojson" | "geo-json" | "json" => Ok(Self::GeoJson),
            "kml" => Ok(Self::Kml),
            "gpx" => Ok(Self::Gpx),
            _ => Err("format must be one of: geojson, kml, gpx"),
        }
    }

    fn extension(self) -> &'static str {
        match self {
            Self::GeoJson => "geojson",
            Self::Kml => "kml",
            Self::Gpx => "gpx",
        }
    }

    fn content_type(self) -> &'static str {
        match self {
            Self::GeoJson => "application/geo+json",
            Self::Kml => "application/vnd.google-earth.kml+xml",
            Self::Gpx => "application/gpx+xml",
        }
    }
}

struct ExportShape {
    shape_id: String,
    coordinates: Vec<(f64, f64)>,
}

struct ExportStop {
    stop_id: String,
    name: String,
    longitude: f64,
    latitude: f64,
}

struct ExportDocument {
    chateau: String,
    route_id: String,
    route_name: String,
    color: String,
    shapes: Vec<ExportShape>,
    stops: Vec<ExportStop>,
}

#[actix_web::get("/export_route_geom")]
pub async fn export_route_geom(
    query: web::Query<ExportQuery>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
) -> HttpResponse {
    let query = query.into_inner();

    if query.chateau.trim().is_empty() || query.route_id.trim().is_empty() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "chateau and route_id are required",
        );
    }

    let format = match ExportFormat::parse(query.format.as_deref()) {
        Ok(format) => format,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message),
    };

    let conn_pool = pool.as_ref();
    let mut conn = match conn_pool.get().await {
        Ok(conn) => conn,
        Err(error) => {
            eprintln!("Error connecting to postgres for route export: {error}");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Error connecting to postgres",
            );
        }
    };
    let conn = &mut conn;

    let route_rows = catenary::schema::gtfs::routes::dsl::routes
        .filter(catenary::schema::gtfs::routes::dsl::chateau.eq(&query.chateau))
        .filter(catenary::schema::gtfs::routes::dsl::route_id.eq(&query.route_id))
        .limit(1)
        .select(Route::as_select())
        .load::<Route>(conn)
        .await;

    let route = match route_rows {
        Ok(mut routes) => match routes.pop() {
            Some(route) => route,
            None => return error_response(StatusCode::NOT_FOUND, "Route not found"),
        },
        Err(error) => {
            eprintln!("Error fetching route for export: {error}");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Could not fetch route information",
            );
        }
    };

    let direction_patterns = catenary::schema::gtfs::direction_pattern_meta::dsl::direction_pattern_meta
        .filter(
            catenary::schema::gtfs::direction_pattern_meta::dsl::chateau.eq(&query.chateau),
        )
        .filter(
            catenary::schema::gtfs::direction_pattern_meta::dsl::route_id.eq(&query.route_id),
        )
        .select(DirectionPatternMeta::as_select())
        .load::<DirectionPatternMeta>(conn)
        .await;

    let direction_patterns = match direction_patterns {
        Ok(patterns) => patterns,
        Err(error) => {
            eprintln!("Error fetching direction patterns for export: {error}");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Could not fetch route direction patterns",
            );
        }
    };

    let mut shape_ids = direction_patterns
        .iter()
        .filter_map(|pattern| pattern.gtfs_shape_id.clone())
        .collect::<BTreeSet<String>>();

    if let Some(route_shape_ids) = &route.shapes_list {
        shape_ids.extend(route_shape_ids.iter().filter_map(|shape_id| shape_id.clone()));
    }

    if shape_ids.is_empty() {
        return error_response(StatusCode::NOT_FOUND, "No shapes found for route");
    }

    let shape_ids = shape_ids.into_iter().collect::<Vec<String>>();
    let shape_rows = catenary::schema::gtfs::shapes::dsl::shapes
        .filter(catenary::schema::gtfs::shapes::dsl::chateau.eq(&query.chateau))
        .filter(catenary::schema::gtfs::shapes::dsl::shape_id.eq_any(&shape_ids))
        .select(Shape::as_select())
        .load::<Shape>(conn)
        .await;

    let shape_rows = match shape_rows {
        Ok(shapes) => shapes,
        Err(error) => {
            eprintln!("Error fetching shapes for export: {error}");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Could not fetch route shapes",
            );
        }
    };

    let shape_color = shape_rows.iter().find_map(|shape| shape.color.as_deref());
    let color = normalize_color(route.color.as_deref().or(shape_color));

    let mut shape_map = BTreeMap::<String, ExportShape>::new();
    for shape in shape_rows {
        let coordinates = shape
            .linestring
            .points
            .iter()
            .filter(|point| point.x.is_finite() && point.y.is_finite())
            .map(|point| (point.x, point.y))
            .collect::<Vec<(f64, f64)>>();

        if coordinates.len() >= 2 {
            shape_map.entry(shape.shape_id.clone()).or_insert(ExportShape {
                shape_id: shape.shape_id,
                coordinates,
            });
        }
    }

    let shapes = shape_map.into_values().collect::<Vec<ExportShape>>();
    if shapes.is_empty() {
        return error_response(StatusCode::NOT_FOUND, "No usable shapes found for route");
    }

    let stops = if query.include_stops {
        match fetch_stops(conn, &query.chateau, &direction_patterns).await {
            Ok(stops) => stops,
            Err(response) => return response,
        }
    } else {
        Vec::new()
    };

    let route_name = route
        .short_name
        .as_deref()
        .filter(|name| !name.trim().is_empty())
        .or_else(|| {
            route
                .long_name
                .as_deref()
                .filter(|name| !name.trim().is_empty())
        })
        .unwrap_or(&route.route_id)
        .to_string();

    let document = ExportDocument {
        chateau: query.chateau,
        route_id: query.route_id,
        route_name,
        color,
        shapes,
        stops,
    };

    let body = match format {
        ExportFormat::GeoJson => render_geojson(&document),
        ExportFormat::Kml => render_kml(&document),
        ExportFormat::Gpx => render_gpx(&document),
    };

    let filename = format!(
        "{}-{}.{}",
        safe_filename_component(&document.chateau),
        safe_filename_component(&document.route_id),
        format.extension()
    );

    HttpResponse::Ok()
        .insert_header((header::CONTENT_TYPE, format.content_type()))
        .insert_header((
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{filename}\""),
        ))
        .insert_header((header::CACHE_CONTROL, "public, max-age=3600"))
        .body(body)
}

async fn fetch_stops(
    conn: &mut diesel_async::AsyncPgConnection,
    chateau: &str,
    direction_patterns: &[DirectionPatternMeta],
) -> Result<Vec<ExportStop>, HttpResponse> {
    let mut direction_pattern_ids = direction_patterns
        .iter()
        .map(|pattern| pattern.direction_pattern_id.clone())
        .collect::<Vec<String>>();
    direction_pattern_ids.sort();
    direction_pattern_ids.dedup();

    if direction_pattern_ids.is_empty() {
        return Ok(Vec::new());
    }

    let direction_rows = catenary::schema::gtfs::direction_pattern::dsl::direction_pattern
        .filter(catenary::schema::gtfs::direction_pattern::dsl::chateau.eq(chateau))
        .filter(
            catenary::schema::gtfs::direction_pattern::dsl::direction_pattern_id
                .eq_any(&direction_pattern_ids),
        )
        .select(DirectionPatternRow::as_select())
        .load::<DirectionPatternRow>(conn)
        .await;

    let direction_rows = match direction_rows {
        Ok(rows) => rows,
        Err(error) => {
            eprintln!("Error fetching direction rows for export: {error}");
            return Err(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Could not fetch route stops",
            ));
        }
    };

    let mut stop_ids = direction_rows
        .into_iter()
        .map(|row| row.stop_id)
        .collect::<Vec<_>>();
    stop_ids.sort();
    stop_ids.dedup();

    if stop_ids.is_empty() {
        return Ok(Vec::new());
    }

    let stop_rows = catenary::schema::gtfs::stops::dsl::stops
        .filter(catenary::schema::gtfs::stops::dsl::chateau.eq(chateau))
        .filter(catenary::schema::gtfs::stops::dsl::gtfs_id.eq_any(&stop_ids))
        .select(Stop::as_select())
        .load::<Stop>(conn)
        .await;

    let stop_rows = match stop_rows {
        Ok(stops) => stops,
        Err(error) => {
            eprintln!("Error fetching stops for export: {error}");
            return Err(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Could not fetch route stops",
            ));
        }
    };

    let mut stops = BTreeMap::<String, ExportStop>::new();
    for stop in stop_rows {
        let Some(point) = stop.point else {
            continue;
        };

        if !point.x.is_finite() || !point.y.is_finite() {
            continue;
        }

        let name = stop
            .displayname
            .filter(|name| !name.trim().is_empty())
            .or_else(|| stop.name.filter(|name| !name.trim().is_empty()))
            .unwrap_or_else(|| stop.gtfs_id.clone());

        stops.entry(stop.gtfs_id.clone()).or_insert(ExportStop {
            stop_id: stop.gtfs_id,
            name,
            longitude: point.x,
            latitude: point.y,
        });
    }

    Ok(stops.into_values().collect())
}

fn render_geojson(document: &ExportDocument) -> String {
    let route_color = format!("#{}", document.color);
    let mut features = Vec::<Value>::new();

    for shape in &document.shapes {
        let coordinates = shape
            .coordinates
            .iter()
            .map(|(longitude, latitude)| json!([longitude, latitude]))
            .collect::<Vec<Value>>();

        features.push(json!({
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": coordinates
            },
            "properties": {
                "feature_type": "route_shape",
                "chateau": &document.chateau,
                "route_id": &document.route_id,
                "shape_id": &shape.shape_id,
                "name": &document.route_name,
                "color": &route_color,
                "stroke": &route_color,
                "stroke-width": 4,
                "stroke-opacity": 1.0
            }
        }));
    }

    for stop in &document.stops {
        features.push(json!({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [stop.longitude, stop.latitude]
            },
            "properties": {
                "feature_type": "stop",
                "chateau": &document.chateau,
                "route_id": &document.route_id,
                "stop_id": &stop.stop_id,
                "name": &stop.name,
                "color": &route_color,
                "marker-color": &route_color,
                "marker-size": "small"
            }
        }));
    }

    json!({
        "type": "FeatureCollection",
        "features": features
    })
    .to_string()
}

fn render_kml(document: &ExportDocument) -> String {
    let route_name = xml_escape(&document.route_name);
    let chateau = xml_escape(&document.chateau);
    let route_id = xml_escape(&document.route_id);
    let kml_color = kml_color(&document.color);
    let display_color = format!("#{}", document.color);

    let mut output = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
<kml xmlns=\"http://www.opengis.net/kml/2.2\">\n<Document>\n",
    );
    output.push_str(&format!("<name>{route_name}</name>\n"));
    output.push_str(&format!(
        "<Style id=\"route-style\"><LineStyle><color>{kml_color}</color><width>4</width></LineStyle></Style>\n"
    ));
    output.push_str(&format!(
        "<Style id=\"stop-style\"><IconStyle><color>{kml_color}</color><scale>0.8</scale></IconStyle></Style>\n"
    ));

    for shape in &document.shapes {
        let shape_id = xml_escape(&shape.shape_id);
        let placemark_name = if document.shapes.len() == 1 {
            route_name.clone()
        } else {
            format!("{route_name} ({shape_id})")
        };
        let coordinates = shape
            .coordinates
            .iter()
            .map(|(longitude, latitude)| format!("{longitude:.7},{latitude:.7},0"))
            .collect::<Vec<String>>()
            .join(" ");

        output.push_str("<Placemark>\n");
        output.push_str(&format!("<name>{placemark_name}</name>\n"));
        output.push_str("<styleUrl>#route-style</styleUrl>\n");
        output.push_str("<ExtendedData>");
        output.push_str(&format!(
            "<Data name=\"chateau\"><value>{chateau}</value></Data>"
        ));
        output.push_str(&format!(
            "<Data name=\"route_id\"><value>{route_id}</value></Data>"
        ));
        output.push_str(&format!(
            "<Data name=\"shape_id\"><value>{shape_id}</value></Data>"
        ));
        output.push_str(&format!(
            "<Data name=\"color\"><value>{display_color}</value></Data>"
        ));
        output.push_str("</ExtendedData>\n");
        output.push_str(&format!(
            "<LineString><tessellate>1</tessellate><coordinates>{coordinates}</coordinates></LineString>\n"
        ));
        output.push_str("</Placemark>\n");
    }

    for stop in &document.stops {
        let stop_name = xml_escape(&stop.name);
        let stop_id = xml_escape(&stop.stop_id);
        output.push_str("<Placemark>\n");
        output.push_str(&format!("<name>{stop_name}</name>\n"));
        output.push_str("<styleUrl>#stop-style</styleUrl>\n");
        output.push_str(&format!(
            "<ExtendedData><Data name=\"stop_id\"><value>{stop_id}</value></Data><Data name=\"route_id\"><value>{route_id}</value></Data></ExtendedData>\n"
        ));
        output.push_str(&format!(
            "<Point><coordinates>{:.7},{:.7},0</coordinates></Point>\n",
            stop.longitude, stop.latitude
        ));
        output.push_str("</Placemark>\n");
    }

    output.push_str("</Document>\n</kml>\n");
    output
}

fn render_gpx(document: &ExportDocument) -> String {
    let route_name = xml_escape(&document.route_name);
    let route_id = xml_escape(&document.route_id);

    let mut output = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
<gpx version=\"1.1\" creator=\"Catenary Maps\"\n\
 xmlns=\"http://www.topografix.com/GPX/1/1\"\n\
 xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n\
 xmlns:gpx_style=\"http://www.topografix.com/GPX/gpx_style/0/2\"\n\
 xsi:schemaLocation=\"http://www.topografix.com/GPX/1/1 http://www.topografix.com/GPX/1/1/gpx.xsd http://www.topografix.com/GPX/gpx_style/0/2 http://www.topografix.com/GPX/gpx_style/0/2/gpx_style.xsd\">\n",
    );
    output.push_str(&format!(
        "<metadata><name>{route_name}</name><desc>Route {route_id}</desc></metadata>\n"
    ));

    // GPX requires waypoints to appear before tracks.
    for stop in &document.stops {
        let stop_name = xml_escape(&stop.name);
        let stop_id = xml_escape(&stop.stop_id);
        output.push_str(&format!(
            "<wpt lat=\"{:.7}\" lon=\"{:.7}\"><name>{stop_name}</name><desc>Stop {stop_id}</desc></wpt>\n",
            stop.latitude, stop.longitude
        ));
    }

    for shape in &document.shapes {
        let shape_id = xml_escape(&shape.shape_id);
        let track_name = if document.shapes.len() == 1 {
            route_name.clone()
        } else {
            format!("{route_name} ({shape_id})")
        };

        output.push_str("<trk>\n");
        output.push_str(&format!("<name>{track_name}</name>\n"));
        output.push_str("<type>Transit</type>\n");
        output.push_str("<extensions><gpx_style:line>");
        output.push_str(&format!(
            "<gpx_style:color>{}</gpx_style:color>",
            document.color
        ));
        output.push_str("<gpx_style:opacity>1.0</gpx_style:opacity>");
        output.push_str("<gpx_style:width>4</gpx_style:width>");
        output.push_str("</gpx_style:line></extensions>\n");
        output.push_str("<trkseg>\n");

        for (longitude, latitude) in &shape.coordinates {
            output.push_str(&format!(
                "<trkpt lat=\"{latitude:.7}\" lon=\"{longitude:.7}\"/>\n"
            ));
        }

        output.push_str("</trkseg>\n</trk>\n");
    }

    output.push_str("</gpx>\n");
    output
}

fn normalize_color(value: Option<&str>) -> String {
    let raw = value.unwrap_or_default().trim();
    let value = raw.strip_prefix('#').unwrap_or(raw);

    if value.len() == 6 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        value.to_ascii_uppercase()
    } else {
        "000000".to_string()
    }
}

fn kml_color(rgb: &str) -> String {
    format!("ff{}{}{}", &rgb[4..6], &rgb[2..4], &rgb[0..2]).to_ascii_lowercase()
}

fn xml_escape(value: &str) -> String {
    html_escape::encode_text(value).into_owned()
}

fn safe_filename_component(value: &str) -> String {
    let component = value
        .chars()
        .take(80)
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_string();

    if component.is_empty() {
        "route".to_string()
    } else {
        component
    }
}

fn error_response(status: StatusCode, message: &str) -> HttpResponse {
    HttpResponse::build(status).json(json!({ "error": message }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_gtfs_colors() {
        assert_eq!(normalize_color(Some("#12abEF")), "12ABEF");
        assert_eq!(normalize_color(Some("not-a-color")), "000000");
        assert_eq!(normalize_color(None), "000000");
    }

    #[test]
    fn converts_rgb_to_kml_abgr() {
        assert_eq!(kml_color("112233"), "ff332211");
    }

    #[test]
    fn sanitizes_download_filename_components() {
        assert_eq!(safe_filename_component("A/B C"), "A_B_C");
        assert_eq!(safe_filename_component("///"), "route");
    }
}
