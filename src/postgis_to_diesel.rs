pub fn polygon_geo_to_diesel(
    polygon_geo: geo::Polygon,
) -> postgis_diesel::types::Polygon<postgis_diesel::types::Point> {
    postgis_diesel::types::Polygon {
        rings: vec![polygon_geo
            .clone()
            .into_inner()
            .0
            .into_iter()
            .map(|coord| {
                postgis_diesel::types::Point::new(coord.x, coord.y, Some(crate::WGS_84_SRID))
            })
            .collect()],
        srid: Some(crate::WGS_84_SRID),
    }
}

pub fn multi_polygon_geo_to_diesel(
    multipolygon_geo: geo::MultiPolygon,
) -> postgis_diesel::types::MultiPolygon<postgis_diesel::types::Point> {
    postgis_diesel::types::MultiPolygon {
        srid: Some(crate::WGS_84_SRID),
        polygons: multipolygon_geo
            .into_iter()
            .map(|polygon| polygon_geo_to_diesel(polygon))
            .collect(),
    }
}
