// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

pub fn polygon_geo_to_diesel(
    polygon_geo: geo::Polygon,
) -> postgis_diesel::types::Polygon<postgis_diesel::types::Point> {
    postgis_diesel::types::Polygon {
        rings: vec![
            polygon_geo
                .clone()
                .into_inner()
                .0
                .into_iter()
                .map(|coord| {
                    postgis_diesel::types::Point::new(coord.x, coord.y, Some(crate::WGS_84_SRID))
                })
                .collect(),
        ],
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
            .map(polygon_geo_to_diesel)
            .collect(),
    }
}

pub fn diesel_multi_polygon_to_geo(
    multipolygon_diesel: postgis_diesel::types::MultiPolygon<postgis_diesel::types::Point>,
) -> geo::MultiPolygon {
    geo::MultiPolygon::new(
        multipolygon_diesel
            .polygons
            .into_iter()
            .map(diesel_polygon_to_geo)
            .collect(),
    )
}

pub fn diesel_polygon_to_geo(
    polygon_diesel: postgis_diesel::types::Polygon<postgis_diesel::types::Point>,
) -> geo::Polygon {
    let rings: Vec<geo::LineString> = polygon_diesel
        .rings
        .into_iter()
        .map(|ring| {
            geo::LineString::from_iter(ring.into_iter().map(|point| geo::Coord {
                x: point.x,
                y: point.y,
            }))
        })
        .collect();

    let exterior = rings[0].clone();
    let interior = rings.into_iter().skip(1).collect();

    geo::Polygon::new(exterior, interior)
}
