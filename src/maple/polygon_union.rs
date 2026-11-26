use geo::algorithm::orient::{Direction, Orient};
use geo::algorithm::unary_union;
use geo::{MultiPolygon, Polygon};

/// Dissolve all polygons in one overlay, avoiding repeated processing of the
/// growing result of a left-fold union. Preserve disconnected components.
pub(super) fn union_polygons(polygons: Vec<Polygon<f64>>) -> MultiPolygon<f64> {
    if polygons.len() <= 1 {
        return MultiPolygon(polygons);
    }

    // unary_union uses a signed fill rule: all exteriors must have the same
    // winding, with holes wound in the opposite direction. Feed hulls and
    // segment buffers are not guaranteed to arrive with consistent winding.
    let polygons = polygons
        .into_iter()
        .map(|polygon| polygon.orient(Direction::Default))
        .collect::<Vec<_>>();
    unary_union(&polygons)
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo::{Area, BooleanOps, LineString};
    use rayon::prelude::*;

    fn rectangle(x: f64, y: f64, width: f64, height: f64) -> Polygon<f64> {
        Polygon::new(
            LineString::from(vec![
                (x, y),
                (x + width, y),
                (x + width, y + height),
                (x, y + height),
                (x, y),
            ]),
            vec![],
        )
    }

    fn sequential_union(polygons: &[Polygon<f64>]) -> MultiPolygon<f64> {
        let mut result = MultiPolygon(vec![]);
        for polygon in polygons {
            result = result.union(polygon);
        }
        result
    }

    fn assert_matches_sequential(polygons: Vec<Polygon<f64>>, components: usize) {
        let expected = sequential_union(&polygons);
        let actual = union_polygons(polygons);
        assert_eq!(actual.0.len(), components);
        let tolerance = expected.unsigned_area().max(1.0) * 1e-7;
        assert!(actual.xor(&expected).unsigned_area() < tolerance);
    }

    #[test]
    fn empty_and_single_polygon() {
        assert!(union_polygons(vec![]).0.is_empty());
        let polygon = rectangle(0.0, 0.0, 2.0, 2.0);
        assert_eq!(
            union_polygons(vec![polygon.clone()]),
            MultiPolygon(vec![polygon])
        );
    }

    #[test]
    fn overlaps_containment_duplicates_and_mixed_winding() {
        let outer = rectangle(0.0, 0.0, 4.0, 4.0);
        assert_matches_sequential(
            vec![
                outer.clone(),
                rectangle(2.0, 0.0, 4.0, 4.0).orient(Direction::Reversed),
                rectangle(1.0, 1.0, 1.0, 1.0),
                outer,
            ],
            1,
        );
    }

    #[test]
    fn touching_and_disconnected_components() {
        assert_matches_sequential(
            vec![
                rectangle(0.0, 0.0, 2.0, 2.0),
                rectangle(2.0, 0.0, 2.0, 2.0),
                rectangle(10.0, 0.0, 2.0, 2.0),
            ],
            2,
        );
    }

    #[test]
    fn holes_remain_unless_filled_by_another_polygon() {
        let donut = Polygon::new(
            rectangle(0.0, 0.0, 10.0, 10.0).exterior().clone(),
            vec![rectangle(2.0, 2.0, 6.0, 6.0).exterior().clone()],
        );
        let union = union_polygons(vec![donut.clone(), rectangle(12.0, 0.0, 2.0, 2.0)]);
        assert_eq!(
            union.0.iter().map(|p| p.interiors().len()).sum::<usize>(),
            1
        );
        assert!((union.unsigned_area() - 68.0).abs() < 1e-6);
        assert_matches_sequential(vec![donut, rectangle(2.0, 2.0, 6.0, 6.0)], 1);
    }

    #[test]
    fn parallel_jobs_match_serial_results() {
        let inputs = (0..32)
            .map(|i| {
                vec![
                    rectangle(i as f64, 0.0, 2.0, 2.0),
                    rectangle(i as f64 + 1.0, 0.0, 2.0, 2.0),
                ]
            })
            .collect::<Vec<_>>();
        let serial = inputs
            .iter()
            .cloned()
            .map(union_polygons)
            .collect::<Vec<_>>();
        let parallel = inputs
            .into_par_iter()
            .map(union_polygons)
            .collect::<Vec<_>>();
        assert_eq!(serial, parallel);
    }

    // cargo test --release --bin maple polygon_union::tests::benchmark_many_hulls -- --ignored --nocapture
    #[test]
    #[ignore = "manual performance comparison"]
    fn benchmark_many_hulls() {
        for count in [100, 500, 1000] {
            for spacing in [0.75, 3.0] {
                let polygons = (0..count)
                    .map(|i| rectangle(i as f64 * spacing, 0.0, 2.0, 2.0))
                    .collect::<Vec<_>>();
                let start = std::time::Instant::now();
                let sequential = sequential_union(&polygons);
                let sequential_time = start.elapsed();
                let start = std::time::Instant::now();
                let batch = union_polygons(polygons);
                let batch_time = start.elapsed();
                assert!(batch.xor(&sequential).unsigned_area() < sequential.unsigned_area() * 1e-7);
                println!(
                    "{count} hulls, spacing {spacing}: sequential {sequential_time:?}, batch {batch_time:?}"
                );
            }
        }
    }
}
