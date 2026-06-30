use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use rstar::{AABB, RTree, RTreeObject};
use std::sync::Arc;

pub struct ChateauRTree {
    pub tree: RTree<ChateauBoundingBox>,
}

#[derive(Clone, Debug)]
pub struct ChateauBoundingBox {
    pub chateau_id: String,
    pub aabb: AABB<[f64; 2]>,
}

impl RTreeObject for ChateauBoundingBox {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        self.aabb
    }
}

impl ChateauRTree {
    pub async fn load(pool: &Arc<CatenaryPostgresPool>) -> Self {
        let mut conn = pool.get().await.unwrap();

        let chateaus = catenary::schema::gtfs::chateaus::dsl::chateaus
            .select(catenary::models::Chateau::as_select())
            .load::<catenary::models::Chateau>(&mut conn)
            .await
            .unwrap_or_default();

        let mut items = Vec::new();

        for ch in chateaus {
            if let Some(hull) = ch.hull {
                let mut min_x = f64::MAX;
                let mut min_y = f64::MAX;
                let mut max_x = f64::MIN;
                let mut max_y = f64::MIN;

                let mut has_points = false;

                for poly in hull.polygons {
                    for ring in poly.rings {
                        for pt in ring {
                            has_points = true;
                            if pt.x < min_x {
                                min_x = pt.x;
                            }
                            if pt.x > max_x {
                                max_x = pt.x;
                            }
                            if pt.y < min_y {
                                min_y = pt.y;
                            }
                            if pt.y > max_y {
                                max_y = pt.y;
                            }
                        }
                    }
                }

                if has_points {
                    let aabb = AABB::from_corners([min_x, min_y], [max_x, max_y]);
                    items.push(ChateauBoundingBox {
                        chateau_id: ch.chateau,
                        aabb,
                    });
                }
            }
        }

        Self {
            tree: RTree::bulk_load(items),
        }
    }

    pub fn locate_in_envelope(
        &self,
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
    ) -> std::collections::HashSet<String> {
        let search_aabb = AABB::from_corners([min_x, min_y], [max_x, max_y]);
        self.tree
            .locate_in_envelope_intersecting(&search_aabb)
            .map(|c| c.chateau_id.clone())
            .collect()
    }
}
