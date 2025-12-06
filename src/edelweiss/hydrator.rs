use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::schema::gtfs;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use std::collections::HashMap;

pub struct Hydrator {
    pool: CatenaryPostgresPool,
}

impl Hydrator {
    pub fn new(pool: CatenaryPostgresPool) -> Self {
        Self { pool }
    }

    pub async fn hydrate_stops(
        &self,
        stop_ids: Vec<(String, String)>, // (chateau, station_id)
    ) -> anyhow::Result<HashMap<(String, String), String>> {
        println!("Hydrating stops {:#?}", stop_ids);
        use futures::stream::{self, StreamExt};
        use gtfs::stations::dsl::*;

        // Collect all unique station IDs
        let mut unique_ids = Vec::new();
        for (_, id) in &stop_ids {
            unique_ids.push(id.clone());
        }
        unique_ids.sort();
        unique_ids.dedup();

        // Query stations by ID (stations are global, no chateau filter needed)
        let chunks: Vec<Vec<String>> = unique_ids.chunks(100).map(|c| c.to_vec()).collect();
        let results = stream::iter(chunks) // Batch query
            .map(|ids| {
                let pool = self.pool.clone();
                async move {
                    let mut conn = pool.get().await.map_err(|e| anyhow::anyhow!(e))?;
                    stations
                        .filter(station_id.eq_any(ids))
                        .select((station_id, name))
                        .load::<(String, String)>(&mut conn)
                        .await
                        .map_err(|e| anyhow::anyhow!(e))
                }
            })
            .buffer_unordered(32)
            .collect::<Vec<_>>()
            .await;

        let mut id_to_name = HashMap::new();
        for res in results {
            let rows = res?;
            for (id, n) in rows {
                id_to_name.insert(id, n);
            }
        }

        // Map back to (chateau, id) keys
        let mut final_map = HashMap::new();
        for (c, id) in stop_ids {
            if let Some(n) = id_to_name.get(&id) {
                final_map.insert((c, id), n.clone());
            }
        }

        Ok(final_map)
    }

    pub async fn hydrate_routes(
        &self,
        route_ids: Vec<(String, String)>, // (chateau, route_id)
    ) -> anyhow::Result<HashMap<(String, String), String>> {
        use futures::stream::{self, StreamExt};
        use gtfs::routes::dsl::*;

        println!("Hydrating routes {:#?}", route_ids);
        // Group by chateau
        let mut by_chateau: HashMap<String, Vec<String>> = HashMap::new();
        for (c, id) in route_ids {
            by_chateau.entry(c).or_default().push(id);
        }

        let results = stream::iter(by_chateau)
            .map(|(c_val, ids)| {
                let pool = self.pool.clone();
                async move {
                    let mut conn = pool.get().await.map_err(|e| anyhow::anyhow!(e))?;
                    routes
                        .filter(chateau.eq(c_val))
                        .filter(route_id.eq_any(ids))
                        .select((chateau, route_id, short_name, long_name))
                        .load::<(String, String, Option<String>, Option<String>)>(&mut conn)
                        .await
                        .map_err(|e| anyhow::anyhow!(e))
                }
            })
            .buffer_unordered(32)
            .collect::<Vec<_>>()
            .await;

        let mut map = HashMap::new();
        for res in results {
            let rows = res?;
            for (c, id, short, long) in rows {
                let name = match (short, long) {
                    (Some(s), Some(l)) => format!("{} - {}", s, l),
                    (Some(s), None) => s,
                    (None, Some(l)) => l,
                    (None, None) => "Unknown Route".to_string(),
                };
                map.insert((c, id), name);
            }
        }
        Ok(map)
    }
}
