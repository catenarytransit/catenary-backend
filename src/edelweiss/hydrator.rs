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
        stop_ids: Vec<(String, String)>, // (chateau, gtfs_id)
    ) -> anyhow::Result<HashMap<(String, String), String>> {
        use gtfs::stops::dsl::*;
        let mut conn = self.pool.get().await?;

        // Extract just the IDs for the IN clause
        let ids: Vec<String> = stop_ids.iter().map(|(_, id)| id.clone()).collect();

        let results = stops
            .filter(gtfs_id.eq_any(&ids))
            .select((chateau, gtfs_id, name))
            .load::<(String, String, Option<String>)>(&mut conn)
            .await?;

        let mut map = HashMap::new();
        // Create a set of requested (chateau, id) for fast lookup if needed,
        // but since we return a map keyed by (chateau, id), we can just insert all matches.
        // The caller will only look up what they asked for.
        for (c, id, name_opt) in results {
            if let Some(n) = name_opt {
                map.insert((c, id), n);
            }
        }
        Ok(map)
    }

    pub async fn hydrate_routes(
        &self,
        route_ids: Vec<(String, String)>, // (chateau, route_id)
    ) -> anyhow::Result<HashMap<(String, String), String>> {
        use gtfs::routes::dsl::*;
        let mut conn = self.pool.get().await?;

        let ids: Vec<String> = route_ids.iter().map(|(_, id)| id.clone()).collect();

        let results = routes
            .filter(route_id.eq_any(&ids))
            .select((chateau, route_id, short_name, long_name))
            .load::<(String, String, Option<String>, Option<String>)>(&mut conn)
            .await?;

        let mut map = HashMap::new();
        for (c, id, short, long) in results {
            let name = match (short, long) {
                (Some(s), Some(l)) => format!("{} - {}", s, l),
                (Some(s), None) => s,
                (None, Some(l)) => l,
                (None, None) => "Unknown Route".to_string(),
            };
            map.insert((c, id), name);
        }
        Ok(map)
    }
}
