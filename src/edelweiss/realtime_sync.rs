//! Optional, RAM-only Aspen routing feed synchronization for Edelweiss.
//!
//! Opt-in staging integration: receives routing-only source pages, validates an
//! uninterrupted revision, and atomically replaces each chateau snapshot.
//! This intentionally does NOT change RAPTOR results until static trip/service
//! identity resolution and safe realtime reconstruction are implemented.

use anyhow::{Context, Result, bail, ensure};
use catenary::EtcdConnectionIps;
use catenary::aspen::lib::{
    ChateauMetadataEtcd, spawn_aspen_client_from_ip,
};
use catenary::etcd_cache::EtcdCache;
use catenary::routing_common::realtime_source::{
    MAX_ROUTING_PAGE_SIZE, ROUTING_SOURCE_PROTOCOL, RoutingPageState,
    RoutingSourcePageRequest, RoutingTripUpdate,
};
use catenary::routing_common::route_manager::Router;
use futures::StreamExt;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tarpc::context;

const MAX_UPDATES_PER_SOURCE: usize = 20_000;
const MAX_STOPS_PER_SOURCE: usize = 500_000;
const SYNC_CONCURRENCY: usize = 4;

#[derive(Debug)]
pub struct SourceSnapshot {
    pub revision_ms: u64,
    pub updates: Vec<RoutingTripUpdate>,
}

#[derive(Default)]
pub struct RealtimeSources {
    inner: RwLock<HashMap<String, Arc<SourceSnapshot>>>,
    /// Regional source identity, including the source's attempt ID.
    /// GTFS stop IDs must never be joined across these identities.
    pub region_sources: BTreeMap<String, Vec<(u16, String, String, String)>>,
}

impl RealtimeSources {
    pub fn from_router(router: &Router) -> Result<Self> {
        let mut region_sources = BTreeMap::new();
        for region in &router.transit {
            let tt = region.timetable.view()?;
            let mut sources = Vec::with_capacity(tt.sources().len());
            for (i, _source) in tt.sources().iter().enumerate() {
                let idx = u16::try_from(i).context("source index overflow")?;
                let (chateau, feed, attempt) = tt.source(catenary::routing_common::timetable::SourceIdx(idx))
                    .context("unreadable timetable source")?;
                sources.push((idx, chateau.to_owned(), feed.to_owned(), attempt.to_owned()));
            }
            region_sources.insert(region.id.clone(), sources);
        }
        Ok(Self { inner: RwLock::new(HashMap::new()), region_sources })
    }

    pub fn chateaux(&self) -> BTreeSet<String> {
        self.region_sources.values()
            .flat_map(|sources| sources.iter().map(|(_, c, _, _)| c.clone()))
            .collect()
    }

    pub fn get(&self, chateau: &str) -> Option<Arc<SourceSnapshot>> {
        self.inner.read().unwrap_or_else(|e| e.into_inner())
            .get(chateau).cloned()
    }

    fn publish(&self, chateau: String, snapshot: SourceSnapshot) {
        let mut all = self.inner.write().unwrap_or_else(|e| e.into_inner());
        if all.get(&chateau).is_some_and(|current| current.revision_ms >= snapshot.revision_ms) {
            return;
        }
        all.insert(chateau, Arc::new(snapshot));
    }

    pub fn stats(&self) -> (usize, usize) {
        let all = self.inner.read().unwrap_or_else(|e| e.into_inner());
        (all.len(), all.values().map(|v| v.updates.len()).sum())
    }
}

/// Fetch with an explicit page transaction. A single changed generation,
/// missing page, oversized feed or failed RPC aborts without changing the
/// previously published source snapshot. All intermediate pages stay in RAM.
async fn fetch_source(
    socket: std::net::SocketAddr,
    chateau: &str,
    revision: Option<u64>,
) -> Result<Option<SourceSnapshot>> {
    let client = spawn_aspen_client_from_ip(&socket).await
        .with_context(|| format!("connecting to Aspen {socket}"))?;
    let mut offset = 0u32;
    let mut expected = None;
    let mut updates = Vec::new();
    let mut stop_count = 0usize;
    loop {
        let request = RoutingSourcePageRequest {
            chateau_id: chateau.to_owned(),
            if_revision_ms: if offset == 0 { revision } else { None },
            expected_revision_ms: expected,
            offset,
            limit: MAX_ROUTING_PAGE_SIZE as u16,
        };
        let page = tokio::time::timeout(
            Duration::from_secs(15),
            client.get_routing_source_page(context::current(), request),
        ).await.context("Aspen RPC deadline exceeded")??
            .context("Aspen has no authoritative chateau snapshot")?;
        ensure!(page.protocol == ROUTING_SOURCE_PROTOCOL, "incompatible Aspen routing RPC protocol");
        ensure!(page.total_updates as usize <= MAX_UPDATES_PER_SOURCE, "source exceeds realtime update budget");
        match page.state {
            RoutingPageState::NotModified if offset == 0 => {
                return Ok(None);
            }
            RoutingPageState::RestartRequired => bail!("source revision changed during snapshot paging"),
            RoutingPageState::Data { updates: page_updates, next_offset } => {
                if offset == 0 {
                    expected = Some(page.revision_ms);
                }
                ensure!(expected == Some(page.revision_ms), "Aspen revision changed during snapshot paging");
                stop_count += page_updates.iter().map(|u| u.stops.len()).sum::<usize>();
                ensure!(stop_count <= MAX_STOPS_PER_SOURCE, "source exceeds realtime stop budget");
                updates.extend(page_updates);
                ensure!(updates.len() <= MAX_UPDATES_PER_SOURCE, "source exceeds realtime update budget");
                if let Some(next) = next_offset {
                    ensure!(next > offset && next as usize == updates.len(), "invalid Aspen page cursor");
                    offset = next;
                } else {
                    ensure!(updates.len() == page.total_updates as usize, "Aspen snapshot has missing updates");
                    return Ok(Some(SourceSnapshot {
                        revision_ms: page.revision_ms,
                        updates,
                    }));
                }
            }
            RoutingPageState::NotModified => bail!("unexpected conditional response mid-pagination"),
        }
    }
}

pub async fn synchronize(
    sources: Arc<RealtimeSources>,
    etcd_urls: Vec<String>,
    username: Option<String>,
    password: Option<String>,
    interval: Duration,
) -> Result<()> {
    let options = match (username, password) {
        (Some(username), Some(password)) => Some(
            etcd_client::ConnectOptions::new()
                .with_user(username, password)
                .with_keep_alive(Duration::from_secs(1), Duration::from_secs(5)),
        ),
        _ => None,
    };
    let assignments = EtcdCache::<ChateauMetadataEtcd>::new(
        Arc::new(EtcdConnectionIps { ip_addresses: etcd_urls }),
        Arc::new(options),
        "/aspen_assigned_chateaux/",
    ).await.map_err(anyhow::Error::msg)?;

    let chateaux: Vec<String> = sources.chateaux().into_iter().collect();
    eprintln!("Edelweiss routing source sync: {} chateaux; RAM-only, observe-only mode", chateaux.len());
    loop {
        futures::stream::iter(chateaux.iter().cloned())
            .for_each_concurrent(SYNC_CONCURRENCY, |chateau| {
                let sources = Arc::clone(&sources);
                let assigned = assignments.get(&chateau);
                async move {
                    let Some(assigned) = assigned else {
                        // Keep the last consistent snapshot on transient etcd gaps.
                        return;
                    };
                    let current = sources.get(&chateau);
                    match fetch_source(
                        assigned.socket,
                        &chateau,
                        current.as_ref().map(|s| s.revision_ms),
                    ).await {
                        Ok(Some(snapshot)) => sources.publish(chateau, snapshot),
                        Ok(None) => {},
                        Err(error) => eprintln!("Edelweiss routing source sync: {chateau}: {error:#}"),
                    }
                }
            })
            .await;
        let (active, updates) = sources.stats();
        eprintln!("Edelweiss routing source sync: {active} source snapshots; {updates} routing updates retained in RAM");
        tokio::time::sleep(interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_is_monotonic_and_old_readers_remain_valid() {
        let snapshots = RealtimeSources::default();
        snapshots.publish("a".to_string(), SourceSnapshot { revision_ms: 5, updates: vec![] });
        let old = snapshots.get("a").unwrap();
        snapshots.publish("a".to_string(), SourceSnapshot { revision_ms: 4, updates: vec![] });
        assert_eq!(snapshots.get("a").unwrap().revision_ms, 5);
        snapshots.publish("a".to_string(), SourceSnapshot { revision_ms: 6, updates: vec![] });
        assert_eq!(snapshots.get("a").unwrap().revision_ms, 6);
        assert_eq!(old.revision_ms, 5);
    }
}
