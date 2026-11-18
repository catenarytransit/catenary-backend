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
use catenary::routing_common::realtime_identity::{
    GtfsIdentityCatalog, RealtimeGtfsResolver, ResolvedInstance,
};
use catenary::routing_common::timetable::{LocationIdx, SourceIdx};
use catenary::routing_common::routing_regions::RoutingRegionsConfig;
use catenary::routing_common::realtime_source::{
    MAX_ROUTING_PAGE_SIZE, ROUTING_SOURCE_PROTOCOL, RoutingPageState,
    RoutingSourcePageRequest, RoutingTripUpdate,
};
use catenary::routing_common::route_manager::Router;
use futures::StreamExt;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, RwLock};
use std::path::Path;
use std::time::Duration;
use tarpc::context;

const MAX_UPDATES_PER_SOURCE: usize = 20_000;
const MAX_STOPS_PER_SOURCE: usize = 500_000;
const SYNC_CONCURRENCY: usize = 4;

#[derive(Debug, Clone)]
pub struct ResolvedStop {
    pub original: LocationIdx,
    pub assigned: Option<LocationIdx>,
    pub stop_update_index: u32,
}

#[derive(Debug, Clone)]
pub struct ResolvedTrip {
    pub region: String,
    pub source: SourceIdx,
    pub update_id: String,
    pub service_id: String,
    pub original: ResolvedInstance,
    pub stops: Vec<ResolvedStop>,
}

#[derive(Debug, Default, Clone)]
pub struct ResolutionStats {
    pub matched: usize,
    pub no_match: usize,
    pub ambiguous: usize,
    pub unresolved_stops: usize,
}

#[derive(Debug)]
pub struct SourceSnapshot {
    pub revision_ms: u64,
    pub updates: Vec<RoutingTripUpdate>,
    /// Fully validated scheduled instances, retained for a future overlay
    /// builder; no route suppression is enabled by these lookups yet.
    pub resolved: Vec<ResolvedTrip>,
    pub stats: ResolutionStats,
}

#[derive(Default)]
pub struct RealtimeSources {
    inner: RwLock<HashMap<String, Arc<SourceSnapshot>>>,
    /// Regional source identity, including the source's attempt ID.
    /// GTFS stop IDs must never be joined across these identities.
    pub region_sources: BTreeMap<String, Vec<(u16, String, String, String)>>,
    /// Built once by Gentian; only loaded when RAM-only sync is enabled.
    /// These are static metadata and never rewritten during realtime updates.
    pub identity_catalogs: BTreeMap<String, Arc<GtfsIdentityCatalog>>,
}

impl RealtimeSources {
    pub fn from_router(router: &Router, routing_config: &Path) -> Result<Self> {
        let mut region_sources = BTreeMap::new();
        let mut identity_catalogs = BTreeMap::new();
        let config = RoutingRegionsConfig::load(routing_config)?;
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
            let timetable_path = config.timetable_path(routing_config, &region.id, None)?;
            let sidecar_path = timetable_path.with_extension("rt-identities.bin");
            if sidecar_path.is_file() {
                let data = std::fs::read(&sidecar_path)
                    .with_context(|| format!("reading {}", sidecar_path.display()))?;
                let catalog: GtfsIdentityCatalog =
                    catenary::bincode_deserialize(&data)
                    .with_context(|| format!("decoding {}", sidecar_path.display()))?;
                catalog.validate(&tt)
                    .with_context(|| format!("validating {}", sidecar_path.display()))?;
                identity_catalogs.insert(region.id.clone(), Arc::new(catalog));
            } else {
                eprintln!("Edelweiss routing source sync: region {} has no GTFS service-identity sidecar yet; compile Gentian with this PR", region.id);
            }
        }
        Ok(Self { inner: RwLock::new(HashMap::new()), region_sources, identity_catalogs })
    }

    /// Resolve a complete Chateau snapshot against each source-scoped Gentian
    /// catalog. Multiple feed matches for one update are ambiguous: never
    /// choose a transport from another feed or invent a service-day match.
    pub fn resolve_for_chateau(
        &self,
        router: &Router,
        chateau: &str,
        snapshot: &mut SourceSnapshot,
    ) {
        let mut alternatives = BTreeMap::<(String, usize), Vec<ResolvedTrip>>::new();
        let mut stop_errors = 0usize;
        for region in &router.transit {
            let Some(catalog) = self.identity_catalogs.get(&region.id) else {
                continue;
            };
            let Some(sources) = self.region_sources.get(&region.id) else {
                continue;
            };
            let Ok(tt) = region.timetable.view() else { continue };
            for &(source, ref assigned_chateau, _, _) in sources {
                if assigned_chateau != chateau { continue; }
                let source = SourceIdx(source);
                let Ok(resolver) = RealtimeGtfsResolver::new(
                    &tt, catalog, source, &snapshot.updates,
                ) else {
                    continue;
                };
                for (update_index, update) in snapshot.updates.iter().enumerate() {
                    let Ok(original) = resolver.instance(source, update) else { continue };
                    let mut stops = Vec::new();
                    let mut all_stops_resolved = true;
                    for (index, stop) in update.stops.iter().enumerate() {
                        if let Ok((original_stop, assigned)) =
                            resolver.stop_for_update(source, stop)
                        {
                            stops.push(ResolvedStop {
                                original: original_stop,
                                assigned,
                                stop_update_index: index as u32,
                            });
                        } else {
                            // GTFS stop_sequence is not necessarily contiguous
                            // and the current V6 timetable discarded original
                            // stop_sequence numbers. Never guess an occurrence
                            // index or silently drop a rerouted stop.
                            all_stops_resolved = false;
                            stop_errors += 1;
                        }
                    }
                    if !all_stops_resolved { continue; }
                    let Some(service) = catalog.service_id(original.transport) else { continue };
                    alternatives
                        .entry((region.id.clone(), update_index))
                        .or_default()
                        .push(ResolvedTrip {
                            region: region.id.clone(),
                            source,
                            update_id: update.update_id.clone(),
                            service_id: service.to_owned(),
                            original,
                            stops,
                        });
                }
            }
        }
        let mut matched = 0;
        let mut ambiguous = 0;
        let mut unresolved = 0;
        for region in &router.transit {
            if !self.identity_catalogs.contains_key(&region.id) {
                continue;
            }
            for (index, _) in snapshot.updates.iter().enumerate() {
                match alternatives.remove(&(region.id.clone(), index)) {
                    Some(mut candidates) if candidates.len() == 1 => {
                        snapshot.resolved.push(candidates.pop().unwrap());
                        matched += 1;
                    }
                    Some(_) => ambiguous += 1,
                    None => unresolved += 1,
                }
            }
        }
        snapshot.stats = ResolutionStats {
            matched,
            ambiguous,
            no_match: unresolved,
            unresolved_stops: stop_errors,
        };
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
        .map_err(|error| anyhow::anyhow!("connecting to Aspen {socket}: {error}"))?;
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
                        resolved: Vec::new(),
                        stats: ResolutionStats::default(),
                    }));
                }
            }
            RoutingPageState::NotModified => bail!("unexpected conditional response mid-pagination"),
        }
    }
}

pub async fn synchronize(
    sources: Arc<RealtimeSources>,
    router: Arc<Router>,
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
                let router = Arc::clone(&router);
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
                        Ok(Some(snapshot)) => {
                            // Region-scale source and trip-ID scans can take
                            // significant CPU time; never block an Actix/Tokio
                            // reactor while resolving the new Aspen snapshot.
                            let resolving_sources = Arc::clone(&sources);
                            let resolving_router = Arc::clone(&router);
                            let resolving_chateau = chateau.clone();
                            match tokio::task::spawn_blocking(move || {
                                let mut snapshot = snapshot;
                                let started = std::time::Instant::now();
                                resolving_sources.resolve_for_chateau(
                                    &resolving_router, &resolving_chateau, &mut snapshot,
                                );
                                (snapshot, started.elapsed())
                            }).await {
                                Ok((snapshot, elapsed)) => {
                                    eprintln!(
                                        "Edelweiss routing resolver {chateau}: matched={} ambiguous={} no_match={} unresolved_stops={} elapsed_ms={:.1}",
                                        snapshot.stats.matched,
                                        snapshot.stats.ambiguous,
                                        snapshot.stats.no_match,
                                        snapshot.stats.unresolved_stops,
                                        elapsed.as_secs_f64() * 1000.0,
                                    );
                                    sources.publish(chateau, snapshot);
                                },
                                Err(error) => eprintln!(
                                    "Edelweiss routing resolver {chateau}: task failed: {error}",
                                ),
                            }
                        },
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
        snapshots.publish("a".to_string(), SourceSnapshot { revision_ms: 5, updates: vec![], resolved: vec![], stats: ResolutionStats::default() });
        let old = snapshots.get("a").unwrap();
        snapshots.publish("a".to_string(), SourceSnapshot { revision_ms: 4, updates: vec![], resolved: vec![], stats: ResolutionStats::default() });
        assert_eq!(snapshots.get("a").unwrap().revision_ms, 5);
        snapshots.publish("a".to_string(), SourceSnapshot { revision_ms: 6, updates: vec![], resolved: vec![], stats: ResolutionStats::default() });
        assert_eq!(snapshots.get("a").unwrap().revision_ms, 6);
        assert_eq!(old.revision_ms, 5);
    }
}
