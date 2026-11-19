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
use catenary::aspen_dataset::AspenisedTripScheduleRelationship as TripRelationship;
use catenary::routing_common::realtime_prepare::{
    PrepareError, PreparedAction, added_action, scheduled_action,
};
use catenary::routing_common::realtime_overlay::{
    RealtimeOverlay, RealtimeOverlayBuilder, RtRunInput, StaticInstance,
};
use catenary::routing_common::realtime_region::{
    ChateauRealtimeShard, RegionRealtimeSnapshot,
};
use catenary::routing_common::realtime_identity::{
    GtfsIdentityCatalog, RealtimeGtfsResolver, ResolvedInstance,
};
use catenary::routing_common::timetable::SourceIdx;
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
pub struct ResolvedTrip {
    pub region_index: u16,
    pub source: SourceIdx,
    pub update_index: u32,
    pub service_id_index: u32,
    pub original: ResolvedInstance,
}

#[derive(Debug, Default, Clone)]
pub struct ResolutionStats {
    pub matched: usize,
    pub no_match: usize,
    pub ambiguous: usize,
    pub unresolved_stops: usize,
    pub prepared_runs: usize,
    pub prepared_cancellations: usize,
    pub prepared_added: usize,
    pub invalid_updates: usize,
}

#[derive(Debug)]
pub struct SourceSnapshot {
    pub revision_ms: u64,
    /// Number of updates received, retained without the expanded RPC buffer.
    pub update_count: usize,
    pub updates: Vec<RoutingTripUpdate>,
    /// Fully validated scheduled instances, retained for a future overlay
    /// builder; no route suppression is enabled by these lookups yet.
    pub resolved: Vec<ResolvedTrip>,
    /// One immutable chateau-local region overlay per region. These are
    /// staging snapshots: RAPTOR does not consume them yet.
    pub overlays: BTreeMap<u16, Arc<RealtimeOverlay>>,
    pub stats: ResolutionStats,
}

#[derive(Default)]
pub struct RealtimeSources {
    inner: RwLock<HashMap<String, Arc<SourceSnapshot>>>,
    /// A region pin is one immutable revision vector across all loaded
    /// chateaux. Replaced after each completed Aspen synchronization sweep.
    region_snapshots: RwLock<BTreeMap<String, Arc<RegionRealtimeSnapshot>>>,
    /// The compiled Gentian source table, not geography, defines membership.
    region_chateaux: BTreeMap<String, BTreeSet<String>>,
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
        let mut region_chateaux = BTreeMap::new();
        let mut region_snapshots = BTreeMap::new();
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
            let expected: BTreeSet<String> = sources.iter()
                .map(|(_, chateau, _, _)| chateau.clone())
                .collect();
            region_chateaux.insert(region.id.clone(), expected.clone());
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
                // An empty baseline is incomplete until every chateau is
                // fetched, including those with zero realtime changes.
                let baseline = RegionRealtimeSnapshot::compose(
                    &tt, catalog.timetable_fingerprint, 0, &expected, Vec::new(),
                )?;
                region_snapshots.insert(region.id.clone(), Arc::new(baseline));
                identity_catalogs.insert(region.id.clone(), Arc::new(catalog));
            } else {
                eprintln!("Edelweiss routing source sync: region {} has no GTFS service-identity sidecar yet; compile Gentian with this PR", region.id);
            }
        }
        Ok(Self {
            inner: RwLock::new(HashMap::new()),
            region_snapshots: RwLock::new(region_snapshots),
            region_chateaux,
            region_sources,
            identity_catalogs,
        })
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
        let mut stats = ResolutionStats::default();
        for (region_index, region) in router.transit.iter().enumerate() {
            let Some(catalog) = self.identity_catalogs.get(&region.id) else { continue };
            let Some(sources) = self.region_sources.get(&region.id) else { continue };
            let Ok(tt) = region.timetable.view() else { continue };
            let Ok(region_index) = u16::try_from(region_index) else { continue };

            // A Chateau may contain several GTFS feeds. Each update must
            // match exactly one candidate source within this region; never
            // assign a same-named trip or stop to an arbitrary feed.
            let mut scheduled: Vec<Vec<(ResolvedTrip, PreparedAction)>> =
                (0..snapshot.updates.len()).map(|_| Vec::new()).collect();
            let mut added: Vec<Vec<PreparedAction>> =
                (0..snapshot.updates.len()).map(|_| Vec::new()).collect();

            for &(source, ref assigned_chateau, _, _) in sources {
                if assigned_chateau != chateau { continue; }
                let source = SourceIdx(source);
                let Ok(resolver) = RealtimeGtfsResolver::new(
                    &tt, catalog, source, &snapshot.updates,
                ) else {
                    continue;
                };
                for (update_index, update) in snapshot.updates.iter().enumerate() {
                    if matches!(
                        update.schedule_relationship,
                        Some(TripRelationship::Added | TripRelationship::Unscheduled | TripRelationship::Duplicated)
                    ) {
                        // A known public route is required when the descriptor
                        // carries route_id. Without one, stop membership alone
                        // must uniquely identify the static source.
                        if update.route_id.as_deref().is_some_and(|route_id|
                            tt.route_by_source_id(source, route_id).is_none()
                        ) {
                            continue;
                        }
                        if let Ok(action) = added_action(&resolver, source, update) {
                            added[update_index].push(action);
                        }
                        continue;
                    }

                    let Ok(original) = resolver.instance(source, update) else { continue };
                    let action = match scheduled_action(
                        &tt, &resolver, source, original, update,
                    ) {
                        Ok(action) => action,
                        Err(PrepareError::Identity(_)) => {
                            stats.unresolved_stops += 1;
                            continue;
                        }
                        Err(_) => {
                            stats.invalid_updates += 1;
                            continue;
                        }
                    };
                    let Some(row) = catalog.transports.get(original.transport.idx()) else { continue };
                    let Ok(update_index_u32) = u32::try_from(update_index) else { continue };
                    scheduled[update_index].push((
                        ResolvedTrip {
                            region_index,
                            source,
                            update_index: update_index_u32,
                            service_id_index: row.service_id,
                            original,
                        },
                        action,
                    ));
                }
            }

            let mut actions = Vec::new();
            for (index, _) in snapshot.updates.iter().enumerate() {
                if !added[index].is_empty() {
                    if added[index].len() == 1 {
                        actions.push(added[index].pop().expect("one added action"));
                    } else {
                        stats.ambiguous += 1;
                    }
                } else {
                    match scheduled[index].len() {
                        1 => {
                            let (trip, action) = scheduled[index].pop().expect("one scheduled action");
                            snapshot.resolved.push(trip);
                            stats.matched += 1;
                            actions.push(action);
                        }
                        0 => stats.no_match += 1,
                        _ => stats.ambiguous += 1,
                    }
                }
            }

            // Safeguard against multiple GTFS-RT entities targeting the same
            // scheduled instance. Neither cancellation nor replacement takes
            // effect unless exactly one complete action targets that instance.
            let mut scheduled_actions = BTreeMap::<StaticInstance, Vec<PreparedAction>>::new();
            let mut unscheduled_actions = Vec::new();
            for action in actions {
                let key = match &action {
                    PreparedAction::Cancel(transport, day) =>
                        Some(StaticInstance { transport: transport.0, day: day.0 }),
                    PreparedAction::Run(run) =>
                        run.original.map(|(transport, day)| StaticInstance {
                            transport: transport.0, day: day.0,
                        }),
                    PreparedAction::NoChange => None,
                };
                if let Some(key) = key {
                    scheduled_actions.entry(key).or_default().push(action);
                } else if !matches!(action, PreparedAction::NoChange) {
                    unscheduled_actions.push(action);
                }
            }

            // Intern only dynamic patterns for each chateau, not every route
            // in the whole region. The timetable remains mmap-backed and the
            // published overlay lives exclusively in RAM.
            let mut builder = RealtimeOverlayBuilder::new_sparse(
                &tt, catalog.timetable_fingerprint, snapshot.revision_ms,
            );
            for (_instance, mut candidates) in scheduled_actions {
                if candidates.len() != 1 {
                    stats.ambiguous += 1;
                    continue;
                }
                match candidates.pop().expect("one scheduled action") {
                    PreparedAction::Cancel(transport, day) => {
                        if builder.cancel_static(transport, day).is_ok() {
                            stats.prepared_cancellations += 1;
                        } else {
                            stats.invalid_updates += 1;
                        }
                    }
                    PreparedAction::Run(run) => {
                        if builder.add_run(run).is_ok() {
                            stats.prepared_runs += 1;
                        } else {
                            stats.invalid_updates += 1;
                        }
                    }
                    PreparedAction::NoChange => {}
                }
            }
            let mut added_by_instance = BTreeMap::<(u16, String), Vec<RtRunInput>>::new();
            for action in unscheduled_actions {
                if let PreparedAction::Run(run) = action {
                    let Some(ref id) = run.external_id else {
                        stats.invalid_updates += 1;
                        continue;
                    };
                    added_by_instance.entry((run.source.0, id.clone()))
                        .or_default().push(run);
                }
            }
            for (_instance, mut candidates) in added_by_instance {
                if candidates.len() != 1 {
                    // Two different GTFS-RT entities may report one physical
                    // added trip; never scan it twice or arbitrarily choose
                    // one contradictory event-time vector.
                    stats.ambiguous += 1;
                    continue;
                }
                let run = candidates.pop().expect("one added run");
                if builder.add_run(run).is_ok() {
                    stats.prepared_added += 1;
                    stats.prepared_runs += 1;
                } else {
                    stats.invalid_updates += 1;
                }
            }
            match builder.finish() {
                Ok(overlay) => {
                    snapshot.overlays.insert(region_index, Arc::new(overlay));
                }
                Err(_) => stats.invalid_updates += 1,
            }
        }
        snapshot.stats = stats;
    }

    /// Pin one atomic snapshot covering all available chateaux in a region.
    /// The returned Arc is stable throughout RAPTOR/PONG and reconstruction,
    /// even if Aspen publishes a newer source revision simultaneously.
    pub fn pin_region(&self, region: &str) -> Option<Arc<RegionRealtimeSnapshot>> {
        self.region_snapshots
            .read().unwrap_or_else(|e| e.into_inner())
            .get(region)
            .cloned()
    }

    /// Recompose each changed region exactly once after a complete round of
    /// parallel Aspen RPC downloads. Overlays and their time/stop arrays are
    /// kept in Arc shards; only sparse regional lookup indexes are rebuilt.
    /// This avoids O(chateaux x region) reassembly on every source download.
    pub fn publish_regions(&self, router: &Router) -> Result<usize> {
        let sources = self.inner.read().unwrap_or_else(|e| e.into_inner());
        let previous = self.region_snapshots.read()
            .unwrap_or_else(|e| e.into_inner());
        let mut pending = Vec::new();

        for (region_index, region) in router.transit.iter().enumerate() {
            let Some(catalog) = self.identity_catalogs.get(&region.id) else { continue };
            let Some(expected) = self.region_chateaux.get(&region.id) else { continue };
            let region_index = u16::try_from(region_index)
                .context("region index exceeds realtime source index width")?;
            let mut shards = Vec::new();
            for chateau in expected {
                let Some(snapshot) = sources.get(chateau) else { continue };
                let Some(overlay) = snapshot.overlays.get(&region_index) else {
                    continue;
                };
                if overlay.revision != snapshot.revision_ms {
                    // Prevent a stale generation of the chateau's own
                    // preparation from contaminating a region snapshot.
                    continue;
                }
                shards.push(ChateauRealtimeShard {
                    chateau: chateau.clone(),
                    overlay: Arc::clone(overlay),
                });
            }

            if previous.get(&region.id).is_some_and(|current| {
                current.chateaux.len() == shards.len()
                    && current.chateaux.iter().zip(&shards).all(|(a, b)|
                        a.chateau == b.chateau && Arc::ptr_eq(&a.overlay, &b.overlay)
                    )
            }) {
                continue;
            }

            let next_seq = previous.get(&region.id)
                .map_or(1, |current| current.publication_seq.saturating_add(1));
            let tt = region.timetable.view()?;
            let composed = RegionRealtimeSnapshot::compose(
                &tt, catalog.timetable_fingerprint, next_seq, expected, shards,
            ).with_context(|| format!("composing region realtime snapshot {}", region.id))?;
            pending.push((region.id.clone(), Arc::new(composed)));
        }
        drop(previous);
        drop(sources);
        let mut regions = self.region_snapshots.write()
            .unwrap_or_else(|e| e.into_inner());
        let count = pending.len();
        for (id, snapshot) in pending {
            regions.insert(id, snapshot);
        }
        Ok(count)
    }

    /// A Chateau can disappear from Aspen assignments or be invalidated by
    /// operations. Explicit withdrawal removes its RT shard on the next
    /// composition; other chateaux remain usable in the same pinned region.
    pub fn withdraw_chateau(&self, chateau: &str) -> bool {
        self.inner.write().unwrap_or_else(|e| e.into_inner())
            .remove(chateau).is_some()
    }

    pub fn region_status(&self) -> Vec<(String, u64, usize, usize, usize, usize)> {
        let snapshots = self.region_snapshots.read()
            .unwrap_or_else(|e| e.into_inner());
        snapshots.iter().map(|(region, snapshot)| (
            region.clone(),
            snapshot.publication_seq,
            snapshot.chateaux.len(),
            snapshot.expected_chateaux,
            snapshot.run_count(),
            snapshot.suppressed_count(),
        )).collect()
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
        (all.len(), all.values().map(|v| v.update_count).sum())
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
                        update_count: updates.len(),
                        updates,
                        resolved: Vec::new(),
                        overlays: BTreeMap::new(),
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
                                Ok((mut snapshot, elapsed)) => {
                                    eprintln!(
                                        "Edelweiss routing resolver {chateau}: matched={} ambiguous={} no_match={} unresolved_stops={} elapsed_ms={:.1}",
                                        snapshot.stats.matched,
                                        snapshot.stats.ambiguous,
                                        snapshot.stats.no_match,
                                        snapshot.stats.unresolved_stops,
                                        elapsed.as_secs_f64() * 1000.0,
                                    );
                                    // The full Aspen source page buffer is
                                    // needed only during preparation. Retain
                                    // compressed overlay shards and numeric
                                    // diagnostics instead of storing a second
                                    // expanded copy of every GTFS-RT entity.
                                    snapshot.updates = Vec::new();
                                    snapshot.resolved = Vec::new();
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
        match sources.publish_regions(&router) {
            Ok(changed) if changed > 0 => {
                for (region, revision, covered, expected, runs, suppressed) in sources.region_status() {
                    eprintln!(
                        "Edelweiss RT region {region}: revision={revision} sources={covered}/{expected} runs={runs} suppressed={suppressed} complete={}",
                        covered == expected,
                    );
                }
            }
            Ok(_) => {}
            Err(error) => eprintln!("Edelweiss RT region snapshot compose failed: {error:#}"),
        }
        let (active, updates) = sources.stats();
        eprintln!("Edelweiss routing source sync: {active} source snapshots; {updates} routing updates processed into RAM overlays");
        tokio::time::sleep(interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_is_monotonic_and_old_readers_remain_valid() {
        let snapshots = RealtimeSources::default();
        snapshots.publish("a".to_string(), SourceSnapshot { revision_ms: 5, update_count: 0, updates: vec![], resolved: vec![], overlays: BTreeMap::new(), stats: ResolutionStats::default() });
        let old = snapshots.get("a").unwrap();
        snapshots.publish("a".to_string(), SourceSnapshot { revision_ms: 4, update_count: 0, updates: vec![], resolved: vec![], overlays: BTreeMap::new(), stats: ResolutionStats::default() });
        assert_eq!(snapshots.get("a").unwrap().revision_ms, 5);
        snapshots.publish("a".to_string(), SourceSnapshot { revision_ms: 6, update_count: 0, updates: vec![], resolved: vec![], overlays: BTreeMap::new(), stats: ResolutionStats::default() });
        assert_eq!(snapshots.get("a").unwrap().revision_ms, 6);
        assert_eq!(old.revision_ms, 5);
    }
}
