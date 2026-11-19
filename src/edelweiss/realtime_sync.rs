//! Optional, RAM-only Aspen routing feed synchronization for Edelweiss.
//!
//! Opt-in staging integration: receives routing-only source pages, validates an
//! uninterrupted revision, and atomically replaces each chateau snapshot.
//! This intentionally does NOT change RAPTOR results until static trip/service
//! identity resolution and safe realtime reconstruction are implemented.

use anyhow::{Context, Result, bail, ensure};
use catenary::EtcdConnectionIps;
use catenary::aspen::lib::{ChateauMetadataEtcd, spawn_aspen_client_from_ip};
use catenary::aspen_dataset::AspenisedTripScheduleRelationship as TripRelationship;
use catenary::etcd_cache::EtcdCache;
use catenary::routing_common::realtime_identity::{
    GtfsIdentityCatalog, RealtimeGtfsResolver, ResolveIssue, ResolvedInstance,
};
use catenary::routing_common::realtime_overlay::{
    RealtimeOverlay, RealtimeOverlayBuilder, RtRunInput, StaticInstance,
};
use catenary::routing_common::realtime_prepare::{
    PrepareError, PreparedAction, added_action, scheduled_action,
};
use catenary::routing_common::realtime_region::{ChateauRealtimeShard, RegionRealtimeSnapshot};
use catenary::routing_common::realtime_source::{
    MAX_ROUTING_PAGE_SIZE, ROUTING_SOURCE_PROTOCOL, RoutingPageState, RoutingSourcePageRequest,
    RoutingTripUpdate,
};
use catenary::routing_common::route_manager::Router;
use catenary::routing_common::routing_regions::RoutingRegionsConfig;
use catenary::routing_common::timetable::SourceIdx;
use futures::StreamExt;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tarpc::context;

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
    /// Count the highest-progress resolution failure for each unmatched update.
    pub no_match_reasons: BTreeMap<&'static str, usize>,
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
    /// Only a subset of compiled static chateaux participate in Aspen RT.
    /// Once an assignment is observed, retain it in the expected set even if
    /// the worker disappears; coverage must then explicitly show it missing.
    known_rt_chateaux: RwLock<BTreeSet<String>>,
    /// Regional source identity, including the source's attempt ID.
    /// GTFS stop IDs must never be joined across these identities.
    pub region_sources: BTreeMap<String, Vec<(u16, String, String, String)>>,
    /// Built once by Gentian; only loaded when RAM-only sync is enabled.
    /// These are static metadata and never rewritten during realtime updates.
    pub identity_catalogs: BTreeMap<String, Arc<GtfsIdentityCatalog>>,
}

fn region_contains_chateau(
    region_chateaux: &BTreeMap<String, BTreeSet<String>>,
    region: &str,
    chateau: &str,
) -> bool {
    region_chateaux
        .get(region)
        .is_some_and(|names| names.contains(chateau))
}

fn record_rejection(slot: &mut Option<ResolveIssue>, issue: ResolveIssue) {
    if slot
        .as_ref()
        .is_none_or(|previous| issue.diagnostic_depth() > previous.diagnostic_depth())
    {
        *slot = Some(issue);
    }
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
                let (chateau, feed, attempt) = tt
                    .source(catenary::routing_common::timetable::SourceIdx(idx))
                    .context("unreadable timetable source")?;
                sources.push((idx, chateau.to_owned(), feed.to_owned(), attempt.to_owned()));
            }
            let expected: BTreeSet<String> = sources
                .iter()
                .map(|(_, chateau, _, _)| chateau.clone())
                .collect();
            region_chateaux.insert(region.id.clone(), expected.clone());
            region_sources.insert(region.id.clone(), sources);
            let timetable_path = config.timetable_path(routing_config, &region.id, None)?;
            let sidecar_path = timetable_path.with_extension("rt-identities.bin");
            if sidecar_path.is_file() {
                let data = std::fs::read(&sidecar_path)
                    .with_context(|| format!("reading {}", sidecar_path.display()))?;
                let catalog: GtfsIdentityCatalog = catenary::bincode_deserialize(&data)
                    .with_context(|| format!("decoding {}", sidecar_path.display()))?;
                catalog
                    .validate(&tt)
                    .with_context(|| format!("validating {}", sidecar_path.display()))?;
                // An empty baseline is incomplete until every chateau is
                // fetched, including those with zero realtime changes.
                let baseline = Arc::new(RegionRealtimeSnapshot::compose(
                    &tt,
                    catalog.timetable_fingerprint,
                    0,
                    &BTreeSet::new(),
                    Vec::new(),
                )?);
                router.publish_realtime(&region.id, Arc::clone(&baseline))?;
                region_snapshots.insert(region.id.clone(), baseline);
                identity_catalogs.insert(region.id.clone(), Arc::new(catalog));
            } else {
                eprintln!(
                    "Edelweiss routing source sync: region {} has no GTFS service-identity sidecar yet; compile Gentian with this PR",
                    region.id
                );
            }
        }
        Ok(Self {
            inner: RwLock::new(HashMap::new()),
            region_snapshots: RwLock::new(region_snapshots),
            region_chateaux,
            known_rt_chateaux: RwLock::new(BTreeSet::new()),
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
            // A Chateau normally has compiled GTFS data in only one region.
            // Without this guard, every update was counted as no_match in
            // every unrelated region (typically four false no_candidate_source
            // failures per update). The identity sidecar is scoped to its
            // corresponding timetable, not a global catalog.
            if !region_contains_chateau(&self.region_chateaux, &region.id, chateau) {
                continue;
            }
            let Some(catalog) = self.identity_catalogs.get(&region.id) else {
                continue;
            };
            let Some(sources) = self.region_sources.get(&region.id) else {
                continue;
            };
            let Ok(tt) = region.timetable.view() else {
                continue;
            };
            let Ok(region_index) = u16::try_from(region_index) else {
                continue;
            };

            // A Chateau may contain several GTFS feeds. Each update must
            // match exactly one candidate source within this region; never
            // assign a same-named trip or stop to an arbitrary feed.
            let mut scheduled: Vec<Vec<(ResolvedTrip, PreparedAction)>> =
                (0..snapshot.updates.len()).map(|_| Vec::new()).collect();
            let mut added: Vec<Vec<PreparedAction>> =
                (0..snapshot.updates.len()).map(|_| Vec::new()).collect();
            let mut rejections: Vec<Option<ResolveIssue>> =
                (0..snapshot.updates.len()).map(|_| None).collect();
            let mut preparation_rejections: Vec<Option<&'static str>> =
                (0..snapshot.updates.len()).map(|_| None).collect();

            for &(source, ref assigned_chateau, _, _) in sources {
                if assigned_chateau != chateau {
                    continue;
                }
                let source = SourceIdx(source);
                let resolver =
                    match RealtimeGtfsResolver::new(&tt, catalog, source, &snapshot.updates) {
                        Ok(resolver) => resolver,
                        Err(issue) => {
                            for rejection in &mut rejections {
                                record_rejection(rejection, issue.clone());
                            }
                            continue;
                        }
                    };
                for (update_index, update) in snapshot.updates.iter().enumerate() {
                    if matches!(
                        update.schedule_relationship,
                        Some(
                            TripRelationship::Added
                                | TripRelationship::Unscheduled
                                | TripRelationship::Duplicated
                        )
                    ) {
                        // A known public route is required when the descriptor
                        // carries route_id. Without one, stop membership alone
                        // must uniquely identify the static source.
                        if update.route_id.as_deref().is_some_and(|route_id| {
                            tt.route_by_source_id(source, route_id).is_none()
                        }) {
                            preparation_rejections[update_index]
                                .get_or_insert("added_route_not_in_source");
                            continue;
                        }
                        match added_action(&resolver, source, update) {
                            Ok(action) => added[update_index].push(action),
                            Err(error) => {
                                preparation_rejections[update_index]
                                    .get_or_insert(error.diagnostic_code());
                            }
                        }
                        continue;
                    }

                    let original = match resolver.instance(source, update) {
                        Ok(original) => original,
                        Err(issue) => {
                            record_rejection(&mut rejections[update_index], issue);
                            continue;
                        }
                    };
                    let action = match scheduled_action(&tt, &resolver, source, original, update) {
                        Ok(action) => action,
                        Err(PrepareError::Identity(issue)) => {
                            stats.unresolved_stops += 1;
                            preparation_rejections[update_index]
                                .get_or_insert(issue.diagnostic_code());
                            continue;
                        }
                        Err(error) => {
                            stats.invalid_updates += 1;
                            preparation_rejections[update_index]
                                .get_or_insert(error.diagnostic_code());
                            continue;
                        }
                    };
                    let Some(row) = catalog.transports.get(original.transport.idx()) else {
                        continue;
                    };
                    let Ok(update_index_u32) = u32::try_from(update_index) else {
                        continue;
                    };
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
                            let (trip, action) =
                                scheduled[index].pop().expect("one scheduled action");
                            snapshot.resolved.push(trip);
                            stats.matched += 1;
                            actions.push(action);
                        }
                        0 => {
                            stats.no_match += 1;
                            let reason = preparation_rejections[index]
                                .or_else(|| {
                                    rejections[index]
                                        .as_ref()
                                        .map(ResolveIssue::diagnostic_code)
                                })
                                .unwrap_or("no_resolved_action");
                            *stats.no_match_reasons.entry(reason).or_default() += 1;
                        }
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
                    PreparedAction::Cancel(transport, day) => Some(StaticInstance {
                        transport: transport.0,
                        day: day.0,
                    }),
                    PreparedAction::Run(run) => {
                        run.original.map(|(transport, day)| StaticInstance {
                            transport: transport.0,
                            day: day.0,
                        })
                    }
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
                &tt,
                catalog.timetable_fingerprint,
                snapshot.revision_ms,
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
                    added_by_instance
                        .entry((run.source.0, id.clone()))
                        .or_default()
                        .push(run);
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
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(region)
            .cloned()
    }

    /// Recompose each changed region exactly once after a complete round of
    /// parallel Aspen RPC downloads. Overlays and their time/stop arrays are
    /// kept in Arc shards; only sparse regional lookup indexes are rebuilt.
    /// This avoids O(chateaux x region) reassembly on every source download.
    pub fn publish_regions(&self, router: &Router) -> Result<usize> {
        let sources = self.inner.read().unwrap_or_else(|e| e.into_inner());
        let known_rt = self
            .known_rt_chateaux
            .read()
            .unwrap_or_else(|e| e.into_inner());
        let previous = self
            .region_snapshots
            .read()
            .unwrap_or_else(|e| e.into_inner());
        let mut pending = Vec::new();

        for (region_index, region) in router.transit.iter().enumerate() {
            let Some(catalog) = self.identity_catalogs.get(&region.id) else {
                continue;
            };
            let Some(compiled) = self.region_chateaux.get(&region.id) else {
                continue;
            };
            let expected: BTreeSet<String> = compiled.intersection(&known_rt).cloned().collect();
            let region_index = u16::try_from(region_index)
                .context("region index exceeds realtime source index width")?;
            let tt = region.timetable.view()?;
            let next = compose_region(
                &tt,
                catalog.timetable_fingerprint,
                region_index,
                &expected,
                &sources,
                previous.get(&region.id),
            )
            .with_context(|| format!("composing region realtime snapshot {}", region.id))?;
            let Some(snapshot) = next else { continue };
            pending.push((region.id.clone(), snapshot));
        }
        drop(previous);
        drop(known_rt);
        drop(sources);
        let mut regions = self
            .region_snapshots
            .write()
            .unwrap_or_else(|e| e.into_inner());
        let count = pending.len();
        for (id, snapshot) in pending {
            // Publish the exact same Arc into the owning TransitRegion.
            // Queries can pin it directly through Router::pin_realtime.
            router.publish_realtime(&id, Arc::clone(&snapshot))?;
            regions.insert(id, snapshot);
        }
        Ok(count)
    }

    /// A Chateau can disappear from Aspen assignments or be invalidated by
    /// operations. Explicit withdrawal removes its RT shard on the next
    /// composition; other chateaux remain usable in the same pinned region.
    pub fn withdraw_chateau(&self, chateau: &str) -> bool {
        self.inner
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(chateau)
            .is_some()
    }

    /// Mark Aspen chateaux seen in at least one valid etcd assignment sweep.
    /// Never add a chateau solely because it is present in static Gentian.
    pub fn note_realtime_assignments(&self, assigned: impl IntoIterator<Item = String>) {
        let compiled = self.chateaux();
        let mut known = self
            .known_rt_chateaux
            .write()
            .unwrap_or_else(|e| e.into_inner());
        known.extend(
            assigned
                .into_iter()
                .filter(|chateau| compiled.contains(chateau)),
        );
    }

    pub fn region_status(&self) -> Vec<(String, u64, usize, usize, usize, usize, bool)> {
        let snapshots = self
            .region_snapshots
            .read()
            .unwrap_or_else(|e| e.into_inner());
        snapshots
            .iter()
            .map(|(region, snapshot)| {
                (
                    region.clone(),
                    snapshot.publication_seq,
                    snapshot.chateaux.len(),
                    snapshot.expected_chateaux,
                    snapshot.run_count(),
                    snapshot.suppressed_count(),
                    snapshot.is_complete(),
                )
            })
            .collect()
    }

    pub fn chateaux(&self) -> BTreeSet<String> {
        self.region_sources
            .values()
            .flat_map(|sources| sources.iter().map(|(_, c, _, _)| c.clone()))
            .collect()
    }

    pub fn get(&self, chateau: &str) -> Option<Arc<SourceSnapshot>> {
        self.inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(chateau)
            .cloned()
    }

    fn publish(&self, chateau: String, snapshot: SourceSnapshot) {
        let mut all = self.inner.write().unwrap_or_else(|e| e.into_inner());
        if all
            .get(&chateau)
            .is_some_and(|current| current.revision_ms >= snapshot.revision_ms)
        {
            return;
        }
        all.insert(chateau, Arc::new(snapshot));
    }

    pub fn stats(&self) -> (usize, usize) {
        let all = self.inner.read().unwrap_or_else(|e| e.into_inner());
        (all.len(), all.values().map(|v| v.update_count).sum())
    }
}

/// Recompose one region from chateau-local immutable shards. This function
/// also serves as a deterministic test seam for coverage/revisions and avoids
/// duplicating entire stop/time arrays whenever a single source changes.
fn compose_region(
    tt: &catenary::routing_common::timetable::ZeroCopyTimetable<'_>,
    generation: [u8; 32],
    region_index: u16,
    expected: &BTreeSet<String>,
    source_snapshots: &HashMap<String, Arc<SourceSnapshot>>,
    previous: Option<&Arc<RegionRealtimeSnapshot>>,
) -> Result<Option<Arc<RegionRealtimeSnapshot>>> {
    let mut shards = Vec::new();
    for chateau in expected {
        let Some(source) = source_snapshots.get(chateau) else {
            continue;
        };
        let Some(overlay) = source.overlays.get(&region_index) else {
            continue;
        };
        if overlay.revision != source.revision_ms {
            continue;
        }
        shards.push(ChateauRealtimeShard {
            chateau: chateau.clone(),
            overlay: Arc::clone(overlay),
        });
    }
    if previous.is_some_and(|current| {
        current.publication_seq > 0
            && current.chateaux.len() + current.missing_chateaux.len() == expected.len()
            && current
                .chateaux
                .iter()
                .all(|shard| expected.contains(&shard.chateau))
            && current
                .missing_chateaux
                .iter()
                .all(|id| expected.contains(id))
            && current.chateaux.len() == shards.len()
            && current
                .chateaux
                .iter()
                .zip(&shards)
                .all(|(a, b)| a.chateau == b.chateau && Arc::ptr_eq(&a.overlay, &b.overlay))
    }) {
        return Ok(None);
    }

    let next_seq = previous
        .map_or(Some(1), |current| current.publication_seq.checked_add(1))
        .context("realtime region publication sequence overflow")?;
    Ok(Some(Arc::new(RegionRealtimeSnapshot::compose(
        tt, generation, next_seq, expected, shards,
    )?)))
}

/// Fetch with an explicit page transaction. A single changed generation,
/// missing page, oversized feed or failed RPC aborts without changing the
/// previously published source snapshot. All intermediate pages stay in RAM.
async fn fetch_source(
    socket: std::net::SocketAddr,
    chateau: &str,
    revision: Option<u64>,
) -> Result<Option<SourceSnapshot>> {
    let client = spawn_aspen_client_from_ip(&socket)
        .await
        .map_err(|error| anyhow::anyhow!("connecting to Aspen {socket}: {error}"))?;
    let mut offset = 0u32;
    let mut expected = None;
    let mut updates = Vec::new();
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
        )
        .await
        .context("Aspen RPC deadline exceeded")??
        .context("Aspen has no authoritative chateau snapshot")?;
        ensure!(
            page.protocol == ROUTING_SOURCE_PROTOCOL,
            "incompatible Aspen routing RPC protocol"
        );
        match page.state {
            RoutingPageState::NotModified if offset == 0 => {
                return Ok(None);
            }
            RoutingPageState::RestartRequired => {
                bail!("source revision changed during snapshot paging")
            }
            RoutingPageState::Data {
                updates: page_updates,
                next_offset,
            } => {
                if offset == 0 {
                    expected = Some(page.revision_ms);
                }
                ensure!(
                    expected == Some(page.revision_ms),
                    "Aspen revision changed during snapshot paging"
                );
                updates.extend(page_updates);
                if let Some(next) = next_offset {
                    ensure!(
                        next > offset && next as usize == updates.len(),
                        "invalid Aspen page cursor"
                    );
                    offset = next;
                } else {
                    ensure!(
                        updates.len() == page.total_updates as usize,
                        "Aspen snapshot has missing updates"
                    );
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
            RoutingPageState::NotModified => {
                bail!("unexpected conditional response mid-pagination")
            }
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
        Arc::new(EtcdConnectionIps {
            ip_addresses: etcd_urls,
        }),
        Arc::new(options),
        "/aspen_assigned_chateaux/",
    )
    .await
    .map_err(anyhow::Error::msg)?;

    let chateaux: Vec<String> = sources.chateaux().into_iter().collect();
    eprintln!(
        "Edelweiss routing source sync: {} chateaux; RAM-only, observe-only mode",
        chateaux.len()
    );
    let mut unassigned_passes = HashMap::<String, u8>::new();
    loop {
        let mut observed_assignments = Vec::new();
        // Protect against indefinite retention of a cancelled or delayed run
        // when its Aspen assignment disappears. Transient etcd gaps are
        // tolerated for five passes; then this chateau becomes explicitly
        // missing from the next region snapshot (static service is untouched).
        for chateau in &chateaux {
            if assignments.get(chateau).is_none() {
                let missed = unassigned_passes.entry(chateau.clone()).or_default();
                *missed = missed.saturating_add(1);
                if *missed == 5 && sources.withdraw_chateau(chateau) {
                    eprintln!(
                        "Edelweiss RT source {chateau}: withdrew stale RAM overlay after 5 missing assignment sweeps"
                    );
                }
            } else {
                unassigned_passes.remove(chateau);
                observed_assignments.push(chateau.clone());
            }
        }
        sources.note_realtime_assignments(observed_assignments);
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
                                        "Edelweiss routing resolver {chateau}: matched={} ambiguous={} no_match={} no_match_reasons={:?} unresolved_stops={} elapsed_ms={:.1}",
                                        snapshot.stats.matched,
                                        snapshot.stats.ambiguous,
                                        snapshot.stats.no_match,
                                        snapshot.stats.no_match_reasons,
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
                for (region, revision, covered, expected, runs, suppressed, complete) in
                    sources.region_status()
                {
                    eprintln!(
                        "Edelweiss RT region {region}: revision={revision} sources={covered}/{expected} runs={runs} suppressed={suppressed} complete={}",
                        complete,
                    );
                }
            }
            Ok(_) => {}
            Err(error) => eprintln!("Edelweiss RT region snapshot compose failed: {error:#}"),
        }
        let (active, updates) = sources.stats();
        eprintln!(
            "Edelweiss routing source sync: {active} source snapshots; {updates} routing updates processed into RAM overlays"
        );
        tokio::time::sleep(interval).await;
    }
}

/// Isolated SoCal Actions fixture-import mode. The same decoder/projection
/// contracts are used as Aspen's routing-only RPC, but the test does not need
/// etcd, a running Aspen instance, or write access to production state.
///
/// The input is a saved protobuf from Birch's /gtfs_rt?feed_type=trip endpoint;
/// the static feed selects an unambiguous source/chateau in the compiled
/// Gentian timetable. This checks the complete source -> prepared overlay ->
/// regional publication path, including the Router's pinned Arc snapshot.
pub fn probe_raw_gtfs_rt(
    router: &Router,
    routing_config: &Path,
    region_id: &str,
    static_feed_id: &str,
    protobuf: &[u8],
    minimum_runs: usize,
) -> Result<serde_json::Value> {
    let index = router
        .transit
        .iter()
        .position(|r| r.id == region_id)
        .with_context(|| format!("unknown probe region {region_id}"))?;
    let region = &router.transit[index];
    let tt = region.timetable.view()?;
    let source_candidates = (0..tt.sources().len())
        .filter_map(|i| {
            let idx = u16::try_from(i).ok()?;
            let (chateau, feed, _) = tt.source(SourceIdx(idx))?;
            (feed == static_feed_id).then_some((chateau.to_owned(), idx))
        })
        .collect::<Vec<_>>();
    ensure!(
        source_candidates.len() == 1,
        "static feed {} matched {} Gentian sources (expected exactly one)",
        static_feed_id,
        source_candidates.len(),
    );
    let chateau = source_candidates[0].0.clone();
    let source_idx = source_candidates[0].1;

    let updates =
        catenary::routing_common::realtime_source::project_raw_gtfs_rt_trip_feed(protobuf)?;
    ensure!(
        !updates.is_empty(),
        "Birch trip feed has no decodable TripUpdates"
    );
    let received_count = updates.len();
    let sources = RealtimeSources::from_router(router, routing_config)?;
    let mut source_snapshot = SourceSnapshot {
        revision_ms: 1,
        update_count: received_count,
        updates,
        resolved: Vec::new(),
        overlays: BTreeMap::new(),
        stats: ResolutionStats::default(),
    };
    sources.resolve_for_chateau(router, &chateau, &mut source_snapshot);
    let rt = source_snapshot
        .overlays
        .get(&(index as u16))
        .context("GTFS-RT fixture failed to prepare the selected region")?;
    let catalog = sources
        .identity_catalogs
        .get(region_id)
        .context("Gentian identity sidecar is unavailable for the probe region")?;
    ensure!(
        rt.timetable_generation == catalog.timetable_fingerprint,
        "probe's prepared overlay has a stale timetable generation"
    );
    let expected = BTreeSet::from([chateau.clone()]);
    let pinned = Arc::new(RegionRealtimeSnapshot::compose(
        &tt,
        catalog.timetable_fingerprint,
        1,
        &expected,
        vec![ChateauRealtimeShard {
            chateau: chateau.clone(),
            overlay: Arc::clone(rt),
        }],
    )?);
    router.publish_realtime(region_id, Arc::clone(&pinned))?;
    let published = router
        .pin_realtime(region_id)
        .context("Router failed to publish a pinned regional snapshot")?;
    ensure!(
        Arc::ptr_eq(&pinned, &published),
        "Router published a different snapshot"
    );
    ensure!(
        published.is_complete(),
        "single-source realtime fixture is incomplete"
    );
    ensure!(
        published.run_count() >= minimum_runs,
        "only {} realtime runs were prepared (required {})",
        published.run_count(),
        minimum_runs
    );

    let mut validated_events = 0usize;
    for shard in &published.chateaux {
        for run in &shard.overlay.runs {
            let start = run.event_from as usize;
            let end = run.event_to as usize;
            ensure!(end > start, "realtime run has no valid event interval");
            let slots = (start..end)
                .map(|i| {
                    let offset = shard.overlay.event_offsets_seconds[i];
                    shard.overlay.event_epoch + i64::from(offset)
                })
                .collect::<Vec<_>>();
            ensure!(
                slots.windows(2).all(|x| x[0] <= x[1]),
                "realtime run has non-monotonic Unix event times"
            );
            validated_events += slots.len();
        }
    }

    Ok(serde_json::json!({
        "region": region_id,
        "chateau": chateau,
        "static_feed": static_feed_id,
        "source_index": source_idx,
        "decoded_trip_updates": received_count,
        "matched_scheduled_instances": source_snapshot.stats.matched,
        "ambiguous_instances": source_snapshot.stats.ambiguous,
        "unmatched_instances": source_snapshot.stats.no_match,
        "unmatched_reasons": source_snapshot.stats.no_match_reasons,
        "rejected_updates": source_snapshot.stats.invalid_updates,
        "unresolved_stops": source_snapshot.stats.unresolved_stops,
        "prepared_runs": published.run_count(),
        "prepared_added": source_snapshot.stats.prepared_added,
        "prepared_cancellations": source_snapshot.stats.prepared_cancellations,
        "suppressed_static_instances": published.suppressed_count(),
        "indexed_stops": published.indexed_stop_count(),
        "validated_exact_second_events": validated_events,
        "snapshot_publication_seq": published.publication_seq,
        "snapshot_complete": published.is_complete(),
        "timing_epoch": published.event_epoch,
        "snapshot_shards": published.chateaux.len(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> Vec<u8> {
        use catenary::routing_common::timetable::{
            Coordinate, LocationIdx, StringRef, TimetableOwned,
        };
        let mut tt = TimetableOwned::empty(20_000, 4);
        let a = tt.add_source("chateau-a", "feed-a", "attempt-a").unwrap();
        let b = tt.add_source("chateau-b", "feed-b", "attempt-b").unwrap();
        for source in [a, a, b, b] {
            tt.coordinates.push(Coordinate::default());
            tt.location_id.push(StringRef::default());
            tt.location_name.push(StringRef::default());
            tt.location_source.push(source);
            tt.location_parent.push(LocationIdx::INVALID);
            tt.location_type.push(0);
            tt.location_transfer_seconds.push(120);
            tt.location_routes_offsets.push(0);
            tt.footpaths_out_offsets.push(0);
            tt.footpaths_in_offsets.push(0);
        }
        tt.encode().unwrap()
    }

    fn source_snapshot(
        tt: &catenary::routing_common::timetable::ZeroCopyTimetable<'_>,
        name: &str,
        source: u16,
        revision: u64,
    ) -> SourceSnapshot {
        use catenary::routing_common::realtime_overlay::{RealtimeOverlayBuilder, RtPatternInput};
        use catenary::routing_common::timetable::{LocationIdx, PackedStop};
        let mut builder = RealtimeOverlayBuilder::new_sparse(tt, [9; 32], revision);
        let stop = u32::from(source) * 2;
        let epoch = i64::from(tt.header().first_utc_day) * 86_400;
        builder
            .add_run(RtRunInput {
                source: SourceIdx(source),
                original: None,
                service_date: 20_000,
                external_id: Some(format!("{name}-{revision}")),
                pattern: RtPatternInput::Stops(vec![
                    PackedStop::new(LocationIdx(stop), true, true, true, true).unwrap(),
                    PackedStop::new(LocationIdx(stop + 1), true, true, true, true).unwrap(),
                ]),
                metadata: None,
                event_times: vec![epoch + revision as i64, epoch + revision as i64 + 100],
            })
            .unwrap();
        SourceSnapshot {
            revision_ms: revision,
            update_count: 1,
            updates: Vec::new(),
            resolved: Vec::new(),
            overlays: BTreeMap::from([(0, Arc::new(builder.finish().unwrap()))]),
            stats: ResolutionStats::default(),
        }
    }

    #[test]
    fn preparation_errors_have_specific_stable_log_categories() {
        assert_eq!(
            PrepareError::Identity(ResolveIssue::UnknownStopId("stop".into())).diagnostic_code(),
            "unknown_stop_id",
        );
        assert_eq!(
            PrepareError::MissingEventTime.diagnostic_code(),
            "prepare_missing_event_time",
        );
        assert_eq!(
            PrepareError::InvalidEventTimes.diagnostic_code(),
            "prepare_invalid_event_times",
        );
        assert_eq!(
            PrepareError::InvalidStopOrder.diagnostic_code(),
            "prepare_invalid_stop_order",
        );
    }

    #[test]
    fn realtime_resolution_scopes_chateau_to_compiled_regions() {
        let region_chateaux = BTreeMap::from([
            ("europe".to_owned(), BTreeSet::from(["schweiz".to_owned()])),
            (
                "us".to_owned(),
                BTreeSet::from(["metro~losangeles".to_owned()]),
            ),
            ("canada".to_owned(), BTreeSet::from(["ttc".to_owned()])),
        ]);
        assert!(region_contains_chateau(
            &region_chateaux,
            "europe",
            "schweiz"
        ));
        assert!(!region_contains_chateau(&region_chateaux, "us", "schweiz"));
        assert!(!region_contains_chateau(
            &region_chateaux,
            "canada",
            "schweiz"
        ));
        assert!(!region_contains_chateau(
            &region_chateaux,
            "australia",
            "schweiz"
        ));
        assert!(region_contains_chateau(
            &region_chateaux,
            "us",
            "metro~losangeles"
        ));
    }

    #[test]
    fn diagnostic_rejection_prefers_later_matching_stage() {
        let mut rejection = None;
        record_rejection(&mut rejection, ResolveIssue::TripIdNotFound);
        record_rejection(&mut rejection, ResolveIssue::RouteIdMismatch);
        record_rejection(&mut rejection, ResolveIssue::TripIdNotFound);
        assert_eq!(rejection, Some(ResolveIssue::RouteIdMismatch));
        record_rejection(&mut rejection, ResolveIssue::InactiveTrafficDay);
        assert_eq!(rejection.unwrap().diagnostic_code(), "inactive_traffic_day");
    }

    #[test]
    fn static_chateaux_without_aspen_assignments_do_not_block_rt_coverage() {
        use catenary::routing_common::timetable::ZeroCopyTimetable;
        let bytes = fixture();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let empty_expected = BTreeSet::new();
        let no_snapshots = HashMap::new();
        let baseline = Arc::new(
            RegionRealtimeSnapshot::compose(&tt, [9; 32], 0, &empty_expected, Vec::new()).unwrap(),
        );
        assert!(!baseline.is_complete()); // no first Aspen sweep yet

        let ready = compose_region(
            &tt,
            [9; 32],
            0,
            &empty_expected,
            &no_snapshots,
            Some(&baseline),
        )
        .unwrap()
        .unwrap();
        assert_eq!(ready.publication_seq, 1);
        assert!(ready.is_complete()); // no live RT assignments, static remains valid
        assert!(
            compose_region(
                &tt,
                [9; 32],
                0,
                &empty_expected,
                &no_snapshots,
                Some(&ready),
            )
            .unwrap()
            .is_none()
        );

        let newly_assigned = BTreeSet::from(["chateau-a".to_owned()]);
        let waiting = compose_region(
            &tt,
            [9; 32],
            0,
            &newly_assigned,
            &no_snapshots,
            Some(&ready),
        )
        .unwrap()
        .unwrap();
        assert!(!waiting.is_complete());
        assert_eq!(waiting.missing_chateaux, ["chateau-a"]);
    }

    #[test]
    fn region_snapshot_is_atomic_across_chateaux_and_withdrawals() {
        use catenary::routing_common::timetable::{LocationIdx, ZeroCopyTimetable};
        let bytes = fixture();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let expected = BTreeSet::from(["chateau-a".into(), "chateau-b".into()]);
        let mut sources = HashMap::new();
        sources.insert(
            "chateau-a".into(),
            Arc::new(source_snapshot(&tt, "chateau-a", 0, 1)),
        );
        let initial = compose_region(&tt, [9; 32], 0, &expected, &sources, None)
            .unwrap()
            .unwrap();
        assert_eq!(initial.publication_seq, 1);
        assert!(!initial.is_complete());
        assert_eq!(initial.missing_chateaux, ["chateau-b"]);
        assert_eq!(initial.run_count(), 1);
        let pinned = Arc::clone(&initial);

        sources.insert(
            "chateau-b".into(),
            Arc::new(source_snapshot(&tt, "chateau-b", 1, 1)),
        );
        let full = compose_region(&tt, [9; 32], 0, &expected, &sources, Some(&initial))
            .unwrap()
            .unwrap();
        assert!(full.is_complete());
        assert_eq!(full.run_count(), 2);
        assert_eq!(full.publication_seq, 2);
        assert_eq!(pinned.run_count(), 1);
        assert!(Arc::ptr_eq(
            &pinned.chateaux[0].overlay,
            &full.chateaux[0].overlay
        ));
        assert_eq!(full.patterns_at(LocationIdx(2)).len(), 1);
        assert_eq!(
            full.chateau_revisions().collect::<Vec<_>>(),
            vec![("chateau-a", 1), ("chateau-b", 1)]
        );
        assert!(
            compose_region(&tt, [9; 32], 0, &expected, &sources, Some(&full))
                .unwrap()
                .is_none()
        );

        sources.insert(
            "chateau-a".into(),
            Arc::new(source_snapshot(&tt, "chateau-a", 0, 2)),
        );
        let replacement = compose_region(&tt, [9; 32], 0, &expected, &sources, Some(&full))
            .unwrap()
            .unwrap();
        assert_eq!(replacement.publication_seq, 3);
        assert!(Arc::ptr_eq(
            &full.chateaux[1].overlay,
            &replacement.chateaux[1].overlay
        ));
        assert!(!Arc::ptr_eq(
            &full.chateaux[0].overlay,
            &replacement.chateaux[0].overlay
        ));
        assert_eq!(
            full.chateau_revisions().collect::<Vec<_>>(),
            vec![("chateau-a", 1), ("chateau-b", 1)]
        );
        assert_eq!(
            replacement.chateau_revisions().collect::<Vec<_>>(),
            vec![("chateau-a", 2), ("chateau-b", 1)]
        );

        sources.remove("chateau-b");
        let withdrawn = compose_region(&tt, [9; 32], 0, &expected, &sources, Some(&replacement))
            .unwrap()
            .unwrap();
        assert!(!withdrawn.is_complete());
        assert_eq!(withdrawn.missing_chateaux, ["chateau-b"]);
        assert!(withdrawn.patterns_at(LocationIdx(2)).is_empty());
        assert_eq!(full.run_count(), 2);
    }

    #[test]
    fn region_builder_rejects_cross_generation_stale_chateau_overlay() {
        use catenary::routing_common::timetable::ZeroCopyTimetable;
        let bytes = fixture();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let expected = BTreeSet::from(["chateau-a".to_owned()]);
        let sources = HashMap::from([(
            "chateau-a".to_owned(),
            Arc::new(source_snapshot(&tt, "chateau-a", 0, 1)),
        )]);
        assert!(compose_region(&tt, [3; 32], 0, &expected, &sources, None).is_err());
    }

    #[test]
    fn publish_is_monotonic_and_old_readers_remain_valid() {
        let snapshots = RealtimeSources::default();
        snapshots.publish(
            "a".to_string(),
            SourceSnapshot {
                revision_ms: 5,
                update_count: 0,
                updates: vec![],
                resolved: vec![],
                overlays: BTreeMap::new(),
                stats: ResolutionStats::default(),
            },
        );
        let old = snapshots.get("a").unwrap();
        snapshots.publish(
            "a".to_string(),
            SourceSnapshot {
                revision_ms: 4,
                update_count: 0,
                updates: vec![],
                resolved: vec![],
                overlays: BTreeMap::new(),
                stats: ResolutionStats::default(),
            },
        );
        assert_eq!(snapshots.get("a").unwrap().revision_ms, 5);
        snapshots.publish(
            "a".to_string(),
            SourceSnapshot {
                revision_ms: 6,
                update_count: 0,
                updates: vec![],
                resolved: vec![],
                overlays: BTreeMap::new(),
                stats: ResolutionStats::default(),
            },
        );
        assert_eq!(snapshots.get("a").unwrap().revision_ms, 6);
        assert_eq!(old.revision_ms, 5);
    }
}
