# Edelweiss realtime rebuild and reclamation

Edelweiss follows MOTIS's clean realtime rebuild model, scoped to individual Aspen chateaux. The Gentian timetable stays mmap-backed and immutable.

## Publication

- Changed Aspen revisions are fetched as complete, revision-checked paged transactions.
- Unchanged revisions are fetched in full after ten minutes, instead of retaining old realtime routes.
- Every refresh uses a fresh sparse overlay builder. No previous dynamic patterns, event arrays, or suppression indexes are carried over.
- Replacements and added runs older than the 72-hour historical cutoff are pruned using their final scheduled and realtime event timestamps. Cancellations use the static final event time. Unresolved static bounds are kept.
- Regional publication shares unchanged chateau shards by Arc and rebuilds only regional sparse indexes when the underlying shards change.
- Searches pin the previous Arc through RAPTOR/PONG and reconstruction. An old overlay is reclaimed after its final reader exits.
- A full rebuild at the same Aspen source revision is accepted. Older source revisions remain rejected.

## Failure and freshness

A failed paging transaction or failed build is never published. Transient outages retain the previous complete source revision. A continuously failing source is withdrawn after ten minutes to avoid lingering suppression. Five missing etcd assignment sweeps also withdraw the chateau.

A successful NotModified response confirms connection health, but does not reset the force-refresh timer.

## Operations and verification

A full rebuild temporarily holds both old and new chateau overlays. Freed Rust allocations might remain in jemalloc RSS for reuse. Measure live allocation and per-chateau counts alongside RSS.

The sync resolver logs expired_runs and expired_cancellations; the regional publisher logs runs and suppressed counts.

Test the following before production rollout:

1. A removed feed entity disappears after a complete rebuild.
2. An unchanged Aspen revision can publish a new Arc and remove expired events.
3. An in-flight query retains its previously pinned Arc until completion.
4. An expired cancellation no longer suppresses the static trip.
5. Delayed and overnight journeys remain valid until their last event is outside the retention horizon.
6. Partial paging and preparation failures do not replace the published state.
7. Persistent unreachable sources are withdrawn, while successful NotModified polls remain healthy.

The 72-hour past retention, ten-minute refresh, and ten-minute stale-failure timeout are initial defaults, not independently verified production values. Match historical query and saved-trip requirements before deployment.
