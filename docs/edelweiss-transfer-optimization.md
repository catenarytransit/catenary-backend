# Transfer optimization after Nigiri reconstruction

The live PONG path now optimizes each reconstructed ride / footpath / ride
connection in passenger travel order, for both departure and arrive-by queries.
Previously only the older reconstruction paths invoked transfer optimization.
The scheduled vehicles, service days, journey endpoints and boarding count stay
fixed. Validation runs before and after optimization.

## Upstream implementation

This ports the static, no-via transfer portion of Nigiri
[`82e55ced`](https://github.com/motis-project/nigiri/tree/82e55cedfc184ea3a33082619ba937d172e9c575):

- `src/routing/raptor/reconstruct.cc`: optimization after putting legs in
  passenger order, regardless of search direction.
- `src/routing/raptor/optimize_footpaths.cc`: `optimize_transfers` and
  `get_penalty`.
- `src/loader/init_finish.cc`: `assign_importance`.
- `include/nigiri/timetable.h`: `locations::get_root_idx`.

Candidate iteration follows C++: first-vehicle alighting positions from
`board + 1` through the end of the run; second-vehicle boarding positions from
zero up to its fixed alighting position; then outgoing directed footpaths.
The first matching footpath is considered, and only a strictly lower penalty
replaces the current choice. This includes riding past the original alighting
stop to shorten the walk while still catching the same next vehicle.

In seconds, the penalty is:

```
walking_seconds
  - 180 * station_importance / maximum_importance  [equivalent station only]
  - 0.2 * clamp(connection_buffer_seconds, 0, 900)
```

Station importance sums active service days for all transports on each
location's incident routes, weighted by the first section's service class.
Contributions are accumulated at the immediate parent station when present.
The fallback route class is used when a Gentian route has no section classes.
The literal upstream priority array is retained, including its three implicit
zero entries: the comments on class names in upstream `assign_importance`
are older than its enum. Root lookup follows upstream, including its cycle guard.
Wide integer accumulation avoids overflow, and zero maximum importance gives
no station reward rather than dividing by zero.

These derived arrays are cached in `LowerBoundTopology` when the timetable is
loaded and shared with query states by `Arc`. Existing version-5 Gentian files
remain compatible; recompiling the timetable is not required.

## Catenary adaptations

- Walking durations retain exact seconds. GTFS directed prohibitions and
  minimum connection times remain mandatory, independently of the penalty.
  Buffer reward uses departure minus arrival minus physical walk duration.
- The original candidate is scored using its **transfer** endpoints. Upstream
  passes the surrounding rides' outer endpoints for this baseline. Using the
  transfer endpoints consistently prevents station rewards from repeatedly
  selecting the unchanged transfer on subsequent optimization passes.
- This change covers internal transfers. It does not claim full parity for
  endpoint offsets, vias, time-dependent footpaths or realtime run changes;
  those require corresponding query/timetable support.

## Verification

Run `cargo test --manifest-path tools/raptor-check/Cargo.toml`.
The production PONG regression covers both query directions and checks the
524-second to 149-second later-stop shortcut, fixed vehicle identities,
unchanged journey keys, and station rewarding through the cached topology.
Removing the live reconstruction hook makes the shortcut regression fail.
Other tests cover class/calendar weighting, parent stations, empty service,
strict ties, exact connection boundaries, idempotence and GTFS constraints.
