# PING bounds and PONG cursor limits

The changes follow Nigiri commit `4701529b53791f0fe9ce277ccd57077490a223fa`:

- [raptor.h](https://github.com/motis-project/nigiri/blob/4701529b53791f0fe9ce277ccd57077490a223fa/include/nigiri/routing/raptor/raptor.h): `fill_bounds` and `within_bounds`.
- [pong.cc](https://github.com/motis-project/nigiri/blob/4701529b53791f0fe9ce277ccd57077490a223fa/src/routing/raptor/pong.cc): `kMaxLookAhead`, loop guard, final result filter, and searched interval.

## Static transfer bounds

Row zero is copied from PING's round times. Higher rows fold the previous row with the current round by minimum for forward PING and maximum for backward PING. PONG indexes these rows by its remaining boarding budget, using inclusive comparisons and station-transfer compensation.

Nigiri replaces bounds with pass-all values only at realtime time-dependent footpath stops, selected by PING direction and profile (outgoing for forward PING, incoming for backward PING). Edelweiss's current Query/timetable adapter has no realtime time-dependent footpaths or vias. Its exception set is therefore empty. Finite GTFS transfer minima and prohibitions do not disable bounds. Both the production NigiriRaptor and the older ProfileSearchState point adapter follow this rule.

Gentian stores reflexive GTFS minima separately from location transfer seconds. Bound compensation uses the maximum of those two finite durations, matching the self-transfer relaxation. This is a format adaptation: upstream already stores its station-change duration on the location. Non-reflexive pair minima and prohibitions remain enforced during relaxation. The transfer-sensitive mask remains an optimization for rule lookup, not an exemption from pruning.

If realtime time-dependent footpaths are introduced, their direction/profile-specific stop sets must be passed to bounds construction and applied *after* row folding, as in Nigiri. Static GTFS transfer-rule membership must not serve as that set.

## Six-day PONG cursor cap

In the adapter's seconds and half-open query intervals:

| Operation | Forward | Arrive-by |
| --- | --- | --- |
| Cap | interval.from + 6 days | interval.to - 6 days |
| Continue sweeping | cursor < cap | cursor > cap |
| Retain reconstructed result | departure < cap | arrival > cap |
| Searched endpoint | min(cursor, cap) | max(cursor, cap) + 1 second |

The final result predicate uses the oriented PONG destination key (physical departure for forward queries and physical arrival for arrive-by). A sweep can jump past the cap, so stopping the loop alone is insufficient: results must also be filtered and interval statistics clamped. The one-second step replaces Nigiri's one-minute step because the adapter preserves seconds. The existing per-journey travel-time limit and PING's extra day of waiting remain separate limits.

## Validation

Run `cargo test --manifest-path tools/raptor-check/Cargo.toml --locked --lib`.

Regression coverage checks finite folded bounds at permitted/prohibited transfer-rule stops in both directions, inclusive compensation for a reflexive minimum, and full daily-service PONG searches which cannot satisfy the requested connection count before the six-day cap. Those searches verify the strict cutoff, valid reconstructed journeys, and clamped interval statistics in both directions. Existing pair-minimum, prohibition, serial/parallel, and footpath tests remain enabled.

This validates the static adapter; it does not claim realtime, via, or full-system C++/Rust routing equivalence. Nigiri's MIT notice is retained in `third_party/nigiri-LICENSE`.
