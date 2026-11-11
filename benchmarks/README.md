# Nearby Departures SQL benchmark

This benchmark compares the **existing direction-first metadata query followed by
the itinerary-row query** to the **new selected-stop-first join**. It does not
time the whole HTTP request, the full Rust pipeline, Aspen, or the spatial
selection phase.

## Reproducible CI (synthetic Swiss-shaped dataset)

The `Nearby Departures PostgreSQL benchmark` workflow runs on pull requests
and can be run manually through **Actions → Nearby Departures PostgreSQL
benchmark → Run workflow**. It uses PostgreSQL 16 and the 16 versioned Swiss
landmark coordinates in `swiss_landmarks.json`.

The default synthetic fixture is generated deterministically: 72 stop IDs per
landmark, 24 directions per landmark, 1,200 itineraries per landmark, and 22
stop occurrences per itinerary. It is **not Swiss GTFS**. Each landmark is
benchmarked separately using the same stop and direction IDs, with two warmup
runs and seven timed runs that alternate query order. The benchmark **fails**
if the old and new result multisets differ. It writes
`nearby-benchmark.json` as a downloadable workflow artifact.

For an identical SQL comparison on a baseline commit vs PR commit, run the
workflow on each ref with the same workflow, fixture and rounds. For this PR,
the same workflow also measures the old and new SQL plans side by side on
the same database snapshot, so you need not compare different runner hosts
to establish the query-level improvement. Results from different GitHub
runners should not be treated as a controlled A/B test.

## Real imported Swiss GTFS

A **real-world** benchmark requires importing Swiss GTFS into a PostgreSQL
database with the production `gtfs` schema first, including `stops`,
`direction_pattern`, `itinerary_pattern`, and
`itinerary_pattern_meta`. The benchmark does **not** import GTFS itself.
Use the existing Maple import pipeline and validate that the feed is assigned
to château `schweiz`.

To run locally with an already populated database:

```bash
python -m pip install 'psycopg[binary]==3.2.3'
DATABASE_URL='postgresql://...' python benchmarks/nearby_postgres.py \
  --mode imported --rounds 7 --radius-m 500 \
  --output switzerland-real-gtfs.json
```

The GitHub Actions **imported** input uses the repository secret
`NEARBY_BENCH_DATABASE_URL` to connect to an **existing, benchmark-only**
PostgreSQL instance containing Swiss GTFS. It does **not** create the
database, fetch the Swiss ZIP, or run Maple. Restrict network access and use
a read-only DB account for this mode. On pull requests, the workflow defaults
to synthetic data and does not need access to this secret.

The imported-mode spatial lookup uses PostGIS geography from
`gtfs.stops.point`. Landmarks without nearby imported stops are marked
as skipped; the run fails if all landmarks are skipped. The query's
returned-record parity is checked for each included landmark.

## Comparing meaningful numbers

Look at each landmark's `legacy_median_ms`,
`stop_first_median_ms`, `ratio`, `rows`, and `parity`.
Both SQL variants are executed against the **same rows**, with query order
alternating between iterations. Timings include client-side fetching and
decoding for the requested rows.

This **does not** establish end-to-end API improvement. Test the actual
Nearby Departures HTTP response with the same coordinates, departure time,
radius and realtime settings for complete-system validation. In particular,
nearest-stop-per-direction filtering and route distance limits still run in
Rust; the SQL benchmark intentionally isolates the itinerary lookup.
