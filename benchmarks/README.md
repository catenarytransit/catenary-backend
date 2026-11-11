# Full Swiss Nearby Departures benchmark

This workflow benchmarks the **real Birch HTTP endpoint**, using a real GTFS import, not synthetic data or an isolated SQL query. It follows the repo's Southern California integration-test pattern, but imports **only** Transitland feed `f-u0-switzerland` using the normal Maple binary.

## GitHub Actions

Run [Swiss Nearby Departures real GTFS benchmark](../.github/workflows/nearby-postgres-benchmark.yml) from the Actions tab, or let the PR trigger it. The job:

1. Starts PostGIS PostgreSQL 15, etcd, and Elasticsearch as service containers.
2. Checks out `transitland-atlas`, builds Maple/Birch, and runs the actual Diesel migrations.
3. Configures `[maple].only_feed_ids = ["f-u0-switzerland"]`, sets `ONLY_FEED_IDS` and `SKIP_PFAEDLE=1`, and imports through Maple. Both pfaedle launchers in `src/maple/gtfs_process.rs` return without executing external tools when this flag is set. Non-pfaedle processing and data import remain enabled. Outside this workflow, the flag defaults to off.
4. Asserts the required static tables are populated, the château is `schweiz`, and that `trips_compressed` contains *only* the requested source feed.
5. Builds both the baseline Birch (PR base commit) and candidate Birch (PR merge commit) on the **same runner**; both binaries read the **same imported PostgreSQL database**.
6. Selects a deterministic 09:00 Europe/Zurich GTFS service day from the **imported calendar**, and sends identical URL parameters to both implementations, including `skip_realtime=true` to isolate static-schedule correctness and query speed.
7. Makes warmup requests and then repeated HTTP requests at **16 Swiss railway landmarks** from `swiss_landmarks.json`. Each request measures client wall time plus the existing detailed server timing instrumentation.
8. Canonicalizes responses (excluding `debug` timings and ignoring irrelevant JSON array ordering), asserts baseline/candidate parity across sampled requests, reports per-location median latency, and fails on mismatches.
9. Uploads JSON benchmark results, baseline/candidate responses, Maple logs, Birch logs, exact revision SHAs, and a Markdown results table as a GitHub Actions artifact. The table is also written to the workflow job summary.

The workflow uses a direct Git checkout for the backend instead of `actions/checkout` because the repository has a `temp/pfaedle` gitlink without a valid `.gitmodules` URL. This avoids the failing submodule credential-cleanup step without installing or initializing pfaedle. The Transitland Atlas checkout still uses `actions/checkout` separately.

The GitHub workflow produces concrete timing results only **after** Maple has successfully downloaded and ingested the actual Swiss feed. The CI run may require significant build time and network download capacity.

The SQL-only test in `nearby_postgres.py` is no longer used by this workflow; the complete HTTP test is `nearby_full.py`.

## Reproduce locally

After importing `f-u0-switzerland` into a production-schema PostgreSQL database (see the workflow's `catenaryconfig.toml` and commands), with etcd and Elasticsearch available:

```bash
python3 -m pip install 'psycopg[binary]==3.2.3'
python3 benchmarks/nearby_full.py \
  --database-url postgres://postgres:postgres@localhost:5432/catenary \
  --baseline-binary benchmark-bin/baseline-birch \
  --candidate-binary benchmark-bin/candidate-birch \
  --baseline-ref BASE_COMMIT_SHA --candidate-ref PR_COMMIT_SHA \
  --rounds 3 --output-dir benchmark-results
```

The benchmark fails if the GTFS data is missing or if the versions produce different timetable responses. The benchmark intentionally **does not** compare realtime data because Aspen updates are non-deterministic. The two implementations run sequentially, so PostgreSQL warm-cache effects may favor the second binary. For robust latency conclusions, repeat with reversed ordering or in several workflow runs; compare the per-stage server timings as well as HTTP timings.

**No results are claimed before GitHub Actions has successfully completed.**
