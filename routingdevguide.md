### Install Postgres

You can do this either in docker or without docker. 

### setup postgres

Install the cargo diesel cli.

```bash
diesel database setup
diesel migration run
```

### Run the run local graph gen docker container

```bash
./run_local_graph_gen.sh
```

To run it without regenerating the OSM data

```bash
./run_local_graph_gen.sh --gentian-only
```


### Start the query server

```bash
./run_local_edelweiss.sh
```

At the same time, send it queries

```bash
./run_queries_local.sh
```

### Startup diagnostics and service endpoints

Edelweiss writes startup progress to stderr (visible in the systemd journal)
without requiring a log-level setting. Logs identify each region, timetable
path, stop/transport count, and time spent loading geometry, mapping and
validating the timetable, checking routing sources, building metadata indexes,
building lower-bound topology, and loading platform metadata. A final message
reports the total startup time and HTTP bind address.

The timetable router and embedded OSR graphs load concurrently on separate
threads. Startup logs report each loader's elapsed time and their combined wall
time. Both loaders finish before errors are propagated, precomputed OSR matches
are checked, or HTTP and CLI queries run. Individual regions within each loader
retain their existing loading order.

- `GET /` returns JSON identifying Edelweiss, its package version, and API paths.
- `GET /health` returns HTTP 200 with
  `{"service":"edelweiss","status":"ok"}` and `Cache-Control: no-store`.

HTTP starts after all configured routing regions load successfully. During
startup, the health endpoint is unavailable; allow sufficient startup-probe
time before treating the process as unhealthy. The health response confirms the
HTTP service, loaded timetable router and embedded OSR graphs, without
querying PostgreSQL. Street queries run inside Edelweiss; a separate OSR worker
is no longer required. Gentian timetables must be rebuilt for V6. See
[OSR match storage and migration](docs/osr-matched-routing-migration.md).

With the default listen address:

```bash
curl http://127.0.0.1:40428/
curl http://127.0.0.1:40428/health
```
# Startup memory residency

Edelweiss can advise the kernel to reclaim file pages touched by validation,
index construction, and native OSR loading. This is opt-in in the selected
`--catenary-config` file:

```toml
[edelweiss]
startup_mmap_advice = "pageout"
```

Supported modes are `none` (default, unchanged behavior), `cold` (prefer these
pages for reclamation), and `pageout` (request reclamation now). Linux 5.4+ is
required for the enabled modes. Unsupported operations log warnings and do
not prevent startup. Unknown setting values fail configuration parsing.

Advice runs once after both loaders and precomputed OSR match checks finish,
before either CLI routing or HTTP traffic. It covers transit timetable mappings
and OSR's owned file mappings. It preserves OSR's private relocated pointers;
heap-backed graph metadata, derived indexes, platform trees, and allocator
memory are not advised. Existing `[osr_worker.region_load_modes]` controls
which OSR metadata is mmap-backed independently of this setting.

Startup logs report successfully advised bytes/mappings and failures per region;
those counts are not measurements of reclaimed memory. There is no guaranteed
RSS ceiling. Queries can fault needed pages back in, increasing first-query
latency. Compare `/proc/PID/smaps_rollup` RSS, `Pss_File`, and `Pss_Anon` before
and after startup, plus first-query and warm-query latency and route parity,
when choosing a mode. Do not apply advice after each request.
