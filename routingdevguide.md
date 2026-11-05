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

- `GET /` returns JSON identifying Edelweiss, its package version, and API paths.
- `GET /health` returns HTTP 200 with
  `{"service":"edelweiss","status":"ok"}` and `Cache-Control: no-store`.

HTTP starts after all configured routing regions load successfully. During
startup, the health endpoint is unavailable; allow sufficient startup-probe
time before treating the process as unhealthy. The health response confirms the
HTTP service and loaded router, without probing the separate OSR worker or
PostgreSQL.

With the default listen address:

```bash
curl http://127.0.0.1:40428/
curl http://127.0.0.1:40428/health
```
