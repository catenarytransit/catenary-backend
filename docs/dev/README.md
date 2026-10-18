# Local dev/test setup: Docker for services, pixi for everything else

This mirrors `.github/workflows/full-test-pipeline.yml` as closely as
possible, split into two tracks so a laptop used for other things doesn't
get permanent system services or packages installed on it:

- **Docker** runs the stateful backing services the pipeline depends on:
  Postgres+PostGIS, etcd, Elasticsearch, and Redis. These are disposable
  containers you can start/stop/delete at will (`docker compose down -v`
  leaves no trace).
- **pixi** provides the Rust toolchain and native build libraries (protobuf,
  OpenSSL, CBC, libpq, cmake, pkg-config, ...) in a project-local,
  non-sandboxed environment — no `apt install` of anything, no files or
  services touching the rest of your system outside this repo.

The actual catenary binaries (`maple`, `birch`, `spruce`, `alpenrose`,
`aspen`, `aspenleader`) are built and run on the host via pixi, *not* inside
Docker — they just talk to the Dockerized services over `localhost`. This
keeps the inner dev loop (`cargo build`, rerun a binary) fast, while still
isolating the heavy/always-on daemons.

See `../../readme.md` for the non-Docker/non-pixi version of these same
steps (plain `apt install` on Ubuntu), which is what this guide adapts.

## 1. Prerequisites

- Docker + the Compose plugin (`docker compose version`).
- [pixi](https://pixi.sh) (`curl -fsSL https://pixi.sh/install.sh | sh`).

Nothing else needs to be installed on the host.

## 2. Start the backing services

From the repo root:

```bash
docker compose -f docs/dev/docker-compose.yml up -d
```

This starts, with the same images/versions and credentials the CI job uses:

| Service       | Image                              | Host port (127.0.0.1 only) |
|---------------|-------------------------------------|-----------------------------|
| postgres      | `postgis/postgis:15-3.3`            | 5432                        |
| etcd          | `quay.io/coreos/etcd:v3.5.9`        | 2379                        |
| elasticsearch | `elasticsearch:8.11.1`              | 9200                        |
| redis         | `redis:7-alpine`                    | 6379                        |

Redis isn't in the GitHub Actions `services:` block, but the CI job
`apt-get install`s the `redis` package, which starts `redis-server` as a
system service — `aspen` (`src/aspen/main.rs`) hardcodes
`redis://127.0.0.1/`, so it's a real dependency even though it's not
declared as a `services:` container. See `docker-compose.yml` for details.

Check everything is healthy:

```bash
docker compose -f docs/dev/docker-compose.yml ps
```

Tear down later with:

```bash
docker compose -f docs/dev/docker-compose.yml down      # keep the postgres volume
docker compose -f docs/dev/docker-compose.yml down -v   # also delete ingested data
```

### Laptop-specific notes

- Ports are bound to `127.0.0.1` only (not `0.0.0.0`) since the default
  credentials here (`postgres`/`postgres`, Elasticsearch security disabled)
  are CI defaults, not meant to be reachable from your LAN.
- Elasticsearch may refuse to start if the host's `vm.max_map_count` is too
  low (a common Docker-on-Linux gotcha, unrelated to this repo):
  `sudo sysctl -w vm.max_map_count=262144` if the container keeps restarting.
- Elasticsearch is capped at 512MB heap (`ES_JAVA_OPTS`) to match CI; bump it
  in `docker-compose.yml` if ingestion is too slow on your machine.
- `docker compose ... down` (without `-v`) stops everything and frees RAM/CPU
  without losing the Postgres data volume, so you don't have to re-ingest
  GTFS data every session.

## 3. Set up the pixi environment

From the repo root (this reads `../../pixi.toml`):

```bash
pixi install
pixi shell
```

`pixi shell` drops you into a shell with `cargo`, `rustc`, `pkg-config`,
`openssl`, `cmake`, `libprotobuf` (provides `protoc`), `coin-or-cbc`,
`postgresql` (provides `psql`), `git`, `jq`, and `curl` — everything
`readme.md`'s "Install Dependencies" section installs via `apt`, scoped to
this project only. Alternatively, prefix each command below with `pixi run`
instead of entering the shell.

## 4. Fetch feed metadata and configure

```bash
git clone https://github.com/catenarytransit/transitland-atlas.git transitland-atlas
cp docs/dev/catenaryconfig.local.toml.example catenaryconfig.toml
mkdir -p temp/gtfs_zip temp/gtfs_uncompressed
```

`catenaryconfig.local.toml.example` points at the same `localhost` ports the
compose file publishes, and (like CI) restricts Maple/Alpenrose to a single
small feed (`f-vvs~de` / `f-vvs~de~rt`, Stuttgart) for a fast smoke test
rather than ingesting the whole atlas. Widen `only_feed_ids` (or remove it)
once you want to test against more feeds.

## 5. Build

```bash
pixi run build
# equivalent to:
# cargo build --release --bin maple --bin spruce --bin birch --bin alpenrose --bin aspen --bin aspenleader
```

## 6. Run migrations

```bash
pixi run migrate
```

This runs `docs/dev/run-migrations.sh`, which applies every
`migrations/*/up.sql` in order via `psql`, exactly like the CI "Run Database
Migrations" step. It defaults to the credentials in `docker-compose.yml`;
override `PGHOST`/`PGPORT`/`PGUSER`/`PGDATABASE`/`PGPASSWORD` if you changed
those.

## 7. Ingest and run services

```bash
pixi run maple          # one-shot GTFS ingest, exits when done

pixi run spruce &
pixi run birch &
pixi run alpenrose &
pixi run aspen &
pixi run aspenleader &
```

(Same binaries, same order, as the CI "Run Ingest" and "Run background
services" steps. Background them with `&`/your terminal multiplexer of
choice, or run each in its own pane.)

## 8. Smoke-test, same queries as CI

```bash
curl -s "http://127.0.0.1:17419/nearbydeparturesfromcoordsv3?lat=48.7839&lon=9.1817" | jq .
curl -s "http://127.0.0.1:17419/fetch_full_trip_updates_dataset?chateau=vvs~de" | head -c 2000
```

(`17419` is Birch's hardcoded listen port, `src/birch/server.rs`.)

## 9. Stop everything

```bash
# Ctrl-C / kill the background binaries, then:
docker compose -f docs/dev/docker-compose.yml down   # add -v to also wipe data
```

## Troubleshooting

### `undefined symbol: __libc_csu_init` / `__libc_csu_fini` at link time

If `pixi run build` fails with `rust-lld: error: undefined symbol:
__libc_csu_init` (or `__libc_csu_fini`), referenced from `Scrt1.o` in the
pixi environment's sysroot, this is a toolchain-sandboxing issue, not a bug
in catenary-backend:

- `pixi.toml` must depend on `gcc_linux-64` and `gxx_linux-64` (not just
  `rust`). Those packages carry the conda-forge compiler *activation
  scripts* that set `CC`/`CXX`/`CFLAGS`/`LDFLAGS`/`CMAKE_ARGS` to point at
  the pixi environment. Without them, some `*-sys` build scripts (notably
  `bzip2-sys`, which calls `pkg-config` and falls back to a system search)
  can end up linking against the host's own `libbz2`/`libc` instead of the
  conda-provided ones. That mixes this environment's `sysroot_linux-64`
  (pinned to an old, broadly-compatible glibc ABI) with whatever glibc the
  host actually has — and newer glibc releases (>=2.34) removed
  `__libc_csu_init`/`__libc_csu_fini`, which the sysroot's CRT startup
  object still expects.
- If you hit this *after* already building once (e.g. you ran `cargo build`
  natively before setting up pixi, or you edited `pixi.toml` and
  reinstalled), stale `target/release/build/<crate>-*/output` files from
  the earlier toolchain can keep a bad link search path cached indefinitely
  — Cargo only reruns a build script when its own declared triggers
  change, not when the compiler underneath it changes. Clear the specific
  crate's cache (`pixi run cargo clean -p bzip2-sys --release`) or, if in
  doubt, `pixi run cargo clean --release` for a fully fresh build.

## Files in this directory

- `docker-compose.yml` — Postgres+PostGIS, etcd, Elasticsearch, Redis.
- `run-migrations.sh` — applies `migrations/*/up.sql`, used by `pixi run migrate`.
- `catenaryconfig.local.toml.example` — config template for this setup.
- `../../pixi.toml` — Rust toolchain + native build deps (kept at the repo
  root since that's where pixi/cargo expect their manifests).
