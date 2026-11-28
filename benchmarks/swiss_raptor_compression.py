#!/usr/bin/env python3
"""Compare unmodified main against experimental Maple v2 + packed RAPTOR.
Both use one Swiss GTFS snapshot, one OSR graph, and one runner.
"""
import csv
from datetime import date, datetime, timedelta
import hashlib
import json
import math
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys
import time
from zoneinfo import ZoneInfo
import zipfile

ROOT = Path.cwd()
OUT = ROOT / "benchmark-results"
WORK = ROOT / "temp" / "swiss-raptor"
FEED = "f-u0-switzerland"
URL = "https://tulip.catenarymaps.org/debug/schedule/f-u0-switzerland"
GEOFABRIK = "https://download.geofabrik.de/europe/switzerland.poly"
OSR = ROOT / "temp/osr-data/regions/europe/switzerland"
VARIANTS = ("main", "maple-v2-experimental")
BASE_URL = os.environ["DATABASE_URL"]
BIN = ROOT / "target/release"
MAIN_BIN = ROOT / "benchmark-bin/main"

def binaries(mode):
    return MAIN_BIN if mode == "main" else BIN

sys.path.insert(0, str(ROOT / "src/routing_regions"))
from prepare_socal_test import geojson_from_poly
from run_timed_query import TIMING_FIELDS

def run(cmd, log, env=None, timeout=None):
    with (OUT / log).open("w") as f:
        p = subprocess.run([str(x) for x in cmd], stdout=f, stderr=subprocess.STDOUT,
                           env=env, timeout=timeout, check=False)
    if p.returncode:
        print((OUT / log).read_text(errors="replace")[-12000:], file=sys.stderr)
        raise RuntimeError(f"{cmd[0]} failed: {p.returncode}; see {log}")

def sql(url, statement):
    return subprocess.check_output(["psql", url, "-X", "-v", "ON_ERROR_STOP=1",
                                    "-Atc", statement], text=True).strip()

def download(url, target):
    subprocess.run(["curl", "-fL", "--retry", "5", "--retry-all-errors",
                    "--connect-timeout", "180", "--max-time", "2400",
                    "-o", str(target), url], check=True)

def dump(path, data):
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")

def digest(path):
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()

def patch_atlas(local_url):
    # Preserve the genuine feed/operator/chateau metadata; alter only the
    # static URL in the isolated checkout. No production catalog is changed.
    found = 0
    def visit(node):
        nonlocal found
        if isinstance(node, dict):
            if node.get("id") == FEED and isinstance(node.get("urls"), dict):
                assert "static_current" in node["urls"], "missing static URL"
                node["urls"]["static_current"] = local_url
                found += 1
            for value in node.values():
                visit(value)
        elif isinstance(node, list):
            for value in node:
                visit(value)
    for path in (ROOT / "transitland-atlas").rglob("*.dmfr.json"):
        if FEED not in path.read_text():
            continue
        doc = json.loads(path.read_text())
        before = found
        visit(doc)
        if found != before:
            dump(path, doc)
    assert found == 1, f"expected one Swiss feed in catalog, found {found}"

def config(url):
    path = ROOT / "catenaryconfig.toml"
    path.write_text(f"""[maple]
transitland = "./transitland-atlas"
use_girolle = false
no_elastic = true
delete_before_ingest = false
threads_gtfs = 2
force_ingest_all = true
bypass_temp_delete = false
gtfs_zip_temp = "./temp/gtfs_zip"
gtfs_uncompressed_temp = "./temp/gtfs_uncompressed"
only_feed_ids = ["{FEED}"]

[postgres]
database_url = "{url}"
max_connections = 16

[osm_manager]
data_dir = "temp/osr-data"
temp_dir = "temp/osr-working"
osr_extract = "./osr-extract"
regions = ["europe/switzerland"]
""")
    return path

def region_config(mode):
    target = WORK / mode / "regions.toml"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(f"""schema_version = 1
data_dir = "{target.parent}"
[[regions]]
id = "switzerland"
name = "Swiss compression benchmark"
geojson = ["{WORK / 'switzerland.geojson'}"]
""")
    return target

def database(mode):
    return BASE_URL.rsplit("/", 1)[0] + "/" + ("catenary" if mode == "main" else "catenary_v2")

def env_for(mode):
    return dict(os.environ, DATABASE_URL=database(mode),
                GENTIAN_BENCH_PACKED_OFFSETS="0" if mode == "main" else "1",
                MAPLE_SYRUP_V2_CONFIG=str(MAIN_BIN / "maple_syrup_v2.toml" if mode == "main" else WORK / "v2.toml"))

def table_counts(mode):
    url = database(mode)
    result = {}
    for table in ("itinerary_pattern", "maple_v2_structure", "maple_v2_trip_major",
                  "maple_v2_stop_unified", "trips_compressed", "stops"):
        result[table] = int(sql(url, f"SELECT count(*) FROM gtfs.{table} WHERE onestop_feed_id = '{FEED}'"))
    result["database_bytes"] = int(sql(url, "SELECT pg_database_size(current_database())"))
    assert result["trips_compressed"] > 0 and result["stops"] > 0
    if mode == "main":
        assert result["itinerary_pattern"] > 0 and result["maple_v2_trip_major"] == 0
    else:
        assert result["itinerary_pattern"] == 0 and result["maple_v2_trip_major"] > 0
        assert result["maple_v2_stop_unified"] > 0
    dump(OUT / f"{mode}-database.json", result)
    return result

def compile_mode(mode, first_day):
    url = database(mode)
    env = env_for(mode)
    cfg = config(url)
    rc = region_config(mode)
    dest = rc.parent / "switzerland.bin"
    started = time.perf_counter()
    run([binaries(mode) / "maple"], f"{mode}-maple.log", env=env, timeout=7200)
    ingestion = time.perf_counter() - started
    counts = table_counts(mode)
    run(["psql", url, "-v", "ON_ERROR_STOP=1", "-c", "ANALYZE"], f"{mode}-analyze.log")
    raw = subprocess.check_output([binaries(mode) / "gentian", "--region", "switzerland",
                                   "--regions-config", rc, "--list-chateaux"], env=env, text=True)
    selection = json.loads(raw)
    dump(OUT / f"{mode}-selection.json", selection)
    assert {s["onestop_feed_id"] for s in selection["sources"]} == {FEED}, selection
    started = time.perf_counter()
    run([binaries(mode) / "gentian", "--region", "switzerland", "--regions-config", rc,
         "--first-utc-day", first_day, "--day-count", "35",
         "--osr-dir", OSR, "--max-seconds", "900", "--candidate-meters", "1200",
         "--schedule-memory-mib", "128", "--scratch-dir", WORK / "scratch",
         "--build", "--output", dest], f"{mode}-gentian.log", env=env, timeout=7200)
    compile_seconds = time.perf_counter() - started
    info = json.loads(subprocess.check_output([binaries(mode) / "gentian", "--inspect-timetable", dest], text=True))
    assert info["route_count"] > 0 and {x["feed_id"] for x in info["sources"]} == {FEED}
    dump(OUT / f"{mode}-timetable.json", info)
    result = {"ingest_seconds": ingestion, "compile_seconds": compile_seconds,
              "database_bytes": counts["database_bytes"]}
    if mode == "maple-v2-experimental":
        log = (OUT / f"{mode}-gentian.log").read_text()
        match = re.search(r"packed_chains=(\d+) raw_event_words=(\d+) stored_event_words=(\d+)", log)
        assert match and int(match[1]) > 0 and int(match[3]) < int(match[2]), log[-3000:]
        result.update(packed_chains=int(match[1]), raw_event_words=int(match[2]),
                      stored_event_words=int(match[3]))
    return result

def sizes(mode):
    folder = WORK / mode
    result = {p.name: p.stat().st_size for p in sorted(folder.glob("switzerland.*")) if p.is_file()}
    assert result["switzerland.bin"] > 0 and result["switzerland.routing-index.bin"] > 0
    result["total_binary_bytes"] = sum(v for k, v in result.items() if k.endswith(".bin"))
    return result

def queries(day):
    landmarks = {x["id"]: x for x in json.loads((ROOT / "benchmarks/swiss_landmarks.json").read_text())}
    # Station and non-station endpoints, short/long, urban/intercity/alpine.
    landmarks.update({
        "zurich-polyterrasse": {"lat": 47.3767, "lon": 8.5480},
        "bern-bundeshaus": {"lat": 46.9466, "lon": 7.4441},
        "geneva-jet": {"lat": 46.2083, "lon": 6.1540},
        "lausanne-cathedral": {"lat": 46.5227, "lon": 6.6351},
        "lucerne-chapel-bridge": {"lat": 47.0515, "lon": 8.3075},
        "basel-munster": {"lat": 47.5560, "lon": 7.5929},
    })
    pairs = [
        ("zurich-hb", "winterthur-hb"), ("geneve-cornavin", "lausanne-gare"),
        ("bern-hb", "basel-sbb"), ("zurich-hb", "geneve-cornavin"),
        ("luzern-bahnhof", "interlaken-ost"), ("brig-bahnhof", "zermatt-bahnhof"),
        ("chur-bahnhof", "st-moritz-bahnhof"), ("zurich-hb", "lugano-stazione"),
        ("zurich-polyterrasse", "bern-bundeshaus"), ("geneva-jet", "lausanne-cathedral"),
        ("lucerne-chapel-bridge", "basel-munster"),
    ]
    stamp = int(datetime(day.year, day.month, day.day, 8, 0,
                         tzinfo=ZoneInfo("Europe/Zurich")).timestamp())
    for a, b in pairs:
        name = a + "--" + b
        request = {"from": {k: landmarks[a][k] for k in ("lat", "lon")},
                   "to": {k: landmarks[b][k] for k in ("lat", "lon")},
                   "departure_unix_seconds": stamp, "access": [], "egress": [],
                   "direct_walk_seconds": None}
        path = OUT / f"{name}-request.json"
        dump(path, request)
        yield name, path

def query(mode, name, request, iteration):
    prefix = OUT / f"{name}-{mode}-{iteration}"
    timing_path = prefix.with_suffix(".timings.json")
    cfg = config(database(mode))
    rc = WORK / mode / "regions.toml"
    env = env_for(mode)
    cmd = [binaries(mode) / "edelweiss", "--regions-config", rc, "--catenary-config", cfg,
           "--query", request, "--query-timings", timing_path, "--database-url", database(mode)]
    start = time.perf_counter_ns()
    with prefix.with_suffix(".response.json").open("w") as stdout, prefix.with_suffix(".log").open("w") as stderr:
        p = subprocess.run([str(x) for x in cmd], env=env, stdout=stdout, stderr=stderr, timeout=300)
    wall = (time.perf_counter_ns() - start) / 1e6
    assert p.returncode == 0, f"{prefix} failed"
    log = prefix.with_suffix(".log").read_text()
    assert "precompiled routing index mapped" in log, f"routing index not mapped: {prefix}"
    if mode == "maple-v2-experimental":
        m = re.search(r"mapped packed_chains=(\d+)", log)
        assert m and int(m[1]) > 0, f"packed routing index not used: {prefix}"
    response = json.loads(prefix.with_suffix(".response.json").read_text())
    timing = json.loads(timing_path.read_text())
    for key in TIMING_FIELDS:
        assert isinstance(timing[key], (float, int)) and math.isfinite(timing[key]) and timing[key] >= 0
    itineraries = response["itineraries"]
    assert itineraries, f"{prefix}: no transit itineraries; inspect logs"
    # Compare Pareto objectives, not volatile internal source attempt IDs.
    signature = sorted({(i["startTime"], i["endTime"], i["transfers"]) for i in itineraries})
    return signature, {"route": name, "variant": mode, "round": iteration,
                       "journey_count": len(itineraries), "cli_wall_ms": wall, **timing}

def percentile(xs, p):
    return sorted(xs)[max(0, math.ceil(len(xs) * p) - 1)]

def report(rows, builds, parity_errors):
    with (OUT / "samples.csv").open("w") as f:
        fields = ["route", "variant", "round", "journey_count", "raptor_ms", "query_total_ms", "cli_wall_ms"]
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    dump(OUT / "samples.json", rows)
    dump(OUT / "builds-and-sizes.json", builds)
    lines = ["## Swiss RAPTOR compression benchmark", "",
             "All latencies in ms. One excluded warm-up per route/variant; each sample starts a fresh CLI process.",
             "RAPTOR is the engine phase, not OSR or process startup. P95 is nearest-rank (small samples).", "",
             "| Variant | Main timetable bytes | Routing index bytes | All timetable .bin bytes | DB bytes | Maple s | Gentian s |",
             "|---|---:|---:|---:|---:|---:|---:|"]
    for mode in VARIANTS:
        b = builds[mode]
        s = b["sizes"]
        lines.append(f"| {mode} | {s['switzerland.bin']:,} | {s['switzerland.routing-index.bin']:,} | {s['total_binary_bytes']:,} | {b['database_bytes']:,} | {b['ingest_seconds']:.1f} | {b['compile_seconds']:.1f} |")
    lines += ["", "| Binary metric | Main bytes | Experimental bytes | Reduction | Main / experimental |",
              "|---|---:|---:|---:|---:|"]
    for key in ("switzerland.bin", "switzerland.routing-index.bin", "total_binary_bytes"):
        a = builds["main"]["sizes"][key]
        b = builds["maple-v2-experimental"]["sizes"][key]
        lines.append(f"| {key} | {a:,} | {b:,} | {(1-b/a)*100:.2f}% | {a/b:.3f}x |")
    lines += ["", "| Route | Variant | N | RAPTOR median | RAPTOR p95 | Query median | CLI median | Speedup vs main |",
              "|---|---|---:|---:|---:|---:|---:|---:|"]
    for route in dict.fromkeys(r["route"] for r in rows):
        base = statistics.median(r["raptor_ms"] for r in rows if r["route"] == route and r["variant"] == "main")
        for mode in VARIANTS:
            samples = [r for r in rows if r["route"] == route and r["variant"] == mode]
            median = statistics.median(r["raptor_ms"] for r in samples)
            speedup = base / median if median else float("inf")
            lines.append(f"| {route} | {mode} | {len(samples)} | {median:.3f} | {percentile([r['raptor_ms'] for r in samples], .95):.3f} | {statistics.median(r['query_total_ms'] for r in samples):.3f} | {statistics.median(r['cli_wall_ms'] for r in samples):.3f} | {speedup:.2f}x |")
    lines += ["", f"Pareto objective parity mismatches: {len(parity_errors)}.",
              f"Packed chains: {builds['maple-v2-experimental']['packed_chains']:,}.",
              "See builds-and-sizes.json for every binary size and packed event-word counts."]
    text = "\n".join(lines) + "\n"
    (OUT / "summary.md").write_text(text)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
            f.write(text)
    print(text)
    dump(OUT / "parity-errors.json", parity_errors)

def main():
    WORK.mkdir(parents=True, exist_ok=True)
    OUT.mkdir(exist_ok=True)
    for folder in ("temp/gtfs_zip", "temp/gtfs_uncompressed"):
        (ROOT / folder).mkdir(parents=True, exist_ok=True)
    rounds = int(os.environ.get("BENCH_ROUNDS", "5"))
    assert 1 <= rounds <= 20
    day_text = os.environ.get("BENCH_SERVICE_DATE")
    day = date.fromisoformat(day_text) if day_text else datetime.now(ZoneInfo("Europe/Zurich")).date() + timedelta(days=1)
    if not day_text:
        while day.weekday() >= 5:
            day += timedelta(days=1)
    first_day = (day - date(1970, 1, 1)).days - 7
    archive = WORK / "switzerland.zip"
    download(URL, archive)
    with zipfile.ZipFile(archive) as z:
        assert {"stops.txt", "trips.txt", "stop_times.txt"}.issubset(set(z.namelist())), "not a GTFS archive"
    dump(OUT / "inputs.json", {"feed": FEED, "url": URL, "gtfs_sha256": digest(archive),
                               "service_date": day.isoformat(), "first_utc_day": first_day, "days": 35, "rounds": rounds})
    download(GEOFABRIK, WORK / "switzerland.poly")
    geojson_from_poly(WORK / "switzerland.poly", WORK / "switzerland.geojson")
    config(BASE_URL)
    run([ROOT / "src/osm_manager/target/release/osm_manager", "--config", "catenaryconfig.toml", "run"],
        "osr-manager.log", timeout=7200)
    assert (OSR / "routing.bin").stat().st_size > 0
    graph_files = {p.name: p.stat().st_size for p in OSR.iterdir() if p.is_file()}
    dump(OUT / "osr-sizes.json", graph_files)
    (WORK / "v2.toml").write_text('feeds = ["f-u0-switzerland"]\n')
    run(["diesel", "migration", "run"], "migration.log")
    # Empty migrated database cloned before ingest: no stale attempts or
    # shared physical layout can contaminate either variant.
    sql(BASE_URL, "CREATE DATABASE catenary_v2 TEMPLATE catenary")
    server_log = (OUT / "snapshot-server.log").open("w")
    server = subprocess.Popen([sys.executable, "-m", "http.server", "8765", "--bind", "127.0.0.1",
                               "--directory", str(WORK)], stdout=server_log, stderr=subprocess.STDOUT)
    try:
        from urllib.request import urlopen
        for attempt in range(50):
            try:
                with urlopen("http://127.0.0.1:8765/", timeout=1):
                    break
            except OSError:
                time.sleep(.2)
        else:
            raise RuntimeError("local snapshot HTTP server did not start")
        patch_atlas("http://127.0.0.1:8765/switzerland.zip")
        builds = {}
        for mode in VARIANTS:
            builds[mode] = compile_mode(mode, first_day)
            builds[mode]["sizes"] = sizes(mode)
        dump(OUT / "builds-and-sizes.json", builds)
        rows = []
        parity_errors = []
        for name, request in queries(day):
            baseline = None
            # Rotate execution order each repetition to reduce systematic
            # cache/thermal advantage. Round zero is excluded from timing.
            for iteration in range(rounds + 1):
                order = VARIANTS[iteration % len(VARIANTS):] + VARIANTS[:iteration % len(VARIANTS)]
                for mode in order:
                    signature, sample = query(mode, name, request, iteration)
                    if baseline is None:
                        baseline = signature
                    if signature != baseline:
                        parity_errors.append({"route": name, "variant": mode, "round": iteration,
                                              "expected": baseline, "actual": signature})
                    if iteration:
                        rows.append(sample)
                    dump(OUT / "samples.json", rows)
        report(rows, builds, parity_errors)
        assert not parity_errors, "RAPTOR Pareto parity failed; inspect artifact"
    finally:
        server.terminate()
        server.wait(timeout=10)
        server_log.close()

if __name__ == "__main__":
    main()
