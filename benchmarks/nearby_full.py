#!/usr/bin/env python3
"""Full Nearby Departures HTTP benchmark against a real Maple-imported Swiss GTFS.

Builds are supplied by GitHub Actions. Both servers use the same PostgreSQL
database, fixed departure timestamp, and swiss_landmarks.json. Realtime is
disabled deliberately because this is a GTFS-static performance comparison.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import statistics
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen
from zoneinfo import ZoneInfo

import psycopg

LANDMARKS = json.loads((Path(__file__).parent / "swiss_landmarks.json").read_text())
FEED = "f-u0-switzerland"
CHATEAU = "schweiz"
URL = "http://127.0.0.1:17419"


def validate_dataset(conn):
    tables = ("stops", "direction_pattern", "itinerary_pattern",
              "itinerary_pattern_meta", "trips_compressed")
    counts = {}
    with conn.cursor() as cur:
        for table in tables:
            # Table identifiers are chosen from the constant above, never input.
            cur.execute(
                f"SELECT COUNT(*) FROM gtfs.{table} WHERE onestop_feed_id = %s",
                (FEED,),
            )
            counts[table] = cur.fetchone()[0]
        cur.execute(
            "SELECT chateau, count(*) FROM gtfs.stops "
            "WHERE onestop_feed_id = %s GROUP BY chateau",
            (FEED,),
        )
        chateaux = dict(cur.fetchall())
        cur.execute(
            "SELECT DISTINCT onestop_feed_id FROM gtfs.trips_compressed "
            "ORDER BY onestop_feed_id"
        )
        actual_feeds = [r[0] for r in cur.fetchall()]
    if any(value == 0 for value in counts.values()):
        raise RuntimeError(f"Maple import is incomplete: {counts}")
    if CHATEAU not in chateaux:
        raise RuntimeError(
            f"Expected '{CHATEAU}' from feed {FEED}; found {chateaux}"
        )
    if actual_feeds != [FEED]:
        raise RuntimeError(
            f"Only {FEED} should be imported; trips contain {actual_feeds}"
        )
    return {"feed_id": FEED, "chateau": CHATEAU,
            "tables": counts, "chateaux": chateaux}


def choose_service_day(conn):
    """Pick a busy current-or-future Swiss GTFS date, accounting for exceptions.

    This runs once. The exact UNIX timestamp is used for both implementations.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT service_id, gtfs_start_date, gtfs_end_date, "
            "monday, tuesday, wednesday, thursday, friday, saturday, sunday "
            "FROM gtfs.calendar WHERE chateau = %s", (CHATEAU,)
        )
        calendars = list(cur.fetchall())
        cur.execute(
            "SELECT service_id, gtfs_date, exception_type "
            "FROM gtfs.calendar_dates WHERE chateau = %s",
            (CHATEAU,),
        )
        exceptions = {}
        for service, day, kind in cur.fetchall():
            exceptions[(service, day)] = kind

    today = datetime.now(ZoneInfo("Europe/Zurich")).date()
    days = [today + timedelta(days=i) for i in range(14)]
    # Use a valid future date if no imported service covers the next fortnight.
    valid_starts = [c[1] for c in calendars if c[2] >= today]
    valid_starts.extend(day for _, day in exceptions if day >= today)
    if valid_starts and all(
        not any(start <= day <= end for _, start, end, *_ in calendars)
        and not any(ex_day == day for _, ex_day in exceptions)
        for day in days
    ):
        start = min(valid_starts)
        days = [start + timedelta(days=i) for i in range(14)]

    def active_count(day):
        active = set()
        for service, start, end, *weekday_flags in calendars:
            if start <= day <= end and weekday_flags[day.weekday()]:
                active.add(service)
        for (service, exception_day), kind in exceptions.items():
            if exception_day == day:
                if kind == 1:
                    active.add(service)
                elif kind == 2:
                    active.discard(service)
        return len(active)

    # Ties favor the earliest date, chosen independently of the code revision.
    chosen = max(days, key=active_count)
    count = active_count(chosen)
    if count == 0:
        raise RuntimeError("No active Swiss services in GTFS calendar window")
    departure = int(
        datetime(chosen.year, chosen.month, chosen.day, 9, 0,
                 tzinfo=ZoneInfo("Europe/Zurich")).timestamp()
    )
    return {"service_date": chosen.isoformat(),
            "departure_time_unix": departure,
            "active_service_ids": count,
            "local_departure_time": "09:00 Europe/Zurich"}


def canonical(obj):
    """Ignore unstable JSON object/array iteration order, but not values."""
    if isinstance(obj, dict):
        return {k: canonical(v) for k, v in sorted(obj.items())
                if k != "debug"}
    if isinstance(obj, list):
        values = [canonical(v) for v in obj]
        return sorted(values,
                      key=lambda v: json.dumps(
                          v, sort_keys=True, ensure_ascii=False,
                          separators=(",", ":")))
    return obj


def digest(obj):
    return hashlib.sha256(json.dumps(
        canonical(obj), ensure_ascii=False, sort_keys=True,
        separators=(",", ":")).encode("utf-8")).hexdigest()


def query_counts(debug):
    groups = {}
    for q in debug.get("sql_queries", []):
        name = str(q.get("query", "unknown"))
        entry = groups.setdefault(
            name, {"queries": 0, "rows": 0, "query_sum_ms": 0.0})
        entry["queries"] += 1
        entry["rows"] += int(q.get("returned_rows", 0))
        entry["query_sum_ms"] += float(q.get("query_ms", 0))
    return groups


def url_for(location, epoch, radius):
    params = {
        "lat": location["lat"], "lon": location["lon"],
        "radius": radius,
        "departure_time": epoch,
        "skip_realtime": "true",
        "rt_timeout_ms": 0,
        "limit_per_station": 10,
        "limit_per_headsign": 20,
    }
    return URL + "/nearbydeparturesfromcoordsv3?" + urlencode(params)


def get_json(url, timeout):
    with urlopen(url, timeout=timeout) as response:
        if response.status != 200:
            raise RuntimeError(f"Unexpected HTTP status {response.status}")
        data = response.read()
    return json.loads(data)


def start_server(binary, label, out, startup_timeout):
    logfile = out / f"{label}-birch.log"
    stream = logfile.open("wb")
    proc = subprocess.Popen([str(Path(binary).resolve())],
                            stdout=stream, stderr=subprocess.STDOUT)
    stream.close()
    deadline = time.monotonic() + startup_timeout
    try:
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise RuntimeError(
                    f"{label} Birch exited with status {proc.returncode}; "
                    f"check {logfile}")
            try:
                with urlopen(URL + "/microtime", timeout=2) as r:
                    if r.status == 200:
                        return proc
            except (URLError, TimeoutError, ConnectionError, HTTPError):
                pass
            time.sleep(0.5)
        raise RuntimeError(f"{label} Birch startup timed out; see {logfile}")
    except Exception:
        stop_server(proc)
        raise


def stop_server(proc):
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=12)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)


def record(label, location, url, timeout, store, save_response=False):
    before = time.perf_counter()
    response = get_json(url, timeout)
    client_ms = (time.perf_counter() - before) * 1000
    debug = response.get("debug")
    if not isinstance(debug, dict) or "total_time_ms" not in debug:
        raise RuntimeError(f"{label}/{location['id']}: no backend debug trace")
    count = len(response.get("local", [])) + len(response.get("long_distance", []))
    info = {
        "client_wall_ms": round(client_ms, 3),
        "server_total_ms": debug["total_time_ms"],
        "pipeline_ms": debug.get("pipeline_processing_time_ms"),
        "stops_fetch_ms": debug.get("stops_fetch_time_ms"),
        "initial_stop_count": debug.get("initial_stop_count"),
        "route_groups": count,
        "response_hash": digest(response),
        "sql": query_counts(debug),
    }
    if save_response:
        (store / f"{label}-{location['id']}.json").write_text(
            json.dumps(canonical(response), ensure_ascii=False, sort_keys=True,
                       indent=2) + "\n"
        )
    return info


def run_variant(args, label, binary, fixture, directory):
    proc = start_server(binary, label, directory, args.startup_timeout)
    results = {}
    try:
        # Warmups are intentionally excluded from duration statistics.
        for loc in LANDMARKS:
            url = url_for(loc, fixture["departure_time_unix"], args.radius_m)
            for _ in range(args.warmups):
                record(label, loc, url, args.timeout, directory)
            samples = []
            for index in range(args.rounds):
                samples.append(record(label, loc, url, args.timeout,
                                      directory, save_response=index == 0))
            results[loc["id"]] = samples
            median = statistics.median(x["client_wall_ms"] for x in samples)
            print(f"{label:10s} {loc['id']:24s} {median:9.1f} ms "
                  f"groups={samples[0]['route_groups']}", flush=True)
    finally:
        stop_server(proc)
    return results


def compare(baseline, candidate, fixture, dataset, args, output_dir):
    locations = []
    mismatches = []
    populated = 0
    for loc in LANDMARKS:
        left = baseline[loc["id"]]
        right = candidate[loc["id"]]
        b_hashes = {sample["response_hash"] for sample in left}
        c_hashes = {sample["response_hash"] for sample in right}
        same = len(b_hashes) == len(c_hashes) == 1 and b_hashes == c_hashes
        if not same:
            mismatches.append(loc["id"])
        if left[0]["route_groups"] > 0:
            populated += 1
        old_median = statistics.median(x["client_wall_ms"] for x in left)
        new_median = statistics.median(x["client_wall_ms"] for x in right)
        locations.append({
            "id": loc["id"], "name": loc["name"],
            "lat": loc["lat"], "lon": loc["lon"], "parity": same,
            "baseline_median_ms": round(old_median, 3),
            "candidate_median_ms": round(new_median, 3),
            "speedup": round(old_median / max(0.001, new_median), 3),
            "baseline": left, "candidate": right,
        })

    report = {
        "kind": "real-world GTFS static HTTP integration benchmark",
        "note": "Same Maple import and database; realtime disabled. "
                "Runs are sequential and may have PostgreSQL cache-order bias.",
        "feed": dataset,
        "fixture": fixture,
        "configuration": {
            "radius_m": args.radius_m, "warmups": args.warmups,
            "rounds": args.rounds, "skip_realtime": True,
            "baseline_ref": args.baseline_ref,
            "candidate_ref": args.candidate_ref,
        },
        "populated_locations": populated,
        "parity_failures": mismatches,
        "locations": locations,
    }
    (output_dir / "nearby-full-benchmark.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n"
    )

    rows = [
        "## Switzerland Nearby Departures full HTTP benchmark",
        "",
        f"**Feed:** \`{FEED}\` (Maple import into PostgreSQL)",
        f"**Service date:** {fixture['service_date']} "
        f"at {fixture['local_departure_time']}",
        f"**Request settings:** radius={args.radius_m:g}m, "
        "realtime disabled, fixed UNIX departure time",
        "",
        "| Landmark | Baseline median (ms) | PR median (ms) | Speedup | Parity |",
        "| --- | ---: | ---: | ---: | --- |",
    ]
    for loc in locations:
        rows.append(
            f"| {loc['name']} | {loc['baseline_median_ms']:.1f} | "
            f"{loc['candidate_median_ms']:.1f} | "
            f"{loc['speedup']:.2f}x | "
            f"{'PASS' if loc['parity'] else 'FAIL'} |"
        )
    rows += [
        "",
        f"Populated locations: {populated}/{len(LANDMARKS)}",
        f"Response parity mismatches: {len(mismatches)}",
        "",
        "Both versions run against one real imported Swiss GTFS database; "
        "timings include HTTP, SQL, Rust filtering, serialization and compression. "
        "Realtime is deliberately disabled. Execution is sequential "
        "(baseline then PR), so PostgreSQL cache effects remain possible.",
    ]
    markdown = "\n".join(rows) + "\n"
    (output_dir / "summary.md").write_text(markdown)
    if os.getenv("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
            summary.write(markdown)
    print(markdown, flush=True)
    if mismatches:
        raise AssertionError("Response parity failures: " + ", ".join(mismatches))
    if populated < args.minimum_populated:
        raise AssertionError(
            f"Only {populated} landmarks returned scheduled services; "
            f"minimum required is {args.minimum_populated}"
        )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    p.add_argument("--baseline-binary", required=True)
    p.add_argument("--candidate-binary", required=True)
    p.add_argument("--baseline-ref", required=True)
    p.add_argument("--candidate-ref", required=True)
    p.add_argument("--output-dir", default="benchmark-results")
    p.add_argument("--radius-m", type=float, default=1500)
    p.add_argument("--warmups", type=int, default=1)
    p.add_argument("--rounds", type=int, default=3)
    p.add_argument("--timeout", type=float, default=180)
    p.add_argument("--startup-timeout", type=float, default=120)
    p.add_argument("--minimum-populated", type=int, default=5)
    args = p.parse_args()
    if not args.database_url or args.rounds < 1 or args.radius_m <= 0:
        p.error("DATABASE_URL, positive rounds and radius are required")
    directory = Path(args.output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    with psycopg.connect(args.database_url, autocommit=True) as conn:
        dataset = validate_dataset(conn)
        fixture = choose_service_day(conn)
    (directory / "fixture.json").write_text(
        json.dumps({"dataset": dataset, "timing": fixture}, indent=2) + "\n"
    )
    print("Imported GTFS:", json.dumps(dataset), flush=True)
    print("Fixed departure:", json.dumps(fixture), flush=True)
    baseline = run_variant(
        args, "baseline", args.baseline_binary, fixture, directory)
    candidate = run_variant(
        args, "candidate", args.candidate_binary, fixture, directory)
    compare(baseline, candidate, fixture, dataset, args, directory)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"BENCHMARK FAILED: {error}", file=sys.stderr, flush=True)
        raise
