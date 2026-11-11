#!/usr/bin/env python3
"""Deterministic SQL-level benchmark of nearby itinerary lookup strategies.

Runs on PostgreSQL 15+ with only psycopg 3; does not require the Rust server.
The synthetic dataset is deliberately labeled synthetic. A separate mode reads
an existing GTFS import without modifying it.

Outputs JSON and fails when old/new result sets differ.
"""
import argparse
import json
import os
import statistics
import time
from pathlib import Path

import psycopg

LANDMARKS = json.loads((Path(__file__).parent / "swiss_landmarks.json").read_text())


def synthetic_schema(conn):
    with conn.cursor() as c:
        c.execute("DROP SCHEMA IF EXISTS gtfs CASCADE")
        c.execute("CREATE SCHEMA gtfs")
        c.execute("""CREATE TABLE gtfs.stops (
            chateau text NOT NULL, gtfs_id text NOT NULL,
            lat double precision NOT NULL, lon double precision NOT NULL)""")
        c.execute("""CREATE TABLE gtfs.direction_pattern (
            chateau text NOT NULL, stop_id text NOT NULL, direction_pattern_id text NOT NULL)""")
        c.execute("""CREATE TABLE gtfs.itinerary_pattern_meta (
            onestop_feed_id text NOT NULL, attempt_id text NOT NULL,
            itinerary_pattern_id text NOT NULL, chateau text NOT NULL,
            direction_pattern_id text, route_id text NOT NULL,
            timezone text NOT NULL, row_count integer NOT NULL)""")
        c.execute("""CREATE TABLE gtfs.itinerary_pattern (
            onestop_feed_id text NOT NULL, attempt_id text NOT NULL,
            itinerary_pattern_id text NOT NULL, chateau text NOT NULL,
            stop_id text NOT NULL, stop_sequence integer NOT NULL,
            departure_time_since_start integer)""")
        c.execute("CREATE INDEX ON gtfs.direction_pattern (chateau, stop_id)")
        c.execute("CREATE INDEX ON gtfs.itinerary_pattern_meta (chateau, direction_pattern_id)")
        c.execute("CREATE INDEX ON gtfs.itinerary_pattern (chateau, stop_id)")
        c.execute("CREATE INDEX ON gtfs.itinerary_pattern (chateau, itinerary_pattern_id, stop_sequence)")
        for landmark in LANDMARKS:
            key = landmark["id"]
            lat = landmark["lat"]
            lon = landmark["lon"]
            c.execute("""INSERT INTO gtfs.stops
                SELECT 'schweiz', %s || '-stop-' || n,
                %s + ((n %% 9) - 4) * 0.0009,
                %s + ((n / 9) - 4) * 0.0011
                FROM generate_series(0, 71) AS n""", (key, lat, lon))
            c.execute("""INSERT INTO gtfs.direction_pattern
                SELECT 'schweiz', %s || '-stop-' || s,
                       %s || '-dir-' || d
                FROM generate_series(0, 71) s
                CROSS JOIN generate_series(0, 23) d""", (key, key))
            c.execute("""INSERT INTO gtfs.itinerary_pattern_meta
                SELECT %s, 'fixed-attempt', %s || '-itin-' || p, 'schweiz',
                       %s || '-dir-' || (p %% 24), %s || '-route-' || (p %% 12),
                       'Europe/Zurich', 22
                FROM generate_series(0, %s - 1) p""", (key, key, key, key, args_patterns))
            c.execute("""INSERT INTO gtfs.itinerary_pattern
                SELECT %s, 'fixed-attempt', %s || '-itin-' || p, 'schweiz',
                       %s || '-stop-' || ((p * 7 + k * 3) %% 72),
                       k, k * 180
                FROM generate_series(0, %s - 1) p
                CROSS JOIN generate_series(0, 21) k""", (key, key, key, args_patterns))
        c.execute("ANALYZE")
    conn.commit()


def selected_stop_ids(conn, landmark, radius_m):
    # PostGIS-free bounding and exact haversine distance; same inputs each run.
    lat, lon = landmark["lat"], landmark["lon"]
    with conn.cursor() as c:
        c.execute("""SELECT gtfs_id FROM gtfs.stops
            WHERE chateau = 'schweiz' AND
                6371000 * 2 * asin(least(1, sqrt(
                    power(sin(radians(lat - %s) / 2), 2) +
                    cos(radians(%s)) * cos(radians(lat)) *
                    power(sin(radians(lon - %s) / 2), 2)
                ))) <= %s ORDER BY gtfs_id""", (lat, lat, lon, radius_m))
        return [x[0] for x in c.fetchall()]


def benchmark_one(conn, landmark, radius_m, rounds):
    stops = selected_stop_ids(conn, landmark, radius_m)
    if not stops:
        raise RuntimeError("No GTFS stops for " + landmark["id"])
    # Fix the stop set across old and new. Preselect direction IDs just as
    # Nearby Departures does; measure metadata+stop rows, not spatial search.
    with conn.cursor() as c:
        c.execute("""SELECT DISTINCT direction_pattern_id
            FROM gtfs.direction_pattern WHERE chateau = 'schweiz'
              AND stop_id = ANY(%s::text[])""", (stops,))
        dirs = [r[0] for r in c.fetchall()]
    if not dirs:
        raise RuntimeError("No direction patterns for " + landmark["id"])

    old_meta = """SELECT itinerary_pattern_id
        FROM gtfs.itinerary_pattern_meta
        WHERE chateau = 'schweiz' AND direction_pattern_id = ANY(%s::text[])"""
    old_rows = """SELECT stop_id, itinerary_pattern_id, stop_sequence
        FROM gtfs.itinerary_pattern
        WHERE chateau = 'schweiz' AND stop_id = ANY(%s::text[])
          AND itinerary_pattern_id = ANY(%s::text[])"""
    new_rows = """SELECT ip.stop_id, ip.itinerary_pattern_id, ip.stop_sequence
        FROM gtfs.itinerary_pattern ip
        INNER JOIN gtfs.itinerary_pattern_meta im
            ON im.onestop_feed_id = ip.onestop_feed_id
            AND im.attempt_id = ip.attempt_id
            AND im.itinerary_pattern_id = ip.itinerary_pattern_id
        WHERE ip.chateau = 'schweiz' AND im.chateau = 'schweiz'
          AND ip.stop_id = ANY(%s::text[])
          AND im.direction_pattern_id = ANY(%s::text[])"""

    def legacy(cur):
        cur.execute(old_meta, (dirs,))
        patterns = sorted(set(x[0] for x in cur.fetchall()))
        if not patterns:
            return []
        cur.execute(old_rows, (stops, patterns))
        return cur.fetchall()

    def stop_first(cur):
        cur.execute(new_rows, (stops, dirs))
        return cur.fetchall()

    def once(fn):
        with conn.cursor() as cur:
            start = time.perf_counter()
            records = fn(cur)
            return (time.perf_counter() - start) * 1000, records

    # Warm caches equally. Timed iterations alternate execution order.
    for _ in range(2):
        once(legacy)
        once(stop_first)
    baseline, new = [], []
    for n in range(rounds):
        first, second = ((legacy, stop_first) if n % 2 == 0
                         else (stop_first, legacy))
        for fn in (first, second):
            duration, rows = once(fn)
            (baseline if fn is legacy else new).append(duration)
            if fn is legacy:
                old_result = sorted(rows)
            else:
                new_result = sorted(rows)
        if old_result != new_result:
            raise AssertionError(
                f"{landmark['id']} parity mismatch: old={len(old_result)} new={len(new_result)}"
            )
    return {
        "id": landmark["id"], "label": landmark["name"],
        "lat": landmark["lat"], "lon": landmark["lon"],
        "stops": len(stops), "directions": len(dirs),
        "rows": len(old_result),
        "legacy_median_ms": round(statistics.median(baseline), 3),
        "stop_first_median_ms": round(statistics.median(new), 3),
        "ratio": round(statistics.median(baseline) / max(0.001, statistics.median(new)), 3),
        "parity": True,
    }


def main():
    global args_patterns
    p = argparse.ArgumentParser()
    p.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    p.add_argument("--mode", choices=["synthetic", "imported"], default="synthetic")
    p.add_argument("--patterns-per-landmark", type=int, default=1200)
    p.add_argument("--radius-m", type=float, default=500)
    p.add_argument("--rounds", type=int, default=7)
    p.add_argument("--output", default="nearby-benchmark.json")
    args = p.parse_args()
    if not args.database_url:
        p.error("DATABASE_URL or --database-url is required")
    args_patterns = args.patterns_per_landmark
    if args.rounds < 1 or args_patterns < 1:
        p.error("rounds and patterns-per-landmark must be positive")
    with psycopg.connect(args.database_url) as conn:
        if args.mode == "synthetic":
            synthetic_schema(conn)
        results = []
        for landmark in LANDMARKS:
            try:
                result = benchmark_one(conn, landmark, args.radius_m, args.rounds)
            except RuntimeError:
                if args.mode == "imported":
                    results.append({"id": landmark["id"], "status": "no nearby stops"})
                    continue
                raise
            results.append(result)
            print(f"{result['label']}: {result['legacy_median_ms']:.2f} ms -> "
                  f"{result['stop_first_median_ms']:.2f} ms ({result['ratio']:.2f}x)")
    measured = [x for x in results if x.get("parity")]
    if not measured:
        raise RuntimeError("No landmark has benchmarkable data")
    report = {
        "fixture": args.mode,
        "patterns_per_landmark": args_patterns if args.mode == "synthetic" else None,
        "radius_m": args.radius_m,
        "rounds": args.rounds,
        "description": "SQL-only comparison; does not measure HTTP, realtime or complete Rust pipeline",
        "locations": results,
        "aggregate": {
            "legacy_median_ms": round(statistics.median(x["legacy_median_ms"] for x in measured), 3),
            "stop_first_median_ms": round(statistics.median(x["stop_first_median_ms"] for x in measured), 3),
            "all_parity": all(x["parity"] for x in measured),
        }
    }
    Path(args.output).write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report["aggregate"], indent=2))


if __name__ == "__main__":
    main()
