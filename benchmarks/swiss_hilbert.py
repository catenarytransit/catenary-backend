#!/usr/bin/env python3
"""Paired, sequential, warm-service main/source/Hilbert routing experiment.

Uses the standard library only. Each block starts one service, warms every
fixture, measures every fixture once, then stops it. Variant order rotates
and reverses, query order is seeded, and services never compete for CPU/RAM.
"""
import argparse
from collections import defaultdict
from contextlib import contextmanager
import csv
from datetime import date, datetime, time, timedelta
import json
import math
import os
from pathlib import Path
import random
import statistics
import subprocess
import time as clock
import urllib.error
import urllib.request
from zoneinfo import ZoneInfo

VARIANTS = ("main", "source", "hilbert")
METRICS = ("raptor_ms", "query_total_ms", "osr_parallel_wall_ms", "http_ms")


def fixtures(catalog, monday):
    if monday.weekday() != 0:
        raise ValueError("--monday must be a Monday")
    result = []
    for kind, a, b in catalog["pairs"]:
        for a, b in ((a, b), (b, a)):
            for day, hour in ((monday, 8), (monday + timedelta(days=5), 10)):
                depart = datetime.combine(day, time(hour), ZoneInfo("Europe/Zurich"))
                result.append({
                    "id": f"{a}--{b}--{day}", "kind": kind,
                    "label": f'{catalog["places"][a]["name"]} → {catalog["places"][b]["name"]} ({day} {hour:02}:00)',
                    "request": {
                        "from": {k: catalog["places"][a][k] for k in ("lat", "lon")},
                        "to": {k: catalog["places"][b][k] for k in ("lat", "lon")},
                        "departure_unix_seconds": int(depart.timestamp()),
                        "access": [], "egress": [], "direct_walk_seconds": None,
                        "preTransitModes": ["WALK"], "postTransitModes": ["WALK"],
                        "timetable_view": True, "detailed_legs": False,
                        "search_window_seconds": 3600,
                        "max_search_window_seconds": 3600,
                        "min_itineraries": 1, "timeout_seconds": 120,
                    },
                })
    return result


def place(p):
    # Numeric location indexes change intentionally. GTFS identity and the
    # actual access point must still be identical, including at transfer legs.
    return {k: p.get(k) for k in ("stopId", "lat", "lon")}


def canonical(response):
    def journey(j):
        return {
            **{k: j.get(k) for k in ("startTime", "endTime", "duration", "transfers")},
            "legs": [{
                **{k: leg.get(k) for k in ("mode", "startTime", "endTime", "duration",
                                          "chateau", "tripId", "routeId")},
                "from": place(leg["from"]), "to": place(leg["to"]),
            } for leg in j["legs"]],
        }
    # Ignore response order, cursors, layout-derived journey hashes, debug
    # counters/timers and geometry. Keep duplicate journeys visible.
    return {field: sorted(json.dumps(journey(j), sort_keys=True) for j in response[field])
            for field in ("itineraries", "direct")}


def validate(response):
    debug = response["debugOutput"]
    stats = [debug["raptor"], *debug.get("pages", {}).values()]
    if any(s.get("profile_budget_exhausted", False) for s in stats):
        raise ValueError("Search budget exhausted; partial output is not a valid timing sample")
    if not response["itineraries"]:
        raise ValueError("No transit journey; check feed coverage and landmark access")
    timings = debug["timings"]
    for metric in METRICS[:-1]:
        value = timings[metric]
        if not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise ValueError(f"Invalid {metric}: {value}")
    return timings


@contextmanager
def service(binary, config, catenary_config, log):
    with log.open("w") as stream:
        env = os.environ.copy()
        native = binary.parent / ("main-lib" if binary.name.startswith("main-") else "candidate-lib")
        env["LD_LIBRARY_PATH"] = str(native.resolve())
        proc = subprocess.Popen([
            str(binary.resolve()), "--regions-config", str(config.resolve()),
            "--catenary-config", str(catenary_config.resolve()),
            "--listen", "127.0.0.1:40428",
        ], stdout=stream, stderr=subprocess.STDOUT, env=env)
        try:
            deadline = clock.monotonic() + 300
            while True:
                if proc.poll() is not None:
                    raise RuntimeError(f"Service exited {proc.returncode}; see {log}")
                try:
                    with urllib.request.urlopen("http://127.0.0.1:40428/health", timeout=1) as r:
                        if r.status == 200:
                            break
                except (urllib.error.URLError, TimeoutError):
                    pass
                if clock.monotonic() >= deadline:
                    raise TimeoutError(f"Service did not become healthy; see {log}")
                clock.sleep(0.25)
            yield
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


def request(payload):
    req = urllib.request.Request("http://127.0.0.1:40428/v1/route",
                                 json.dumps(payload).encode(), {"Content-Type": "application/json"})
    start = clock.perf_counter_ns()
    with urllib.request.urlopen(req, timeout=180) as r:
        result = json.load(r)
    elapsed = (clock.perf_counter_ns() - start) / 1e6
    return result, elapsed


def percentile(values, p):
    return sorted(values)[max(0, math.ceil(len(values) * p) - 1)]


def geometric(values):
    return math.exp(statistics.mean(math.log(v) for v in values))


def confidence(ratios):
    # Paired block bootstrap: resample whole rounds, retaining all query pairs
    # and machine-condition correlation within each block. Conditional on this
    # fixed workload and snapshot; not a confidence interval for all routing.
    rng = random.Random(1701)
    draws = [geometric(rng.choices(ratios, k=len(ratios))) for _ in range(5000)]
    return [percentile(draws, .025), percentile(draws, .975)]


def report(rows, cases, out):
    measured = [r for r in rows if r["phase"] == "measure"]
    index = {(r["round"], r["variant"], r["query"]): r for r in measured}
    rounds = sorted({r["round"] for r in measured})
    aggregates = []
    for group in ("all", "intercity", "local"):
        ids = [c["id"] for c in cases if group == "all" or c["kind"] == group]
        for base, candidate in (("main", "source"), ("main", "hilbert"), ("source", "hilbert")):
            for metric in METRICS:
                ratios = [geometric(index[(r, base, q)][metric] / index[(r, candidate, q)][metric]
                                    for q in ids) for r in rounds]
                aggregates.append(dict(group=group, baseline=base, candidate=candidate,
                                       metric=metric, speedup=geometric(ratios), ci95=confidence(ratios)))
    failures = [r for r in rows if not r["parity"]]
    result = {"aggregates": aggregates, "parity_failures": len(failures), "rounds": len(rounds)}
    (out / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    lines = ["## Swiss Hilbert routing experiment", "",
             f"{len(cases)} fixed queries; {len(rounds)} measured rounds; parity failures: **{len(failures)}**.",
             "Startup excluded. Every process warms every query before measurement. "
             "Nearest-rank p95; warm OS/file cache. All three variants run sequentially on one runner.", "",
             "Speedup > 1 means the candidate is faster. CI: 95% paired round bootstrap, "
             "conditional on this workload. A CI spanning 1 is inconclusive. "
             "Parity failures invalidate performance conclusions.", "",
             "| Workload | Comparison | Metric | Speedup | 95% CI |",
             "| --- | --- | --- | ---: | --- |"]
    for a in aggregates:
        if a["metric"] in ("raptor_ms", "query_total_ms"):
            lines.append(f'| {a["group"]} | {a["baseline"]} → {a["candidate"]} | {a["metric"]} | '
                         f'{a["speedup"]:.3f}× | {a["ci95"][0]:.3f}–{a["ci95"][1]:.3f} |')
    lines += ["", "| Query | Main RAPTOR median/p95 ms | Source median/p95 ms | Hilbert median/p95 ms | Source/Hilbert |",
              "| --- | ---: | ---: | ---: | ---: |"]
    detail = []
    for c in cases:
        medians = {}
        formatted = []
        for v in VARIANTS:
            samples = [index[(r, v, c["id"])]["raptor_ms"] for r in rounds]
            medians[v] = statistics.median(samples)
            formatted.append(f"{medians[v]:.3f}/{percentile(samples, .95):.3f}")
            for metric in METRICS:
                values = [index[(r, v, c["id"])][metric] for r in rounds]
                detail.append(dict(query=c["id"], variant=v, metric=metric,
                                   median=statistics.median(values), p95=percentile(values, .95)))
        lines.append(f'| {c["label"]} | ' + " | ".join(formatted) +
                     f' | {medians["source"] / medians["hilbert"]:.3f}× |')
    (out / "per-query.json").write_text(json.dumps(detail, indent=2) + "\n")
    summary = "\n".join(lines) + "\n"
    (out / "summary.md").write_text(summary)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
            f.write(summary)
    print(summary)
    if failures:
        raise RuntimeError(f"{len(failures)} parity failures; inspect responses and samples.jsonl")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--monday", type=date.fromisoformat, required=True)
    p.add_argument("--rounds", type=int, default=7)
    p.add_argument("--catalog", type=Path, default=Path("benchmarks/swiss_routing_queries.json"))
    p.add_argument("--output", type=Path, default=Path("benchmark-results"))
    p.add_argument("--bin-dir", type=Path, default=Path("benchmark-bin"))
    p.add_argument("--catenary-config", type=Path, default=Path("catenaryconfig.toml"))
    args = p.parse_args()
    if args.rounds < 3:
        p.error("at least three measured rounds are required")
    out = args.output
    out.mkdir(parents=True, exist_ok=True)
    cases = fixtures(json.loads(args.catalog.read_text()), args.monday)
    (out / "requests.json").write_text(json.dumps(cases, indent=2, ensure_ascii=False) + "\n")
    rows, references = [], {}
    with (out / "samples.jsonl").open("w") as raw:
        for round_no in range(args.rounds):
            # Balanced six-block cycle, including both orders of each pair.
            orders = ((0, 1, 2), (1, 2, 0), (2, 0, 1), (2, 1, 0), (0, 2, 1), (1, 0, 2))
            order = [VARIANTS[i] for i in orders[round_no % 6]]
            queries = list(cases)
            random.Random(42 + round_no).shuffle(queries)
            for variant in order:
                dest = out / variant / f"round-{round_no}"
                dest.mkdir(parents=True, exist_ok=True)
                binary = args.bin_dir / ("main-edelweiss" if variant == "main" else "candidate-edelweiss")
                with service(binary, out / variant / "regions.toml", args.catenary_config, dest / "server.log"):
                    for phase in ("warmup", "measure"):
                        for case in queries:
                            response, http_ms = request(case["request"])
                            # Persist even invalid responses before validation raises.
                            (dest / f'{phase}-{case["id"]}.json').write_text(json.dumps(response) + "\n")
                            timings = validate(response)
                            key = case["id"]
                            value = canonical(response)
                            if key not in references:
                                references[key] = value
                            row = dict(round=round_no, variant=variant, query=key, kind=case["kind"],
                                       phase=phase, parity=value == references[key], http_ms=http_ms,
                                       journeys=len(response["itineraries"]), **{k: timings[k] for k in METRICS[:-1]})
                            rows.append(row)
                            raw.write(json.dumps(row) + "\n")
                            raw.flush()
                            print(f'{round_no} {variant} {phase} {key}: {timings["raptor_ms"]:.2f} ms parity={row["parity"]}', flush=True)
    with (out / "samples.csv").open("w") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    report(rows, cases, out)


if __name__ == "__main__":
    main()
