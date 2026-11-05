#!/usr/bin/env python3
"""Compare worker counts using Edelweiss's in-query timers and exact responses.

Example (run on the machine holding the production timetable and OSR worker):
  python3 tools/benchmark-raptor-workers.py --binary target/release/edelweiss \
      --query /tmp/la-query.json --workers 1 2 4 --runs 7 -- \
      --regions-config routing_regions.toml --catenary-config catenaryconfig.toml

Each run starts a fresh process. Reported timings exclude startup; warmups warm
OS/database caches, not persistent per-process state. Worker order rotates to
reduce ordering bias. No changes are made to the query or search budgets.
"""
import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys


def positive(value):
    value = int(value)
    if value < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--query", required=True, type=Path)
    parser.add_argument("--workers", nargs="+", type=positive, default=[1, 2, 4])
    parser.add_argument("--runs", type=positive, default=7)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("edelweiss_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.warmups < 0:
        parser.error("--warmups must be nonnegative")
    workers = sorted(set([1, *args.workers]))
    query = json.loads(args.query.read_text())
    timestamps = [query.get(k) for k in ("departure_unix_seconds", "arrival_unix_seconds")]
    if sum(isinstance(t, int) and t > 0 for t in timestamps) != 1:
        parser.error("query must supply exactly one fixed positive departure/arrival timestamp")
    if query.get("timetable_view", True) is not True:
        parser.error("query must use timetable_view=true to exercise the PONG route workers")
    extra = args.edelweiss_args
    if extra[:1] == ["--"]:
        extra = extra[1:]
    command = [str(args.binary.resolve()), "--query", str(args.query.resolve()), *extra]
    samples = {n: [] for n in workers}
    expected = None
    for run in range(args.warmups + args.runs):
        shift = run % len(workers)
        for count in workers[shift:] + workers[:shift]:
            env = dict(os.environ, EDELWEISS_RAPTOR_WORKERS=str(count))
            process = subprocess.run(command, env=env, capture_output=True, text=True, check=True)
            response = json.loads(process.stdout)
            debug = response["debugOutput"]
            stats = debug["raptor"]
            if stats["profile_budget_exhausted"]:
                raise RuntimeError("search budget exhausted; completed-search equivalence cannot be assessed")
            actual = {k: v for k, v in response.items() if k != "debugOutput"}
            if expected is None:
                expected = actual
            elif actual != expected:
                raise RuntimeError(f"response differs from the one-worker baseline with {count} workers")
            if run >= args.warmups:
                samples[count].append((debug["timings"], stats))
            print(f"completed {'warmup' if run < args.warmups else 'sample'} {run + 1}, workers={count}", file=sys.stderr)
    def median(n, section, key):
        return statistics.median(row[section][key] for row in samples[n])
    baseline = median(1, 0, "raptor_ms")
    print("workers  RAPTOR ms  route scan ms  merge ms  total ms  RAPTOR speedup  parallel rounds  max workers")
    for count in workers:
        raptor = median(count, 0, "raptor_ms")
        print(f"{count:7d}  {raptor:9.3f}  {median(count, 1, 'route_scan_ms'):13.3f}"
              f"  {median(count, 1, 'route_scan_merge_ms'):8.3f}"
              f"  {median(count, 0, 'query_total_ms'):8.3f}  {baseline / raptor:13.2f}x"
              f"  {median(count, 1, 'parallel_route_scan_rounds'):15.0f}"
              f"  {median(count, 1, 'route_scan_workers'):11.0f}")
    print("All completed responses match exactly, excluding debug timings and counters.")


if __name__ == "__main__":
    main()
