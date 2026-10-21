#!/usr/bin/env python3
"""Run one Edelweiss CLI fixture and publish timing details to GitHub Actions.

The CLI's own monotonic timers measure query phases excluding startup.
The subprocess timer additionally measures cold binary startup, mmap setup,
the complete query and JSON output. Both are reported: neither is called
the other, and cargo build time is excluded from the measurements.
"""
import argparse
import json
import math
import os
from pathlib import Path
import subprocess
import sys
from time import perf_counter_ns


TIMING_FIELDS = (
    "selection_ms",
    "access_ms",
    "egress_ms",
    "direct_walk_ms",
    "osr_parallel_wall_ms",
    "raptor_ms",
    "metadata_ms",
    "query_total_ms",
)


def run_query(args: argparse.Namespace) -> dict:
    command = [
        str(args.binary),
        "--regions-config", str(args.regions_config),
        "--catenary-config", str(args.catenary_config),
        "--query", str(args.query),
        "--query-timings", str(args.timings),
        "--database-url", args.database_url,
    ]
    start = perf_counter_ns()
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    command_wall_ms = (perf_counter_ns() - start) / 1_000_000
    args.stderr.write_text(completed.stderr, encoding="utf-8")
    if completed.stderr:
        sys.stderr.write(completed.stderr)
    if completed.returncode != 0:
        raise RuntimeError(
            f"{args.label}: Edelweiss exited {completed.returncode} "
            f"({command_wall_ms:.3f} ms); see {args.stderr}"
        )

    response = json.loads(completed.stdout)
    timings = json.loads(args.timings.read_text(encoding="utf-8"))
    for key in TIMING_FIELDS:
        value = timings.get(key)
        if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            raise ValueError(f"{args.label}: missing or invalid query timing {key}")
    if timings["query_total_ms"] + 1 < timings["raptor_ms"]:
        raise ValueError(f"{args.label}: RAPTOR phase exceeds measured query time")

    journeys = response["routing"]["journeys"]
    region = response["routing"]["selection"]["transit_region"]
    timings.update(
        command_wall_ms=command_wall_ms,
        journey_count=len(journeys),
        transit_region=region,
        fixture=args.label,
    )
    args.result.write_text(json.dumps(response, indent=2) + "\n", encoding="utf-8")
    args.timings.write_text(json.dumps(timings, indent=2) + "\n", encoding="utf-8")

    print(f"{args.label}: {len(journeys)} Pareto journeys in transit region {region}")
    print(
        f"  RAPTOR {timings['raptor_ms']:.3f} ms; "
        f"total in-engine {timings['query_total_ms']:.3f} ms; "
        f"cold CLI wall {command_wall_ms:.3f} ms"
    )
    print(
        f"  OSR parallel wall {timings['osr_parallel_wall_ms']:.3f} ms; "
        f"access {timings['access_ms']:.3f} ms; "
        f"egress {timings['egress_ms']:.3f} ms; "
        f"direct {timings['direct_walk_ms']:.3f} ms; "
        f"metadata {timings['metadata_ms']:.3f} ms"
    )

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as summary:
            summary.write(
                f"| {args.label} | {len(journeys)} | "
                f"{timings['raptor_ms']:.3f} | "
                f"{timings['access_ms']:.3f} | "
                f"{timings['egress_ms']:.3f} | "
                f"{timings['direct_walk_ms']:.3f} | "
                f"{timings['osr_parallel_wall_ms']:.3f} | "
                f"{timings['metadata_ms']:.3f} | "
                f"{timings['query_total_ms']:.3f} | "
                f"{command_wall_ms:.3f} |\n"
            )
    return timings


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--label", required=True)
    p.add_argument("--binary", type=Path, required=True)
    p.add_argument("--regions-config", type=Path, required=True)
    p.add_argument("--catenary-config", type=Path, required=True)
    p.add_argument("--database-url", required=True)
    p.add_argument("--query", type=Path, required=True)
    p.add_argument("--result", type=Path, required=True)
    p.add_argument("--timings", type=Path, required=True)
    p.add_argument("--stderr", type=Path, required=True)
    args = p.parse_args()
    for path in (args.result, args.timings, args.stderr):
        path.parent.mkdir(parents=True, exist_ok=True)
    run_query(args)


if __name__ == "__main__":
    main()
