#!/usr/bin/env python3
"""Check the OSR TCP worker accepts simultaneous independent routing calls.

This checks response validity and absence of failures, not a timing threshold:
CI host scheduling and the OSM graph size are variable.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import socket
from threading import Barrier


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=40501)
    parser.add_argument("--region", required=True)
    parser.add_argument("--workers", type=int, default=3)
    args = parser.parse_args()
    assert 2 <= args.workers <= 16
    start = Barrier(args.workers)

    def query(number: int) -> dict:
        payload = {
            "region": args.region,
            "latitude": 33.6461,
            "longitude": -117.8427,
            "max_seconds": 900,
            "stops": [{
                "stop": number,
                "latitude": 33.6461,
                "longitude": -117.8427,
            }],
        }
        start.wait(timeout=20)
        with socket.create_connection((args.address, args.port), timeout=60) as stream:
            stream.settimeout(60)
            stream.sendall((json.dumps(payload) + "\n").encode("utf-8"))
            with stream.makefile("r", encoding="utf-8") as reader:
                result = json.loads(reader.readline())
        assert result["error"] is None, result
        assert isinstance(result["reachable"], list), result
        assert all("stop" in row and "seconds" in row
                   for row in result["reachable"]), result
        return result

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        results = list(pool.map(query, range(args.workers)))
    assert len(results) == args.workers
    print(f"OSR concurrent TCP smoke test passed: {len(results)} simultaneous requests")


if __name__ == "__main__":
    main()
