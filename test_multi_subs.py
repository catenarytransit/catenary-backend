"""Live Ramonda multi-subscription regression.

Requires a running Ramonda instance and two valid GTFS trip instances.
Tests full reply identity, independent subscriptions, unsubscribe-one, and
unsubscribe-all without assuming that two trips have different GTFS IDs.

Example:
 python test_multi_subs.py --uri ws://127.0.0.1:52772/ws/ --chateau metro \\
   --trip-id-1 100 --date-1 20261119 --trip-id-2 101 --date-2 20261119
"""
import argparse
import asyncio
import json
import websockets


def identity(chateau, trip_id, route_id, service_date, start_time):
    return {
        "chateau": chateau,
        "trip_id": trip_id,
        "route_id": route_id,
        "start_date": service_date,
        "start_time": start_time,
    }


def key(sub):
    return tuple(sub.get(k) for k in
                 ("chateau", "trip_id", "route_id", "start_date", "start_time"))


async def test(uri, a, b, deadline):
    if key(a) == key(b):
        raise ValueError("two distinct service instances are required")
    async with websockets.connect(uri, ping_interval=20) as ws:
        for subscription in (a, b):
            await ws.send(json.dumps({"type": "subscribe_trip", **subscription}))

        seen_initial = set()
        expires = asyncio.get_running_loop().time() + deadline
        while len(seen_initial) < 2:
            remaining = expires - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise AssertionError("timed out waiting for both initial_trip frames")
            message = json.loads(await asyncio.wait_for(ws.recv(), remaining))
            kind = message.get("type")
            if kind in ("initial_trip", "update_trip"):
                sub = message.get("subscription")
                assert isinstance(sub, dict), message
                assert key(sub) in {key(a), key(b)}, message
                if kind == "initial_trip":
                    seen_initial.add(key(sub))
            elif kind == "error":
                raise AssertionError(f"server error: {message!r}")

        print("Received both initial trips with unambiguous service identities")
        await ws.send(json.dumps({"type": "unsubscribe_trip", **a}))
        # Any previously queued frames may still be in the TCP buffer. Drain
        # for a grace period, then probe for additional late deliveries.
        drain_until = asyncio.get_running_loop().time() + 1.0
        while asyncio.get_running_loop().time() < drain_until:
            try:
                await asyncio.wait_for(ws.recv(), 0.1)
            except asyncio.TimeoutError:
                pass
        observe_until = asyncio.get_running_loop().time() + 3.0
        while asyncio.get_running_loop().time() < observe_until:
            try:
                message = json.loads(await asyncio.wait_for(ws.recv(), 0.3))
            except asyncio.TimeoutError:
                continue
            if message.get("type") in ("initial_trip", "update_trip"):
                assert key(message["subscription"]) != key(a), (
                    f"unsubscribed trip continued streaming: {message!r}"
                )
        await ws.send(json.dumps({"type": "unsubscribe_all_trips"}))
        await asyncio.sleep(1.0)
        print("Unsubscribe-one and unsubscribe-all commands completed")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", default="ws://127.0.0.1:52772/ws/")
    parser.add_argument("--chateau", required=True)
    parser.add_argument("--trip-id-1", required=True)
    parser.add_argument("--trip-id-2", required=True)
    parser.add_argument("--route-id-1", default=None)
    parser.add_argument("--route-id-2", default=None)
    parser.add_argument("--date-1", default=None)
    parser.add_argument("--date-2", default=None)
    parser.add_argument("--start-time-1", default=None)
    parser.add_argument("--start-time-2", default=None)
    parser.add_argument("--deadline", type=float, default=30.0)
    args = parser.parse_args()
    first = identity(args.chateau, args.trip_id_1, args.route_id_1, args.date_1,
                     args.start_time_1)
    second = identity(args.chateau, args.trip_id_2, args.route_id_2, args.date_2,
                      args.start_time_2)
    asyncio.run(test(args.uri, first, second, args.deadline))


if __name__ == "__main__":
    main()
