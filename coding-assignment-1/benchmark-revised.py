"""Concurrent correctness-aware benchmark. Each worker owns its keys.

Each iteration performs PUT -> GET -> DELETE -> GET (expected 404).
All four responses are checked; expected 404 responses count as successes.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import math
import statistics
import threading
import time
import uuid

import requests


def positive_int(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return number


def worker(base_url, worker_id, iterations, run_id, barrier):
    latencies = []
    failures = []
    with requests.Session() as session:
        barrier.wait()
        for i in range(iterations):
            key = f"bench_{run_id}_{worker_id}_{i}"
            value = f"value_{worker_id}_{i}"
            url = f"{base_url}/{key}"
            operations = [
                ("PUT", {"json": {"value": value}}, 201, {"key": key, "value": value}),
                ("GET", {}, 200, {"key": key, "value": value}),
                ("DELETE", {}, 200, {"key": key, "deleted": True}),
                ("GET", {}, 404, {"error": f"Key '{key}' not found"}),
            ]
            for method, kwargs, expected_status, expected_body in operations:
                start = time.perf_counter()
                try:
                    response = session.request(method, url, timeout=10, **kwargs)
                    elapsed = time.perf_counter() - start
                    if (
                        response.status_code != expected_status
                        or response.json() != expected_body
                    ):
                        raise ValueError(
                            f"expected {expected_status} {expected_body}, got "
                            f"{response.status_code} {response.text[:200]}"
                        )
                    latencies.append(elapsed)
                except (requests.RequestException, ValueError) as exc:
                    failures.append(f"{method} {key}: {exc}")
    return latencies, failures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--threads", type=positive_int, default=3)
    parser.add_argument(
        "--iterations",
        type=positive_int,
        default=100,
        help="cycles per thread; each cycle sends four requests",
    )
    args = parser.parse_args()
    barrier = threading.Barrier(args.threads + 1)
    run_id = uuid.uuid4().hex
    with ThreadPoolExecutor(max_workers=args.threads) as pool:
        futures = [
            pool.submit(
                worker, args.url.rstrip("/"), i, args.iterations, run_id, barrier
            )
            for i in range(args.threads)
        ]
        started = time.perf_counter()
        barrier.wait()
        results = [future.result() for future in futures]
        elapsed = time.perf_counter() - started
    latencies = [latency for values, _ in results for latency in values]
    failures = [failure for _, errors in results for failure in errors]
    attempted = args.threads * args.iterations * 4
    print("\nFinal Results:")
    print(f"Total operations: {attempted}")
    print(f"Successful operations: {len(latencies)}")
    print(f"Failed operations: {len(failures)}")
    print(f"Total time: {elapsed:.3f} seconds")
    print(
        f"Throughput (successful): {len(latencies) / elapsed:.2f} operations per second"
    )
    if latencies:
        ordered = sorted(latencies)
        print(
            f"Average Latency (successful): {statistics.mean(latencies) * 1000:.3f} ms"
        )
        print(
            f"p95 Latency (successful): {ordered[math.ceil(len(ordered) * .95) - 1] * 1000:.3f} ms"
        )
    for failure in failures[:10]:
        print(f"ERROR: {failure}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
