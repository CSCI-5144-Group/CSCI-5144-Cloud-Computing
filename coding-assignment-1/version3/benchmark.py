import math
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import requests

BASE_URL = "http://127.0.0.1:8080"
NUM_THREADS = 3
OPS_PER_THREAD = 100  # Must be even: each pair contains one POST and one GET.
REQUEST_TIMEOUT = 5  # Seconds.


def checked_request(method, key, expected_status, expected_body, value=None):
    """Return elapsed time and an optional error message."""
    kwargs = {}
    if method == "POST":
        kwargs["json"] = {"value": value}

    started = time.perf_counter()
    try:
        response = requests.request(
            method,
            f"{BASE_URL}/{key}",
            timeout=REQUEST_TIMEOUT,
            **kwargs,
        )

        if response.status_code != expected_status:
            raise ValueError(
                f"Expected status {expected_status}, got "
                f"{response.status_code}: {response.text[:200]}"
            )

        if response.json() != expected_body:
            raise ValueError(f"Unexpected response body: {response.text[:200]}")

        error = None
    except (requests.RequestException, ValueError) as exc:
        error = f"{method} {key}: {exc}"

    elapsed = time.perf_counter() - started
    return elapsed, error


def worker_thread(worker_id, run_id, start_event):
    """Each worker writes and reads its own keys in order."""
    latencies = []
    errors = []

    start_event.wait()

    for i in range(OPS_PER_THREAD // 2):
        key = f"bench_{run_id}_{worker_id}_{i}"
        value = f"value_{worker_id}_{i}"
        expected_body = {"key": key, "value": value}

        operations = [
            ("POST", 201),
            ("GET", 200),
        ]

        for method, expected_status in operations:
            elapsed, error = checked_request(
                method,
                key,
                expected_status,
                expected_body,
                value=value,
            )

            if error is None:
                latencies.append(elapsed)
            else:
                errors.append(error)

    return latencies, errors


def main():
    if NUM_THREADS < 1 or OPS_PER_THREAD < 2 or OPS_PER_THREAD % 2:
        raise ValueError(
            "NUM_THREADS must be positive and OPS_PER_THREAD "
            "must be a positive even number."
        )

    run_id = uuid.uuid4().hex

    # Check connectivity and missing-key behavior outside the timed workload.
    missing_key = f"missing_{run_id}"
    _, error = checked_request(
        "GET",
        missing_key,
        404,
        {"error": f"Key '{missing_key}' not found"},
    )

    if error is not None:
        print(f"Preflight check failed: {error}")
        print("Check that the server is running at BASE_URL.")
        return 1

    print("Missing-key check: PASS (expected 404; excluded from timing)")

    start_event = threading.Event()

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [
            executor.submit(worker_thread, i, run_id, start_event)
            for i in range(NUM_THREADS)
        ]

        started = time.perf_counter()
        start_event.set()

        results = [future.result() for future in futures]
        total_time = time.perf_counter() - started

    latencies = [
        latency for worker_latencies, _ in results for latency in worker_latencies
    ]
    errors = [error for _, worker_errors in results for error in worker_errors]

    successful = len(latencies)
    failed = len(errors)
    attempted = successful + failed

    print("\nFinal Results:")
    print(f"Attempted operations: {attempted}")
    print(f"Successful operations: {successful}")
    print(f"Failed operations: {failed}")
    print(f"Total time: {total_time:.5f} seconds")
    print(
        f"Successful throughput: "
        f"{successful / total_time:.2f} operations per second"
    )

    if latencies:
        ordered = sorted(latencies)
        average = sum(latencies) / successful
        p95 = ordered[math.ceil(successful * 0.95) - 1]

        print(f"Average latency (successful): {average * 1000:.3f} ms")
        print(f"p95 latency (successful): {p95 * 1000:.3f} ms")
    else:
        print("Average latency (successful): N/A")
        print("p95 latency (successful): N/A")

    for error in errors[:10]:
        print(f"ERROR: {error}")

    if failed > 10:
        print(f"... {failed - 10} additional errors omitted.")

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
