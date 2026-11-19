"""Measure KV operations from the client's point of view."""

import math
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from statistics import mean
from threading import Barrier
from time import perf_counter

import requests


@dataclass
class Measurement:
    nodes: int
    replication_factor: int
    trial: int
    method: str
    attempts: int
    successful: int
    errors: int
    elapsed_seconds: float
    throughput_ops_s: float
    mean_ms: float | None
    p99_ms: float | None
    first_error: str
    system: str = "assignment3"
    workers: int = 1


# Verify HTTP status and the returned record for each measured operation.
def request_key(session, base_url, method, key, value, expected_status):
    url = f"{base_url}/{key}"
    try:
        if method == "PUT":
            response = session.put(url, json={"value": value}, timeout=5)
        else:
            response = session.get(url, timeout=5)
        if response.status_code != expected_status:
            return f"{method} {key}: expected HTTP {expected_status}, received {response.status_code}"
        if response.json() != {"key": key, "value": value}:
            return f"{method} {key}: returned key or value did not match"
    except (requests.RequestException, ValueError) as exc:
        return f"{method} {key}: {exc}"
    return ""


# Reuse one HTTP session per worker and keep individual latency samples.
def run_worker(base_url, method, keys, value, expected_status, gate, starts, duration):
    with requests.Session() as session:
        session.trust_env = False
        latencies = []
        errors = 0
        first_error = ""
        gate.wait()
        index = 0
        while index < len(keys) or duration > 0:
            if duration > 0 and perf_counter() >= starts[0] + duration:
                break
            key = keys[index % len(keys)]
            index += 1
            started = perf_counter()
            error = request_key(session, base_url, method, key, value, expected_status)
            latency = (perf_counter() - started) * 1000
            if error:
                errors += 1
                first_error = first_error or error
            else:
                latencies.append(latency)
        return perf_counter(), latencies, errors, first_error


# Start workers together and exclude thread creation from elapsed time.
def measure(base_url, method, keys, value, workers, *, nodes, replication_factor, trial,
            expected_status=200, duration=0):
    if not 1 <= workers <= len(keys) or not math.isfinite(duration) or duration < 0:
        raise ValueError("Require nonempty worker partitions and a finite nonnegative duration")
    starts = []
    gate = Barrier(workers, action=lambda: starts.append(perf_counter()))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(run_worker, base_url, method, keys[index::workers],
                            value, expected_status, gate, starts, duration)
            for index in range(workers)
        ]
        results = [future.result() for future in futures]
    elapsed = max(end for end, _, _, _ in results) - starts[0]
    samples = sorted(latency for _, latencies, _, _ in results for latency in latencies)
    return Measurement(
        nodes=nodes, replication_factor=replication_factor, trial=trial, method=method,
        attempts=len(samples) + sum(row[2] for row in results), successful=len(samples), errors=sum(row[2] for row in results),
        elapsed_seconds=elapsed, throughput_ops_s=len(samples) / elapsed,
        mean_ms=mean(samples) if samples else None,
        p99_ms=samples[math.ceil(0.99 * len(samples)) - 1] if samples else None,
        first_error=next((row[3] for row in results if row[3]), ""), workers=workers,
    )
