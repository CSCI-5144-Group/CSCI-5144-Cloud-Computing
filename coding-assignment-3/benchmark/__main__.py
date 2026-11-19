"""Run a repeatable one-, two-, and three-node Compose benchmark."""

import argparse
import math
import os
import platform
import subprocess
import sys
from copy import copy
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic, sleep
from uuid import uuid4

import requests

from benchmark.report import write_report
from benchmark.workload import measure

ROOT = Path(__file__).resolve().parents[1]


# Wait for the expected membership before generating traffic.
def wait_for_router(base_url, nodes):
    deadline = monotonic() + 45
    status = ""
    with requests.Session() as session:
        session.trust_env = False
        while monotonic() < deadline:
            try:
                response = session.get(f"{base_url}/_admin/status", timeout=1)
                response.raise_for_status()
                ready = response.json()
                if (isinstance(ready, dict) and ready.get("nodes") == nodes
                        and ready.get("replication_factor") == min(2, len(nodes))
                        and ready.get("pending_writes") == 0):
                    return
                status = f"Unexpected router status: {ready}"
            except (requests.RequestException, ValueError) as exc:
                status = str(exc)
            sleep(0.5)
    raise RuntimeError(f"Router did not become ready for {nodes}: {status}")


# Wait for the unchanged Assignment 2 server to expose its HTTP API.
def wait_for_assignment2(base_url):
    deadline = monotonic() + 45
    with requests.Session() as session:
        session.trust_env = False
        while monotonic() < deadline:
            try:
                response = session.get(f"{base_url}/openapi.json", timeout=1)
                if response.status_code == 200 and "paths" in response.json():
                    return
            except (requests.RequestException, ValueError):
                pass
            sleep(0.5)
    raise RuntimeError("Assignment 2 server did not become ready")


# Use a disposable Compose project and persist results outside its containers.
@contextmanager
def cluster(nodes, run_id, port, *, system="assignment3"):
    project = f"kvbench-{run_id}-{system}-{len(nodes)}"
    env = dict(os.environ, INITIAL_NODES=",".join(nodes), ROUTER_PORT=str(port))
    compose = "benchmark/compose.assignment2.yaml" if system == "assignment2" else "compose.yaml"
    cmd = ["docker", "compose", "-f", compose]
    if system == "assignment3":
        cmd.extend(["-f", "benchmark/compose.quiet.yaml"])
    cmd.extend(["-p", project])
    services = ["router"] if system == "assignment2" else ["router", *(f"node-{name}" for name in nodes)]
    base_url = f"http://127.0.0.1:{port}"
    completed = False
    try:
        print(f"Starting {system}, {len(nodes)} node(s) in {project}...", flush=True)
        subprocess.run(
            [*cmd, "up", "-d", "--no-build", *services],
            check=True, cwd=ROOT, env=env,
        )
        if system == "assignment2":
            wait_for_assignment2(base_url)
        else:
            wait_for_router(base_url, nodes)
        yield base_url
        completed = True
    except (RuntimeError, subprocess.CalledProcessError):
        subprocess.run([*cmd, "logs", "--tail", "25"], check=False, cwd=ROOT, env=env)
        raise
    finally:
        cleanup = subprocess.run([*cmd, "down"], check=completed, cwd=ROOT, env=env)
        if cleanup.returncode:
            print(f"Compose cleanup failed for {project}", file=sys.stderr)


# Seed and verify data, then repeat the same update/read phases at each size.
def run_workload(base_url, count, args, rows, failures, *, system="assignment3"):
    config = {"nodes": count, "replication_factor": min(2, count), "workers": args.workers}
    keys = [f"bench-{index:08d}" for index in range(args.keys)]
    seed = "0" * args.value_bytes
    for method, code in (("PUT", 201), ("GET", 200)):
        print(f"  Seeding / verifying with {method}...", flush=True)
        result = measure(base_url, method, keys, seed, trial=0, expected_status=code, **config)
        if result.errors:
            raise RuntimeError(f"{count} nodes: {method} preparation failed: {result.first_error}")

    duration = getattr(args, "duration", 0)
    warmup = getattr(args, "warmup", 0)
    for trial in range(1, args.trials + 1):
        value = ("A" if trial % 2 else "B") * args.value_bytes
        for method in ("PUT", "GET"):
            if warmup > 0:
                print(f"  Warming up {method} for {warmup:g}s...", flush=True)
                prepared = measure(base_url, method, keys, value, trial=0,
                                   duration=warmup, **config)
                if prepared.errors:
                    raise RuntimeError(f"{system} warm-up failed: {prepared.first_error}")
            length = f"{duration:g}s" if duration else f"{len(keys)} requests"
            print(f"  Trial {trial}/{args.trials}: {method}, {args.workers} clients ({length})",
                  flush=True)
            row = measure(base_url, method, keys, value, trial=trial, duration=duration, **config)
            row.system = system
            rows.append(row)
            print(f"    {row.throughput_ops_s:,.0f} successful ops/s, {row.attempts:,} attempts, "
                  f"{row.elapsed_seconds:.2f}s, {row.errors} errors",
                  flush=True)
            if row.errors:
                failures.append(f"{system}, {count} nodes, trial {trial}, {method}: "
                                f"{row.errors} errors. First error: {row.first_error}")


# Validate workload sizes and always retain measurements already collected.
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--include-assignment2", action="store_true",
                        help="Include Assignment 2 using the same workload and quiet logging")
    parser.add_argument("--keys", type=int, default=1000)
    parser.add_argument("--workers", type=int, nargs="+", default=[8, 16, 32, 64],
                        help="Concurrent client counts to sweep")
    parser.add_argument("--duration", type=float, default=10,
                        help="Measured seconds per phase, excluding warm-up")
    parser.add_argument("--warmup", type=float, default=2,
                        help="Warm-up seconds before each measured phase")
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--value-bytes", type=int, default=128)
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--output", type=Path, help="New directory for this run")
    args = parser.parse_args()
    if any(not 1 <= workers <= args.keys for workers in args.workers) or args.trials < 1 or args.value_bytes < 1:
        parser.error("Require 1 <= workers <= keys, positive trials, and positive value-bytes")
    if (not math.isfinite(args.duration) or args.duration <= 0
            or not math.isfinite(args.warmup) or args.warmup < 0):
        parser.error("Require a positive finite duration and a finite nonnegative warm-up")
    args.workers = list(dict.fromkeys(args.workers))
    if not 1 <= args.port <= 65535:
        parser.error("Port must be between 1 and 65535")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%SZ")
    run_id = uuid4().hex[:8]
    output = args.output or ROOT / "results" / f"{timestamp}-{run_id}"
    if output.exists():
        parser.error(f"Output already exists: {output}")

    rows, failures = [], []
    try:
        subprocess.run(["docker", "compose", "-f", "compose.yaml", "build", "router"],
                       check=True, cwd=ROOT)
        if args.include_assignment2:
            subprocess.run(
                ["docker", "compose", "-f", "benchmark/compose.assignment2.yaml", "build", "router"],
                check=True, cwd=ROOT,
            )
        configurations = [("assignment3", count) for count in (1, 2, 3)]
        if args.include_assignment2:
            configurations.insert(0, ("assignment2", 1))
        for workers in args.workers:
            workload = copy(args)
            workload.workers = workers
            print(f"Concurrency sweep: {workers} clients", flush=True)
            for system, count in configurations:
                with cluster(list("abc"[:count]), run_id, args.port, system=system) as base_url:
                    run_workload(base_url, count, workload, rows, failures, system=system)
    except (OSError, RuntimeError, subprocess.CalledProcessError, KeyboardInterrupt) as exc:
        failures.append(str(exc) or type(exc).__name__)
        print(f"Benchmark incomplete: {failures[-1]}", file=sys.stderr)
    finally:
        if rows:
            settings = dict(vars(args), host=platform.platform())
            write_report(output, rows, settings, failures)
            print(f"Benchmark results: {output.resolve()}", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
