"""Run a repeatable one-, two-, and three-node Compose benchmark."""

import argparse
import os
import platform
import subprocess
import sys
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


# Use a disposable Compose project and persist results outside its containers.
@contextmanager
def cluster(nodes, run_id, port):
    project = f"kvbench-{run_id}-{len(nodes)}"
    env = dict(os.environ, INITIAL_NODES=",".join(nodes), ROUTER_PORT=str(port))
    cmd = ["docker", "compose", "-f", "compose.yaml", "-p", project]
    base_url = f"http://127.0.0.1:{port}"
    completed = False
    try:
        print(f"Starting {len(nodes)} node(s) in {project}...", flush=True)
        subprocess.run(
            [*cmd, "up", "-d", "--no-build", "router", *(f"node-{name}" for name in nodes)],
            check=True, cwd=ROOT, env=env,
        )
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
def run_workload(base_url, count, args, rows, failures):
    config = {"nodes": count, "replication_factor": min(2, count), "workers": args.workers}
    keys = [f"bench-{index:08d}" for index in range(args.keys)]
    seed = "0" * args.value_bytes
    for method, code in (("PUT", 201), ("GET", 200)):
        print(f"  Seeding / verifying with {method}...", flush=True)
        result = measure(base_url, method, keys, seed, trial=0, expected_status=code, **config)
        if result.errors:
            raise RuntimeError(f"{count} nodes: {method} preparation failed: {result.first_error}")

    for trial in range(1, args.trials + 1):
        value = ("A" if trial % 2 else "B") * args.value_bytes
        for method in ("PUT", "GET"):
            print(f"  Trial {trial}/{args.trials}: {method} ({len(keys)} requests)", flush=True)
            row = measure(base_url, method, keys, value, trial=trial, **config)
            rows.append(row)
            print(f"    {row.throughput_ops_s:,.0f} successful ops/s, {row.errors} errors",
                  flush=True)
            if row.errors:
                failures.append(f"{count} nodes, trial {trial}, {method}: "
                                f"{row.errors} errors. First error: {row.first_error}")


# Validate workload sizes and always retain measurements already collected.
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--keys", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--value-bytes", type=int, default=128)
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--output", type=Path, help="New directory for this run")
    args = parser.parse_args()
    if not 1 <= args.workers <= args.keys or args.trials < 1 or args.value_bytes < 1:
        parser.error("Require 1 <= workers <= keys, positive trials, and positive value-bytes")
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
        for count in (1, 2, 3):
            with cluster(list("abc"[:count]), run_id, args.port) as base_url:
                run_workload(base_url, count, args, rows, failures)
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
