# Version 1: local setup and execution

## Scope and design

This baseline uses FastAPI, Uvicorn, an in-memory dictionary, and one global
threading lock. Synchronous routes run in FastAPI's thread pool. The lock makes
store operations atomic within one process. Run exactly one server process.

GET, PUT, and DELETE operations are logged to the terminal with timestamps.
POST is an alias for PUT to support the instructor's benchmark. There is no WAL,
snapshot, background worker, or persistence. Restarting the server clears all data;
existing disk data from earlier versions is not loaded or deleted.

## Install dependencies

Use Python 3.11 (the baseline environment is Python 3.11.9).
From the repository root, on macOS or Linux:

```sh
cd coding-assignment-1
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-lock.txt
```

If `.venv` already exists, reuse it. `requirements.txt` lists direct dependencies;
`requirements-lock.txt` pins direct and transitive dependencies for reproduction.
A virtual environment is recommended but optional. The interpreter running the
server needs FastAPI, Uvicorn, and Pydantic; the benchmark interpreter needs requests.

## Terminal A: start the server

From `coding-assignment-1`:

```sh
source .venv/bin/activate
python server.py
```

Wait for Uvicorn's `Application startup complete` and listening message.
Keep this terminal open. Stop the server with Ctrl+C after testing.
The default address is `http://127.0.0.1:8080`. API documentation is at `/docs`.
Optional arguments: `--host` and `--port`.

## Terminal B: run the instructor's benchmark

From `coding-assignment-1` in a second terminal:

```sh
source .venv/bin/activate
python benchmark.py
```

Running a Python client script still sends HTTP requests. The benchmark does not
start the server automatically. `Connection refused` indicates no service accepted
the connection at the target address; it is not a missing-key response.

The instructor's script is unchanged. With an initially empty store and a completed run:

- It queues 300 requests: 150 POSTs for even-numbered keys and 150 GETs for odd-numbered keys.
- POSTs should succeed; GETs should return 404 because their keys were never written.
- Its `raise_for_status()` prints those expected 404 responses as errors.
- It prints `Total operations: 600`, although only 300 requests were queued.
- Its reported throughput uses 600 divided by elapsed time, not successful operations.
- Average latency includes only successful requests still in the latency queue;
  the monitoring thread consumes samples every three seconds.
- Its queue-empty check can race with queue removal, and requests have no timeout.

Preserve the raw output and explain these limitations in the report. A completed
run with connection failures is not a valid server performance measurement. If every
request fails, the latency sample list is empty and the script prints `nan`.
The optional `benchmark-revised.py` uses a different workload; report its results separately.

## Manual correctness checks

```sh
curl -i -X PUT http://127.0.0.1:8080/name -H 'Content-Type: application/json' -d '{"value":"Kevin"}'
curl -i http://127.0.0.1:8080/name
curl -i -X DELETE http://127.0.0.1:8080/name
curl -i http://127.0.0.1:8080/name
```

Expect 201, 200, 200, and 404. Updating an existing key returns 200.
The write body requires `value`, which may be any JSON value, including null.
Malformed bodies, missing values, and non-finite numbers return 400.
Keys occupy a single URL path segment. Authentication, size limits, durable
storage, finer locking, and multi-node operation are future work.
