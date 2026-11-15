# Assignment 3 Distributed Key-Value Store

Assignment 3 keeps the public KV API from Assignment 2. A single HTTP router
splits keys across one to three SQLite storage nodes using a consistent hash
ring. There is one copy with one node and two copies on distinct nodes with
two or three nodes. Each node has an independent Docker volume.

## Run with Docker Compose

Run the commands in `coding-assignment-3/`. Use one Compose project for all
steps in this section.

```sh
INITIAL_NODES=a docker compose -p kv3 up -d --build router node-a
docker compose -p kv3 logs -f router node-a
```

The initial node list applies to a fresh router volume. After enrollment the
router uses the saved membership on restart. To start with all three nodes on
fresh volumes, run `docker compose -p kv3 up -d --build` with the default
`INITIAL_NODES=a,b,c`. For a second deployment, choose another project name and
set `ROUTER_PORT` to an unused port.

The router publishes port 8080 on localhost. Storage nodes communicate over the
Compose network. The administrative endpoints are intended for this local
coursework setup. Run one router worker for a cluster.

```sh
curl -fsS http://127.0.0.1:8080/_admin/status
curl -fsS -X PUT http://127.0.0.1:8080/alice \
  -H 'Content-Type: application/json' -d '{"value":"hello"}'
curl -fsS http://127.0.0.1:8080/alice
```

The router logs startup, membership changes, read failovers, pending writes and
their recovery, and maintenance milestones. It also prints cumulative HTTP
status counts, the active membership, and pending writes every five seconds,
including idle periods. Set `PROGRESS_INTERVAL` to change the interval in
seconds. Use `docker compose -p kv3 logs -f router` to follow these updates.

## Change the cluster size

Start the new process before requesting its enrollment. The router holds
normal KV traffic while the maintenance operation scans, installs, and
verifies new placement. The active ring changes only after this succeeds.

```sh
docker compose -p kv3 up -d node-b
curl -fsS -X PUT http://127.0.0.1:8080/_admin/membership \
  -H 'Content-Type: application/json' -d '{"nodes":["a","b"]}'

docker compose -p kv3 up -d node-c
curl -fsS -X PUT http://127.0.0.1:8080/_admin/membership \
  -H 'Content-Type: application/json' -d '{"nodes":["a","b","c"]}'
curl -fsS http://127.0.0.1:8080/alice
```

Wait for a new node to log that it is ready before enrolling it. To remove a
node, submit the shorter list while all current nodes are online, then stop
its container. The current implementation retains old copies on disk after
moving keys. The router reads data only from the active owners.

### Use custom node names

The supplied Compose file defines A, B, and C. The router accepts up to three
configured node IDs and resolves them through `NODE_URLS`. For a local
deployment with custom IDs, start distinct storage processes and give each
its own SQLite file. For example, run the following in separate terminals:

```sh
NODE_NAME=d NODE_DB=data/d.sqlite3 python -m uvicorn storage.node:app --port 8081
NODE_NAME=e NODE_DB=data/e.sqlite3 python -m uvicorn storage.node:app --port 8082
NODE_NAME=f NODE_DB=data/f.sqlite3 python -m uvicorn storage.node:app --port 8083
```

Then start a fresh router using that deployment:

```sh
NODE_URLS='{"d":"http://127.0.0.1:8081","e":"http://127.0.0.1:8082","f":"http://127.0.0.1:8083"}' \
INITIAL_NODES=d ROUTER_DB=data/custom-router.sqlite3 \
  python -m uvicorn routing.app:app --port 8080
curl -fsS -X PUT http://127.0.0.1:8080/_admin/membership \
  -H 'Content-Type: application/json' -d '{"nodes":["d","e","f"]}'
```

On a fresh router, omitting `INITIAL_NODES` enrolls the entire configured list.
Membership changes can select any configured ID while the router is running.
To change the address map itself, update `NODE_URLS` and restart the router.
Keep the IDs and volumes for existing members when changing their addresses.

## Failover and replacement

- A read tries the primary first and uses the replica when the primary is
  unavailable. An authoritative missing key returns HTTP 404.
- A write or delete is acknowledged only after every assigned copy confirms
  it. If a copy is already unavailable at preflight time, the write returns
  HTTP 503 and the prior value remains readable from a healthy copy.
- If a failure interrupts a journaled write, it returns HTTP 503 and may
  complete in the background. Reads and new writes for that key return 503
  while the write is pending. Retries reuse the original revision.
- Restarting a node with its original volume brings back its existing
  identity and data. A blank replacement must be enrolled manually. To repair
  it, bring the other nodes online, start the replacement at the same service
  address, and call `POST /_admin/repair/{node}`. The router reconstructs and
  verifies its records before trusting the new identity.
- Deletes are stored as revisioned tombstones so a repair or migration
  preserves the deletion. Tombstones are retained.

For a three-node failover demo, `alice` is assigned to nodes B and C in that
order. After writing it, stop B and read it again. The router logs a read from
C. Restart B to demonstrate recovery with its original volume.

```sh
docker compose -p kv3 stop node-b
curl -fsS http://127.0.0.1:8080/alice
docker compose -p kv3 up -d node-b
```

To demonstrate a *lost* volume on a disposable three-node deployment, export a
backup first, then remove only the stopped B container and its volume. This
deletes that volume permanently.

```sh
docker compose -p kv3 stop node-b
docker compose -p kv3 rm -f node-b
docker volume rm kv3_node-b
docker compose -p kv3 up -d node-b
# Wait for node-b to log that it is ready.
curl -fsS -X POST http://127.0.0.1:8080/_admin/repair/b
```

## Backup and restore

An export requires the active nodes to be available and the write journal to
be settled. The JSON contains one copy of each logical record, including
tombstones and the revision clock. Keep a copy outside the cluster's volumes.

```sh
mkdir -p data
curl -fsS http://127.0.0.1:8080/_admin/backup -o data/kv3-backup.json

ROUTER_PORT=8090 INITIAL_NODES=a,b docker compose -p kv3-restore \
  up -d --build router node-a node-b
curl -fsS http://127.0.0.1:8090/_admin/status
curl -fsS -X POST http://127.0.0.1:8090/_admin/restore \
  -H 'Content-Type: application/json' --data-binary @data/kv3-backup.json
curl -fsS http://127.0.0.1:8090/alice
```

Use a fresh router and fresh node volumes for restore. If it is interrupted,
retry with the same snapshot. The router keeps normal KV traffic closed until
the restored placement has been verified. The snapshot can be restored to a
different number of nodes from the original cluster.

## Run the benchmark

With Docker running, run these commands from `coding-assignment-3/`:

```sh
python -m pip install -r requirements-lock.txt
python -m benchmark
```

The benchmark builds the router and node image once. It runs separate Docker
Compose projects with fresh volumes for one, two, and three nodes, and cleans
up each test project after its turn. It uses port 18080 by default. Set
`--port` if that port is in use. The default workload is 1,000 keys, eight
workers, and three trials per configuration. For a quick deployment check,
use `python -m benchmark --keys 30 --workers 3 --trials 1`.

It seeds and verifies the same dataset in each configuration, then measures
PUT updates and GET reads separately. After a measured phase, the runner
prints the throughput and error count. It writes each run to a new folder
under `results/`:

- `report.md` has the summary and explains the workload and replication.
- `trials.csv` has the raw results, including request failures.
- `charts.html` plots throughput and p99 latency and opens without a server.

The report summarizes the median metrics from trials with no request errors.
Use `--output` for a specific new folder. The output can be committed with
the source for team review. One node writes one copy, and two or three nodes
write two copies. A comparison from one to two nodes therefore includes
the cost of adding replication as well as the change in node count.

## Code layout and verification

| Directory | Responsibility |
| --- | --- |
| `routing/` | Public API, router startup, consistent hashing, failover, internal HTTP client |
| `management/` | Administrative routes, membership changes, replica repair, backup and restore |
| `storage/` | Node HTTP service, shared record format, SQLite storage and the router journal |
| `benchmark/` | Compose sweep, timed workloads, summary report and charts |
| `tests/` | Routing, API, persistence and benchmark-output tests |
| `results/` | [Functional verification summary](results/verification.md) and benchmark runs |

Run the tests with `python -m unittest discover -s tests -v` after installing
`requirements-lock.txt`. They exercise actual HTTP handlers and SQLite files
with an in-process transport that can simulate node outages.

The router is a single process. Container replicas on one host share that
host's failure domain. A copied backup can restore the data after host loss.
