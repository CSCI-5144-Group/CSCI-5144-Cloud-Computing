# Assignment 3 Distributed Key-Value Store

Assignment 3 keeps the public KV API from Assignment 2. A single HTTP router
splits keys across one to three in-memory storage nodes using a consistent hash
ring. There is one copy with one node and two copies on distinct nodes with
two or three nodes. Each node owns an independent in-memory dictionary.

## Run with Docker Compose

Run the commands in `coding-assignment-3/`. Use one Compose project for all
steps in this section.

```sh
INITIAL_NODES=a docker compose -p kv3 up -d --build router node-a
docker compose -p kv3 logs -f router node-a
```

The initial node list is applied when the router starts. To start with all
three nodes, run `docker compose -p kv3 up -d --build` with the default
`INITIAL_NODES=a,b,c`. For a second deployment, choose another project name
and set `ROUTER_PORT` to an unused port.

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
its container. The current implementation retains old copies in memory after
moving keys. The router reads data only from the active owners.

### Use custom node names

The supplied Compose file defines A, B, and C. The router accepts up to three
configured node IDs and resolves them through `NODE_URLS`. For a local
deployment with custom IDs, start distinct storage processes and assign each a different listening port. For example, run the following in separate terminals:

```sh
NODE_NAME=d python -m uvicorn storage.node:app --port 8081
NODE_NAME=e python -m uvicorn storage.node:app --port 8082
NODE_NAME=f python -m uvicorn storage.node:app --port 8083
```

Then start a router using those empty nodes:

```sh
NODE_URLS='{"d":"http://127.0.0.1:8081","e":"http://127.0.0.1:8082","f":"http://127.0.0.1:8083"}' \
INITIAL_NODES=d \
  python -m uvicorn routing.app:app --port 8080
curl -fsS -X PUT http://127.0.0.1:8080/_admin/membership \
  -H 'Content-Type: application/json' -d '{"nodes":["d","e","f"]}'
```

On router startup, omitting `INITIAL_NODES` enrolls the entire configured list.
Membership changes can select any configured ID while the router is running.
To change the address map itself, update `NODE_URLS` and restart the router.
Restart the storage nodes together with the router when changing startup configuration.

## Failover and replacement

- A read tries the primary first and uses the replica when the primary is
  unavailable. An authoritative missing key returns HTTP 404.
- A write or delete is acknowledged only after every assigned copy confirms
  it. If a copy is already unavailable at preflight time, the write returns
  HTTP 503 and the prior value remains readable from a healthy copy.
- If a failure interrupts a pending write, it returns HTTP 503 and may
  complete in the background. Reads and new writes for that key return 503
  while the write is pending. Retries reuse the original revision.
- Each node restart clears its data and generates a new process identity.
  Reads can use its surviving replica. Bring the other nodes online and call
  `POST /_admin/repair/{node}` before the restarted node resumes normal service.
  The router copies and verifies its records before enrolling the new identity.
- Router membership, revision counters, and pending writes also live in memory.
  When restarting the router, restart all storage nodes to initialize a fresh
  cluster. Startup rejects nodes containing records because the previous
  coordinator's pending writes and membership have been lost.
- Deletes are stored as revisioned tombstones so a repair or migration
  preserves the deletion. Tombstones are retained.

For a three-node failover demo, `alice` is assigned to nodes B and C in that
order. Write it, stop B, and read it from C. Restart B and repair it from the
surviving replica. Wait for B to log that it is ready before calling repair.

```sh
docker compose -p kv3 stop node-b
curl -fsS http://127.0.0.1:8080/alice
docker compose -p kv3 up -d node-b
# Wait for node-b to log that it is ready.
curl -fsS -X POST http://127.0.0.1:8080/_admin/repair/b
```

Data lasts only while the processes holding it remain alive. A single-node
restart loses all data. With replicas, repair a restarted node before stopping
the other owner of its keys. To reset the entire cluster, stop all services
with `docker compose -p kv3 down` and then start them together.

## Run the benchmark

With Docker running, run these commands from `coding-assignment-3/`:

```sh
python -m pip install -r requirements-lock.txt
python -m benchmark
```

The benchmark builds the router and node image once. It runs separate Docker
Compose projects with fresh processes for one, two, and three nodes, and cleans
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
| `management/` | Administrative routes, membership changes, replica repair |
| `storage/` | Node HTTP service, shared record format, in-memory node storage and router state |
| `benchmark/` | Compose sweep, timed workloads, summary report and charts |
| `tests/` | Routing, API, in-memory state and benchmark-output tests |
| `results/` | [Functional verification summary](results/verification.md) and benchmark runs |

Run the tests with `python -m unittest discover -s tests -v` after installing
`requirements-lock.txt`. They exercise actual HTTP handlers and isolated memory stores
with an in-process transport that can simulate node outages.

The router is a single process. Container replicas on one host share that
host's failure domain. All data and coordination state are volatile.

## Request-path behavior

Node reads, version checks, and mutations use a lock-protected dictionary.
Batch mutations validate every record before publishing any update, so a
conflicting revision leaves the entire batch unapplied. Same-key operations
are serialized by the router, and successful writes still require confirmation
from every assigned copy. The retry worker handles interrupted writes while
the router process is alive.

The results in dated folders from earlier revisions describe those revisions.
Run the benchmark again to measure this implementation on the same Docker host.

## Implementation decisions and challenges

The Assignment 2 HTTP API is reused at the public router. The consistent hash
ring uses SHA-256 and 128 virtual positions per physical node. Walking
clockwise chooses a primary and then a distinct replica. The configured
membership supports one to three nodes with arbitrary IDs.

The initial design stored node records and router coordination state in
SQLite. The current design uses lock-protected dictionaries to remove disk
transactions from the request path. Each mutation still has a revision, and
the router serializes operations on the same key. Deep copies prevent JSON
values returned to callers from modifying stored records accidentally.

A write can reach one owner before the other fails. The router keeps that
mutation pending, blocks new requests for its key, and retries the same
revision. This handles temporary node failures while the router stays alive.
It does not recover pending operations after router process loss.

Changing placement while requests are running can cause missing or stale
reads. Maintenance drains active requests and rejects new KV requests,
collects authoritative records, installs and verifies the target copies,
and then publishes the new ring. Deletes carry tombstones so older copies
cannot reintroduce deleted values during repair.

The supplied benchmark measures stable configurations on fresh processes.
The functional tests separately exercise live membership changes. Interpret
scaling results alongside replication: the one-node setup stores one copy,
while the two- and three-node setups store two copies. All configurations
share the same Docker host and single router, so measured scaling depends on
those shared resources as well as the storage nodes.
