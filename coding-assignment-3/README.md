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

To compare Assignment 2 with all three Assignment 3 configurations, run:

```sh
python -m benchmark --include-assignment2
```

The comparison uses the same client code and quiet operation logging for
both assignments. The Assignment 2 container overrides its startup command
to suppress its per-request INFO logs. The Assignment 3 benchmark overlay
suppresses routine logs on the router and nodes. Both retain warnings and
errors and use one Uvicorn worker per server. Regular deployments keep their
existing progress logging.

The default benchmark sweeps 8, 16, 32, and 64 concurrent clients, with 1,000
keys and 128-byte ASCII values. Each PUT and GET phase has a two-second
warm-up followed by ten measured seconds, repeated three times. Workers
cycle through disjoint key partitions, so faster implementations complete
more requests in the same time window. PUT updates existing keys, and GET
verifies the latest values. Only complete request/response operations count.
The elapsed time includes the final in-flight requests finishing after the
deadline.

For a short deployment check:

```sh
python -m benchmark --include-assignment2 --workers 8 --duration 1 --warmup 0.2 --trials 1
```

For the full comparison:

```sh
python -m benchmark --include-assignment2 --workers 8 16 32 64 --duration 10 --warmup 2 --trials 3
```

The full comparison needs about 20 minutes plus builds, startup, seeding and
cleanup. It runs configurations sequentially on the same Docker host, with
fresh processes for every implementation, node count, and client count.
The client prints phase progress and the actual operation count, elapsed
time, throughput and errors after each measured phase. The default published
port is 18080; set `--port` if needed.

Each run produces a new directory under `results/`:

- `report.md` groups results by concurrent clients, implementation, node count and method.
- `trials.csv` includes actual attempts and measured elapsed time for every phase.
- `charts.html` compares throughput and p99 separately at each client count.

The report uses the median of valid trial metrics. Trials with request
errors remain in the raw data and are excluded from valid-trial medians.
Use `--output` to choose a specific new folder.

The comparison still includes routing, replication, synchronization and
framework differences. One node stores one copy; two or three nodes store
two copies. Increased concurrency can expose a throughput plateau but does
not identify its cause. Inspect router, client and node resource usage when
diagnosing a bottleneck. Existing dated results use the previous short,
fixed-operation methodology and should be interpreted separately.

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
