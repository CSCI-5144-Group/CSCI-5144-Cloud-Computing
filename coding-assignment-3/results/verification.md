# KV Store Functional Verification

All eight automated tests passed using the actual HTTP handlers and isolated
in-memory stores through an in-process test transport.

| Area | What passed |
| --- | --- |
| Placement and membership | Primary distribution, minimal remapping, one-to-three-node resizing, and configured IDs d/e/f |
| KV and failure recovery | Reads from a surviving replica, repair after node restart, temporary-outage write retries, and preservation of deletions |
| In-memory state | Concurrent revision allocation, all-or-nothing batch updates, JSON value isolation, and loss of data and process identity on restart |
| API and benchmark | Removed export/import routes return HTTP 404, restart against nonempty nodes is rejected, and benchmark workloads produce Markdown, CSV and HTML |

The Docker Compose configuration passed validation. Docker socket access was
denied by the execution sandbox, so actual-container verification remains
pending for this revision. No SQLite connections or
data volumes are used. A restarted node must be repaired from a surviving
replica. Restart the full cluster when restarting the router because its
membership and pending operations are also volatile.

Existing dated benchmark folders describe previous implementations. Run
`python -m benchmark` to measure this revision using actual containers.

## Hash distribution

For 10,000 keys named `key-0` through `key-9999`, primary ownership was:

| Nodes | A | B | C |
| ---: | ---: | ---: | ---: |
| 1 | 10,000 | — | — |
| 2 | 4,893 | 5,107 | — |
| 3 | 3,262 | 3,190 | 3,548 |

These counts verify placement balance for this key set. Request load also
depends on how frequently each key is accessed.

## Unified benchmark verification

The benchmark test now runs the original Assignment 2 HTTP handlers and all
three Assignment 3 configurations through the shared duration-based workload.
It verifies successful repeated operations beyond the initial dataset size,
the measured duration, and report grouping that keeps different client counts
separate. Warm-up requests are excluded from trial output.

Both benchmark Compose configurations validate successfully. The new Docker
comparison is pending because the Docker socket was unavailable during this
verification. Previous dated measurements used the earlier methodology.
