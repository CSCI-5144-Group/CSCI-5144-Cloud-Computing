# KV Store Benchmark

- Host: macOS-27.0.1-arm64-arm-64bit
- Workload: 1000 keys, 128 ASCII bytes per value, 8 concurrent clients, 3 planned trials per configuration.
- Each configuration starts on fresh processes. An untimed seed PUT and verification GET prepare the dataset. Timed PUT updates existing keys. Timed GET verifies the latest values.
- Throughput counts successful operations per second of wall time. Latencies are client-observed. Each result is the median of the corresponding trial metric. p99 is calculated per trial using the nearest-rank method.
- The single-node configuration writes one copy. Two and three nodes write two copies. All containers share the same Docker host.

| System | Nodes | Copies per key | Phase | Valid / planned trials | Throughput (ops/s) | Mean (ms) | p99 (ms) | Errors |
| :--- | ---: | ---: | :--- | ---: | ---: | ---: | ---: | ---: |
| assignment2 | 1 | 1 | PUT | 3 / 3 | 3,501 | 2.23 | 4.10 | 0 |
| assignment2 | 1 | 1 | GET | 3 / 3 | 3,925 | 2.00 | 4.11 | 0 |
| assignment3 | 1 | 1 | PUT | 3 / 3 | 753 | 10.49 | 15.98 | 0 |
| assignment3 | 1 | 1 | GET | 3 / 3 | 1,482 | 5.33 | 8.98 | 0 |
| assignment3 | 2 | 2 | PUT | 3 / 3 | 566 | 14.01 | 21.19 | 0 |
| assignment3 | 2 | 2 | GET | 3 / 3 | 1,495 | 5.27 | 9.05 | 0 |
| assignment3 | 3 | 2 | PUT | 3 / 3 | 559 | 14.21 | 21.40 | 0 |
| assignment3 | 3 | 2 | GET | 3 / 3 | 1,465 | 5.40 | 9.77 | 0 |

Assignment 2 uses its original server and Dockerfile, including per-request logging. Assignment 3 uses a router and summary logging. Both receive the same workload through the same client code on the same Docker host. This compares the complete implementations.

[Charts](charts.html) · [Per-trial data](trials.csv)
