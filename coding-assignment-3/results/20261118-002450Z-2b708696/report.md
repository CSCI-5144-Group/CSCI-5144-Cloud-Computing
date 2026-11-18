# KV Store Benchmark

- Host: macOS-27.0.1-arm64-arm-64bit
- Workload: 1000 keys, 128 ASCII bytes per value, 8 concurrent clients, 3 planned trials per configuration.
- Each configuration starts on fresh processes. An untimed seed PUT and verification GET prepare the dataset. Timed PUT updates existing keys. Timed GET verifies the latest values.
- Throughput counts successful operations per second of wall time. Latencies are client-observed. Each result is the median of the corresponding trial metric. p99 is calculated per trial using the nearest-rank method.
- The single-node configuration writes one copy. Two and three nodes write two copies. All containers share the same Docker host.

| Nodes | Copies per key | Phase | Valid / planned trials | Throughput (ops/s) | Mean (ms) | p99 (ms) | Errors |
| ---: | ---: | :--- | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | PUT | 3 / 3 | 766 | 10.39 | 15.91 | 0 |
| 1 | 1 | GET | 3 / 3 | 1,465 | 5.37 | 9.32 | 0 |
| 2 | 2 | PUT | 3 / 3 | 534 | 14.85 | 25.50 | 0 |
| 2 | 2 | GET | 3 / 3 | 1,464 | 5.39 | 9.21 | 0 |
| 3 | 2 | PUT | 3 / 3 | 543 | 14.54 | 22.61 | 0 |
| 3 | 2 | GET | 3 / 3 | 1,427 | 5.54 | 11.79 | 0 |

[Charts](charts.html) · [Per-trial data](trials.csv)
