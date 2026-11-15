# KV Store Benchmark

- Host: macOS-27.0.1-arm64-arm-64bit
- Workload: 1000 keys, 128 ASCII bytes per value, 8 concurrent clients, 3 planned trials per configuration.
- Each configuration starts on fresh volumes. An untimed seed PUT and verification GET prepare the dataset. Timed PUT updates existing keys. Timed GET verifies the latest values.
- Throughput counts successful operations per second of wall time. Latencies are client-observed. Each result is the median of the corresponding trial metric. p99 is calculated per trial using the nearest-rank method.
- The single-node configuration writes one copy. Two and three nodes write two copies. All containers share the same Docker host.

| Nodes | Copies per key | Phase | Valid / planned trials | Throughput (ops/s) | Mean (ms) | p99 (ms) | Errors |
| ---: | ---: | :--- | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | PUT | 3 / 3 | 292 | 23.70 | 163.97 | 0 |
| 1 | 1 | GET | 3 / 3 | 1,131 | 6.98 | 14.40 | 0 |
| 2 | 2 | PUT | 3 / 3 | 208 | 34.42 | 277.69 | 0 |
| 2 | 2 | GET | 3 / 3 | 1,179 | 6.72 | 13.70 | 0 |
| 3 | 2 | PUT | 3 / 3 | 270 | 25.75 | 142.63 | 0 |
| 3 | 2 | GET | 3 / 3 | 1,214 | 6.49 | 11.14 | 0 |

[Charts](charts.html) · [Per-trial data](trials.csv)
