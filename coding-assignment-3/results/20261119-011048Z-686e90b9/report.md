# KV Store Benchmark

- Host: macOS-27.0.1-arm64-arm-64bit
- Workload: 1000 keys, 128 ASCII bytes per value, client counts [8, 16, 32, 64], 3 planned trials per configuration.
- Each phase runs for 10 measured seconds after 2 warm-up seconds. Workers repeatedly traverse disjoint key partitions until the deadline. In-flight requests finish before the phase ends.
- Each configuration starts on fresh processes. An untimed seed PUT and verification GET prepare the dataset. Timed PUT updates existing keys. Timed GET verifies the latest values.
- Throughput counts successful operations per second of wall time. Latencies are client-observed. Each result is the median of the corresponding trial metric. p99 is calculated per trial using the nearest-rank method.
- The single-node configuration writes one copy. Two and three nodes write two copies. All containers share the same Docker host.

- HTTP access and routine operation logs are suppressed for both systems. Warnings and errors remain visible. Benchmark progress is printed by the client.
| Clients | System | Nodes | Copies per key | Phase | Valid / planned trials | Throughput (ops/s) | Mean (ms) | p99 (ms) | Errors |
| ---: | :--- | ---: | ---: | :--- | ---: | ---: | ---: | ---: | ---: |
| 8 | assignment2 | 1 | 1 | PUT | 3 / 3 | 3,834 | 2.09 | 4.19 | 0 |
| 8 | assignment2 | 1 | 1 | GET | 3 / 3 | 4,880 | 1.64 | 3.33 | 0 |
| 8 | assignment3 | 1 | 1 | PUT | 3 / 3 | 789 | 10.13 | 15.78 | 0 |
| 8 | assignment3 | 1 | 1 | GET | 3 / 3 | 1,524 | 5.25 | 8.61 | 0 |
| 8 | assignment3 | 2 | 2 | PUT | 3 / 3 | 568 | 14.07 | 21.70 | 0 |
| 8 | assignment3 | 2 | 2 | GET | 3 / 3 | 1,476 | 5.42 | 9.37 | 0 |
| 8 | assignment3 | 3 | 2 | PUT | 3 / 3 | 551 | 14.52 | 22.49 | 0 |
| 8 | assignment3 | 3 | 2 | GET | 3 / 3 | 1,494 | 5.35 | 8.82 | 0 |
| 16 | assignment2 | 1 | 1 | PUT | 3 / 3 | 4,205 | 3.80 | 7.71 | 0 |
| 16 | assignment2 | 1 | 1 | GET | 3 / 3 | 5,210 | 3.07 | 6.38 | 0 |
| 16 | assignment3 | 1 | 1 | PUT | 3 / 3 | 806 | 19.84 | 30.22 | 0 |
| 16 | assignment3 | 1 | 1 | GET | 3 / 3 | 1,558 | 10.26 | 16.80 | 0 |
| 16 | assignment3 | 2 | 2 | PUT | 3 / 3 | 580 | 27.55 | 42.26 | 0 |
| 16 | assignment3 | 2 | 2 | GET | 3 / 3 | 1,574 | 10.16 | 16.41 | 0 |
| 16 | assignment3 | 3 | 2 | PUT | 3 / 3 | 552 | 28.96 | 50.50 | 0 |
| 16 | assignment3 | 3 | 2 | GET | 3 / 3 | 1,444 | 11.07 | 21.11 | 0 |
| 32 | assignment2 | 1 | 1 | PUT | 3 / 3 | 4,394 | 7.28 | 14.51 | 0 |
| 32 | assignment2 | 1 | 1 | GET | 3 / 3 | 5,036 | 6.35 | 14.35 | 0 |
| 32 | assignment3 | 1 | 1 | PUT | 3 / 3 | 831 | 38.46 | 53.66 | 0 |
| 32 | assignment3 | 1 | 1 | GET | 3 / 3 | 1,576 | 20.28 | 32.18 | 0 |
| 32 | assignment3 | 2 | 2 | PUT | 3 / 3 | 586 | 54.59 | 77.21 | 0 |
| 32 | assignment3 | 2 | 2 | GET | 3 / 3 | 1,593 | 20.07 | 29.49 | 0 |
| 32 | assignment3 | 3 | 2 | PUT | 3 / 3 | 580 | 55.12 | 77.58 | 0 |
| 32 | assignment3 | 3 | 2 | GET | 3 / 3 | 1,577 | 20.28 | 30.05 | 0 |
| 64 | assignment2 | 1 | 1 | PUT | 3 / 3 | 4,477 | 14.28 | 29.19 | 0 |
| 64 | assignment2 | 1 | 1 | GET | 3 / 3 | 5,197 | 12.30 | 30.82 | 0 |
| 64 | assignment3 | 1 | 1 | PUT | 3 / 3 | 820 | 77.89 | 112.90 | 0 |
| 64 | assignment3 | 1 | 1 | GET | 3 / 3 | 1,496 | 42.72 | 78.61 | 0 |
| 64 | assignment3 | 2 | 2 | PUT | 3 / 3 | 586 | 108.89 | 149.39 | 0 |
| 64 | assignment3 | 2 | 2 | GET | 3 / 3 | 1,576 | 40.54 | 55.46 | 0 |
| 64 | assignment3 | 3 | 2 | PUT | 3 / 3 | 555 | 114.96 | 176.63 | 0 |
| 64 | assignment3 | 3 | 2 | GET | 3 / 3 | 1,538 | 41.55 | 57.58 | 0 |

Assignment 2 uses its original server and Dockerfile with operation logging disabled at startup. Assignment 3 uses a router with routine logs suppressed. Both receive the same workload through the same client code on the same Docker host. This compares the complete implementations.

[Charts](charts.html) · [Per-trial data](trials.csv)
