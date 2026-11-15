# KV Store Functional Verification

All six automated tests passed using the real HTTP handlers and SQLite
databases with an in-process test transport.

| Area | What passed |
| --- | --- |
| Placement and membership | Primary distribution and minimal remapping, one-to-three-node resizing, custom IDs d/e/f, and membership persistence across router restart |
| KV and failure recovery | Reads and writes across resize, reads from a surviving replica, replacement of an empty node, replay of an interrupted write, and preservation of deletions |
| Backup and restore | Restore to a fresh cluster at a different size, preserved revisions, and resuming an interrupted restore after router restart |
| Benchmark | Seed, timed update and read phases, value verification at all three sizes, and generation of Markdown, CSV and HTML outputs |

The Docker Compose configuration also passed validation. The Docker daemon
was unavailable during verification, so this summary contains no measured
Docker throughput or latency. Run `python -m benchmark` to generate those
measurements in a dated results folder.
