# Knot

> Distributed fault-tolerant key-value store with Raft consensus and a custom LSM-tree storage engine, written in C++20.

[![CI](https://github.com/akshat/knot/actions/workflows/ci.yml/badge.svg)](https://github.com/akshat/knot/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/20)

## What it is

Knot is a from-scratch implementation of the kind of distributed database that powers modern infrastructure. It implements:

- **Raft consensus** for leader election, log replication, snapshots, and dynamic membership
- **Custom LSM-tree storage engine** with WAL, MemTable, SSTables, leveled compaction, bloom filters, and block cache
- **Crash recovery** with strong durability guarantees
- **Benchmarking harness** producing reproducible numbers
- **Failure injection** for chaos testing
- **Linearizability verification** via Porcupine

> Inspired by etcd, RocksDB, and Apache Cassandra — at student scale, on a laptop, with the internals fully exposed.

## Architecture

(Diagram coming — see [docs/architecture.md](docs/architecture.md))

## Quick start

```bash
# Prereqs (macOS): brew install cmake ninja clang-format
# vcpkg: git clone https://github.com/microsoft/vcpkg ~/vcpkg && ~/vcpkg/bootstrap-vcpkg.sh

# Configure + build
cmake --preset=default
cmake --build build/default

# Run the daemon
./build/default/knotd
```

## Documentation

| Doc | What's inside |
|---|---|
| [PRD.md](PRD.md) | Full product requirements: features, tech, structure, TODO |
| [docs/architecture.md](docs/architecture.md) | System design, data flow, threading |
| [docs/raft-design.md](docs/raft-design.md) | Consensus internals, RPCs, edge cases |
| [docs/storage-design.md](docs/storage-design.md) | LSM tree, WAL, SSTable format, compaction |
| [docs/benchmarks.md](docs/benchmarks.md) | Methodology, workloads, target numbers |
| [docs/phases.md](docs/phases.md) | 12-week phased roadmap with day-by-day TODOs |
| [docs/journal.md](docs/journal.md) | Daily development journal |

## Benchmarks

(Will be populated by end of Week 4 — see [docs/benchmarks.md](docs/benchmarks.md))

| Workload | Cluster | Throughput | p99 latency |
|---|---|---|---|
| Load | 5-node | TBD | TBD |
| A 50/50 | 5-node | TBD | TBD |
| B 95/5 | 5-node | TBD | TBD |

## License

MIT — see [LICENSE](LICENSE).
