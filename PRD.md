# Knot — Product Requirements Document (PRD)

> Distributed Fault-Tolerant Key-Value Store with Raft Consensus and Custom LSM Storage Engine.
> _Working name: **Knot** — rename as desired (alternatives: Helix, Stratus, Lattice, Aether, CrowKV)._

---

## 0. Document control

| Field | Value |
|---|---|
| Owner | Akshat |
| Status | Draft v1 |
| Target completion (core sprint) | 4 weeks @ 8–10 hrs/day (~280 hrs) |
| Target completion (full vision) | ~12 weeks total |
| Resume readiness milestone | End of Week 4 |
| Final ship target | End of Week 12 |

---

## 1. Vision

Build a production-inspired distributed KV database in modern C++ that demonstrates **real systems engineering depth** — Raft consensus, custom LSM storage engine, fault tolerance, dynamic membership, observability, benchmarking, and a live cluster visualization — comparable in concept (not scale) to systems like **etcd**, **RocksDB**, and **Apache Cassandra**.

The system must function simultaneously as:

- A distributed database
- A storage engine
- A consensus engine
- A benchmarking platform
- An interactive distributed-systems simulator

---

## 2. Goals & non-goals

### 2.1 Goals

- Implement Raft consensus from scratch (election, log replication, persistence, snapshots, membership changes).
- Build a custom LSM-tree storage engine from scratch (WAL, MemTable, SSTable, leveled compaction, bloom filters, block cache).
- Provide strong durability and crash recovery semantics.
- Support **dynamic cluster membership** via joint consensus.
- Provide a **benchmarking harness** producing verifiable performance numbers.
- Provide a **failure injection / chaos engine**.
- Provide a **real-time visualization dashboard** (Next.js + WebSockets + React Flow + Recharts).
- Provide observability (structured logs, Prometheus metrics).
- Verify correctness with **linearizability testing** (Porcupine).
- Document the system thoroughly (architecture docs, blog post writeup, demo video).

### 2.2 Non-goals (v1)

- Wire-compatibility with etcd/Redis/RocksDB.
- Production-grade security (TLS, auth) — deferred to Phase 4.
- Cross-datacenter geo-replication at real scale — simulated only.
- SQL or complex query layer.

---

## 3. Success criteria

A v1 ship is successful if all of the following are true:

1. A 5-node cluster runs continuously for 1 hour under mixed workload without data loss or divergence.
2. Leader failover completes in **< 2 seconds** p99 after `SIGKILL`.
3. Sustained throughput of **≥ 10,000 PUT/sec** at **p99 < 5 ms** in a 5-node cluster on a single laptop.
4. Linearizability is verified via Porcupine on 200+ randomized chaos scenarios.
5. Cluster recovers correctly from leader crash, follower crash, full restart, and network partition.
6. Dashboard renders live cluster topology, leader changes, replication lag, and benchmark plots in real time.
7. GitHub repo includes README, architecture docs, benchmark results, architecture diagram, and a blog post.

---

## 4. High-level architecture

```
                  ┌────────────────────────────────┐
                  │   Next.js Dashboard (browser)  │
                  │  Topology · Logs · Bench plots │
                  └─────────────┬──────────────────┘
                                │  WebSocket / REST
                  ┌─────────────▼──────────────────┐
                  │   Cluster Controller / Gateway  │
                  │  (lightweight HTTP service)     │
                  └─────────────┬──────────────────┘
                                │  TCP (binary, protobuf)
       ┌────────────────────────┼────────────────────────┐
       │                        │                        │
   ┌───▼───┐                ┌───▼───┐                ┌───▼───┐
   │Node A │◄──── Raft ────►│Node B │◄──── Raft ────►│Node C │
   │Leader │                │Follow │                │Follow │
   └───┬───┘                └───┬───┘                └───┬───┘
       │ Each node runs the same vertical stack:           │
       ▼                                                    ▼
┌─────────────────────────────────────────────────────────────┐
│  Network Layer (Boost.Asio, async TCP, protobuf framing)    │
├─────────────────────────────────────────────────────────────┤
│  Raft Module                                                │
│   ├─ FSM (Follower/Candidate/Leader)                        │
│   ├─ Election timer + heartbeat                             │
│   ├─ Log replication (AppendEntries, RequestVote)           │
│   ├─ Snapshot manager (InstallSnapshot)                     │
│   ├─ Membership manager (joint consensus)                   │
│   └─ Persistent state (term, votedFor, log)                 │
├─────────────────────────────────────────────────────────────┤
│  Replicated State Machine (KV apply, single-threaded)       │
├─────────────────────────────────────────────────────────────┤
│  LSM Storage Engine                                         │
│   ├─ WAL (append + fsync)                                   │
│   ├─ MemTable (concurrent skiplist)                         │
│   ├─ Immutable MemTable queue (flush)                       │
│   ├─ SSTables (L0..L6, leveled)                             │
│   ├─ Bloom filters per SSTable                              │
│   ├─ Block cache (LRU)                                      │
│   ├─ Background compactor (leveled compaction)              │
│   └─ MANIFEST (live SSTable tracking)                       │
├─────────────────────────────────────────────────────────────┤
│  Observability (structured logs, Prometheus /metrics)       │
├─────────────────────────────────────────────────────────────┤
│  Persistent storage (filesystem, fsync, mmap)               │
└─────────────────────────────────────────────────────────────┘
```

---

## 5. Functional requirements (FR)

### 5.1 Consensus & replication
- **FR-C1** — Implement Raft leader election with randomized timeouts (150–300 ms).
- **FR-C2** — Implement heartbeats at 50 ms cadence.
- **FR-C3** — Implement AppendEntries log replication with `prevLogIndex/prevLogTerm` matching.
- **FR-C4** — Persist `currentTerm`, `votedFor`, and `log[]` to disk; fsync before responding to RPCs.
- **FR-C5** — Implement quorum-based commit advancement.
- **FR-C6** — Implement InstallSnapshot RPC for slow followers.
- **FR-C7** — Implement snapshot creation, log truncation, restart-from-snapshot.
- **FR-C8** — Implement **joint consensus** for membership changes (C_old,new → C_new).
- **FR-C9** — Implement leader-only client request handling; followers redirect with `NotLeader{leaderHint}`.
- **FR-C10** — Support read-index for linearizable reads (optional optimization).

### 5.2 Storage engine
- **FR-S1** — Write-ahead log: append, fsync, sequential read, recovery.
- **FR-S2** — MemTable: concurrent inserts/reads, size threshold flush.
- **FR-S3** — SSTable writer: sorted block-based format (data blocks + index block + bloom filter + metadata + footer).
- **FR-S4** — SSTable reader: binary search by index block, sequential scan, point lookup.
- **FR-S5** — Bloom filter per SSTable for negative lookups.
- **FR-S6** — Block cache: LRU cache of decompressed data blocks.
- **FR-S7** — Leveled compaction: L0..L6, each level 10x larger than previous.
- **FR-S8** — Tombstones for DELETE; GC'd during compaction.
- **FR-S9** — MANIFEST file: atomic tracking of live SSTable files per level.
- **FR-S10** — Crash recovery from WAL + MANIFEST.

### 5.3 Replicated state machine
- **FR-R1** — Commands: `PUT(k, v)`, `GET(k)`, `DELETE(k)`.
- **FR-R2** — Single apply thread consuming committed Raft entries in index order.
- **FR-R3** — Linearizable reads via leader read-index or leader-local with lease.
- **FR-R4** — Idempotent client request handling (client ID + sequence number).

### 5.4 Networking
- **FR-N1** — Async TCP via Boost.Asio.
- **FR-N2** — Length-prefixed binary framing.
- **FR-N3** — Protobuf wire schema for all RPCs.
- **FR-N4** — Connection pooling per peer.
- **FR-N5** — Reconnection with exponential backoff.

### 5.5 Cluster management
- **FR-M1** — Static cluster bootstrap from config file.
- **FR-M2** — Dynamic add-node (joint consensus).
- **FR-M3** — Dynamic remove-node (joint consensus).
- **FR-M4** — Knot recalculation on membership change.
- **FR-M5** — Log catch-up for newly added nodes.

### 5.6 Benchmarking & failure simulation
- **FR-B1** — YCSB-style workloads: A (50/50), B (95/5), C (100% read), Load (100% write).
- **FR-B2** — Latency histograms: p50/p95/p99/p99.9.
- **FR-B3** — Throughput tracking (ops/sec).
- **FR-B4** — Failover time measurement (kill → new leader → first commit).
- **FR-B5** — Replication lag measurement.
- **FR-B6** — Chaos engine: leader kill, follower kill, network partition (`iptables`), slow disk (`tc`).
- **FR-B7** — Scenario scripts: rolling restart, split-brain, cascading failure.

### 5.7 Visualization dashboard
- **FR-V1** — Real-time cluster topology (React Flow).
- **FR-V2** — Leader visualization with role colors.
- **FR-V3** — Heartbeat animations between nodes.
- **FR-V4** — Replication state per follower (matchIndex bar).
- **FR-V5** — Live log stream from each node.
- **FR-V6** — Benchmark plots (Recharts).
- **FR-V7** — Failure injection controls (kill leader, partition).
- **FR-V8** — Storage engine state (MemTable size, SSTable count per level).
- **FR-V9** — Snapshot tracking.
- **FR-V10** — WebSocket-based push updates.

### 5.8 Observability
- **FR-O1** — Structured JSON logs (timestamp, node ID, term, component, level, msg).
- **FR-O2** — Prometheus `/metrics` endpoint per node.
- **FR-O3** — Pre-built Grafana dashboard JSON shipped in repo.
- **FR-O4** — `knotctl` CLI: `status`, `leader`, `members`, `put`, `get`, `del`, `snapshot`, `bench`.

### 5.9 Advanced features (Phases 2–3)
- **FR-A1** — Multi-Raft sharding (key range → Raft group mapping).
- **FR-A2** — Distributed transactions (2PC over multiple Raft groups).
- **FR-A3** — Simulated geo-replication (configurable per-link latency).
- **FR-A4** — Binary replication protocol optimizations (batching, pipelining).
- **FR-A5** — Automated chaos test suite (nightly).

---

## 6. Non-functional requirements (NFR)

| ID | Requirement | Target |
|---|---|---|
| NFR-1 | Throughput (PUT, 5-node) | ≥ 10,000 ops/sec |
| NFR-2 | p99 PUT latency | ≤ 5 ms |
| NFR-3 | Leader failover time | p99 ≤ 2 s |
| NFR-4 | Snapshot install time (1GB state) | ≤ 30 s |
| NFR-5 | Crash recovery time (cold start, 1GB log) | ≤ 60 s |
| NFR-6 | Memory footprint per node | ≤ 512 MB at idle |
| NFR-7 | Code coverage (unit tests) | ≥ 70% |
| NFR-8 | Linearizability checks passed | ≥ 200 scenarios |

---

## 7. Technology stack

### 7.1 Backend (C++17/20)

| Layer | Choice | Rationale |
|---|---|---|
| Language | C++17 (minimum) / C++20 (preferred) | Systems-tier signal, fine-grained control |
| Async I/O | Boost.Asio | Mature, well-supported, async TCP |
| RPC encoding | Protocol Buffers (protobuf) | Schema versioning, fast, ubiquitous |
| Build | CMake (3.20+) + Ninja | Modern, IDE-friendly |
| Package mgmt | vcpkg or Conan | Reproducible deps |
| Logging | spdlog | Async, structured, fast |
| Formatting | fmt (subsumed in C++20 std::format) | Type-safe formatting |
| Testing | GoogleTest + GoogleMock | Standard, well-documented |
| Property testing | rapidcheck (optional) | Randomized op sequences |
| Linearizability | Porcupine (Go binary, called externally) | Industry-standard checker |
| Compression (optional) | snappy or zstd | SSTable block compression |

### 7.2 Storage layer

- Local filesystem (`ext4`/`apfs`)
- `fsync(2)` for durability
- `mmap(2)` for SSTable reads (optional)
- Direct I/O (optional, Linux-only)

### 7.3 Frontend (dashboard)

| Layer | Choice |
|---|---|
| Framework | Next.js 14+ (App Router) |
| Styling | TailwindCSS |
| Graph viz | React Flow |
| Charts | Recharts |
| Live updates | WebSockets (native or socket.io) |
| State | Zustand or React Context |
| Language | TypeScript |

### 7.4 Cluster gateway (between dashboard and nodes)

- Lightweight Node.js / TypeScript service (Fastify or Express) **or** in-process HTTP server in C++ via `Boost.Beast`
- Aggregates per-node `/metrics` and `/state` endpoints
- Pushes deltas to dashboard via WebSocket

### 7.5 Tooling

| Purpose | Tool |
|---|---|
| Container | Docker + docker-compose |
| CI | GitHub Actions (build, test, bench) |
| Chaos | `iptables`, `tc`, `SIGKILL` scripts |
| Metrics | Prometheus + Grafana (optional, dashboards shipped) |
| Diagrams | Excalidraw → PNG |

---

## 8. Detailed module specifications

### 8.1 Raft module

**State enum:** `Follower | Candidate | Leader`

**Persistent state (fsynced before RPC response):**
```cpp
struct PersistentState {
    uint64_t currentTerm;        // monotonic
    std::optional<NodeId> votedFor;
    std::vector<LogEntry> log;   // 1-indexed
};
```

**Volatile state (all nodes):**
```cpp
uint64_t commitIndex;   // highest known committed
uint64_t lastApplied;   // highest applied to state machine
```

**Leader volatile state:**
```cpp
std::map<NodeId, uint64_t> nextIndex;
std::map<NodeId, uint64_t> matchIndex;
```

**RPCs (protobuf):**

```proto
message RequestVoteReq {
  uint64 term = 1;
  string candidate_id = 2;
  uint64 last_log_index = 3;
  uint64 last_log_term = 4;
}
message RequestVoteResp {
  uint64 term = 1;
  bool vote_granted = 2;
}

message AppendEntriesReq {
  uint64 term = 1;
  string leader_id = 2;
  uint64 prev_log_index = 3;
  uint64 prev_log_term = 4;
  repeated LogEntry entries = 5;
  uint64 leader_commit = 6;
}
message AppendEntriesResp {
  uint64 term = 1;
  bool success = 2;
  uint64 conflict_index = 3;   // optimization
  uint64 conflict_term = 4;
}

message InstallSnapshotReq {
  uint64 term = 1;
  string leader_id = 2;
  uint64 last_included_index = 3;
  uint64 last_included_term = 4;
  uint64 offset = 5;
  bytes data = 6;
  bool done = 7;
}
```

**Timers:**
- Election timeout: uniform random in `[150ms, 300ms]`
- Heartbeat interval: `50ms`
- Snapshot trigger: every `10,000` committed entries or `64MB` log size

**Joint consensus:**
- Configuration changes go through transitional `C_old,new` state
- Both old and new majorities required during transition
- Cluster commits transition entry, then commits `C_new`

### 8.2 LSM storage engine

**Write path:**
```
PUT(k,v)
   │
   ▼
Append to WAL  (sequential, fsync optional per batch)
   │
   ▼
Insert into MemTable  (concurrent skiplist)
   │
   ▼ MemTable.size() > 4MB
Move to immutable queue
   │
   ▼
Background flush → L0 SSTable
   │
   ▼ L0.count() > 4 OR Lk.size() > threshold
Background leveled compaction
```

**Read path:**
```
GET(k)
   │
   ▼ Check MemTable
   │ hit? return
   ▼ Check immutable MemTable(s)
   │ hit? return
   ▼ For each L0 SSTable (newest first):
   │   bloom filter? continue
   │   read via block cache + index block
   │   hit? return
   ▼ For each level L1..L6:
   │   binary search MANIFEST for SSTable range
   │   bloom filter check
   │   read via block cache + index block
   │   hit? return
   ▼ return NotFound
```

**SSTable file format:**

```
┌──────────────────────────────┐
│ Data block 1 (~4 KB)         │  sorted KV pairs, optional snappy compression
│ Data block 2                 │
│ ...                          │
│ Data block N                 │
├──────────────────────────────┤
│ Bloom filter block           │
├──────────────────────────────┤
│ Index block                  │  first_key + offset + length per data block
├──────────────────────────────┤
│ Metadata block               │  min/max key, entry count, version, level
├──────────────────────────────┤
│ Footer (fixed 48 bytes)      │  magic + offsets to all blocks
└──────────────────────────────┘
```

**MANIFEST:**
- Append-only log of `AddFile{level, file}` / `RemoveFile{level, file}` records
- Atomic rotation on compact
- Replayed at startup

**Compaction strategy:**
- L0 → L1 trigger: 4 L0 files
- Lk → Lk+1 trigger: total Lk size > 10^k MB
- Pick file in Lk that overlaps fewest keys in Lk+1
- Merge sorted, write new file in Lk+1, atomic MANIFEST update

### 8.3 Replicated state machine

- Single-threaded apply loop
- Consumes from a thread-safe queue fed by Raft's commit advancement
- Idempotency: per-client `(client_id, seq_no)` dedup cache
- Reads: served by leader after read-index check (or via leader lease for relaxed linearizability)

### 8.4 Networking layer

- One `boost::asio::io_context` with worker thread pool (size = `std::thread::hardware_concurrency()`)
- One persistent TCP connection per peer-pair (no per-RPC connect)
- Per-peer `strand` for ordered writes
- Length-prefixed framing: `[4B length][1B type][protobuf payload]`
- Reconnect with jittered exponential backoff (100ms → 5s)

### 8.5 Cluster gateway / control plane

- C++ HTTP server (Boost.Beast) on each node exposing:
  - `GET /status` — node state, term, leader, log indices
  - `GET /metrics` — Prometheus format
  - `GET /members` — current cluster config
  - `POST /chaos/kill` — self-terminate (chaos tests)
  - WebSocket `/events` — push raft events (state changes, log appends, commits)
- Optional Node.js aggregator reads from all 5 nodes and provides unified WS to dashboard

### 8.6 Dashboard

**Pages:**
- `/` — cluster topology (React Flow)
- `/logs` — live log stream (virtualized list)
- `/bench` — benchmark results (Recharts)
- `/chaos` — failure injection controls
- `/storage` — per-node storage state (MemTable / SSTables / snapshots)

**Real-time data model:**
```ts
interface ClusterEvent {
  ts: number;
  node: string;
  type: 'state' | 'log' | 'commit' | 'snapshot' | 'membership';
  payload: unknown;
}
```

### 8.7 Benchmarking harness

- Multi-threaded driver (configurable worker count)
- Workload generator (Zipfian / uniform key distribution)
- HDR histogram for latency
- Output: JSON + matplotlib plots
- CLI: `knot-bench --workload=A --duration=60s --threads=16 --cluster=cfg.toml`

### 8.8 Chaos engine

- Scriptable scenarios (TOML config):
```toml
[scenario "leader_kill"]
steps = [
  { wait = "10s" },
  { kill = "leader" },
  { wait = "30s" },
  { assert = "new_leader_elected" },
]
```
- Implementation: `knot-chaos` binary calling docker/iptables/SIGKILL

### 8.9 Linearizability verification

- Each client logs `(start_ts, end_ts, op, args, result)`
- Aggregate to single history file
- Pipe to Porcupine (Go binary, called from CI)
- Visualize counterexamples if found

---

## 9. Project structure (complete file tree)

```
knot/
├── CMakeLists.txt
├── CMakePresets.json
├── vcpkg.json
├── .clang-format
├── .clang-tidy
├── .github/workflows/
│   ├── ci.yml                 # build + test
│   ├── bench.yml              # nightly benchmark
│   └── chaos.yml              # nightly chaos test
├── README.md
├── LICENSE
├── docs/
│   ├── architecture.md
│   ├── raft-design.md
│   ├── storage-design.md
│   ├── benchmarks.md
│   ├── chaos-scenarios.md
│   ├── dashboard.md
│   └── journal.md             # daily dev journal
├── proto/
│   └── knot.proto
├── include/knot/
│   ├── common/
│   │   ├── types.h
│   │   ├── result.h
│   │   ├── slice.h
│   │   ├── status.h
│   │   └── clock.h
│   ├── raft/
│   │   ├── node.h
│   │   ├── log.h
│   │   ├── state.h
│   │   ├── transport.h
│   │   ├── snapshot.h
│   │   └── membership.h
│   ├── storage/
│   │   ├── engine.h
│   │   ├── wal.h
│   │   ├── memtable.h
│   │   ├── sstable.h
│   │   ├── bloom.h
│   │   ├── cache.h
│   │   ├── compaction.h
│   │   └── manifest.h
│   ├── net/
│   │   ├── server.h
│   │   ├── client.h
│   │   ├── codec.h
│   │   └── peer.h
│   └── obs/
│       ├── log.h
│       └── metrics.h
├── src/
│   ├── common/
│   ├── raft/
│   │   ├── node.cpp
│   │   ├── log.cpp
│   │   ├── election.cpp
│   │   ├── replication.cpp
│   │   ├── snapshot.cpp
│   │   ├── membership.cpp
│   │   └── transport_tcp.cpp
│   ├── storage/
│   │   ├── engine.cpp
│   │   ├── wal.cpp
│   │   ├── memtable.cpp
│   │   ├── sstable_writer.cpp
│   │   ├── sstable_reader.cpp
│   │   ├── bloom.cpp
│   │   ├── cache.cpp
│   │   ├── compaction.cpp
│   │   └── manifest.cpp
│   ├── net/
│   │   ├── server.cpp
│   │   ├── client.cpp
│   │   ├── codec.cpp
│   │   └── peer.cpp
│   ├── obs/
│   │   ├── log.cpp
│   │   └── metrics.cpp
│   ├── server/
│   │   ├── main.cpp           # knotd binary
│   │   ├── config.cpp
│   │   └── http_admin.cpp
│   └── cli/
│       ├── knotctl.cpp      # CLI client
│       ├── knot_bench.cpp   # benchmark driver
│       └── knot_chaos.cpp   # chaos runner
├── tests/
│   ├── unit/
│   │   ├── raft_test.cpp
│   │   ├── log_test.cpp
│   │   ├── wal_test.cpp
│   │   ├── memtable_test.cpp
│   │   ├── sstable_test.cpp
│   │   ├── bloom_test.cpp
│   │   ├── compaction_test.cpp
│   │   └── codec_test.cpp
│   ├── integration/
│   │   ├── cluster_fixture.cpp
│   │   ├── election_test.cpp
│   │   ├── replication_test.cpp
│   │   ├── recovery_test.cpp
│   │   ├── snapshot_test.cpp
│   │   └── membership_test.cpp
│   └── chaos/
│       ├── leader_kill.toml
│       ├── partition.toml
│       ├── rolling_restart.toml
│       └── cascading.toml
├── bench/
│   ├── ycsb_workloads.toml
│   ├── results/
│   │   └── .gitkeep
│   └── plot.py
├── lin/                       # linearizability harness
│   ├── collect.cpp
│   └── porcupine_runner.go
├── deploy/
│   ├── docker-compose.yml
│   ├── Dockerfile.node
│   ├── Dockerfile.dashboard
│   └── grafana/
│       └── knot-dashboard.json
├── dashboard/                 # Next.js app
│   ├── package.json
│   ├── tsconfig.json
│   ├── tailwind.config.ts
│   ├── next.config.mjs
│   ├── app/
│   │   ├── layout.tsx
│   │   ├── page.tsx           # cluster topology
│   │   ├── logs/page.tsx
│   │   ├── bench/page.tsx
│   │   ├── chaos/page.tsx
│   │   └── storage/page.tsx
│   ├── components/
│   │   ├── ClusterGraph.tsx
│   │   ├── NodeCard.tsx
│   │   ├── LogStream.tsx
│   │   ├── BenchChart.tsx
│   │   ├── ChaosPanel.tsx
│   │   └── StoragePanel.tsx
│   ├── lib/
│   │   ├── ws.ts
│   │   ├── api.ts
│   │   └── store.ts
│   └── styles/
│       └── globals.css
└── gateway/                   # optional Node.js aggregator
    ├── package.json
    ├── src/
    │   ├── index.ts
    │   ├── aggregator.ts
    │   └── ws-hub.ts
    └── tsconfig.json
```

---

## 10. On-disk data layout (per node)

```
data/node-1/
├── raft/
│   ├── current_term
│   ├── voted_for
│   ├── log/
│   │   ├── 000001.log         # 64 MB segments
│   │   ├── 000002.log
│   │   └── ...
│   └── snapshots/
│       └── snap-000123.bin
├── storage/
│   ├── wal/
│   │   ├── 000001.wal
│   │   └── 000002.wal
│   ├── sst/
│   │   ├── L0/
│   │   ├── L1/
│   │   ├── L2/
│   │   ├── L3/
│   │   ├── L4/
│   │   ├── L5/
│   │   └── L6/
│   └── MANIFEST
└── meta/
    ├── node.id
    └── cluster.toml
```

---

## 11. Full TODO list (phased, ordered)

> **Phase 1 = 4-week core sprint (resume-ready).**
> **Phases 2–4 = stretch, post-sprint.**

### Phase 1 — Core sprint (Weeks 1–4, ~280 hrs)

#### Week 1 — Repo + storage engine standalone (70 hrs)

##### Day 1 (10 hrs) — Bootstrap
- [ ] Init repo, `.gitignore`, `LICENSE`, `README.md` skeleton
- [ ] CMake + CMakePresets.json + Ninja build
- [ ] vcpkg manifest with: boost, protobuf, spdlog, fmt, gtest
- [ ] `.clang-format`, `.clang-tidy`
- [ ] Hello-world `knotd` binary that logs via spdlog
- [ ] GitHub Actions CI: build + lint
- [ ] Define `proto/knot.proto` skeleton (empty messages)
- [ ] Set up `docs/journal.md` for daily notes

##### Day 2 (10 hrs) — WAL
- [ ] `include/knot/storage/wal.h` — interface
- [ ] `src/storage/wal.cpp` — append + fsync + sequential reader
- [ ] Segment rotation at 64 MB
- [ ] CRC32 per record
- [ ] Unit test: write N records, kill, reopen, verify
- [ ] Unit test: corruption detection
- [ ] First commit

##### Day 3 (10 hrs) — MemTable
- [ ] Concurrent skiplist (or `std::map` + `std::shared_mutex` for v1)
- [ ] PUT / GET / DELETE (tombstone) operations
- [ ] Size accounting
- [ ] Snapshot iterator for flush
- [ ] Unit tests

##### Day 4 (10 hrs) — SSTable writer
- [ ] Block builder (data block)
- [ ] Index block builder
- [ ] Bloom filter builder
- [ ] Footer encoding
- [ ] `SSTableWriter::Add(k, v)` / `Finish()`
- [ ] Unit test: write + read with raw reader

##### Day 5 (10 hrs) — SSTable reader
- [ ] Footer decoder
- [ ] Index block reader
- [ ] Data block reader (with optional snappy decompression)
- [ ] Bloom filter check
- [ ] `SSTableReader::Get(k)` and iterator
- [ ] Unit tests: 1M keys roundtrip

##### Day 6 (10 hrs) — Flush + MANIFEST
- [ ] Immutable MemTable queue
- [ ] Background flush thread
- [ ] MANIFEST append-only format
- [ ] Atomic MANIFEST rotation
- [ ] Recovery: replay MANIFEST on startup

##### Day 7 (10 hrs) — Engine integration + first benchmark
- [ ] `StorageEngine` class wiring WAL + MemTable + L0 SSTables + MANIFEST
- [ ] End-to-end PUT/GET/DELETE
- [ ] Single-node benchmark: 1M PUTs, 1M GETs
- [ ] Record results in `bench/results/week1.json`
- [ ] Plot via `bench/plot.py`
- [ ] Tag `v0.1-storage-engine`

#### Week 2 — Raft core, in-process (70 hrs)

##### Day 8 (10 hrs) — Raft scaffolding
- [ ] `LogEntry`, `Term`, `NodeId`, `PersistentState` types
- [ ] `RaftLog` with append, truncate, slice operations
- [ ] In-memory `Transport` interface (no network yet)
- [ ] 3 in-process `RaftNode` instances communicating via in-memory transport

##### Day 9 (10 hrs) — Election
- [ ] Election timer with randomized timeout
- [ ] `RequestVote` RPC handlers (sender + receiver)
- [ ] Follower → Candidate → Leader transitions
- [ ] Term updates on higher-term message
- [ ] Unit tests: single election, split vote, term increment

##### Day 10 (10 hrs) — Heartbeats
- [ ] `AppendEntries` (empty payload) heartbeat
- [ ] Leader heartbeat loop at 50 ms
- [ ] Follower resets election timer on valid AE
- [ ] Test: leader stays leader for 30 s

##### Day 11 (10 hrs) — Log replication
- [ ] Leader appends client commands
- [ ] `AppendEntries` with entries
- [ ] Follower applies and acknowledges
- [ ] Leader advances `matchIndex` and `commitIndex` on quorum
- [ ] Followers advance commit on next AE

##### Day 12 (10 hrs) — Log consistency
- [ ] `prevLogIndex/prevLogTerm` check on follower
- [ ] Conflict truncation
- [ ] Leader retry with decremented `nextIndex`
- [ ] Conflict-term optimization
- [ ] Unit tests: divergent logs converge

##### Day 13 (10 hrs) — Persistence
- [ ] `currentTerm`, `votedFor`, log persisted to disk
- [ ] fsync before responding to RPCs
- [ ] Restart node, recover state
- [ ] Integration test: kill + restart entire cluster, verify

##### Day 14 (10 hrs) — Real TCP transport
- [ ] Boost.Asio TCP server
- [ ] Protobuf serialization of RPCs
- [ ] Length-prefixed framing
- [ ] Per-peer connection with reconnect
- [ ] Swap in-memory transport for TCP transport
- [ ] 3 processes on localhost talk to each other
- [ ] Tag `v0.2-raft-core`

#### Week 3 — Replicated KV + snapshots + membership (70 hrs)

##### Day 15 (10 hrs) — State machine wiring
- [ ] Apply loop thread on each node
- [ ] Wire LSM engine as the state machine
- [ ] `Apply(LogEntry)` → PUT/GET/DELETE on engine
- [ ] Test: writes replicated to all 3 nodes

##### Day 16 (10 hrs) — Client RPC + CLI
- [ ] Client RPC: `Put`, `Get`, `Delete` (leader-only)
- [ ] `NotLeader{leaderHint}` redirect
- [ ] Client-side leader discovery + retry
- [ ] `knotctl` CLI binary
- [ ] Idempotency (client_id, seq_no) dedup cache

##### Day 17 (10 hrs) — Snapshots
- [ ] Snapshot trigger (every 10K entries)
- [ ] Full state dump to snapshot file
- [ ] `InstallSnapshot` RPC
- [ ] Followers install snapshot when behind
- [ ] Log truncation after snapshot

##### Day 18 (10 hrs) — Recovery from snapshot
- [ ] Restart-from-snapshot path
- [ ] Snapshot + WAL replay coordination
- [ ] Integration test: kill cluster, restart, verify state

##### Day 19 (10 hrs) — Joint-consensus membership
- [ ] `ConfChange` log entry type
- [ ] C_old,new transitional state
- [ ] Knot requires both old and new majorities during transition
- [ ] Add-node flow with log catch-up
- [ ] Remove-node flow

##### Day 20 (10 hrs) — Bug week, day 1
- [ ] Stress test cluster for 30 min mixed workload
- [ ] Hunt and fix bugs surfaced by stress
- [ ] Add regression tests

##### Day 21 (10 hrs) — Bug week, day 2
- [ ] Continue bug hunt
- [ ] Document fixed bugs in `docs/journal.md`
- [ ] Tag `v0.3-replicated-kv`

#### Week 4 — Bench + chaos + linearizability + dashboard + writeup (70 hrs)

##### Day 22 (10 hrs) — Benchmark harness
- [ ] `knot-bench` binary
- [ ] YCSB workloads A / B / C / Load
- [ ] HDR histogram for p50/p95/p99/p99.9
- [ ] JSON output + matplotlib plots
- [ ] Zipfian + uniform key distributions

##### Day 23 (10 hrs) — Run benchmarks, lock in numbers
- [ ] Benchmark 1, 3, 5 node clusters
- [ ] Save results to `bench/results/`
- [ ] Generate plots
- [ ] Update README with numbers
- [ ] **Resume bullets drafted with concrete numbers**

##### Day 24 (10 hrs) — Chaos engine
- [ ] `knot-chaos` binary + TOML scenarios
- [ ] `SIGKILL` leader, follower
- [ ] `iptables` partition scripts
- [ ] `tc` slow disk script
- [ ] Failover time measurement (kill → new leader → first commit)

##### Day 25 (10 hrs) — Leveled compaction
- [ ] L0 → L1 compaction worker
- [ ] Compaction picker (overlap minimization)
- [ ] MANIFEST atomic update
- [ ] Tombstone GC
- [ ] L1 → L2 → ... → L6 (size-tiered triggers)
- [ ] Verify read amplification drops

##### Day 26 (10 hrs) — Linearizability test
- [ ] Client history logger
- [ ] History dumper
- [ ] Go-based Porcupine runner
- [ ] Run 200+ randomized chaos scenarios
- [ ] CI integration

##### Day 27 (10 hrs) — Dashboard MVP
- [ ] Next.js app scaffolding
- [ ] WebSocket connection to gateway/nodes
- [ ] React Flow cluster topology
- [ ] Live log stream component
- [ ] Recharts benchmark plot from JSON
- [ ] Basic chaos controls (kill leader button)

##### Day 28 (10 hrs) — Docs + writeup + ship
- [ ] Polish README with architecture diagram (excalidraw → PNG)
- [ ] Write `docs/architecture.md`, `docs/raft-design.md`, `docs/storage-design.md`
- [ ] Record 60-second demo GIF
- [ ] Write blog post: "Building a Raft KV store in 4 weeks" with one debug-journey deep dive
- [ ] Cross-post on GitHub + dev.to / Medium / personal site
- [ ] Tag `v1.0`
- [ ] **Resume bullets finalized**

### Phase 2 — Dashboard polish + observability (Weeks 5–6)

- [ ] Dashboard: heartbeat animations
- [ ] Dashboard: per-follower replication state (matchIndex bars)
- [ ] Dashboard: storage state panel (MemTable size, SSTables per level)
- [ ] Dashboard: snapshot tracking
- [ ] Dashboard: failure injection panel with scenario library
- [ ] Prometheus `/metrics` endpoint refinement
- [ ] Pre-built Grafana dashboard JSON
- [ ] Bloom filter tuning
- [ ] Block cache LRU implementation
- [ ] Compression (snappy) for SSTable blocks

### Phase 3 — Advanced features (Weeks 7–10)

- [ ] **Multi-Raft sharding**: key range → Raft group mapping
- [ ] Shard router on client side
- [ ] Per-shard log + state machine
- [ ] **Distributed transactions**: 2PC over multiple Raft groups
- [ ] Transaction coordinator
- [ ] Lock manager per shard
- [ ] **Geo-replication simulation**: per-link latency injection
- [ ] **Binary replication protocol optimizations**: batching, pipelining
- [ ] Read-index optimization for linearizable reads
- [ ] Leader lease for relaxed-linearizable reads

### Phase 4 — Production hardening (Weeks 11–12)

- [ ] TLS for inter-node RPC
- [ ] Auth tokens for client RPC
- [ ] Automated nightly chaos suite in CI
- [ ] Continuous linearizability checking
- [ ] Memory leak verification (valgrind / ASan)
- [ ] Thread sanitizer pass (TSan)
- [ ] Performance profiling (perf, flamegraphs)
- [ ] Final blog post series (3–4 posts)
- [ ] Conference-style talk slides
- [ ] Demo video (3–5 min, narrated)

---

## 12. Testing strategy

| Layer | Tool | Coverage target |
|---|---|---|
| Unit | GoogleTest | 70%+ of `src/` |
| Integration | Custom cluster fixture | All Raft + KV happy paths |
| Chaos | `knot-chaos` + TOML | 10+ scenarios |
| Linearizability | Porcupine | 200+ randomized histories |
| Property | rapidcheck (optional) | Storage + Raft log invariants |
| Stress | Custom load gen | 1-hour continuous run |
| Build sanity | clang-tidy + ASan + TSan in CI | Zero warnings |

---

## 13. Deployment / demo

### 13.1 Local dev (single laptop)
- `docker-compose up` brings up 5-node cluster + gateway + dashboard
- Dashboard at `http://localhost:3000`
- CLI: `docker-compose exec node1 knotctl status`

### 13.2 Demo script (for interviews / presentations)
1. Start 5-node cluster.
2. Show dashboard topology + leader.
3. Run benchmark; show real-time throughput plot.
4. Kill leader; show failover in dashboard with elapsed time.
5. Add a 6th node; show log catch-up.
6. Run linearizability check; show "all histories linearizable."
7. Show storage state: MemTable flushes + SSTable compactions.

### 13.3 GitHub presentation
- Pinned repo on profile
- README with badges (CI status, license, language)
- Architecture diagram above the fold
- Benchmark results table
- 30-second demo GIF
- Links to blog post(s)

---

## 14. Resume narrative

### Bullet template (3 lines, FAANG-tier)

> **Knot** — Distributed KV store in C++ (Raft + LSM tree, ~12K LOC) · [github.com/akshat/knot]
> - Implemented Raft consensus (leader election, log replication, snapshotting, joint-consensus membership) over a 5-node TCP cluster; sustained **14.2K writes/sec at p99 1.8 ms** with **p99 leader failover of 1.4 s** under chaos tests.
> - Built custom LSM storage engine (WAL, concurrent skiplist MemTable, leveled SSTables L0–L6, bloom filters, block cache, leveled compaction) achieving **3.1× write throughput** vs naive B-tree baseline at 1M-key workload.
> - Verified linearizability via Porcupine over 200+ randomized network-partition and node-kill scenarios; recovered correctly from leader crash, follower crash, full cluster restart, and split-brain.

### Interview-ready talking points
- "Walk me through Raft" → live demo
- "How did you guarantee durability?" → WAL + fsync + persistent Raft state
- "How did you test correctness?" → linearizability + chaos
- "What was the hardest bug?" → blog post deep dive
- "How would you scale this?" → multi-Raft + sharding (Phase 3)

---

## 15. Risks & mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Raft bugs eat days | High | High | TDD with in-memory transport before networking; 2 dedicated bug days |
| LSM compaction complexity | Medium | Medium | Start with 2-level (L0 → L1), grow to L0–L6 in Week 4 |
| Dashboard time-sink | Medium | High | Dashboard is Day 27 only; CLI is the source of truth |
| Burnout at 8–10 hr/day | Medium | High | 7+ hrs sleep, daily journaling, weekly half-day off |
| C++ build times | Medium | Low | Ninja + precompiled headers + ccache |
| Networking flakiness in tests | Medium | Medium | In-memory transport tests catch 90% of bugs first |
| Phase 3 scope creep | High | Low | Phase 1 ships independently; Phases 2–4 are bonus |

---

## 16. Open questions / decisions to make

- [ ] **Final project name** — Knot, Helix, Stratus, Lattice, Aether, CrowKV, or other?
- [ ] **C++ standard** — C++17 (broader) or C++20 (coroutines, concepts)?
- [ ] **Gateway language** — pure C++ (Boost.Beast) or Node.js aggregator?
- [ ] **Compression** — ship snappy in v1 or defer to Phase 2?
- [ ] **License** — MIT, Apache 2.0, or BSL?
- [ ] **Where to publish blog post** — personal site, dev.to, Medium, Hashnode?

---

## 17. References

- Diego Ongaro, John Ousterhout — _In Search of an Understandable Consensus Algorithm (Extended Version)_ (Raft paper)
- Patrick O'Neil et al. — _The Log-Structured Merge-Tree_
- etcd source code — github.com/etcd-io/etcd
- RocksDB wiki — github.com/facebook/rocksdb/wiki
- Jepsen / Porcupine — anishathalye.com/porcupine
- _Designing Data-Intensive Applications_ — Martin Kleppmann

---

_End of PRD._
