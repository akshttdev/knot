# Architecture

> Companion doc to the top-level PRD. Defines the structural design, component boundaries, data flow, and threading model for Knot.

---

## 1. System overview

Knot is a distributed key-value database built in C++ with three vertically integrated layers per node:

1. **Networking layer** — async TCP via Boost.Asio, protobuf wire format
2. **Raft consensus layer** — replicated log + state machine FSM
3. **LSM storage engine** — WAL, MemTable, SSTables, compaction

All nodes run an identical stack. Clients talk to the leader (with automatic redirect from followers). A dashboard observes the cluster via a gateway aggregator.

---

## 2. Component diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                          QUORUM CLUSTER                              │
│                                                                       │
│   ┌─────────────┐      ┌─────────────┐      ┌─────────────┐         │
│   │   Node A    │◄────►│   Node B    │◄────►│   Node C    │  ...    │
│   │  (Leader)   │ Raft │ (Follower)  │ Raft │ (Follower)  │         │
│   └──────┬──────┘      └──────┬──────┘      └──────┬──────┘         │
│          │                    │                    │                  │
└──────────┼────────────────────┼────────────────────┼─────────────────┘
           │                    │                    │
           │   /metrics  /events (WebSocket)         │
           ▼                    ▼                    ▼
        ┌─────────────────────────────────────────────┐
        │       Gateway (Node.js or C++ Beast)        │
        │   Aggregates node state, fans out to UI     │
        └────────────────────┬────────────────────────┘
                             │ WebSocket / REST
                             ▼
                  ┌────────────────────┐
                  │   Next.js UI       │
                  │  Topology · Logs   │
                  │  Bench · Chaos     │
                  └────────────────────┘

           ┌─ Client side ──────────────┐
           │ knotctl CLI              │
           │ knot-bench               │
           │ knot-chaos               │
           └────────────────────────────┘
```

---

## 3. Per-node vertical stack

```
┌───────────────────────────────────────────────────────┐
│  Admin HTTP server (Boost.Beast)                      │
│    GET /status   GET /metrics   GET /members          │
│    WS  /events   POST /chaos/kill                     │
├───────────────────────────────────────────────────────┤
│  Raft RPC server (Boost.Asio TCP, protobuf)           │
│    RequestVote · AppendEntries · InstallSnapshot      │
├───────────────────────────────────────────────────────┤
│  Raft Node (FSM)                                      │
│    ├─ Election timer + heartbeat                      │
│    ├─ Log (segmented, on disk)                        │
│    ├─ Persistent state (currentTerm, votedFor)        │
│    ├─ Snapshot manager                                │
│    └─ Membership manager (joint consensus)            │
├───────────────────────────────────────────────────────┤
│  Apply loop (single thread)                           │
│    Consumes committed entries, applies to engine      │
├───────────────────────────────────────────────────────┤
│  LSM Storage Engine                                   │
│    ├─ WAL (append + fsync)                            │
│    ├─ MemTable (concurrent skiplist)                  │
│    ├─ Immutable MemTable queue                        │
│    ├─ Flush thread                                    │
│    ├─ SSTables L0..L6                                 │
│    ├─ Bloom filter per SSTable                        │
│    ├─ Block cache (LRU)                               │
│    ├─ Compaction thread (leveled)                     │
│    └─ MANIFEST                                        │
├───────────────────────────────────────────────────────┤
│  Observability                                        │
│    spdlog (JSON) · Prometheus exporter                │
└───────────────────────────────────────────────────────┘
```

---

## 4. Write path (PUT request)

```
Client ─► knotctl ─► TCP ─► Node (Leader)
                                  │
                                  ▼
                          Raft: append to log[]
                                  │
                          AppendEntries to followers
                                  │
                          fsync log on each node
                                  │
                          Knot ack ─► commitIndex++
                                  │
                                  ▼
                          Apply loop dequeues entry
                                  │
                                  ▼
                          StorageEngine.Put(k, v)
                                  │
                          WAL append + fsync
                                  │
                          MemTable insert
                                  │
                          (if MemTable > 4 MB)
                                  ▼
                          Background flush → L0 SSTable
                                  │
                          (if L0 > 4 files)
                                  ▼
                          Compactor merges L0 → L1 → ...
                                  │
                                  ▼
                          Response to client: OK
```

---

## 5. Read path (GET request)

```
Client ─► Node (Leader, or redirected from follower)
                │
                ▼
        Read-index check (optional linearizable read)
                │
                ▼
        StorageEngine.Get(k)
                │
                ├─► MemTable lookup
                ├─► Immutable MemTable(s) lookup
                ├─► For each L0 SSTable (newest first):
                │     ├─► Bloom filter check
                │     └─► Block cache → index block → data block
                └─► For levels L1..L6:
                      ├─► Binary search MANIFEST for overlapping SSTable
                      ├─► Bloom filter check
                      └─► Block cache → index block → data block
                │
                ▼
        Response: value | NotFound
```

---

## 6. Threading model

| Thread / pool | Count | Responsibility |
|---|---|---|
| Asio worker pool | `hardware_concurrency()` | Network I/O, RPC dispatch |
| Raft state thread | 1 | Election timer, log appends, commit advancement |
| Apply thread | 1 | Sequential state machine application |
| Flush thread | 1 | MemTable → L0 SSTable |
| Compaction thread | 1 | Background leveled compaction |
| Snapshot thread | 1 (on demand) | Create + install snapshots |
| Admin HTTP thread | 1 | `/metrics`, `/status`, WebSocket events |

**Concurrency primitives:**
- `std::mutex` for Raft state
- Lock-free skiplist (or `std::shared_mutex` + map) for MemTable
- `std::shared_ptr<const SSTable>` for SSTable readers (immutable)
- Per-peer `boost::asio::strand` for ordered async writes

---

## 7. Key design decisions

| Decision | Choice | Rationale |
|---|---|---|
| Language | C++17/20 | Systems-tier signal, control over memory + threads |
| Async I/O | Boost.Asio | Mature, async-native |
| Wire format | Protobuf | Schema versioning, fast, language-agnostic |
| Storage | Custom LSM | Demonstrates internals, write-optimized |
| Consensus | Raft (not Paxos) | Understandable, well-documented |
| Apply | Single-threaded | Linearizability via serialization |
| Snapshot | Full state dump (v1) | Simpler than incremental; revisit later |
| Membership | Joint consensus | Standard Raft §6, no surprises |
| Dashboard transport | WebSocket | Push-based, low-latency |
| Tests | TDD against in-memory transport | Decouple Raft from network bugs |

---

## 8. Configuration

Each node loads a TOML config at startup:

```toml
[node]
id = "node-1"
listen_addr = "0.0.0.0:7001"
admin_addr  = "0.0.0.0:8001"
data_dir    = "/var/lib/knot/node-1"

[cluster]
members = [
  { id = "node-1", addr = "127.0.0.1:7001" },
  { id = "node-2", addr = "127.0.0.1:7002" },
  { id = "node-3", addr = "127.0.0.1:7003" },
]

[raft]
election_timeout_ms_min = 150
election_timeout_ms_max = 300
heartbeat_interval_ms   = 50
snapshot_log_threshold  = 10000

[storage]
memtable_size_mb        = 4
l0_compaction_trigger   = 4
block_size_kb           = 4
bloom_bits_per_key      = 10
block_cache_mb          = 64
```

---

## 9. Failure handling

| Failure | Detection | Recovery |
|---|---|---|
| Leader crash | Heartbeat timeout on follower | New election within 150–300 ms |
| Follower crash | Leader sees AE failures | Leader retries; follower replays WAL on restart |
| Full cluster restart | Cold start | Each node replays Raft log + WAL + snapshot |
| Network partition | Heartbeat timeout | Majority side elects; minority side rejects writes |
| Disk full | fsync failure | Node refuses writes, logs error, eventually crashes |
| Corruption | CRC mismatch on read | Detected at boundary; record marked bad, recovery from peer |

---

## 10. Assessment criteria (architecture done = ✅)

- [ ] System diagram present in README
- [ ] Per-node stack documented in this file
- [ ] Write path & read path documented with timing
- [ ] Threading model documented, no race conditions in TSan run
- [ ] Configuration format finalized (TOML schema in repo)
- [ ] Every component listed has a matching `include/knot/*.h` header
- [ ] Failure modes enumerated in this doc

---

## 11. TODO — architecture phase

### Setup (Day 1)
- [ ] Finalize project name (Knot or alternative)
- [ ] Choose C++ standard (recommend C++20)
- [ ] Choose gateway language (recommend C++ Beast to keep stack uniform)
- [ ] Pick license (recommend MIT)
- [ ] Create repo, push initial scaffolding
- [ ] Add architecture diagram (excalidraw → `docs/img/arch.png`)

### Module boundaries (Day 1–2)
- [ ] Define `include/knot/common/types.h` (NodeId, Term, LogIndex, Status)
- [ ] Define `Result<T>` error type (no exceptions across module boundaries)
- [ ] Define `Slice` (string_view-like for keys/values)
- [ ] Define `Clock` interface (mockable for tests)

### Configuration (Day 1)
- [ ] Add `config.toml` parser (use `toml++` or `cpptoml`)
- [ ] Validate config at startup
- [ ] Print effective config in logs

### Observability scaffolding (Day 1)
- [ ] Init spdlog with JSON formatter
- [ ] Add `obs::metrics` registry (Counter, Gauge, Histogram)
- [ ] Wire `/metrics` HTTP endpoint with Prometheus text format
- [ ] Wire `/status` HTTP endpoint with current node state

### Cross-cutting (ongoing)
- [ ] Every public function in `include/knot/*` documented
- [ ] No `throw` across module boundaries — return `Result<T>` instead
- [ ] No globals (except logger singleton)
- [ ] Every spawned thread named via `pthread_setname_np`

---

## 12. Self-assessment checklist

Before declaring architecture complete:

- [ ] Can I sketch the diagram from memory? If not, simplify.
- [ ] Does each component have ONE reason to change? (SRP)
- [ ] Can I unit-test each module without spinning up the cluster?
- [ ] Does the threading model survive `ThreadSanitizer`?
- [ ] Is every disk write `fsync`'d where durability is claimed?
- [ ] Is every RPC bounded in size? (No unbounded vectors over the wire.)
- [ ] Is the config schema versioned?

---

_See `raft-design.md` for consensus internals and `storage-design.md` for the LSM engine._
