# Phases & Roadmap

> Phased breakdown of the Knot project. Phase 1 is the 4-week core sprint that makes the project resume-ready. Phases 2–4 extend it into a portfolio-grade distributed systems platform.

---

## Phase summary

| Phase | Duration | Total hrs | Goal | Exit milestone |
|---|---|---|---|---|
| **Phase 0** — Decisions | 1 day | 4–6 | Lock all open decisions, set up tooling | Repo initialized, decisions documented |
| **Phase 1** — Core sprint | 4 weeks | ~280 | Ship resume-ready v1 | Tag `v1.0`, blog post live |
| **Phase 2** — Dashboard + observability | 2 weeks | ~80 | Production-feeling demo | Tag `v1.1`, demo video |
| **Phase 3** — Advanced features | 4 weeks | ~160 | Multi-Raft, transactions, geo-sim | Tag `v2.0` |
| **Phase 4** — Production hardening | 2 weeks | ~80 | TLS, auth, CI chaos, profiling | Tag `v2.1`, talk slides |
| **Total** | ~12 weeks | ~600 | Distributed systems platform | Ship-quality, talk-quality |

Every phase has independent shipping value. If life intervenes after Phase 1, the resume is already strong.

---

# Phase 0 — Decisions & setup (Day 0)

### Goal
Lock the 6 open decisions from the PRD, scaffold the repo, set up tooling so Day 1 starts at 100%.

### TODO
- [ ] Finalize project name (Knot, Helix, Stratus, Lattice, Aether, CrowKV)
- [ ] Choose C++ standard (recommend C++20)
- [ ] Choose gateway language (recommend C++ Beast for stack uniformity)
- [ ] Pick license (recommend MIT)
- [ ] Decide v1 compression (recommend defer to Phase 2)
- [ ] Pick blog platform (personal site / dev.to / Hashnode)
- [ ] `git init`, push to GitHub (public)
- [ ] Add `.gitignore`, `LICENSE`, `README.md` skeleton, `CODE_OF_CONDUCT.md` (optional)
- [ ] Set up branch protection on `main` (require CI pass)
- [ ] Install toolchain locally: clang-17+, CMake 3.20+, Ninja, vcpkg
- [ ] IDE config: `.vscode/` or `.clangd` for completion
- [ ] Create `docs/journal.md` for daily notes

### Exit criteria — Phase 0 done ✅
- [ ] All 6 decisions documented in PRD section 16
- [ ] Empty repo pushed to GitHub
- [ ] Local toolchain builds a hello-world C++20 binary

---

# Phase 1 — Core sprint (Weeks 1–4)

### Goal
Ship a working, benchmarked, crash-tolerant, linearizable distributed KV store with a minimal dashboard and blog post — **resume-ready in 4 weeks.**

### Hours
~280 (8–10 hrs/day × 28 days)

---

## Week 1 — Storage engine standalone (70 hrs)

### Goal
Single-node LSM engine: PUT/GET/DELETE survives crash, has first benchmark numbers.

### TODO

**Day 1 — Bootstrap (10 hrs)**
- [ ] Repo, CMake, vcpkg manifest (boost, protobuf, spdlog, fmt, gtest)
- [ ] `.clang-format`, `.clang-tidy`
- [ ] `knotd` hello-world binary
- [ ] GitHub Actions CI: build + lint
- [ ] `proto/knot.proto` skeleton

**Day 2 — WAL (10 hrs)**
- [ ] `wal.h` interface
- [ ] Append + fsync + sequential read
- [ ] CRC32 per record, 64 MB segment rotation
- [ ] Unit tests: write, crash, replay, corruption detection

**Day 3 — MemTable (10 hrs)**
- [ ] Skiplist (or `std::map` + RWMutex)
- [ ] PUT/GET/DELETE with tombstones
- [ ] Size accounting
- [ ] Snapshot iterator for flush
- [ ] Unit tests

**Day 4 — SSTable writer (10 hrs)**
- [ ] Block builder with restart points
- [ ] Index block builder
- [ ] Bloom filter builder (10 bits/key)
- [ ] Footer encoding
- [ ] `SSTableWriter::Add` / `Finish`

**Day 5 — SSTable reader (10 hrs)**
- [ ] Footer decoder
- [ ] Index block reader, data block reader
- [ ] Bloom filter check
- [ ] `Get` + iterator
- [ ] 1M-key roundtrip test

**Day 6 — Flush + MANIFEST (10 hrs)**
- [ ] Immutable MemTable queue
- [ ] Background flush thread
- [ ] MANIFEST append-only format, atomic rotation
- [ ] Recovery: MANIFEST replay on startup

**Day 7 — Engine integration + first numbers (10 hrs)**
- [ ] `StorageEngine` class wiring WAL + MemTable + L0 SSTables + MANIFEST
- [ ] End-to-end PUT/GET/DELETE
- [ ] Single-node benchmark: 1M PUTs + 1M GETs
- [ ] Save `bench/results/week1-storage.json`
- [ ] Plot via `bench/plot.py`
- [ ] **Tag `v0.1-storage-engine`**

### Exit criteria — Week 1 ✅
- [ ] Engine handles 1M PUTs at ≥ 50 K ops/sec single-node
- [ ] Engine survives `kill -9` mid-write, recovers correctly
- [ ] All unit tests pass
- [ ] First numbers committed to `bench/results/`

---

## Week 2 — Raft core, in-process (70 hrs)

### Goal
3-node Raft cluster: election, log replication, persistent state, TCP transport.

### TODO

**Day 8 — Scaffolding (10 hrs)**
- [ ] `LogEntry`, `Term`, `NodeId`, `PersistentState` types
- [ ] `RaftLog` (append, truncate, slice, lastIndex, term)
- [ ] `Transport` interface
- [ ] `InMemoryTransport` for 3-node in-process tests
- [ ] `RaftNode` skeleton

**Day 9 — Election (10 hrs)**
- [ ] Randomized election timer (150–300 ms)
- [ ] `RequestVote` handlers (sender + receiver)
- [ ] FSM transitions Follower ↔ Candidate ↔ Leader
- [ ] Persist `currentTerm`, `votedFor` with fsync
- [ ] Unit tests: single election, split vote, term increment

**Day 10 — Heartbeats (10 hrs)**
- [ ] Leader heartbeat loop at 50 ms
- [ ] Empty `AppendEntries`
- [ ] Follower resets election timer on valid AE
- [ ] Test: leader stays leader for 30 s

**Day 11 — Log replication (10 hrs)**
- [ ] Leader appends client commands
- [ ] `AppendEntries` with entries
- [ ] Follower applies + acks
- [ ] Leader advances `matchIndex[]`, `commitIndex`
- [ ] Apply loop consumes committed entries

**Day 12 — Log consistency (10 hrs)**
- [ ] `prevLogIndex/prevLogTerm` check
- [ ] Truncation on conflict
- [ ] `conflict_index/conflict_term` optimization
- [ ] Unit tests: divergent logs converge

**Day 13 — Persistence (10 hrs)**
- [ ] Disk format for Raft state
- [ ] Log segments with CRC
- [ ] fsync discipline
- [ ] Crash recovery test

**Day 14 — Real TCP transport (10 hrs)**
- [ ] Asio TCP server + client
- [ ] Protobuf framing (length-prefixed)
- [ ] Per-peer connection with reconnect
- [ ] Swap `InMemoryTransport` → `TcpTransport`
- [ ] 3 processes on localhost talk
- [ ] **Tag `v0.2-raft-core`**

### Exit criteria — Week 2 ✅
- [ ] 3-node cluster elects leader within 500 ms of start
- [ ] PUT replicates to all followers within 100 ms
- [ ] State survives full cluster restart
- [ ] No divergence under 30-min stress

---

## Week 3 — Replicated KV + snapshots + membership (70 hrs)

### Goal
Wire LSM engine as state machine; add snapshots, joint-consensus membership; bug-week to harden.

### TODO

**Day 15 — State machine wiring (10 hrs)**
- [ ] Apply loop thread per node
- [ ] LSM engine = state machine
- [ ] `Apply(LogEntry)` → PUT/GET/DELETE on engine
- [ ] Replication test across 3 nodes

**Day 16 — Client RPC + CLI (10 hrs)**
- [ ] Client RPC: `Put`, `Get`, `Delete` (leader-only)
- [ ] `NotLeader{leaderHint}` redirect
- [ ] Client-side leader discovery + retry
- [ ] `knotctl` CLI
- [ ] Idempotency cache (`client_id`, `seq_no`)

**Day 17 — Snapshots (10 hrs)**
- [ ] Snapshot trigger (10K entries)
- [ ] Full state dump
- [ ] `InstallSnapshot` RPC
- [ ] Follower installs when behind
- [ ] Log truncation post-snapshot

**Day 18 — Recovery from snapshot (10 hrs)**
- [ ] Restart-from-snapshot path
- [ ] Snapshot + WAL replay coordination
- [ ] Integration test

**Day 19 — Joint-consensus membership (10 hrs)**
- [ ] `ConfChange` log entry type
- [ ] C_old,new transitional state
- [ ] Dual-majority enforcement
- [ ] Add-node + remove-node flows
- [ ] Leader step-down on removal

**Day 20 — Bug week, day 1 (10 hrs)**
- [ ] 30-min stress test
- [ ] Hunt + fix bugs
- [ ] Regression tests

**Day 21 — Bug week, day 2 (10 hrs)**
- [ ] Continue bug hunt
- [ ] Document fixes in journal
- [ ] **Tag `v0.3-replicated-kv`**

### Exit criteria — Week 3 ✅
- [ ] 3-node cluster runs 1 hour mixed workload without divergence
- [ ] Snapshot install works on lagging follower
- [ ] Add-node + remove-node verified
- [ ] All known bugs fixed or filed

---

## Week 4 — Bench + chaos + linearizability + dashboard + writeup (70 hrs)

### Goal
Lock in numbers, prove correctness, ship the dashboard MVP and blog post.

### TODO

**Day 22 — Benchmark harness (10 hrs)**
- [ ] `knot-bench` binary
- [ ] YCSB workloads A / B / C / Load
- [ ] HDR histogram (p50/p95/p99/p99.9)
- [ ] JSON output + matplotlib plots
- [ ] Zipfian + uniform key distributions

**Day 23 — Run benchmarks (10 hrs)**
- [ ] Benchmark 1, 3, 5 node clusters
- [ ] Save to `bench/results/v1/`
- [ ] Generate plots
- [ ] Update README table
- [ ] **Draft resume bullets with real numbers**

**Day 24 — Chaos engine (10 hrs)**
- [ ] `knot-chaos` + TOML scenarios
- [ ] SIGKILL leader / follower
- [ ] `iptables` partition script
- [ ] `tc` slow disk script
- [ ] Failover time measurement

**Day 25 — Leveled compaction (10 hrs)**
- [ ] L0 → L1 compaction worker
- [ ] Compaction picker (overlap minimization)
- [ ] Atomic MANIFEST update
- [ ] Tombstone GC at deepest level
- [ ] L1 → ... → L6 (size-tiered triggers)

**Day 26 — Linearizability test (10 hrs)**
- [ ] Client history logger
- [ ] Go-based Porcupine runner
- [ ] Run 200+ randomized scenarios under chaos
- [ ] CI integration

**Day 27 — Dashboard MVP (10 hrs)**
- [ ] Next.js scaffolding
- [ ] WebSocket connection to gateway
- [ ] React Flow cluster topology
- [ ] Live log stream component
- [ ] Recharts benchmark plot from JSON
- [ ] Basic chaos controls (kill-leader button)

**Day 28 — Docs + writeup + ship (10 hrs)**
- [ ] README polish + architecture diagram
- [ ] `docs/architecture.md`, `raft-design.md`, `storage-design.md` finalized
- [ ] 60-sec demo GIF
- [ ] Blog post: "Building a Raft KV store in 4 weeks" + one debug-journey deep dive
- [ ] Cross-post (GitHub + chosen blog platform)
- [ ] **Tag `v1.0`**
- [ ] **Finalize resume bullets**

### Exit criteria — Phase 1 ✅
- [ ] All 8 NFR targets met or honestly reported
- [ ] Linearizability: ≥ 200 scenarios pass Porcupine
- [ ] Leader failover p99 ≤ 2 s
- [ ] README has architecture diagram + bench table + demo GIF
- [ ] Blog post live with permalink
- [ ] GitHub repo public, pinned, CI passing
- [ ] Resume bullets drafted and reviewed

---

# Phase 2 — Dashboard polish + observability (Weeks 5–6, ~80 hrs)

### Goal
Take the MVP dashboard and observability layer from "works" to "demo-worthy." This is what makes the project memorable in interviews.

### Hours
~40 hrs/week × 2

### TODO

**Week 5 — Dashboard polish**
- [ ] Heartbeat animations between nodes (React Flow edge animations)
- [ ] Per-follower replication state (matchIndex progress bars)
- [ ] Storage panel: MemTable size gauge, SSTable count per level, compaction in-flight indicator
- [ ] Snapshot tracking: last snapshot index per node, install progress
- [ ] Term ribbon: visualize term changes over time
- [ ] Failure injection UI: scenario library (kill leader, partition X-Y, slow disk on X)
- [ ] Cluster timeline view: elections, commits, snapshots over time
- [ ] Mobile-responsive layout
- [ ] Dark mode

**Week 6 — Observability layer**
- [ ] Refine Prometheus `/metrics` (raft_term, raft_commit_index, raft_apply_lag, storage_memtable_bytes, storage_sst_files{level}, storage_compaction_seconds, rpc_latency_seconds)
- [ ] Pre-built Grafana dashboard JSON (`deploy/grafana/knot-dashboard.json`)
- [ ] Docker compose includes Prometheus + Grafana
- [ ] Structured log enrichment (trace IDs across replicate → apply)
- [ ] Bloom filter performance tuning + measurement
- [ ] Block cache LRU implementation + hit rate metric
- [ ] Snappy block compression (optional but recommended now)
- [ ] Demo video recording (3–5 min narrated walkthrough)

### Exit criteria — Phase 2 ✅
- [ ] Dashboard shows live failover in real time, smoothly
- [ ] Grafana dashboard renders all metrics
- [ ] Block cache hit rate ≥ 80 % on workload C
- [ ] Snappy reduces SSTable size by ≥ 40 %
- [ ] Demo video uploaded to YouTube (unlisted is fine)
- [ ] **Tag `v1.1`**

---

# Phase 3 — Advanced features (Weeks 7–10, ~160 hrs)

### Goal
Move from "single Raft group" to "production-shape": sharding, transactions, geo-replication simulation, performance optimizations.

### Hours
~40 hrs/week × 4

### TODO

## Week 7 — Multi-Raft sharding

- [ ] Shard manager: key range → Raft group mapping
- [ ] Per-shard log + state machine
- [ ] Per-shard Raft instance (multiple `RaftNode` objects per process)
- [ ] Shard router on client side
- [ ] Cross-shard request handling
- [ ] Shard rebalancing API (manual for v1)
- [ ] Tests: 4 shards × 3 nodes = 12 Raft groups in 3 processes
- [ ] Benchmark: aggregate throughput across shards

## Week 8 — Distributed transactions (2PC)

- [ ] Transaction coordinator service
- [ ] Per-shard lock manager
- [ ] 2-phase commit protocol (prepare + commit/abort)
- [ ] Transaction log entries in Raft
- [ ] Deadlock detection (wait-for graph) — simple version
- [ ] Client API: `BeginTxn / Put / Get / Commit / Abort`
- [ ] Tests: atomic multi-key updates across shards
- [ ] Linearizability extended to transactions

## Week 9 — Geo-replication simulation

- [ ] Configurable per-link latency injection
- [ ] Region tagging on nodes
- [ ] Knot-aware routing (read from local region)
- [ ] Leader placement policy (region preference)
- [ ] Tests: simulate 3-region cluster (US, EU, ASIA) with 100 ms inter-region latency
- [ ] Benchmark: read-local latency vs cross-region write latency

## Week 10 — Performance optimizations

- [ ] Pipelined AppendEntries (don't wait for ack before sending next batch)
- [ ] Batched fsync (group commit) tuning
- [ ] Read-index optimization for linearizable reads
- [ ] Leader lease for relaxed-linearizable reads
- [ ] Asynchronous log writes (decouple from RPC thread)
- [ ] Profiling pass with `perf` (Linux) or Instruments (macOS)
- [ ] Flamegraphs in `docs/perf/`
- [ ] Throughput target: 2× Phase 1 numbers

### Exit criteria — Phase 3 ✅
- [ ] Multi-Raft cluster sustains aggregate ≥ 30 K writes/sec
- [ ] 2PC transactions pass linearizability under chaos
- [ ] Geo-sim demonstrates region-aware routing benefits
- [ ] Optimizations yield ≥ 1.5× Phase 1 throughput
- [ ] **Tag `v2.0`**

---

# Phase 4 — Production hardening (Weeks 11–12, ~80 hrs)

### Goal
Take Knot from "portfolio project" to "would survive a thoughtful interviewer's pressure questions about production-readiness."

### Hours
~40 hrs/week × 2

### TODO

## Week 11 — Security + correctness hardening

- [ ] TLS for inter-node RPC (mutual TLS)
- [ ] Cert generation script
- [ ] Auth tokens for client RPC (HMAC-signed)
- [ ] Rate limiting on client API
- [ ] AddressSanitizer (ASan) full run, fix all findings
- [ ] ThreadSanitizer (TSan) full run, fix all findings
- [ ] UndefinedBehaviorSanitizer (UBSan) full run
- [ ] Valgrind on full integration test suite
- [ ] Fuzz the protobuf decoder (libFuzzer)
- [ ] Fuzz the SSTable reader

## Week 12 — CI chaos + writeup + talk

- [ ] Nightly chaos test suite in GitHub Actions
- [ ] Continuous linearizability check on `main`
- [ ] Performance regression detection (alert on > 10 % drop)
- [ ] Three-post blog series:
  - [ ] "Designing a custom LSM tree"
  - [ ] "Implementing Raft from scratch"
  - [ ] "Verifying correctness with Porcupine"
- [ ] Conference-style talk slides (30 min)
- [ ] Final demo video (10 min, narrated, scripted)
- [ ] Talk preview to a peer or mentor for feedback
- [ ] Apply talk to local meetups / online conferences

### Exit criteria — Phase 4 ✅
- [ ] Zero ASan / TSan / UBSan findings
- [ ] Nightly chaos suite green for 7 consecutive days
- [ ] Three blog posts published
- [ ] Talk slides committed
- [ ] Final demo video uploaded
- [ ] **Tag `v2.1`**

---

# Phase X — Stretch / Future Work

Beyond the planned 12 weeks. Only attempt after Phase 4 if interest persists.

- [ ] WASM client (run `knotctl` in the browser)
- [ ] Persistent in-memory mode (DRAM-only with snapshot to disk)
- [ ] Time-travel queries (versioned reads)
- [ ] Range scans + iterators in client API
- [ ] Secondary indexes
- [ ] Compaction throttling under load
- [ ] Snapshot streaming (incremental, not full)
- [ ] Learner / non-voting replicas
- [ ] Witness nodes (vote-only, no log)
- [ ] Pre-vote optimization (paper §9.6)
- [ ] CheckKnot optimization
- [ ] Leadership transfer extension
- [ ] Pluggable storage backend (BadgerDB-compatible)
- [ ] gRPC instead of raw protobuf

---

# Hour budget summary

| Phase | Weeks | Hrs/week | Total hrs | Cumulative |
|---|---|---|---|---|
| Phase 0 | 0.2 | — | 5 | 5 |
| Phase 1 | 4 | 70 | 280 | 285 |
| Phase 2 | 2 | 40 | 80 | 365 |
| Phase 3 | 4 | 40 | 160 | 525 |
| Phase 4 | 2 | 40 | 80 | 605 |
| **Total** | **12.2** |  | **605 hrs** | |

---

# Progress tracking

Update this checklist as phases complete:

- [ ] Phase 0 — Decisions & setup
- [ ] Phase 1 — Core sprint (Week 1)
- [ ] Phase 1 — Core sprint (Week 2)
- [ ] Phase 1 — Core sprint (Week 3)
- [ ] Phase 1 — Core sprint (Week 4) ← **resume-ready milestone**
- [ ] Phase 2 — Dashboard + observability
- [ ] Phase 3 — Multi-Raft sharding
- [ ] Phase 3 — Distributed transactions
- [ ] Phase 3 — Geo-replication sim
- [ ] Phase 3 — Performance optimizations
- [ ] Phase 4 — Security + correctness hardening
- [ ] Phase 4 — CI chaos + writeup + talk
- [ ] Phase X — Stretch

---

# Decision points & off-ramps

If you must stop early, here are the natural stopping points and what the project still demonstrates:

| Stop after | Resume signal |
|---|---|
| **Phase 1** | Distributed KV with Raft + LSM + benchmarks + linearizability. Strong FAANG-tier project. |
| **Phase 2** | Same + production-feeling dashboard + observability. Demo-worthy. |
| **Phase 3** | Same + multi-Raft + transactions + geo-sim. Senior-level systems portfolio. |
| **Phase 4** | Same + hardened + talked-about. Conference-ready, principal-engineer-tier portfolio. |

Each off-ramp leaves a tagged release and a clean README. Nothing is wasted if you stop.

---

# Risks per phase

| Phase | Top risk | Mitigation |
|---|---|---|
| Phase 1 | Raft bugs eat days 20–21 | Bug-week buffer already built in |
| Phase 1 | Burnout at 8–10 hr/day | 7+ hrs sleep, daily journal, weekly half-day off |
| Phase 2 | Dashboard scope creep | Hard cap at 2 weeks; cut features not visuals |
| Phase 3 | Multi-Raft is hard | Single-shard remains shippable; multi-shard is bonus |
| Phase 3 | Transactions are very hard | Document limitations honestly; partial impl ships |
| Phase 4 | Sanitizer findings could be deep | Budget 50% of week 11 just for fixes |

---

_See `architecture.md`, `raft-design.md`, `storage-design.md`, `benchmarks.md` for per-component design and TODOs that the phase TODOs reference._
