# Benchmarks

> Methodology, workloads, target numbers, how to run, and how to report results. These numbers become the resume bullets — treat with care.

---

## 1. Why this matters

Without numbers, "I built a distributed KV store" is interchangeable with every other student project. With numbers — measured, reproducible, plotted — it becomes a serious signal. The bench harness exists to produce defensible numbers that hold up to interviewer scrutiny.

**Golden rule:** every benchmark must be reproducible from the repo with a single command.

---

## 2. Methodology

### 2.1 Hardware spec (report alongside numbers)
- Machine type (e.g., "MacBook Air M2, 8 GB RAM, APFS")
- OS + kernel version
- Disk type (SSD vs HDD), filesystem
- CPU count
- Network setup (loopback / docker bridge / actual NICs)

### 2.2 Setup discipline
- **Warm-up phase**: discard first 30 s of measurements (JIT/cache warmup, OS page cache).
- **Single tenant**: stop all other processes, close browser, disable spotlight, etc.
- **Repeat 3 times**: report median ± stdev of repeated runs.
- **Fixed seeds**: workload generator uses a deterministic seed.
- **Stable build**: run same commit, same build flags (`Release`, `-O2`).
- **No debug logging** during benchmarks (compile with `SPDLOG_ACTIVE_LEVEL=info`).

### 2.3 Measurement
- **Latency** tracked with HDR histogram (`HdrHistogram_c`)
- **Throughput** computed as `ops_completed / wall_time` over the measurement window
- **Time source**: `std::chrono::steady_clock` (monotonic)

---

## 3. Workloads (YCSB-style)

| Workload | Read% | Write% | Distribution | Goal |
|---|---|---|---|---|
| **Load** | 0 | 100 | Sequential keys | Pure ingest throughput |
| **A** | 50 | 50 | Zipfian (θ=0.99) | Update-heavy realistic |
| **B** | 95 | 5 | Zipfian (θ=0.99) | Read-heavy realistic |
| **C** | 100 | 0 | Zipfian (θ=0.99) | Pure read (cache-friendly) |
| **D** | 95 | 5 (insert) | Latest | Insert-then-read-latest |

### Key/value sizes
- Default key: 16 bytes (random hex)
- Default value: 100 bytes
- Configurable via flags: `--key-size`, `--value-size`

### Dataset sizes
- Small: 100 K keys (fits in MemTable + L0)
- Medium: 1 M keys (triggers compaction)
- Large: 10 M keys (full multi-level storage)

---

## 4. Metrics tracked

| Metric | Unit | How |
|---|---|---|
| Throughput | ops/sec | `completed_ops / window_seconds` |
| Latency p50 | µs | HDR histogram |
| Latency p95 | µs | HDR histogram |
| Latency p99 | µs | HDR histogram |
| Latency p99.9 | µs | HDR histogram |
| Latency max | µs | HDR histogram |
| Failover time | ms | `kill_ts → new_leader_first_commit_ts` |
| Recovery time | s | `cold_start_ts → cluster_quorum_reached_ts` |
| Replication lag | ms | `leader_commit_ts(N) - follower_apply_ts(N)` |
| Write amp | ratio | `bytes_written_to_disk / bytes_written_by_user` |

---

## 5. Target numbers (NFR alignment)

These are the numbers you're aiming to publish.

| Workload | Cluster | Target throughput | Target p99 |
|---|---|---|---|
| Load | 1-node | ≥ 50 K ops/s | ≤ 5 ms |
| Load | 3-node | ≥ 15 K ops/s | ≤ 5 ms |
| Load | 5-node | ≥ 10 K ops/s | ≤ 5 ms |
| A (50/50) | 5-node | ≥ 12 K ops/s | ≤ 4 ms |
| B (95/5) | 5-node | ≥ 25 K ops/s | ≤ 2 ms |
| C (100% R) | 5-node | ≥ 50 K ops/s | ≤ 1 ms |

| Event | Target |
|---|---|
| Leader failover p99 | ≤ 2 s |
| Snapshot install (1 GB) | ≤ 30 s |
| Cold start recovery (1 GB log) | ≤ 60 s |

> These are MacBook-class numbers, not production-server numbers. Reporting them honestly with the hardware spec is more credible than inflated server-class claims.

---

## 6. Bench harness (`knot-bench`)

### CLI
```
knot-bench \
  --workload     A \
  --cluster      cfg/3node.toml \
  --keys         1000000 \
  --key-size     16 \
  --value-size   100 \
  --threads      16 \
  --duration     60s \
  --warmup       30s \
  --seed         42 \
  --output       bench/results/2026-05-20-A-3node.json
```

### Output JSON
```json
{
  "meta": {
    "commit": "a1b2c3d",
    "host": "MacBookAir-M2",
    "os": "Darwin 25.2.0",
    "cluster_size": 3,
    "workload": "A",
    "key_count": 1000000,
    "duration_s": 60,
    "warmup_s": 30,
    "threads": 16,
    "seed": 42
  },
  "throughput": {
    "ops_per_sec": 12450,
    "reads_per_sec": 6231,
    "writes_per_sec": 6219
  },
  "latency_us": {
    "read":  { "p50": 380, "p95": 1100, "p99": 1850, "p99_9": 4200, "max": 18000 },
    "write": { "p50": 720, "p95": 2300, "p99": 3900, "p99_9": 8400, "max": 41000 }
  },
  "replication_lag_ms": { "p50": 6, "p99": 22 }
}
```

### Plotting
`bench/plot.py` reads JSON files and emits PNGs:
- Throughput vs cluster size bar chart
- Latency CDF per workload
- Replication lag distribution
- Failover timeline

---

## 7. Failover benchmark

```bash
knot-chaos --scenario leader_kill --cluster cfg/5node.toml > failover.json
```

Procedure:
1. Start 5-node cluster, wait for leader.
2. Drive Workload A at 5 K ops/sec.
3. After 10 s, `SIGKILL` the leader.
4. Measure: (`kill_ts` → `new_leader_elected_ts` → `first_commit_after_election_ts`).
5. Repeat 20 times; report distribution.

---

## 8. Linearizability bench

```bash
knot-bench --workload A --record-history true --output history.log
porcupine_runner history.log
```

Procedure:
1. Run mixed workload with multiple clients.
2. Each client logs `(start_ts, end_ts, op, args, result)` to file.
3. Aggregate histories.
4. Pipe to Porcupine; expect "all histories linearizable" or counterexample.

Run under chaos (`knot-chaos --scenario partition` in parallel) to find subtle bugs.

---

## 9. Comparison baselines (optional, Phase 2+)

For credibility, run head-to-head benchmarks against:

| Baseline | Why |
|---|---|
| **etcd** (Go, Raft) | Industry-standard Raft KV; shows Knot is in the same ballpark |
| **RocksDB** standalone | Pure storage engine comparison (single-node) |
| **LevelDB** standalone | Simpler LSM comparison |
| **Naive map + WAL** | Lower bound to show LSM benefit |

Be honest: Knot will not beat etcd or RocksDB on optimized workloads. Report "within 2–3× of etcd on a fair single-laptop test" as a believable number.

---

## 10. Reporting format (README table)

```markdown
## Benchmark results

Hardware: MacBook Air M2, 8 GB RAM, APFS SSD · Build: Release `-O2`

| Workload | Cluster | Throughput (ops/s) | p50 (µs) | p99 (µs) | p99.9 (µs) |
|----------|---------|--------------------:|---------:|---------:|-----------:|
| Load     | 1-node  | 52,300              | 280      | 1,400    | 3,800      |
| Load     | 3-node  | 18,400              | 540      | 2,100    | 5,200      |
| Load     | 5-node  | 14,200              | 620      | 1,800    | 4,900      |
| A 50/50  | 5-node  | 12,800              | 480      | 1,950    | 4,400      |
| B 95/5   | 5-node  | 28,100              | 220      | 950      | 2,300      |
| C 100% R | 5-node  | 61,200              | 90       | 380      | 1,200      |

Leader failover (kill -9 leader, p99 across 20 trials): **1.42 s**
Linearizability: **passed on 247 randomized histories** (Porcupine)
```

---

## 11. TODO — bench phase (mapped to sprint days)

### Day 7 — First storage-engine numbers
- [ ] Implement minimal bench harness (single-threaded, write-only)
- [ ] Run Load workload at 1M keys
- [ ] Plot
- [ ] Save to `bench/results/week1-storage.json`

### Day 22 — Full harness
- [ ] Multi-threaded client driver
- [ ] All workloads A / B / C / D / Load
- [ ] Zipfian + uniform distributions
- [ ] HDR histogram integration
- [ ] JSON output

### Day 23 — Run the matrix
- [ ] 1-node Load
- [ ] 3-node Load + A + B + C
- [ ] 5-node Load + A + B + C
- [ ] Plot all (`bench/plot.py`)
- [ ] Save canonical results in `bench/results/v1/`
- [ ] Lock the numbers into README

### Day 24 — Failover + chaos numbers
- [ ] Failover benchmark (20 trials)
- [ ] Partition + heal test
- [ ] Cold-start recovery measurement

### Day 26 — Linearizability
- [ ] History collection
- [ ] Porcupine runner integration
- [ ] Run under chaos
- [ ] Report passing scenarios in README

---

## 12. Assessment criteria — bench done = ✅

- [ ] Every NFR target met or honestly reported with reason
- [ ] All results reproducible: `make bench` produces same JSON shape
- [ ] Hardware + build flags recorded in every JSON
- [ ] Plots committed to repo (`docs/img/bench/`)
- [ ] README table populated with real numbers
- [ ] Linearizability check passes on 200+ histories
- [ ] Failover measurement repeatable

---

## 13. Resume bullets — derived from these results

```
- Sustained {THROUGHPUT_5NODE_LOAD} writes/sec on a {CLUSTER_SIZE}-node cluster
  with p99 {P99_WRITE} ms, measured under YCSB Workload A on commodity hardware.

- Achieved p99 leader failover of {FAILOVER_P99} s across {TRIALS} SIGKILL trials
  using randomized election timeouts (150–300 ms) and pipelined AppendEntries.

- Verified linearizability via Porcupine on {SCENARIO_COUNT}+ randomized
  network-partition and node-kill scenarios; zero divergence observed.
```

Fill in the bracketed values from your final `bench/results/v1/*.json` files.

---

## 14. Honesty checklist (don't lie on the resume)

- [ ] Numbers come from a real run, not estimated
- [ ] Hardware is disclosed
- [ ] Cluster size matches what's claimed
- [ ] Latency is end-to-end (client → leader → quorum → response), not just internal
- [ ] No cherry-picked single-run outliers
- [ ] Failover number is p99, not best-case
- [ ] Linearizability passes on real chaos, not happy-path only

If an interviewer asks "how did you measure this?" you should be able to answer in 60 seconds and point to a file in the repo.

---

_See `architecture.md` for component overview and `raft-design.md` / `storage-design.md` for what's being measured._
