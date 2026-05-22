# Storage Engine Design (LSM Tree)

> Detailed design of the LSM-tree storage engine that backs the replicated state machine. Read alongside `architecture.md` and `raft-design.md`.

---

## 1. Why LSM (not B-tree)?

| Property | LSM | B-tree |
|---|---|---|
| Write throughput | High (sequential I/O) | Lower (random I/O on splits) |
| Write amplification | Higher (compaction) | Lower |
| Read amplification | Higher (multiple levels) | Lower |
| Space amplification | Higher (tombstones, duplicates) | Lower |
| Compression | Excellent (block-based) | Moderate |
| Crash recovery | Simple (WAL replay) | Complex (page-level recovery) |

Knot is a **replicated, write-heavy system** — LSM wins. The engineering depth of LSM internals (WAL, MemTable, SSTables, compaction, bloom filters, block cache) is also where the resume signal lives.

---

## 2. Component overview

```
                   ┌──────────────────────┐
        PUT/DEL ──►│         WAL          │── fsync
                   └──────────┬───────────┘
                              │
                              ▼
                   ┌──────────────────────┐
                   │      MemTable        │  (active, mutable)
                   │   concurrent skiplist│
                   └──────────┬───────────┘
                              │  size > 4 MB
                              ▼
                   ┌──────────────────────┐
                   │ Immutable MemTable(s)│  (read-only, queued)
                   └──────────┬───────────┘
                              │
                  flush thread│
                              ▼
                   ┌──────────────────────┐
                   │   L0 SSTables        │  (overlapping keys allowed)
                   └──────────┬───────────┘
                              │  count > 4
                  compactor   ▼
                   ┌──────────────────────┐
                   │   L1 SSTables        │  (sorted, no overlap)
                   └──────────┬───────────┘
                              │
                              ▼ ... up to L6
                   ┌──────────────────────┐
                   │   L6 SSTables        │
                   └──────────────────────┘
```

Plus:
- **Block cache** (LRU) — caches decompressed data blocks
- **Bloom filter** per SSTable — fast negative lookups
- **MANIFEST** — atomic record of live SSTables per level

---

## 3. Write-Ahead Log (WAL)

### 3.1 Purpose
Recover MemTable state after a crash. Every mutation goes to WAL **before** MemTable.

### 3.2 Record format

```
┌──────────┬──────────┬──────────┬─────────────┐
│ CRC32(4) │ Len (4)  │ Type (1) │ Payload (N) │
└──────────┴──────────┴──────────┴─────────────┘
```

- `Type` ∈ { FULL, FIRST, MIDDLE, LAST } for records spanning multiple 32 KB blocks.
- Payload format: `[op_type (1B)][klen (4B)][key][vlen (4B)][value]`. DELETE has no value.

### 3.3 Segment rotation
- New segment every 64 MB
- Old segments deleted only after the MemTable they protect is flushed
- Segment filename: `{epoch}-{seq}.wal`

### 3.4 fsync policy

| Mode | When | Latency | Durability |
|---|---|---|---|
| `sync` | Every PUT | High | Strongest |
| `batch` (default) | Group commit, every N PUTs or T ms | Medium | Loses ≤ T ms on crash |
| `none` | Never (write-back only) | Low | Loses up to OS flush window |

Default: **batch** (group commit). PUT response held until WAL fsync completes.

### 3.5 Recovery
1. Read MANIFEST → know last flushed MemTable's WAL seq
2. Open all WAL segments after that seq
3. Replay records into new MemTable
4. Validate CRC; truncate at first corruption

---

## 4. MemTable

### 4.1 Data structure
- **Concurrent skiplist** (preferred) — lock-free reads, lock-free single-writer inserts
- Fallback for v1: `std::map<Slice, Slice>` + `std::shared_mutex`

### 4.2 Operations
```cpp
class MemTable {
    void Put(Slice key, Slice value);
    void Delete(Slice key);                    // inserts tombstone
    std::optional<Slice> Get(Slice key) const; // tombstone returns "not found"
    size_t ApproxSize() const;
    std::unique_ptr<Iterator> NewIterator() const;
};
```

### 4.3 Lifecycle
1. Active MemTable receives writes.
2. On size threshold (`memtable_size_mb = 4`), engine creates a new active MemTable and marks the old one **immutable**.
3. Flush thread converts immutable MemTable → L0 SSTable, then drops the MemTable and its WAL segment.

### 4.4 Tombstones
DELETE writes a tombstone (entry with empty value + `op=DEL`). Tombstones survive through compaction levels until they meet the **deepest level**, at which point they can be safely dropped.

---

## 5. SSTable (Sorted String Table)

### 5.1 File layout

```
┌──────────────────────────────┐  offset 0
│ Data block 1 (~4 KB)         │
│ Data block 2                 │
│ ...                          │
│ Data block N                 │
├──────────────────────────────┤
│ Bloom filter block           │
├──────────────────────────────┤
│ Index block                  │
├──────────────────────────────┤
│ Metadata block               │
├──────────────────────────────┤
│ Footer (48 bytes, fixed)     │
│   magic (8) | bloom_off (8)  │
│   index_off (8) | meta_off(8)│
│   version (4) | crc (4)      │
└──────────────────────────────┘  EOF
```

### 5.2 Data block

Restart-point compressed (LevelDB-style prefix compression):

```
[Entry 0]
  shared_len(0) | non_shared_len | vlen | key | value
[Entry 1]
  shared_len   | non_shared_len | vlen | key_suffix | value
...
[Restart point 0 offset]
[Restart point 1 offset]
...
[Restart count]
```

Default block size: **4 KB**. Optional Snappy compression on the block before write.

### 5.3 Index block

One entry per data block:
```
first_key_of_block | block_offset | block_length
```

Sorted; binary searchable.

### 5.4 Bloom filter block

Per-SSTable Bloom filter, default **10 bits/key** (≈1% false positive rate).
Stored as: `[num_hashes (1B)][bits (variable)]`.

### 5.5 Metadata block

```
min_key | max_key | entry_count | tombstone_count |
created_at | level | seq_no_range
```

### 5.6 Footer

Fixed 48 bytes at end of file. Magic number `0xDB57AB1E1234ABCD`.

### 5.7 SSTableReader operations

```cpp
class SSTableReader {
    std::optional<Slice> Get(Slice key);
    std::unique_ptr<Iterator> NewIterator();
    bool MayContain(Slice key) const;  // bloom filter
    Slice MinKey() const;
    Slice MaxKey() const;
};
```

`Get(key)`:
1. `MayContain(key)` — bloom filter, may return false → NotFound immediately
2. Binary search index block → candidate data block
3. Block cache lookup → hit or read from disk
4. Linear scan within block (restart points accelerate)

---

## 6. Block cache

- LRU eviction
- Keyed by `(file_id, block_offset)`
- Stores decompressed block payload
- Default size: `block_cache_mb = 64`
- Sharded into N stripes to reduce mutex contention

---

## 7. MANIFEST

Append-only log of changes to the set of live SSTables.

### Records:
```
ADD    | level | file_id | min_key | max_key | size
REMOVE | level | file_id
COMPACT_START | inputs[] | outputs[]
SNAPSHOT      | last_included_index | snapshot_file
```

### Recovery
At startup, replay MANIFEST to reconstruct: which SSTables exist, at which levels, with what key ranges.

### Rotation
When MANIFEST grows large (>1 MB), write a new compact manifest with current state only, and atomically rename.

---

## 8. Compaction (leveled)

### 8.1 Triggers
- **L0 → L1**: number of L0 files ≥ 4
- **Lk → Lk+1** (k ≥ 1): total size of Lk > `10^k MB` (e.g. L1 = 10 MB, L2 = 100 MB, ..., L6 = 1 TB)

### 8.2 Selection
- L0 → L1: pick all L0 files (they overlap) + overlapping L1 files
- Lk → Lk+1 (k ≥ 1): pick one Lk file (round-robin or oldest), plus all L1 files it overlaps

### 8.3 Algorithm
1. Open input iterators (sorted merge)
2. For each unique key, keep newest version
3. Drop tombstones if at bottom level
4. Write output SSTable(s) at Lk+1
5. Update MANIFEST atomically: REMOVE inputs, ADD outputs
6. Delete input files

### 8.4 Concurrency
- One compaction thread (v1); two threads if compaction can't keep up
- Compaction is lock-free vs reads: readers hold `shared_ptr<const SSTableReader>` to existing files; deletion only happens after refcount drops

### 8.5 Write amplification
Theoretical: ~`10 × (levels)` = 60–70× for L0..L6. Tunable via level multiplier.

---

## 9. Crash recovery flow

```
On startup:
  1. Read MANIFEST       → know live SSTables
  2. Read snapshot meta  → know last_included_index for Raft
  3. Open WAL segments   → replay into fresh MemTable
  4. Validate state      → MANIFEST consistent with disk?
  5. Resume Raft         → load currentTerm, votedFor, log
  6. Resume apply loop   → apply any uncommitted-but-committed entries
```

---

## 10. Test plan

### Unit tests
- [ ] WAL: append + recover identity
- [ ] WAL: corruption detected at first bad record
- [ ] MemTable: PUT/GET/DELETE correctness
- [ ] MemTable: concurrent reads + single writer
- [ ] SSTable: write 1M keys, read all, verify
- [ ] SSTable: bloom filter false positive rate ≤ 2%
- [ ] Block cache: hit rate ≥ 80% on hot workload
- [ ] Compaction: L0 → L1 produces no overlap
- [ ] Compaction: tombstone GC at bottom level
- [ ] MANIFEST: replay reproduces live file set

### Integration tests
- [ ] Engine: 1M PUTs survive restart
- [ ] Engine: DELETE'd key returns NotFound after compaction
- [ ] Engine: GET latency p99 < 5 ms with 10M keys
- [ ] Engine: write throughput ≥ 50K ops/sec single-node
- [ ] Engine: compaction does not block writes

### Stress
- [ ] 1-hour continuous mixed workload, no leaks
- [ ] Random kill during compaction, restart, verify

---

## 11. TODO — storage engine (mapped to sprint days)

### Day 2 — WAL
- [ ] `wal.h` interface
- [ ] Record encoding with CRC32
- [ ] Segment rotation at 64 MB
- [ ] fsync policy (batch mode)
- [ ] Sequential reader
- [ ] Unit tests: write/recover/corruption

### Day 3 — MemTable
- [ ] Skiplist (or `std::map` + RWMutex for v1)
- [ ] PUT/GET/DELETE
- [ ] Tombstones
- [ ] Size accounting
- [ ] Snapshot iterator
- [ ] Unit tests

### Day 4 — SSTable writer
- [ ] Block builder with restart points
- [ ] Index block builder
- [ ] Bloom filter builder (10 bits/key)
- [ ] Footer encoding
- [ ] `SSTableWriter::Add` / `Finish`
- [ ] Optional Snappy compression (defer to Phase 2)

### Day 5 — SSTable reader
- [ ] Footer decoder
- [ ] Index block reader
- [ ] Data block reader
- [ ] Bloom filter check
- [ ] `Get` and iterator
- [ ] Unit tests: 1M keys roundtrip

### Day 6 — Flush + MANIFEST
- [ ] Immutable MemTable queue
- [ ] Background flush thread
- [ ] MANIFEST encoding
- [ ] Atomic rotation
- [ ] Recovery: MANIFEST replay

### Day 7 — Engine integration + benchmark
- [ ] `StorageEngine` class
- [ ] End-to-end PUT/GET/DELETE
- [ ] Single-node benchmark: 1M PUTs + 1M GETs
- [ ] Record numbers, plot

### Day 25 — Leveled compaction (Phase 1)
- [ ] L0 → L1 compaction worker
- [ ] Compaction picker (overlap minimization)
- [ ] Atomic MANIFEST update
- [ ] Tombstone GC at bottom level
- [ ] Extend to L1 → L2 → ... → L6 if time permits

### Phase 2 (post-sprint)
- [ ] Block cache LRU
- [ ] Snappy block compression
- [ ] Bloom filter tuning
- [ ] mmap'd reads
- [ ] Direct I/O (Linux)

---

## 12. Assessment criteria — storage engine done = ✅

- [ ] All unit tests pass
- [ ] 1M PUTs at ≥ 50K ops/sec single-node
- [ ] p99 read latency ≤ 5 ms after 10M keys
- [ ] Survives random kill + restart with no data loss
- [ ] Bloom filter false positive rate ≤ 2%
- [ ] Compaction reduces L0 file count under continuous load
- [ ] No memory leaks under valgrind / ASan
- [ ] No data races under ThreadSanitizer

---

## 13. Tunables (config.toml)

```toml
[storage]
memtable_size_mb        = 4
wal_fsync_mode          = "batch"   # sync | batch | none
wal_fsync_interval_ms   = 5
wal_segment_size_mb     = 64
l0_compaction_trigger   = 4
level_size_multiplier   = 10
block_size_kb           = 4
bloom_bits_per_key      = 10
block_cache_mb          = 64
compression             = "none"    # none | snappy | zstd
```

---

## 14. Common pitfalls

1. **fsync skipped in WAL** — Loses durability promise. Always fsync before responding.
2. **Tombstone dropped above bottom level** — Resurrected deletes. Only drop at deepest level.
3. **Concurrent compaction + flush corrupts MANIFEST** — Serialize MANIFEST writes through a single thread.
4. **SSTable file deleted while reader holds it** — Use `shared_ptr<const SSTableReader>`; physical delete only on refcount 0.
5. **Bloom filter size grows unbounded** — Cap by SSTable size; rebuild on compaction.
6. **Block cache mutex contention** — Shard the cache.
7. **Footer at fixed offset breaks on truncated file** — Detect and refuse to open.
8. **Restart points wrong** — Off-by-one in prefix compression silently corrupts reads. Test extensively.

---

_See `benchmarks.md` for how to measure and report storage engine performance._
