# Raft Consensus Design

> Detailed design of the Raft consensus module. Maps directly to the Ongaro/Ousterhout paper (Figure 2). Read alongside `architecture.md`.

---

## 1. Why Raft

- **Understandability** — designed to be teachable; easier to debug and verify than Paxos.
- **Strong leader model** — log flows leader → followers; simpler than multi-Paxos.
- **Well-documented** — paper, lecture videos, reference implementations (etcd, hashicorp/raft, tikv/raft-rs).
- **Industry-proven** — etcd, Consul, CockroachDB, TiKV.

---

## 2. State machine

```
                  startup / higher term seen
       ┌────────────────────────────────────────┐
       │                                         │
       ▼                                         │
  ┌──────────┐  election timeout    ┌──────────┐ │
  │ Follower │ ───────────────────► │Candidate │ │
  └──────────┘                      └────┬─────┘ │
       ▲                                 │       │
       │ AppendEntries (valid term)      │ wins  │
       │                                 │ majority
       │                                 ▼       │
       │                            ┌──────────┐ │
       └────────────────────────────│  Leader  │─┘
            higher term seen        └──────────┘
```

**Invariants:**
- At most one leader per term.
- Leader has all committed entries from previous terms (Leader Completeness Property).
- Logs match up to any committed index across all nodes.

---

## 3. Persistent state (fsync before responding)

```cpp
struct PersistentState {
    uint64_t currentTerm;             // monotonically increasing
    std::optional<NodeId> votedFor;   // vote in currentTerm
    std::vector<LogEntry> log;        // 1-indexed (paper convention)
};

struct LogEntry {
    uint64_t term;
    uint64_t index;
    enum { CMD, NOOP, CONF_CHANGE } type;
    bytes payload;
};
```

**Disk layout:**
- `raft/current_term` — 8 bytes
- `raft/voted_for` — variable length string
- `raft/log/NNNNNN.log` — 64 MB segments, append-only, with CRC32 per record

**Critical rule:** any RPC response that depends on these values must be sent AFTER `fsync` completes.

---

## 4. Volatile state

**All nodes:**
```cpp
uint64_t commitIndex;   // highest known committed
uint64_t lastApplied;   // highest applied to state machine
```

**Leader only:**
```cpp
std::map<NodeId, uint64_t> nextIndex;    // initialized to leader.lastLogIndex + 1
std::map<NodeId, uint64_t> matchIndex;   // initialized to 0
```

---

## 5. RPCs

### 5.1 RequestVote

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
```

**Receiver rules:**
1. If `req.term < currentTerm`: reject.
2. If `req.term > currentTerm`: step down, update term, clear `votedFor`.
3. If `votedFor` is null OR equals `candidate_id`, AND candidate's log is at least as up-to-date: grant vote, persist `votedFor`, fsync.
4. "At least as up-to-date" = (`lastLogTerm > localLastLogTerm`) OR (`lastLogTerm == localLastLogTerm` AND `lastLogIndex >= localLastLogIndex`).

### 5.2 AppendEntries

```proto
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
  uint64 conflict_index = 3;  // optimization (paper §5.3)
  uint64 conflict_term = 4;
}
```

**Receiver rules:**
1. If `req.term < currentTerm`: reject.
2. If `req.term >= currentTerm`: step down to follower if not already, reset election timer.
3. If `log[prevLogIndex].term != prevLogTerm` (or entry doesn't exist): reject with `conflict_index/term`.
4. Truncate any conflicting entries at and after `prevLogIndex + 1`.
5. Append new entries.
6. fsync log.
7. If `leaderCommit > commitIndex`: set `commitIndex = min(leaderCommit, lastNewEntry.index)`.
8. Respond success.

### 5.3 InstallSnapshot

```proto
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

**Receiver rules:**
1. Higher-term check, step down if needed.
2. Write chunks to temporary snapshot file.
3. If `done`: atomically rename, discard log entries ≤ `lastIncludedIndex`, install snapshot to state machine.

---

## 6. Timers

| Timer | Range | Trigger |
|---|---|---|
| Election timeout | uniform `[150ms, 300ms]` | Follower → Candidate if no AE received |
| Heartbeat interval | `50ms` | Leader sends empty AE to all followers |
| Snapshot trigger | every `10,000` committed entries OR `64 MB` log size | Take snapshot, truncate log |

**Randomization is critical** — without jitter, split votes repeat indefinitely.

---

## 7. Election algorithm

```
on election_timeout:
    if state == LEADER: return
    state = CANDIDATE
    currentTerm += 1
    votedFor = self
    persist + fsync
    votes_received = 1
    reset election_timer
    for each peer:
        send RequestVoteReq(currentTerm, self,
                            lastLogIndex, lastLogTerm)

on RequestVoteResp(resp):
    if state != CANDIDATE: return
    if resp.term > currentTerm:
        step_down(resp.term); return
    if resp.vote_granted:
        votes_received += 1
        if votes_received > N/2:
            become_leader()

on become_leader:
    state = LEADER
    for each peer:
        nextIndex[peer]  = log.lastIndex() + 1
        matchIndex[peer] = 0
    send_heartbeats()
    append NOOP entry (commit barrier for new term)
```

---

## 8. Log replication algorithm

```
on client Put(k, v) at leader:
    entry = LogEntry { term: currentTerm,
                       index: log.lastIndex() + 1,
                       type: CMD, payload: encode(k, v) }
    log.append(entry)
    fsync
    replicate_to_peers()
    block until commitIndex >= entry.index
    respond OK

on heartbeat tick (leader, per peer):
    prevLogIndex = nextIndex[peer] - 1
    prevLogTerm  = log[prevLogIndex].term
    entries = log.slice(nextIndex[peer], min(nextIndex[peer] + BATCH, lastIndex))
    send AppendEntriesReq(...)

on AppendEntriesResp(resp, peer):
    if resp.term > currentTerm:
        step_down(resp.term); return
    if resp.success:
        matchIndex[peer] = lastSentIndex
        nextIndex[peer]  = matchIndex[peer] + 1
        advance_commit_index()
    else:
        // backtrack using conflict optimization
        nextIndex[peer] = resp.conflict_index
        retry

advance_commit_index():
    // find largest N such that matchIndex[peer] >= N for majority
    // AND log[N].term == currentTerm  (paper §5.4.2)
    for N from log.lastIndex() downto commitIndex+1:
        if log[N].term != currentTerm: continue
        if majority of matchIndex[*] >= N:
            commitIndex = N
            wake apply_loop
            break
```

**The §5.4.2 rule is the most-bugged part of Raft**: leaders may only commit entries from previous terms by committing an entry from their *own* current term that depends on them. Hence the NOOP entry on leader election.

---

## 9. Safety properties (must hold always)

| Property | What it means |
|---|---|
| Election Safety | At most one leader per term |
| Leader Append-Only | Leader never overwrites or deletes its own log entries |
| Log Matching | If two logs contain an entry with same index + term, all preceding entries match |
| Leader Completeness | If an entry is committed in term T, it's present in all leaders of terms > T |
| State Machine Safety | If a node applies entry E at index i, no other node applies a different entry at index i |

---

## 10. Snapshots

**When to take:**
- Log size exceeds threshold (10K entries or 64 MB)
- Manually triggered via `knotctl snapshot`

**Format:**
```
snap-{lastIncludedIndex}.bin
├── header (last_included_index, last_included_term, cluster_config)
└── state machine dump (sorted KV pairs from LSM engine)
```

**After snapshot:**
- Atomic rename
- Update metadata file pointing to active snapshot
- Truncate log entries ≤ `lastIncludedIndex`

**InstallSnapshot is needed when:**
- A follower's `nextIndex` is behind the leader's first log entry
- A new node joins and must catch up

---

## 11. Membership changes (joint consensus)

**Two-phase protocol:**

1. Leader appends `ConfChange{type=JOINT, old=C_old, new=C_new}` entry.
2. While this entry is uncommitted: decisions require majorities of **both** `C_old` and `C_new`.
3. Once committed: leader appends `ConfChange{type=NEW, new=C_new}`.
4. From now on: decisions require majority of `C_new` only.

**Edge cases:**
- Leader not in `C_new`: leader steps down after committing the new config.
- Removed node may still ping-flood — leader ignores votes from non-members.
- Catch-up phase before adding a node (recommended): leader brings new node up via log replication before formally adding it.

---

## 12. Client request handling

- **Idempotency:** clients include `(client_id, sequence_no)`. Leader maintains a dedup cache; replays return cached response.
- **Leader redirect:** followers respond with `NotLeader{leaderHint}`. Clients retry against hint.
- **Linearizable reads:** leader performs read-index — confirms leadership via heartbeat round before serving. Optimization: leader lease (assume leadership if no higher-term message seen within lease window).

---

## 13. Edge cases & gotchas

| Gotcha | Mitigation |
|---|---|
| Leader commits stale-term entry | Only commit if `log[N].term == currentTerm` (§5.4.2 + NOOP entry on election) |
| Split vote loops | Randomized election timeouts |
| Heartbeats too frequent | Tune to RTT × 5; for localhost, 50 ms is fine |
| `votedFor` not persisted | **MUST** fsync before responding; otherwise double-vote possible after crash |
| Log conflict optimization wrong | Use `conflict_term` + `conflict_index` carefully — getting this wrong breaks convergence |
| Removed node keeps voting | Filter votes from non-members |
| Snapshot during apply | Pause apply thread; snapshot from consistent point |
| Clock drift assumed | Raft does not require clock sync; only monotonic local clocks |

---

## 14. Test plan

### Unit tests (mocked transport)
- [ ] Single-candidate election
- [ ] Split vote → randomized retry → eventual leader
- [ ] Higher-term RequestVote causes step-down
- [ ] Stale-term AppendEntries rejected
- [ ] Log replication on identical logs
- [ ] Log replication on divergent logs converges
- [ ] Persistent state survives restart
- [ ] Vote refused for outdated candidate

### Integration tests (3-node TCP cluster)
- [ ] Cluster elects leader within 500 ms of start
- [ ] PUT replicates to all followers within 100 ms
- [ ] Kill leader → new leader elected within 2 s
- [ ] Kill follower → cluster still serves writes
- [ ] Restart all nodes → state machine recovered
- [ ] Network partition → minority rejects writes, majority continues
- [ ] Snapshot install on lagging follower
- [ ] Membership change: add 4th node
- [ ] Membership change: remove a follower
- [ ] Membership change: remove the leader (graceful step-down)

### Chaos tests
- [ ] Random SIGKILL one node every 30s for 10 min
- [ ] Random partition for 10s, heal, repeat
- [ ] Slow disk on leader (`tc` delay)
- [ ] Slow network between two nodes

### Linearizability
- [ ] 200 randomized histories pass Porcupine

---

## 15. TODO — Raft implementation (mapped to sprint days)

### Day 8 — Scaffolding
- [ ] `LogEntry`, `Term`, `NodeId` types in `common/types.h`
- [ ] `RaftLog` with `append`, `truncate`, `slice`, `lastIndex`, `term(idx)` methods
- [ ] `Transport` interface (abstract)
- [ ] `InMemoryTransport` (links 3 nodes in same process)
- [ ] `RaftNode` skeleton

### Day 9 — Election
- [ ] Election timer with randomized range
- [ ] `RequestVoteReq/Resp` handlers
- [ ] FSM transitions (Follower ↔ Candidate ↔ Leader)
- [ ] Persist `currentTerm`, `votedFor`
- [ ] Unit tests: single election, split vote

### Day 10 — Heartbeats
- [ ] Leader heartbeat loop at 50 ms
- [ ] Empty `AppendEntries`
- [ ] Follower resets timer on valid AE
- [ ] Test: leader stays leader for 30 s

### Day 11 — Log replication
- [ ] Leader log append on client request
- [ ] `AppendEntries` with entries batch
- [ ] Follower applies + fsync
- [ ] Leader tracks `matchIndex[]`, advances `commitIndex`
- [ ] Apply loop consumes committed entries

### Day 12 — Log consistency
- [ ] `prevLogIndex/prevLogTerm` check
- [ ] Truncation on conflict
- [ ] `conflict_index/conflict_term` optimization
- [ ] Unit tests for divergent logs

### Day 13 — Persistence
- [ ] Disk format for `currentTerm`, `votedFor`
- [ ] Log segments with CRC
- [ ] fsync discipline
- [ ] Crash recovery test (kill + restart)

### Day 14 — TCP transport
- [ ] Asio TCP server + client
- [ ] Protobuf framing
- [ ] Per-peer connection
- [ ] Reconnect with backoff
- [ ] Swap `InMemoryTransport` → `TcpTransport`

### Day 17–18 — Snapshots
- [ ] Snapshot writer (full state dump)
- [ ] `InstallSnapshot` RPC
- [ ] Snapshot reader on restart
- [ ] Log truncation post-snapshot

### Day 19 — Membership
- [ ] `ConfChange` log entry type
- [ ] Joint consensus state tracking
- [ ] Add-node flow with catch-up
- [ ] Remove-node flow
- [ ] Leader step-down on removal

---

## 16. Assessment criteria — Raft module done = ✅

- [ ] All unit tests pass (election, replication, conflict, persistence)
- [ ] All integration tests pass (3-node + 5-node)
- [ ] Cluster survives 10 random SIGKILL cycles without divergence
- [ ] Linearizability verified on 200+ histories
- [ ] Failover p99 ≤ 2 s
- [ ] No data loss after full cluster restart
- [ ] `knotctl status` shows correct leader from every node
- [ ] Joint-consensus add + remove works on a live cluster

---

## 17. Common pitfalls — learn from etcd/Jepsen reports

1. **Forgetting to fsync `votedFor`** — split-brain after crash. Always persist before responding.
2. **Committing prior-term entries directly** — read §5.4.2 three times; add the NOOP-on-election trick.
3. **Off-by-one in `nextIndex`/`matchIndex`** — use unit tests obsessively.
4. **Snapshot during apply** — pause apply thread or use copy-on-write iterator over MemTable.
5. **Membership change while old config still has commits in flight** — joint consensus is mandatory; don't shortcut.
6. **Heartbeats too coarse** — followers time out and trigger spurious elections.
7. **Not handling `term` updates uniformly** — every RPC, every response, both directions, must check term and step down if higher.
8. **Election timer reset on non-leader AE** — only reset when the AE is from a *valid* leader (term ≥ current).

---

_See `storage-design.md` for the LSM engine that sits behind the state machine apply loop._
