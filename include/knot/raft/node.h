// Knot — RaftNode (I/O-less consensus core).
//
// Day 8: types, log, transport scaffolding wired into a node with
// stubbed Step handlers. Days 9-13 swap in real Raft logic without
// changing this public interface.
//
// Driver loop (lives in tests today, in knotd later):
//   void StepWorld(RaftNode& n, Transport& t) {
//     n.Tick();
//     for (auto& e : t.Drain())       n.Step(e);
//     for (auto& e : n.TakeOutgoing()) t.Send(std::move(e));
//   }

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

#include <knot/raft/messages.h>
#include <knot/raft/persister.h>
#include <knot/raft/types.h>

namespace knot::raft {

class RaftNode {
public:
    struct Config {
        NodeId self;
        std::vector<NodeId> peers;  // must NOT include self
        std::size_t election_timeout_min_ticks = 15;
        std::size_t election_timeout_max_ticks = 30;
        std::size_t heartbeat_interval_ticks = 5;
        std::optional<std::uint64_t> rng_seed;
        std::shared_ptr<Persister> persister = nullptr;

        // Day 17: called by HandleInstallSnapshot (Task G) and Create's
        // recovery path (Task I) so the FSM (e.g., StorageEngine) can
        // re-apply the snapshot bytes.
        std::function<void(std::string_view bytes)> apply_snapshot_callback;
    };

    [[nodiscard]] static std::unique_ptr<RaftNode> Create(Config cfg);

    ~RaftNode();

    RaftNode(const RaftNode&) = delete;
    RaftNode& operator=(const RaftNode&) = delete;
    RaftNode(RaftNode&&) = delete;
    RaftNode& operator=(RaftNode&&) = delete;

    void Tick();
    void Step(const Envelope& env);
    [[nodiscard]] std::vector<Envelope> TakeOutgoing();
    [[nodiscard]] std::vector<LogEntry> TakeCommitted();  // Day 8: always empty
    [[nodiscard]] std::optional<LogIdx> Submit(EntryType type, std::string payload);

    // Tell the log a snapshot has been taken up to `last_included_index`,
    // covering the entry at that index with term `last_included_term`.
    // The Raft log truncates entries <= last_included_index. The
    // persister saves `snapshot_bytes` for restart.
    void MaybeCreateSnapshot(LogIdx last_included_index, Term last_included_term,
                             std::string_view snapshot_bytes);

    [[nodiscard]] LogIdx LogStartIndex() const;

    // Term of the entry at `idx`. Handles the snapshot-boundary sentinel
    // (`idx == LogStartIndex() - 1` returns the snapshot's last_included_term).
    [[nodiscard]] Term TermAtIndex(LogIdx idx) const;

    [[nodiscard]] const NodeId& Self() const;
    [[nodiscard]] Role CurrentRole() const;
    [[nodiscard]] Term CurrentTerm() const;
    [[nodiscard]] LogIdx CommitIndex() const;
    [[nodiscard]] LogIdx LastLogIndex() const;
    [[nodiscard]] std::size_t TickCount() const;

private:
    struct Impl;
    explicit RaftNode(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
};

}  // namespace knot::raft
