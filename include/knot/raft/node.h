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
#include <memory>
#include <vector>

#include <knot/raft/messages.h>
#include <knot/raft/types.h>

namespace knot::raft {

class RaftNode {
public:
    struct Config {
        NodeId self;
        std::vector<NodeId> peers;  // must NOT include self
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
