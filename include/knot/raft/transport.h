// Knot — Raft transport.
//
// Day 8: InMemoryBus (in-process, mutex-guarded mailboxes).
// Day 14: TcpTransport implements the same Transport interface.
//
// Pull semantics: Drain() is called by the driver loop to take inbound
// messages. The Raft core never receives async callbacks — preserves
// single-threaded determinism inside RaftNode.

#pragma once

#include <memory>
#include <vector>

#include <knot/raft/messages.h>
#include <knot/raft/types.h>

namespace knot::raft {

class Transport {
public:
    virtual ~Transport() = default;

    virtual void Send(Envelope env) = 0;
    [[nodiscard]] virtual std::vector<Envelope> Drain() = 0;

protected:
    // Pure interface: allow subclass-controlled copy/move via = default,
    // but never construct/copy a bare Transport. Satisfies Rule of Five
    // for cppcoreguidelines-special-member-functions.
    Transport() = default;
    Transport(const Transport&) = default;
    Transport(Transport&&) = default;
    Transport& operator=(const Transport&) = default;
    Transport& operator=(Transport&&) = default;
};

class InMemoryBus {
public:
    InMemoryBus();
    ~InMemoryBus();

    InMemoryBus(const InMemoryBus&) = delete;
    InMemoryBus& operator=(const InMemoryBus&) = delete;
    InMemoryBus(InMemoryBus&&) = delete;
    InMemoryBus& operator=(InMemoryBus&&) = delete;

    [[nodiscard]] std::unique_ptr<Transport> Connect(NodeId id);

    // Chaos hooks for Day 11+ tests; usable from Day 8.
    void Partition(const NodeId& a, const NodeId& b);  // drop a<->b (undirected)
    void Heal(const NodeId& a, const NodeId& b);
    void DropAll();  // clear all mailboxes

    struct Impl;

private:
    std::unique_ptr<Impl> impl_;
};

}  // namespace knot::raft
