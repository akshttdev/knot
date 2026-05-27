// Knot — Raft persistent / volatile / leader state structs.

#pragma once

#include <optional>
#include <unordered_map>

#include <knot/raft/types.h>

namespace knot::raft {

// Persistent state — fsynced before the node responds to any RPC.
// Day 8: in RAM only. Day 13 adds Save/Load to disk.
struct PersistentState {
    Term current_term = 0;
    std::optional<NodeId> voted_for;  // empty = haven't voted this term
};

// Volatile state — reset on restart.
struct VolatileState {
    Role role = Role::kFollower;
    LogIdx commit_index = 0;
    LogIdx last_applied = 0;
    NodeId leader_id;  // empty = unknown
};

// Leader-only volatile state — wiped on step-down.
// Defined but not instantiated by RaftNode::Impl until Day 9.
struct LeaderState {
    std::unordered_map<NodeId, LogIdx> next_index;
    std::unordered_map<NodeId, LogIdx> match_index;
};

}  // namespace knot::raft
