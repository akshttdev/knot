// Knot — Raft fundamental types.
//
// Kept in a separate header so that log.h, state.h, messages.h, and
// node.h can all depend on these without pulling in each other.

#pragma once

#include <cstdint>
#include <string>

namespace knot::raft {

using Term = std::uint64_t;
using LogIdx = std::uint64_t;  // 1-indexed; 0 means "no entry"
using NodeId = std::string;

enum class EntryType : std::uint8_t {
    kNoop = 0,
    kCommand = 1,
    kConfChange = 2,
};

enum class Role : std::uint8_t {
    kFollower = 0,
    kCandidate = 1,
    kLeader = 2,
};

struct LogEntry {
    Term term;
    LogIdx index;
    EntryType type;
    std::string payload;  // opaque to Raft; state machine interprets
};

}  // namespace knot::raft
