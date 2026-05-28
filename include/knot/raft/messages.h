// Knot — Raft RPC message types + transport envelope.

#pragma once

#include <variant>
#include <vector>

#include <knot/raft/types.h>

namespace knot::raft {

struct RequestVoteReq {
    Term term;
    NodeId candidate_id;
    LogIdx last_log_index;
    Term last_log_term;
};

struct RequestVoteResp {
    Term term;
    bool vote_granted;
};

struct AppendEntriesReq {
    Term term;
    NodeId leader_id;
    LogIdx prev_log_index;
    Term prev_log_term;
    std::vector<LogEntry> entries;  // empty = heartbeat
    LogIdx leader_commit;
};

struct AppendEntriesResp {
    Term term;
    bool success;
    LogIdx match_index = 0;
    LogIdx conflict_index = 0;  // Day 12: §5.3 fast-back-off hint
    Term conflict_term = 0;     // Day 12: term at conflict_index
};

using Message = std::variant<RequestVoteReq, RequestVoteResp, AppendEntriesReq, AppendEntriesResp>;

// Address-bearing wrapper. Transport speaks in envelopes.
struct Envelope {
    NodeId from;
    NodeId to;
    Message msg;
};

}  // namespace knot::raft
