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
    LogIdx match_index;  // Day 12 will add conflict_index/conflict_term
};

using Message = std::variant<RequestVoteReq, RequestVoteResp, AppendEntriesReq, AppendEntriesResp>;

// Address-bearing wrapper. Transport speaks in envelopes.
struct Envelope {
    NodeId from;
    NodeId to;
    Message msg;
};

}  // namespace knot::raft
