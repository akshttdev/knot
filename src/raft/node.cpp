#include <algorithm>
#include <cstddef>
#include <optional>
#include <random>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <knot/raft/log.h>
#include <knot/raft/node.h>
#include <knot/raft/state.h>

namespace knot::raft {

struct RaftNode::Impl {
    Config cfg;
    PersistentState persistent;
    VolatileState volatile_state;
    RaftLog log;
    std::vector<Envelope> outgoing;
    std::size_t tick_count = 0;

    // Day 9+10 additions.
    std::size_t ticks_since_last_heartbeat = 0;
    std::size_t current_election_timeout = 0;
    std::size_t ticks_since_last_emit = 0;
    std::unordered_set<NodeId> votes_granted;
    LeaderState leader_state;  // only meaningful when role == kLeader
    // Default-init with random_device so the lint is happy; Create()
    // re-seeds deterministically when Config.rng_seed is set.
    std::mt19937_64 rng{std::random_device{}()};

    [[nodiscard]] std::size_t Quorum() const { return ((cfg.peers.size() + 1) / 2) + 1; }

    [[nodiscard]] std::size_t RollElectionTimeout() {
        std::uniform_int_distribution<std::size_t> dist(cfg.election_timeout_min_ticks,
                                                        cfg.election_timeout_max_ticks);
        return dist(rng);
    }

    void BecomeFollower(Term new_term);
    void BecomeCandidate(const NodeId& self_id);
    void BecomeLeader();
    void BroadcastRequestVote();
    void BroadcastAppendEntries();
    void HandleRequestVote(const Envelope& env);
    void HandleRequestVoteResp(const Envelope& env);
    void HandleAppendEntries(const Envelope& env);
    void HandleAppendEntriesResp(const Envelope& env);
    void MaybeAdvanceCommit();
};

void RaftNode::Impl::BecomeFollower(Term new_term) {
    volatile_state.role = Role::kFollower;
    if (new_term > persistent.current_term) {
        persistent.current_term = new_term;
        persistent.voted_for.reset();
        if (cfg.persister) {
            cfg.persister->SaveState(persistent.current_term, persistent.voted_for);
        }
    }
    votes_granted.clear();
    current_election_timeout = RollElectionTimeout();
    ticks_since_last_heartbeat = 0;
    volatile_state.leader_id = "";
}

void RaftNode::Impl::BecomeCandidate(const NodeId& self_id) {
    volatile_state.role = Role::kCandidate;
    ++persistent.current_term;
    persistent.voted_for = self_id;
    votes_granted = {self_id};
    current_election_timeout = RollElectionTimeout();
    ticks_since_last_heartbeat = 0;
    if (cfg.persister) {
        cfg.persister->SaveState(persistent.current_term, persistent.voted_for);
    }
}

void RaftNode::Impl::BecomeLeader() {
    volatile_state.role = Role::kLeader;
    ticks_since_last_emit = 0;
    volatile_state.leader_id = cfg.self;
    leader_state.next_index.clear();
    leader_state.match_index.clear();
    for (const auto& peer : cfg.peers) {
        leader_state.next_index[peer] = log.LastIndex() + 1;
        leader_state.match_index[peer] = 0;
    }
    // §5.4.2: if the log has entries from a prior term, append a no-op in the
    // current term so those prior-term entries get committed as a side-effect
    // once this no-op reaches a quorum.  Fresh leaders (empty log or log already
    // in current term) skip the no-op so the API surface stays predictable.
    if (log.LastIndex() > 0 && log.LastTerm() < persistent.current_term) {
        log.Append(persistent.current_term, EntryType::kNoop, "");
        if (cfg.persister) {
            cfg.persister->AppendLog(log.At(log.LastIndex()));
        }
        // Advance next_index past the no-op for all peers.
        const LogIdx new_ni = log.LastIndex() + 1;
        for (const auto& peer : cfg.peers) {
            leader_state.next_index[peer] = std::max(leader_state.next_index[peer], new_ni);
        }
    }
    BroadcastAppendEntries();
}

void RaftNode::Impl::BroadcastAppendEntries() {
    for (const auto& peer : cfg.peers) {
        const LogIdx ni = leader_state.next_index[peer];
        const LogIdx prev_idx = (ni == 0) ? 0 : (ni - 1);
        const Term prev_term = (prev_idx == 0) ? 0 : log.At(prev_idx).term;
        std::vector<LogEntry> entries = log.Slice(ni, log.LastIndex() + 1);
        outgoing.push_back(
            Envelope{.from = cfg.self,
                     .to = peer,
                     .msg = AppendEntriesReq{.term = persistent.current_term,
                                             .leader_id = cfg.self,
                                             .prev_log_index = prev_idx,
                                             .prev_log_term = prev_term,
                                             .entries = std::move(entries),
                                             .leader_commit = volatile_state.commit_index}});
    }
}

void RaftNode::Impl::BroadcastRequestVote() {
    for (const auto& peer : cfg.peers) {
        outgoing.push_back(Envelope{.from = cfg.self,
                                    .to = peer,
                                    .msg = RequestVoteReq{.term = persistent.current_term,
                                                          .candidate_id = cfg.self,
                                                          .last_log_index = log.LastIndex(),
                                                          .last_log_term = log.LastTerm()}});
    }
}

void RaftNode::Impl::HandleRequestVote(const Envelope& env) {
    const auto& req = std::get<RequestVoteReq>(env.msg);
    RequestVoteResp resp{.term = persistent.current_term, .vote_granted = false};

    if (req.term < persistent.current_term) {
        outgoing.push_back(Envelope{.from = cfg.self, .to = env.from, .msg = resp});
        return;
    }

    const bool candidate_log_ok =
        (req.last_log_term > log.LastTerm()) ||
        (req.last_log_term == log.LastTerm() && req.last_log_index >= log.LastIndex());

    const bool can_vote =
        !persistent.voted_for.has_value() || *persistent.voted_for == req.candidate_id;

    if (can_vote && candidate_log_ok) {
        persistent.voted_for = req.candidate_id;
        ticks_since_last_heartbeat = 0;
        resp.vote_granted = true;
        if (cfg.persister) {
            cfg.persister->SaveState(persistent.current_term, persistent.voted_for);
        }
    }
    resp.term = persistent.current_term;
    outgoing.push_back(Envelope{.from = cfg.self, .to = env.from, .msg = resp});
}

void RaftNode::Impl::HandleRequestVoteResp(const Envelope& env) {
    if (volatile_state.role != Role::kCandidate) {
        return;
    }
    const auto& resp = std::get<RequestVoteResp>(env.msg);
    if (resp.term != persistent.current_term) {
        return;
    }
    if (resp.vote_granted) {
        votes_granted.insert(env.from);
        if (votes_granted.size() >= Quorum()) {
            BecomeLeader();
        }
    }
}

void RaftNode::Impl::HandleAppendEntries(const Envelope& env) {
    const auto& req = std::get<AppendEntriesReq>(env.msg);
    AppendEntriesResp resp{.term = persistent.current_term, .success = false};

    if (req.term < persistent.current_term) {
        outgoing.push_back(Envelope{.from = cfg.self, .to = env.from, .msg = resp});
        return;
    }

    if (volatile_state.role == Role::kCandidate) {
        BecomeFollower(req.term);
    }
    volatile_state.leader_id = req.leader_id;
    ticks_since_last_heartbeat = 0;

    if (!log.Matches(req.prev_log_index, req.prev_log_term)) {
        if (log.LastIndex() < req.prev_log_index) {
            // Log too short: tell leader exactly how far to back up.
            resp.conflict_index = log.LastIndex() + 1;
            resp.conflict_term = 0;
        } else {
            // Term mismatch at prev_log_index: tell leader the first
            // index in OUR log with the conflicting term.
            const Term bad_term = log.At(req.prev_log_index).term;
            LogIdx idx = req.prev_log_index;
            while (idx > 1 && log.At(idx - 1).term == bad_term) {
                --idx;
            }
            resp.conflict_index = idx;
            resp.conflict_term = bad_term;
        }
        resp.success = false;
        resp.term = persistent.current_term;
        outgoing.push_back(Envelope{.from = cfg.self, .to = env.from, .msg = resp});
        return;
    }

    // Find first divergent entry; truncate + append from there.
    std::size_t first_new = 0;
    bool truncated = false;
    for (std::size_t i = 0; i < req.entries.size(); ++i) {
        const LogIdx log_idx = req.prev_log_index + 1 + static_cast<LogIdx>(i);
        if (log.LastIndex() >= log_idx) {
            if (log.At(log_idx).term != req.entries[i].term) {
                log.TruncateFrom(log_idx);
                truncated = true;
                first_new = i;
                break;
            }
            // Existing entry already matches; skip past it.
            first_new = i + 1;
        } else {
            first_new = i;
            break;
        }
    }
    if (first_new < req.entries.size()) {
        std::vector<LogEntry> to_append(
            req.entries.begin() + static_cast<std::ptrdiff_t>(first_new), req.entries.end());
        log.AppendBatch(to_append);
    }
    if (cfg.persister) {
        if (truncated) {
            cfg.persister->TruncateLog(req.prev_log_index + 1 + static_cast<LogIdx>(first_new));
        }
        for (std::size_t i = first_new; i < req.entries.size(); ++i) {
            cfg.persister->AppendLog(req.entries[i]);
        }
    }

    if (req.leader_commit > volatile_state.commit_index) {
        volatile_state.commit_index = std::min(req.leader_commit, log.LastIndex());
    }

    resp.success = true;
    resp.match_index = req.prev_log_index + static_cast<LogIdx>(req.entries.size());
    resp.term = persistent.current_term;
    outgoing.push_back(Envelope{.from = cfg.self, .to = env.from, .msg = resp});
}

void RaftNode::Impl::MaybeAdvanceCommit() {
    if (volatile_state.role != Role::kLeader) {
        return;
    }
    for (LogIdx idx = log.LastIndex(); idx > volatile_state.commit_index; --idx) {
        if (log.At(idx).term != persistent.current_term) {
            // §5.4.2: never commit by counting on a prior-term entry.
            // Keep scanning lower indices though — a higher index may
            // also be from current term; but typically once we find a
            // prior-term entry going down, everything below is too.
            if (idx == 1) {
                break;  // underflow guard
            }
            continue;
        }
        std::size_t count = 1;  // self
        for (const auto& peer : cfg.peers) {
            auto it = leader_state.match_index.find(peer);
            if (it != leader_state.match_index.end() && it->second >= idx) {
                ++count;
            }
        }
        if (count >= Quorum()) {
            volatile_state.commit_index = idx;
            break;
        }
        if (idx == 1) {
            break;  // underflow guard
        }
    }
}

void RaftNode::Impl::HandleAppendEntriesResp(const Envelope& env) {
    if (volatile_state.role != Role::kLeader) {
        return;
    }
    const auto& resp = std::get<AppendEntriesResp>(env.msg);
    if (resp.term != persistent.current_term) {
        return;
    }
    if (resp.success) {
        leader_state.match_index[env.from] = resp.match_index;
        leader_state.next_index[env.from] = resp.match_index + 1;
        MaybeAdvanceCommit();
    } else {
        LogIdx ni = leader_state.next_index[env.from];
        if (resp.conflict_term != 0) {
            // Find LAST index in our log with conflict_term.
            LogIdx last_with_term = 0;
            for (LogIdx i = log.LastIndex(); i >= 1; --i) {
                if (log.At(i).term == resp.conflict_term) {
                    last_with_term = i;
                    break;
                }
                if (i == 1) {
                    break;  // guard against LogIdx underflow at loop boundary
                }
            }
            if (last_with_term > 0) {
                ni = last_with_term + 1;
            } else if (resp.conflict_index > 0) {
                ni = resp.conflict_index;
            }
        } else if (resp.conflict_index > 0) {
            ni = resp.conflict_index;
        } else if (ni > 1) {
            --ni;
        }
        ni = std::max<LogIdx>(ni, 1);
        leader_state.next_index[env.from] = ni;
    }
}

std::unique_ptr<RaftNode> RaftNode::Create(Config cfg) {
    auto impl = std::make_unique<Impl>();
    impl->cfg = std::move(cfg);
    if (impl->cfg.rng_seed.has_value()) {
        impl->rng.seed(*impl->cfg.rng_seed);
    } else {
        std::random_device rd;
        impl->rng.seed((static_cast<std::uint64_t>(rd()) << 32) | rd());
    }
    impl->current_election_timeout = impl->RollElectionTimeout();
    if (impl->cfg.persister) {
        PersistentState loaded;
        std::vector<LogEntry> entries;
        if (impl->cfg.persister->Load(&loaded, &entries)) {
            impl->persistent = loaded;
            if (!entries.empty()) {
                impl->log.AppendBatch(entries);
            }
        }
    }
    return std::unique_ptr<RaftNode>(new RaftNode(std::move(impl)));
}

RaftNode::RaftNode(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
RaftNode::~RaftNode() = default;

void RaftNode::Tick() {
    ++impl_->tick_count;
    ++impl_->ticks_since_last_heartbeat;

    if (impl_->volatile_state.role == Role::kFollower ||
        impl_->volatile_state.role == Role::kCandidate) {
        if (impl_->ticks_since_last_heartbeat >= impl_->current_election_timeout) {
            impl_->BecomeCandidate(impl_->cfg.self);
            impl_->BroadcastRequestVote();
            // Single-node cluster: BecomeCandidate already self-voted;
            // Quorum() == 1 in that case, so we're already a Leader.
            if (impl_->votes_granted.size() >= impl_->Quorum()) {
                impl_->BecomeLeader();
            }
        }
    }
    if (impl_->volatile_state.role == Role::kLeader) {
        ++impl_->ticks_since_last_emit;
        if (impl_->ticks_since_last_emit >= impl_->cfg.heartbeat_interval_ticks) {
            impl_->BroadcastAppendEntries();
            impl_->ticks_since_last_emit = 0;
        }
    }
}

void RaftNode::Step(const Envelope& env) {
    // Higher-term step-down (Raft paper §5.1) — before dispatch.
    const Term incoming_term = std::visit([](auto&& m) -> Term { return m.term; }, env.msg);
    if (incoming_term > impl_->persistent.current_term) {
        impl_->BecomeFollower(incoming_term);
    }

    std::visit(
        [&](auto&& msg) {
            using T = std::decay_t<decltype(msg)>;
            if constexpr (std::is_same_v<T, RequestVoteReq>) {
                impl_->HandleRequestVote(env);
            } else if constexpr (std::is_same_v<T, AppendEntriesReq>) {
                impl_->HandleAppendEntries(env);
            } else if constexpr (std::is_same_v<T, RequestVoteResp>) {
                impl_->HandleRequestVoteResp(env);
            } else if constexpr (std::is_same_v<T, AppendEntriesResp>) {
                impl_->HandleAppendEntriesResp(env);
            }
        },
        env.msg);
}

std::vector<Envelope> RaftNode::TakeOutgoing() {
    std::vector<Envelope> out;
    std::swap(out, impl_->outgoing);
    return out;
}

std::vector<LogEntry> RaftNode::TakeCommitted() {
    if (impl_->volatile_state.last_applied >= impl_->volatile_state.commit_index) {
        return {};
    }
    std::vector<LogEntry> out = impl_->log.Slice(impl_->volatile_state.last_applied + 1,
                                                 impl_->volatile_state.commit_index + 1);
    impl_->volatile_state.last_applied = impl_->volatile_state.commit_index;
    return out;
}

std::optional<LogIdx> RaftNode::Submit(EntryType type, std::string payload) {
    if (impl_->volatile_state.role != Role::kLeader) {
        return std::nullopt;
    }
    impl_->log.Append(impl_->persistent.current_term, type, std::move(payload));
    if (impl_->cfg.persister) {
        impl_->cfg.persister->AppendLog(impl_->log.At(impl_->log.LastIndex()));
    }
    impl_->BroadcastAppendEntries();
    // Optimistically advance next_index so back-off uses the correct baseline.
    const LogIdx new_ni = impl_->log.LastIndex() + 1;
    for (const auto& peer : impl_->cfg.peers) {
        auto& ni = impl_->leader_state.next_index[peer];
        ni = std::max(ni, new_ni);
    }
    return impl_->log.LastIndex();
}

const NodeId& RaftNode::Self() const {
    return impl_->cfg.self;
}
Role RaftNode::CurrentRole() const {
    return impl_->volatile_state.role;
}
Term RaftNode::CurrentTerm() const {
    return impl_->persistent.current_term;
}
LogIdx RaftNode::CommitIndex() const {
    return impl_->volatile_state.commit_index;
}
LogIdx RaftNode::LastLogIndex() const {
    return impl_->log.LastIndex();
}
std::size_t RaftNode::TickCount() const {
    return impl_->tick_count;
}

}  // namespace knot::raft
