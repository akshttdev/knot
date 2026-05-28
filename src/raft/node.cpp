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
    void BroadcastHeartbeat();
    void HandleRequestVote(const Envelope& env);
    void HandleRequestVoteResp(const Envelope& env);
    void HandleAppendEntries(const Envelope& env);
};

void RaftNode::Impl::BecomeFollower(Term new_term) {
    volatile_state.role = Role::kFollower;
    if (new_term > persistent.current_term) {
        persistent.current_term = new_term;
        persistent.voted_for.reset();
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
}

void RaftNode::Impl::BecomeLeader() {
    volatile_state.role = Role::kLeader;
    ticks_since_last_emit = 0;
    volatile_state.leader_id = cfg.self;
    BroadcastHeartbeat();
}

void RaftNode::Impl::BroadcastHeartbeat() {
    for (const auto& peer : cfg.peers) {
        outgoing.push_back(
            Envelope{.from = cfg.self,
                     .to = peer,
                     .msg = AppendEntriesReq{.term = persistent.current_term,
                                             .leader_id = cfg.self,
                                             .prev_log_index = log.LastIndex(),
                                             .prev_log_term = log.LastTerm(),
                                             .entries = {},
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
    AppendEntriesResp resp{.term = persistent.current_term, .success = false, .match_index = 0};

    if (req.term < persistent.current_term) {
        outgoing.push_back(Envelope{.from = cfg.self, .to = env.from, .msg = resp});
        return;
    }

    // Valid leader for this term — accept authority.
    if (volatile_state.role == Role::kCandidate) {
        BecomeFollower(req.term);
    }
    volatile_state.leader_id = req.leader_id;
    ticks_since_last_heartbeat = 0;

    resp.success = true;   // Day 11 adds prev_log_index/term consistency check
    resp.match_index = 0;  // Day 11 fills in real match index
    resp.term = persistent.current_term;
    outgoing.push_back(Envelope{.from = cfg.self, .to = env.from, .msg = resp});
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
            impl_->BroadcastHeartbeat();
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
            } else {
                // AppendEntriesResp — sinked until Task F (leader's response handler).
                (void)msg;
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
    return {};  // Day 11.
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
