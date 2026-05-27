#include <type_traits>
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
};

std::unique_ptr<RaftNode> RaftNode::Create(Config cfg) {
    auto impl = std::make_unique<Impl>();
    impl->cfg = std::move(cfg);
    return std::unique_ptr<RaftNode>(new RaftNode(std::move(impl)));
}

RaftNode::RaftNode(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
RaftNode::~RaftNode() = default;

void RaftNode::Tick() {
    ++impl_->tick_count;
    // Day 9: election timeout; Day 10: heartbeat trigger.
}

void RaftNode::Step(const Envelope& env) {
    // Day 8 stubs: just prove the wiring. Real Raft semantics arrive
    // Day 9 (election) and Days 11-12 (replication).
    std::visit(
        [&](auto&& msg) {
            using T = std::decay_t<decltype(msg)>;
            if constexpr (std::is_same_v<T, RequestVoteReq>) {
                impl_->outgoing.push_back(
                    Envelope{.from = impl_->cfg.self,
                             .to = env.from,
                             .msg = RequestVoteResp{.term = impl_->persistent.current_term,
                                                    .vote_granted = false}});
            } else if constexpr (std::is_same_v<T, AppendEntriesReq>) {
                impl_->outgoing.push_back(
                    Envelope{.from = impl_->cfg.self,
                             .to = env.from,
                             .msg = AppendEntriesResp{.term = impl_->persistent.current_term,
                                                      .success = true,
                                                      .match_index = 0}});
            } else {
                // RequestVoteResp / AppendEntriesResp — sinked on Day 8.
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
