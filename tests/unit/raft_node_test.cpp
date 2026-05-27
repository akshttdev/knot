// Knot — RaftNode unit tests.

#include <cstddef>
#include <memory>
#include <utility>
#include <variant>
#include <vector>

#include <gtest/gtest.h>
#include <knot/raft/node.h>
#include <knot/raft/transport.h>

using namespace knot::raft;

TEST(RaftNodeTest, FreshNodeHasFollowerDefaults) {
    auto n = RaftNode::Create({.self = "node-1", .peers = {"node-2", "node-3"}});
    EXPECT_EQ(n->Self(), "node-1");
    EXPECT_EQ(n->CurrentRole(), Role::kFollower);
    EXPECT_EQ(n->CurrentTerm(), 0U);
    EXPECT_EQ(n->LastLogIndex(), 0U);
    EXPECT_EQ(n->CommitIndex(), 0U);
    EXPECT_EQ(n->TickCount(), 0U);
    EXPECT_TRUE(n->TakeOutgoing().empty());
    EXPECT_TRUE(n->TakeCommitted().empty());

    n->Tick();
    EXPECT_EQ(n->TickCount(), 1U);
}

TEST(RaftNodeTest, RequestVoteReqProducesResponseAddressedBackToSender) {
    auto n = RaftNode::Create({.self = "node-2", .peers = {"node-1", "node-3"}});
    n->Step(Envelope{
        .from = "node-1",
        .to = "node-2",
        .msg = RequestVoteReq{
            .term = 1, .candidate_id = "node-1", .last_log_index = 0, .last_log_term = 0}});

    auto out = n->TakeOutgoing();
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(out[0].from, "node-2");
    EXPECT_EQ(out[0].to, "node-1");
    ASSERT_TRUE(std::holds_alternative<RequestVoteResp>(out[0].msg));
    EXPECT_FALSE(std::get<RequestVoteResp>(out[0].msg).vote_granted);
}

TEST(RaftNodeTest, AppendEntriesReqProducesResponseAddressedBackToSender) {
    auto n = RaftNode::Create({.self = "node-2", .peers = {"node-1", "node-3"}});
    n->Step(Envelope{.from = "node-1",
                     .to = "node-2",
                     .msg = AppendEntriesReq{.term = 1,
                                             .leader_id = "node-1",
                                             .prev_log_index = 0,
                                             .prev_log_term = 0,
                                             .entries = {},
                                             .leader_commit = 0}});

    auto out = n->TakeOutgoing();
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(out[0].to, "node-1");
    ASSERT_TRUE(std::holds_alternative<AppendEntriesResp>(out[0].msg));
    EXPECT_TRUE(std::get<AppendEntriesResp>(out[0].msg).success);
}

TEST(RaftNodeTest, ResponseMessagesAreSinked) {
    auto n = RaftNode::Create({.self = "node-1", .peers = {"node-2"}});
    n->Step(Envelope{
        .from = "node-2", .to = "node-1", .msg = RequestVoteResp{.term = 1, .vote_granted = true}});
    n->Step(Envelope{.from = "node-2",
                     .to = "node-1",
                     .msg = AppendEntriesResp{.term = 1, .success = true, .match_index = 0}});
    EXPECT_TRUE(n->TakeOutgoing().empty());
}

namespace {
// One world-tick: pump the node-and-transport pair forward by one step.
void StepWorld(RaftNode& n, Transport& t) {
    n.Tick();
    for (auto& env : t.Drain()) {
        n.Step(env);
    }
    for (auto& env : n.TakeOutgoing()) {
        t.Send(std::move(env));
    }
}
}  // namespace

TEST(RaftNodeIntegrationTest, ThreeNodeMeshExchangesViaInMemoryBus) {
    InMemoryBus bus;

    std::vector<std::unique_ptr<RaftNode>> nodes;
    std::vector<std::unique_ptr<Transport>> transports;

    const std::vector<NodeId> all = {"node-1", "node-2", "node-3"};
    for (const auto& id : all) {
        std::vector<NodeId> peers;
        for (const auto& other : all) {
            if (other != id) {
                peers.push_back(other);
            }
        }
        nodes.push_back(RaftNode::Create({.self = id, .peers = peers}));
        transports.push_back(bus.Connect(id));
    }

    // Manually inject a RequestVoteReq from node-1 to node-2 via the bus.
    transports[0]->Send(Envelope{
        .from = "node-1",
        .to = "node-2",
        .msg = RequestVoteReq{
            .term = 1, .candidate_id = "node-1", .last_log_index = 0, .last_log_term = 0}});

    // Drive the world for one round: node-2 sees the request and deposits
    // the response into node-1's mailbox.  The final Drain() on transports[0]
    // confirms the message arrived and has the right shape.
    for (std::size_t i = 0; i < nodes.size(); ++i) {
        StepWorld(*nodes[i], *transports[i]);
    }

    auto inbox1 = transports[0]->Drain();
    ASSERT_EQ(inbox1.size(), 1U);
    EXPECT_EQ(inbox1[0].from, "node-2");
    EXPECT_EQ(inbox1[0].to, "node-1");
    EXPECT_TRUE(std::holds_alternative<RequestVoteResp>(inbox1[0].msg));
}
