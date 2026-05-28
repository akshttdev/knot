// Knot — RaftNode unit tests.

#include <cstddef>
#include <memory>
#include <set>
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
    // After Day 9+10 C2: a higher-term RVReq from an up-to-date log is granted.
    EXPECT_TRUE(std::get<RequestVoteResp>(out[0].msg).vote_granted);
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

TEST(RaftNodeTest, ConfigDefaultsArePresentAndSensible) {
    auto n = RaftNode::Create({.self = "a", .peers = {"b", "c"}, .rng_seed = 42});
    EXPECT_EQ(n->CurrentRole(), Role::kFollower);
    EXPECT_EQ(n->CurrentTerm(), 0U);
    EXPECT_EQ(n->TickCount(), 0U);
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

TEST(RaftNodeTest, FollowerBecomesCandidateAfterElectionTimeout) {
    auto n = RaftNode::Create({.self = "a",
                               .peers = {"b", "c"},
                               .election_timeout_min_ticks = 5,
                               .election_timeout_max_ticks = 5,
                               .rng_seed = 1});
    EXPECT_EQ(n->CurrentRole(), Role::kFollower);
    for (int i = 0; i < 5; ++i) {
        n->Tick();
    }
    EXPECT_EQ(n->CurrentRole(), Role::kCandidate);
    EXPECT_EQ(n->CurrentTerm(), 1U);
}

TEST(RaftNodeTest, CandidateBroadcastsRequestVoteToAllPeers) {
    auto n = RaftNode::Create({.self = "a",
                               .peers = {"b", "c"},
                               .election_timeout_min_ticks = 3,
                               .election_timeout_max_ticks = 3,
                               .rng_seed = 1});
    for (int i = 0; i < 3; ++i) {
        n->Tick();
    }
    auto out = n->TakeOutgoing();
    ASSERT_EQ(out.size(), 2U);
    std::set<NodeId> targets;
    for (const auto& env : out) {
        targets.insert(env.to);
        EXPECT_EQ(env.from, "a");
        ASSERT_TRUE(std::holds_alternative<RequestVoteReq>(env.msg));
        const auto& req = std::get<RequestVoteReq>(env.msg);
        EXPECT_EQ(req.term, 1U);
        EXPECT_EQ(req.candidate_id, "a");
        EXPECT_EQ(req.last_log_index, 0U);
        EXPECT_EQ(req.last_log_term, 0U);
    }
    EXPECT_EQ(targets.count("b"), 1U);
    EXPECT_EQ(targets.count("c"), 1U);
}

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

TEST(RaftNodeTest, StaleTermRVReqRejected) {
    auto n = RaftNode::Create({.self = "a", .peers = {"b"}, .rng_seed = 1});
    // Bump term to 5 by sending a high-term RVReq first.
    n->Step(Envelope{.from = "b",
                     .to = "a",
                     .msg = RequestVoteReq{
                         .term = 5, .candidate_id = "b", .last_log_index = 0, .last_log_term = 0}});
    (void)n->TakeOutgoing();  // drain the response to "b"

    // Now a stale-term req at term 1 should be rejected.
    n->Step(Envelope{.from = "b",
                     .to = "a",
                     .msg = RequestVoteReq{
                         .term = 1, .candidate_id = "b", .last_log_index = 0, .last_log_term = 0}});
    auto out = n->TakeOutgoing();
    ASSERT_EQ(out.size(), 1U);
    ASSERT_TRUE(std::holds_alternative<RequestVoteResp>(out[0].msg));
    const auto& r = std::get<RequestVoteResp>(out[0].msg);
    EXPECT_FALSE(r.vote_granted);
    EXPECT_EQ(r.term, 5U);
}

TEST(RaftNodeTest, HigherTermRVReqStepsDownAndGrantsVote) {
    auto n = RaftNode::Create({.self = "a",
                               .peers = {"b"},
                               .election_timeout_min_ticks = 3,
                               .election_timeout_max_ticks = 3,
                               .rng_seed = 1});
    // Drive into Candidate at term 1.
    for (int i = 0; i < 3; ++i) {
        n->Tick();
    }
    (void)n->TakeOutgoing();
    EXPECT_EQ(n->CurrentRole(), Role::kCandidate);
    EXPECT_EQ(n->CurrentTerm(), 1U);

    // Higher-term RVReq from b with up-to-date log -> step down + grant.
    n->Step(Envelope{.from = "b",
                     .to = "a",
                     .msg = RequestVoteReq{
                         .term = 2, .candidate_id = "b", .last_log_index = 0, .last_log_term = 0}});
    auto out = n->TakeOutgoing();
    ASSERT_EQ(out.size(), 1U);
    ASSERT_TRUE(std::holds_alternative<RequestVoteResp>(out[0].msg));
    const auto& r = std::get<RequestVoteResp>(out[0].msg);
    EXPECT_TRUE(r.vote_granted);
    EXPECT_EQ(r.term, 2U);
    EXPECT_EQ(n->CurrentRole(), Role::kFollower);
    EXPECT_EQ(n->CurrentTerm(), 2U);
}

TEST(RaftNodeTest, AlreadyVotedRejectsOtherCandidate) {
    auto n = RaftNode::Create({.self = "a", .peers = {"b", "c"}, .rng_seed = 1});

    n->Step(Envelope{.from = "b",
                     .to = "a",
                     .msg = RequestVoteReq{
                         .term = 1, .candidate_id = "b", .last_log_index = 0, .last_log_term = 0}});
    auto out1 = n->TakeOutgoing();
    ASSERT_EQ(out1.size(), 1U);
    EXPECT_TRUE(std::get<RequestVoteResp>(out1[0].msg).vote_granted);

    n->Step(Envelope{.from = "c",
                     .to = "a",
                     .msg = RequestVoteReq{
                         .term = 1, .candidate_id = "c", .last_log_index = 0, .last_log_term = 0}});
    auto out2 = n->TakeOutgoing();
    ASSERT_EQ(out2.size(), 1U);
    EXPECT_FALSE(std::get<RequestVoteResp>(out2[0].msg).vote_granted);
}

TEST(RaftNodeTest, OutOfDateCandidateLogRejected) {
    // Requires log-mutation API not yet exposed; revisit in Day 11 after
    // real AppendEntries replication exercises this path naturally.
    GTEST_SKIP() << "Requires log-mutation API; revisit in Day 11.";
}

TEST(RaftNodeTest, LeaderBroadcastsHeartbeatsEveryInterval) {
    auto n = RaftNode::Create({.self = "a",
                               .peers = {"b", "c"},
                               .election_timeout_min_ticks = 2,
                               .election_timeout_max_ticks = 2,
                               .heartbeat_interval_ticks = 3,
                               .rng_seed = 1});
    // Become leader (term 1).
    for (int i = 0; i < 2; ++i) {
        n->Tick();
    }
    // Drain the RequestVote broadcast from BecomeCandidate.
    (void)n->TakeOutgoing();
    n->Step(
        Envelope{.from = "b", .to = "a", .msg = RequestVoteResp{.term = 1, .vote_granted = true}});
    ASSERT_EQ(n->CurrentRole(), Role::kLeader);

    // BecomeLeader should have emitted immediate heartbeat to both peers.
    auto first = n->TakeOutgoing();
    ASSERT_EQ(first.size(), 2U);
    for (const auto& env : first) {
        ASSERT_TRUE(std::holds_alternative<AppendEntriesReq>(env.msg));
        const auto& ae = std::get<AppendEntriesReq>(env.msg);
        EXPECT_EQ(ae.term, 1U);
        EXPECT_EQ(ae.leader_id, "a");
        EXPECT_TRUE(ae.entries.empty());
        EXPECT_EQ(ae.prev_log_index, 0U);
        EXPECT_EQ(ae.prev_log_term, 0U);
        EXPECT_EQ(ae.leader_commit, 0U);
    }

    // Three more ticks -> next heartbeat batch.
    for (int i = 0; i < 3; ++i) {
        n->Tick();
    }
    auto second = n->TakeOutgoing();
    ASSERT_EQ(second.size(), 2U);
    for (const auto& env : second) {
        ASSERT_TRUE(std::holds_alternative<AppendEntriesReq>(env.msg));
    }
}

TEST(RaftNodeTest, SingleNodeBecomesLeaderImmediately) {
    auto n = RaftNode::Create({.self = "solo",
                               .peers = {},
                               .election_timeout_min_ticks = 2,
                               .election_timeout_max_ticks = 2,
                               .rng_seed = 1});
    for (int i = 0; i < 2; ++i) {
        n->Tick();
    }
    EXPECT_EQ(n->CurrentRole(), Role::kLeader);
    EXPECT_EQ(n->CurrentTerm(), 1U);
}

TEST(RaftNodeTest, CandidateBecomesLeaderOnMajorityVotes) {
    auto n = RaftNode::Create({.self = "a",
                               .peers = {"b", "c"},
                               .election_timeout_min_ticks = 2,
                               .election_timeout_max_ticks = 2,
                               .rng_seed = 1});
    for (int i = 0; i < 2; ++i) {
        n->Tick();
    }
    ASSERT_EQ(n->CurrentRole(), Role::kCandidate);
    (void)n->TakeOutgoing();

    n->Step(
        Envelope{.from = "b", .to = "a", .msg = RequestVoteResp{.term = 1, .vote_granted = true}});
    EXPECT_EQ(n->CurrentRole(), Role::kLeader);
}

TEST(RaftNodeTest, AnyHigherTermMessageForcesStepDown) {
    auto n = RaftNode::Create({.self = "leader",
                               .peers = {"b"},
                               .election_timeout_min_ticks = 2,
                               .election_timeout_max_ticks = 2,
                               .rng_seed = 1});
    // Drive into Leader of term 1.
    for (int i = 0; i < 2; ++i) {
        n->Tick();
    }
    n->Step(Envelope{
        .from = "b", .to = "leader", .msg = RequestVoteResp{.term = 1, .vote_granted = true}});
    ASSERT_EQ(n->CurrentRole(), Role::kLeader);
    (void)n->TakeOutgoing();

    // Stale leader sees higher-term RVReq -> steps down + grants vote.
    n->Step(Envelope{.from = "b",
                     .to = "leader",
                     .msg = RequestVoteReq{
                         .term = 5, .candidate_id = "b", .last_log_index = 0, .last_log_term = 0}});
    EXPECT_EQ(n->CurrentRole(), Role::kFollower);
    EXPECT_EQ(n->CurrentTerm(), 5U);
}

TEST(RaftNodeTest, HeartbeatResetsFollowerElectionTimer) {
    auto n = RaftNode::Create({.self = "f",
                               .peers = {"l"},
                               .election_timeout_min_ticks = 10,
                               .election_timeout_max_ticks = 10,
                               .rng_seed = 1});
    for (int i = 0; i < 5; ++i) {
        n->Tick();
    }
    EXPECT_EQ(n->CurrentRole(), Role::kFollower);

    // Heartbeat from "l" at term 1.
    n->Step(Envelope{.from = "l",
                     .to = "f",
                     .msg = AppendEntriesReq{.term = 1,
                                             .leader_id = "l",
                                             .prev_log_index = 0,
                                             .prev_log_term = 0,
                                             .entries = {},
                                             .leader_commit = 0}});
    (void)n->TakeOutgoing();

    // Without the reset, would have timed out by tick 10.
    // With the reset, election timer restarts from 0 — needs 10 more ticks.
    for (int i = 0; i < 9; ++i) {
        n->Tick();
    }
    EXPECT_EQ(n->CurrentRole(), Role::kFollower);
    EXPECT_EQ(n->CurrentTerm(), 1U);
}

TEST(RaftNodeTest, HigherTermAEResponseStepsLeaderDown) {
    auto n = RaftNode::Create({.self = "a",
                               .peers = {"b"},
                               .election_timeout_min_ticks = 2,
                               .election_timeout_max_ticks = 2,
                               .rng_seed = 1});
    // Become leader at term 1.
    for (int i = 0; i < 2; ++i) {
        n->Tick();
    }
    n->Step(
        Envelope{.from = "b", .to = "a", .msg = RequestVoteResp{.term = 1, .vote_granted = true}});
    ASSERT_EQ(n->CurrentRole(), Role::kLeader);
    (void)n->TakeOutgoing();

    // Stale leader sees AEResp from a node that's seen a higher term.
    n->Step(Envelope{.from = "b",
                     .to = "a",
                     .msg = AppendEntriesResp{.term = 5, .success = false, .match_index = 0}});
    EXPECT_EQ(n->CurrentRole(), Role::kFollower);
    EXPECT_EQ(n->CurrentTerm(), 5U);
}

TEST(RaftNodeIntegrationTest, ThreeNodeElectsLeaderAndHoldsViaHeartbeats) {
    InMemoryBus bus;

    const std::vector<NodeId> all = {"n1", "n2", "n3"};
    std::vector<std::unique_ptr<RaftNode>> nodes;
    std::vector<std::unique_ptr<Transport>> transports;

    // Different RNG seeds → election timers diverge → exactly one wins.
    std::uint64_t seed = 100;
    for (const auto& id : all) {
        std::vector<NodeId> peers;
        for (const auto& other : all) {
            if (other != id) {
                peers.push_back(other);
            }
        }
        nodes.push_back(RaftNode::Create({.self = id,
                                          .peers = peers,
                                          .election_timeout_min_ticks = 5,
                                          .election_timeout_max_ticks = 15,
                                          .heartbeat_interval_ticks = 2,
                                          .rng_seed = seed++}));
        transports.push_back(bus.Connect(id));
    }

    // Drive 100 ticks of world-time.
    for (int t = 0; t < 100; ++t) {
        for (std::size_t i = 0; i < nodes.size(); ++i) {
            StepWorld(*nodes[i], *transports[i]);
        }
    }

    // Exactly one leader, others follower, all on the same term.
    int leaders = 0;
    int followers = 0;
    Term term = 0;
    NodeId leader_id;
    for (const auto& n : nodes) {
        if (n->CurrentRole() == Role::kLeader) {
            ++leaders;
            term = n->CurrentTerm();
            leader_id = n->Self();
        } else if (n->CurrentRole() == Role::kFollower) {
            ++followers;
        }
    }
    EXPECT_EQ(leaders, 1);
    EXPECT_EQ(followers, 2);
    for (const auto& n : nodes) {
        EXPECT_EQ(n->CurrentTerm(), term);
    }

    // Drive another 100 ticks — heartbeats should hold the leader.
    for (int t = 0; t < 100; ++t) {
        for (std::size_t i = 0; i < nodes.size(); ++i) {
            StepWorld(*nodes[i], *transports[i]);
        }
    }

    int still_leader = 0;
    for (const auto& n : nodes) {
        if (n->CurrentRole() == Role::kLeader && n->Self() == leader_id) {
            ++still_leader;
            EXPECT_EQ(n->CurrentTerm(), term);
        }
    }
    EXPECT_EQ(still_leader, 1);
}
