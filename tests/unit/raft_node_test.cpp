// Knot — RaftNode unit tests.

#include <cstddef>
#include <memory>
#include <set>
#include <utility>
#include <variant>
#include <vector>

#include <gtest/gtest.h>
#include <knot/raft/node.h>
#include <knot/raft/persister.h>
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
    auto n = RaftNode::Create({.self = "f", .peers = {"l"}, .rng_seed = 1});

    // Seed our log via AE: 3 entries at term 5.
    LogEntry e1{.term = 5, .index = 1, .type = EntryType::kCommand, .payload = "a"};
    LogEntry e2{.term = 5, .index = 2, .type = EntryType::kCommand, .payload = "b"};
    LogEntry e3{.term = 5, .index = 3, .type = EntryType::kCommand, .payload = "c"};
    n->Step(Envelope{.from = "l",
                     .to = "f",
                     .msg = AppendEntriesReq{.term = 5,
                                             .leader_id = "l",
                                             .prev_log_index = 0,
                                             .prev_log_term = 0,
                                             .entries = {e1, e2, e3},
                                             .leader_commit = 0}});
    (void)n->TakeOutgoing();
    EXPECT_EQ(n->LastLogIndex(), 3U);
    EXPECT_EQ(n->CurrentTerm(), 5U);

    // RVReq from candidate with last_log_term=3 (< our 5) → reject.
    n->Step(
        Envelope{.from = "c",
                 .to = "f",
                 .msg = RequestVoteReq{
                     .term = 6, .candidate_id = "c", .last_log_index = 10, .last_log_term = 3}});
    auto out = n->TakeOutgoing();
    ASSERT_EQ(out.size(), 1U);
    const auto& r = std::get<RequestVoteResp>(out[0].msg);
    EXPECT_FALSE(r.vote_granted);
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

TEST(RaftNodeTest, SubmitOnFollowerReturnsNullopt) {
    auto n = RaftNode::Create({.self = "f", .peers = {"l"}, .rng_seed = 1});
    EXPECT_EQ(n->CurrentRole(), Role::kFollower);
    EXPECT_EQ(n->Submit(EntryType::kCommand, "x"), std::nullopt);
    EXPECT_EQ(n->LastLogIndex(), 0U);
}

TEST(RaftNodeTest, SubmitOnLeaderAppendsAndReturnsIndex) {
    auto n = RaftNode::Create({.self = "a",
                               .peers = {"b"},
                               .election_timeout_min_ticks = 2,
                               .election_timeout_max_ticks = 2,
                               .rng_seed = 1});
    for (int i = 0; i < 2; ++i) {
        n->Tick();
    }
    n->Step(
        Envelope{.from = "b", .to = "a", .msg = RequestVoteResp{.term = 1, .vote_granted = true}});
    ASSERT_EQ(n->CurrentRole(), Role::kLeader);
    (void)n->TakeOutgoing();

    auto idx = n->Submit(EntryType::kCommand, "hello");
    ASSERT_TRUE(idx.has_value());
    EXPECT_EQ(*idx, 1U);
    EXPECT_EQ(n->LastLogIndex(), 1U);

    auto idx2 = n->Submit(EntryType::kCommand, "world");
    ASSERT_TRUE(idx2.has_value());
    EXPECT_EQ(*idx2, 2U);
    EXPECT_EQ(n->LastLogIndex(), 2U);
}

TEST(RaftNodeTest, LeaderInitializesPerPeerReplicationStateOnElection) {
    auto n = RaftNode::Create({.self = "a",
                               .peers = {"b", "c"},
                               .election_timeout_min_ticks = 2,
                               .election_timeout_max_ticks = 2,
                               .heartbeat_interval_ticks = 5,
                               .rng_seed = 1});
    for (int i = 0; i < 2; ++i) {
        n->Tick();
    }
    // Drain the RequestVote broadcast emitted by BecomeCandidate.
    (void)n->TakeOutgoing();
    n->Step(
        Envelope{.from = "b", .to = "a", .msg = RequestVoteResp{.term = 1, .vote_granted = true}});
    ASSERT_EQ(n->CurrentRole(), Role::kLeader);

    // First heartbeat out of BecomeLeader: with next_index[peer] = 1,
    // BroadcastHeartbeat's prev_log_index comes from log.LastIndex() = 0
    // (since heartbeat currently uses LastLogIndex, not per-peer next).
    // Test the OBSERVABLE: prev_log_index/term are 0 and entries are empty.
    auto out = n->TakeOutgoing();
    ASSERT_EQ(out.size(), 2U);
    for (const auto& env : out) {
        ASSERT_TRUE(std::holds_alternative<AppendEntriesReq>(env.msg));
        const auto& ae = std::get<AppendEntriesReq>(env.msg);
        EXPECT_EQ(ae.prev_log_index, 0U);
        EXPECT_EQ(ae.prev_log_term, 0U);
        EXPECT_TRUE(ae.entries.empty());
    }
}

TEST(RaftNodeTest, SubmitTriggersImmediateBroadcastWithEntry) {
    auto n = RaftNode::Create({.self = "a",
                               .peers = {"b", "c"},
                               .election_timeout_min_ticks = 2,
                               .election_timeout_max_ticks = 2,
                               .rng_seed = 1});
    for (int i = 0; i < 2; ++i) {
        n->Tick();
    }
    n->Step(
        Envelope{.from = "b", .to = "a", .msg = RequestVoteResp{.term = 1, .vote_granted = true}});
    (void)n->TakeOutgoing();  // drain BecomeLeader's initial broadcast

    auto idx = n->Submit(EntryType::kCommand, "v1");
    ASSERT_TRUE(idx.has_value());
    auto out = n->TakeOutgoing();
    ASSERT_EQ(out.size(), 2U);
    for (const auto& env : out) {
        ASSERT_TRUE(std::holds_alternative<AppendEntriesReq>(env.msg));
        const auto& ae = std::get<AppendEntriesReq>(env.msg);
        EXPECT_EQ(ae.prev_log_index, 0U);
        EXPECT_EQ(ae.prev_log_term, 0U);
        ASSERT_EQ(ae.entries.size(), 1U);
        EXPECT_EQ(ae.entries[0].index, 1U);
        EXPECT_EQ(ae.entries[0].term, 1U);
        EXPECT_EQ(ae.entries[0].payload, "v1");
    }
}

TEST(RaftNodeTest, FollowerAcceptsAEWithMatchingPrevLogAndAppends) {
    auto n = RaftNode::Create({.self = "f", .peers = {"l"}, .rng_seed = 1});

    // Anchor at prev=0 to set term/leader; log stays empty.
    n->Step(Envelope{.from = "l",
                     .to = "f",
                     .msg = AppendEntriesReq{.term = 1,
                                             .leader_id = "l",
                                             .prev_log_index = 0,
                                             .prev_log_term = 0,
                                             .entries = {},
                                             .leader_commit = 0}});
    (void)n->TakeOutgoing();
    EXPECT_EQ(n->LastLogIndex(), 0U);

    // Deliver 2 entries with matching prev (=0, term 0).
    LogEntry e1{.term = 1, .index = 1, .type = EntryType::kCommand, .payload = "a"};
    LogEntry e2{.term = 1, .index = 2, .type = EntryType::kCommand, .payload = "b"};
    n->Step(Envelope{.from = "l",
                     .to = "f",
                     .msg = AppendEntriesReq{.term = 1,
                                             .leader_id = "l",
                                             .prev_log_index = 0,
                                             .prev_log_term = 0,
                                             .entries = {e1, e2},
                                             .leader_commit = 0}});
    auto out = n->TakeOutgoing();
    ASSERT_EQ(out.size(), 1U);
    const auto& r = std::get<AppendEntriesResp>(out[0].msg);
    EXPECT_TRUE(r.success);
    EXPECT_EQ(r.match_index, 2U);
    EXPECT_EQ(n->LastLogIndex(), 2U);
}

TEST(RaftNodeTest, FollowerRejectsAEWhenLogTooShort) {
    auto n = RaftNode::Create({.self = "f", .peers = {"l"}, .rng_seed = 1});

    // Empty log, AE claims prev_log_index=5.
    n->Step(Envelope{.from = "l",
                     .to = "f",
                     .msg = AppendEntriesReq{.term = 1,
                                             .leader_id = "l",
                                             .prev_log_index = 5,
                                             .prev_log_term = 1,
                                             .entries = {},
                                             .leader_commit = 0}});
    auto out = n->TakeOutgoing();
    ASSERT_EQ(out.size(), 1U);
    const auto& r = std::get<AppendEntriesResp>(out[0].msg);
    EXPECT_FALSE(r.success);
    EXPECT_EQ(n->LastLogIndex(), 0U);  // log unchanged
}

TEST(RaftNodeTest, FollowerRejectionSendsConflictIndexWhenLogTooShort) {
    auto n = RaftNode::Create({.self = "f", .peers = {"l"}, .rng_seed = 1});
    n->Step(Envelope{.from = "l",
                     .to = "f",
                     .msg = AppendEntriesReq{.term = 1,
                                             .leader_id = "l",
                                             .prev_log_index = 5,
                                             .prev_log_term = 1,
                                             .entries = {},
                                             .leader_commit = 0}});
    auto out = n->TakeOutgoing();
    ASSERT_EQ(out.size(), 1U);
    const auto& r = std::get<AppendEntriesResp>(out[0].msg);
    EXPECT_FALSE(r.success);
    EXPECT_EQ(r.conflict_index, 1U);  // empty log: LastIndex()+1 = 0+1
    EXPECT_EQ(r.conflict_term, 0U);
}

TEST(RaftNodeTest, FollowerRejectionSendsConflictTermOnTermMismatch) {
    auto n = RaftNode::Create({.self = "f", .peers = {"l"}, .rng_seed = 1});
    // Seed: 3 entries at term 1.
    LogEntry e1{.term = 1, .index = 1, .type = EntryType::kCommand, .payload = "a"};
    LogEntry e2{.term = 1, .index = 2, .type = EntryType::kCommand, .payload = "b"};
    LogEntry e3{.term = 1, .index = 3, .type = EntryType::kCommand, .payload = "c"};
    n->Step(Envelope{.from = "l",
                     .to = "f",
                     .msg = AppendEntriesReq{.term = 1,
                                             .leader_id = "l",
                                             .prev_log_index = 0,
                                             .prev_log_term = 0,
                                             .entries = {e1, e2, e3},
                                             .leader_commit = 0}});
    (void)n->TakeOutgoing();

    // Now leader claims prev_log_index=3 with term=2 — mismatch (our log has term=1).
    n->Step(Envelope{.from = "l",
                     .to = "f",
                     .msg = AppendEntriesReq{.term = 2,
                                             .leader_id = "l",
                                             .prev_log_index = 3,
                                             .prev_log_term = 2,
                                             .entries = {},
                                             .leader_commit = 0}});
    auto out = n->TakeOutgoing();
    ASSERT_EQ(out.size(), 1U);
    const auto& r = std::get<AppendEntriesResp>(out[0].msg);
    EXPECT_FALSE(r.success);
    EXPECT_EQ(r.conflict_term, 1U);   // our log's term at idx 3
    EXPECT_EQ(r.conflict_index, 1U);  // first idx in our log with that term
}

TEST(RaftNodeTest, FollowerTruncatesConflictingSuffixAndAppends) {
    auto n = RaftNode::Create({.self = "f", .peers = {"l"}, .rng_seed = 1});

    // Seed: 3 entries at term 1.
    LogEntry e1{.term = 1, .index = 1, .type = EntryType::kCommand, .payload = "a"};
    LogEntry e2{.term = 1, .index = 2, .type = EntryType::kCommand, .payload = "b"};
    LogEntry e3{.term = 1, .index = 3, .type = EntryType::kCommand, .payload = "c"};
    n->Step(Envelope{.from = "l",
                     .to = "f",
                     .msg = AppendEntriesReq{.term = 1,
                                             .leader_id = "l",
                                             .prev_log_index = 0,
                                             .prev_log_term = 0,
                                             .entries = {e1, e2, e3},
                                             .leader_commit = 0}});
    (void)n->TakeOutgoing();
    ASSERT_EQ(n->LastLogIndex(), 3U);

    // New leader at term 2 sends a DIFFERENT entry at index 2 (overwrites idx 2,3).
    LogEntry new2{.term = 2, .index = 2, .type = EntryType::kCommand, .payload = "B'"};
    n->Step(Envelope{.from = "l",
                     .to = "f",
                     .msg = AppendEntriesReq{.term = 2,
                                             .leader_id = "l",
                                             .prev_log_index = 1,
                                             .prev_log_term = 1,
                                             .entries = {new2},
                                             .leader_commit = 0}});
    (void)n->TakeOutgoing();
    EXPECT_EQ(n->LastLogIndex(), 2U);
}

TEST(RaftNodeTest, LeaderAdvancesMatchAndNextOnSuccessfulAEResp) {
    auto n = RaftNode::Create({.self = "a",
                               .peers = {"b"},
                               .election_timeout_min_ticks = 2,
                               .election_timeout_max_ticks = 2,
                               .rng_seed = 1});
    for (int i = 0; i < 2; ++i) {
        n->Tick();
    }
    n->Step(
        Envelope{.from = "b", .to = "a", .msg = RequestVoteResp{.term = 1, .vote_granted = true}});
    ASSERT_EQ(n->CurrentRole(), Role::kLeader);

    // Submit 2 entries to fill log.
    (void)n->Submit(EntryType::kCommand, "x");
    (void)n->Submit(EntryType::kCommand, "y");
    (void)n->TakeOutgoing();

    // Follower acks success at match_index=2.
    n->Step(Envelope{.from = "b",
                     .to = "a",
                     .msg = AppendEntriesResp{.term = 1, .success = true, .match_index = 2}});

    // Next Submit triggers an AE with prev_log_index=2 (next_index advanced to 3).
    (void)n->Submit(EntryType::kCommand, "z");
    auto out = n->TakeOutgoing();
    bool saw_advanced = false;
    for (const auto& env : out) {
        if (env.to == "b") {
            const auto& ae = std::get<AppendEntriesReq>(env.msg);
            if (ae.prev_log_index == 2U) {
                saw_advanced = true;
            }
        }
    }
    EXPECT_TRUE(saw_advanced);
}

TEST(RaftNodeTest, LeaderBacksOffNextIndexUsingConflictIndex) {
    auto n = RaftNode::Create({.self = "a",
                               .peers = {"b"},
                               .election_timeout_min_ticks = 2,
                               .election_timeout_max_ticks = 2,
                               .rng_seed = 1});
    for (int i = 0; i < 2; ++i) {
        n->Tick();
    }
    n->Step(
        Envelope{.from = "b", .to = "a", .msg = RequestVoteResp{.term = 1, .vote_granted = true}});
    // Build leader log to LastIndex=5.
    for (int i = 0; i < 5; ++i) {
        (void)n->Submit(EntryType::kCommand, "x");
    }
    (void)n->TakeOutgoing();

    // Follower rejects with conflict_index=3, conflict_term=0 (log too short).
    n->Step(Envelope{.from = "b",
                     .to = "a",
                     .msg = AppendEntriesResp{.term = 1,
                                              .success = false,
                                              .match_index = 0,
                                              .conflict_index = 3,
                                              .conflict_term = 0}});

    // Next AE to "b" should have prev_log_index=2 (since next_index dropped to 3).
    (void)n->Submit(EntryType::kCommand, "y");
    auto out = n->TakeOutgoing();
    bool saw = false;
    for (const auto& env : out) {
        if (env.to == "b") {
            const auto& ae = std::get<AppendEntriesReq>(env.msg);
            if (ae.prev_log_index == 2U) {
                saw = true;
            }
        }
    }
    EXPECT_TRUE(saw);
}

TEST(RaftNodeTest, LeaderBacksOffByOneIfNoConflictHints) {
    auto n = RaftNode::Create({.self = "a",
                               .peers = {"b"},
                               .election_timeout_min_ticks = 2,
                               .election_timeout_max_ticks = 2,
                               .rng_seed = 1});
    for (int i = 0; i < 2; ++i) {
        n->Tick();
    }
    n->Step(
        Envelope{.from = "b", .to = "a", .msg = RequestVoteResp{.term = 1, .vote_granted = true}});
    for (int i = 0; i < 3; ++i) {
        (void)n->Submit(EntryType::kCommand, "x");
    }
    (void)n->TakeOutgoing();
    // Initial next_index[b] = 4 (LastLogIndex 3 + 1). Reject without hints.
    n->Step(Envelope{.from = "b",
                     .to = "a",
                     .msg = AppendEntriesResp{.term = 1,
                                              .success = false,
                                              .match_index = 0,
                                              .conflict_index = 0,
                                              .conflict_term = 0}});
    (void)n->Submit(EntryType::kCommand, "y");
    auto out = n->TakeOutgoing();
    bool saw_back_off = false;
    for (const auto& env : out) {
        if (env.to == "b") {
            const auto& ae = std::get<AppendEntriesReq>(env.msg);
            // next_index should have decreased by 1 from 4 to 3 ⇒ prev=2.
            if (ae.prev_log_index == 2U) {
                saw_back_off = true;
            }
        }
    }
    EXPECT_TRUE(saw_back_off);
}

TEST(RaftNodeTest, LeaderAdvancesCommitIndexOnMajorityMatch) {
    auto n = RaftNode::Create({.self = "a",
                               .peers = {"b", "c"},
                               .election_timeout_min_ticks = 2,
                               .election_timeout_max_ticks = 2,
                               .rng_seed = 1});
    for (int i = 0; i < 2; ++i) {
        n->Tick();
    }
    n->Step(
        Envelope{.from = "b", .to = "a", .msg = RequestVoteResp{.term = 1, .vote_granted = true}});
    ASSERT_EQ(n->CurrentRole(), Role::kLeader);

    (void)n->Submit(EntryType::kCommand, "x");
    (void)n->TakeOutgoing();
    EXPECT_EQ(n->CommitIndex(), 0U);

    // Quorum in 3-node cluster = 2. Leader counts as 1; one follower ack -> commit.
    n->Step(Envelope{.from = "b",
                     .to = "a",
                     .msg = AppendEntriesResp{.term = 1, .success = true, .match_index = 1}});
    EXPECT_EQ(n->CommitIndex(), 1U);
}

TEST(RaftNodeTest, LeaderDoesNotCommitOnHigherTermResponse) {
    // §5.4.2 boundary: a higher-term AEResp causes step-down BEFORE the
    // commit-advance can fire. So commit_index stays at 0.
    auto n = RaftNode::Create({.self = "a",
                               .peers = {"b", "c"},
                               .election_timeout_min_ticks = 2,
                               .election_timeout_max_ticks = 2,
                               .rng_seed = 1});
    for (int i = 0; i < 2; ++i) {
        n->Tick();
    }
    n->Step(
        Envelope{.from = "b", .to = "a", .msg = RequestVoteResp{.term = 1, .vote_granted = true}});
    (void)n->Submit(EntryType::kCommand, "x");
    (void)n->TakeOutgoing();

    n->Step(Envelope{.from = "b",
                     .to = "a",
                     .msg = AppendEntriesResp{.term = 5, .success = false, .match_index = 0}});
    EXPECT_EQ(n->CurrentRole(), Role::kFollower);
    EXPECT_EQ(n->CommitIndex(), 0U);
}

TEST(RaftNodeTest, FollowerAdvancesCommitIndexFromLeaderCommit) {
    auto n = RaftNode::Create({.self = "f", .peers = {"l"}, .rng_seed = 1});

    LogEntry e1{.term = 1, .index = 1, .type = EntryType::kCommand, .payload = "a"};
    LogEntry e2{.term = 1, .index = 2, .type = EntryType::kCommand, .payload = "b"};
    n->Step(Envelope{.from = "l",
                     .to = "f",
                     .msg = AppendEntriesReq{.term = 1,
                                             .leader_id = "l",
                                             .prev_log_index = 0,
                                             .prev_log_term = 0,
                                             .entries = {e1, e2},
                                             .leader_commit = 2}});
    EXPECT_EQ(n->CommitIndex(), 2U);
}

TEST(RaftNodeTest, TakeCommittedReturnsRangeOnceThenEmpty) {
    auto n = RaftNode::Create({.self = "f", .peers = {"l"}, .rng_seed = 1});

    LogEntry e1{.term = 1, .index = 1, .type = EntryType::kCommand, .payload = "a"};
    LogEntry e2{.term = 1, .index = 2, .type = EntryType::kCommand, .payload = "b"};
    LogEntry e3{.term = 1, .index = 3, .type = EntryType::kCommand, .payload = "c"};
    n->Step(Envelope{.from = "l",
                     .to = "f",
                     .msg = AppendEntriesReq{.term = 1,
                                             .leader_id = "l",
                                             .prev_log_index = 0,
                                             .prev_log_term = 0,
                                             .entries = {e1, e2, e3},
                                             .leader_commit = 3}});
    (void)n->TakeOutgoing();

    auto out = n->TakeCommitted();
    ASSERT_EQ(out.size(), 3U);
    EXPECT_EQ(out[0].payload, "a");
    EXPECT_EQ(out[1].payload, "b");
    EXPECT_EQ(out[2].payload, "c");

    EXPECT_TRUE(n->TakeCommitted().empty());  // already drained
}

TEST(RaftNodeIntegrationTest, ThreeNodeReplicatesSubmittedEntryToAll) {
    InMemoryBus bus;

    const std::vector<NodeId> all = {"n1", "n2", "n3"};
    std::vector<std::unique_ptr<RaftNode>> nodes;
    std::vector<std::unique_ptr<Transport>> transports;

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

    // Drive 60 ticks → leader emerges.
    for (int t = 0; t < 60; ++t) {
        for (std::size_t i = 0; i < nodes.size(); ++i) {
            StepWorld(*nodes[i], *transports[i]);
        }
    }

    RaftNode* leader = nullptr;
    for (const auto& n : nodes) {
        if (n->CurrentRole() == Role::kLeader) {
            leader = n.get();
            break;
        }
    }
    ASSERT_NE(leader, nullptr);

    auto idx = leader->Submit(EntryType::kCommand, "hello");
    ASSERT_TRUE(idx.has_value());
    EXPECT_EQ(*idx, 1U);

    // Drive enough ticks for: leader broadcasts entry → follower acks →
    // leader advances commit_index → next heartbeat carries leader_commit →
    // follower advances its own commit_index.
    for (int t = 0; t < 30; ++t) {
        for (std::size_t i = 0; i < nodes.size(); ++i) {
            StepWorld(*nodes[i], *transports[i]);
        }
    }

    // Every node should see the entry via TakeCommitted.
    for (std::size_t i = 0; i < nodes.size(); ++i) {
        auto applied = nodes[i]->TakeCommitted();
        ASSERT_EQ(applied.size(), 1U)
            << "node " << all[i] << " expected 1 committed entry, got " << applied.size();
        EXPECT_EQ(applied[0].payload, "hello");
        EXPECT_EQ(applied[0].index, 1U);
    }
}

TEST(RaftNodeTest, MaybeCreateSnapshotTruncatesLogAndPersists) {
    auto persister = MakeMemoryPersister();
    auto n = RaftNode::Create({.self = "s",
                               .peers = {},
                               .election_timeout_min_ticks = 2,
                               .election_timeout_max_ticks = 2,
                               .rng_seed = 1,
                               .persister = persister});
    // Become leader (single node self-elects).
    for (int i = 0; i < 2; ++i) {
        n->Tick();
    }
    ASSERT_EQ(n->CurrentRole(), Role::kLeader);
    (void)n->TakeOutgoing();

    for (int i = 0; i < 10; ++i) {
        const auto idx = n->Submit(EntryType::kCommand, "x");
        ASSERT_TRUE(idx.has_value());
    }
    (void)n->TakeOutgoing();

    // Snapshot up to index 5.
    n->MaybeCreateSnapshot(5, n->CurrentTerm(), "snap-bytes");

    EXPECT_EQ(n->LogStartIndex(), 6U);
    EXPECT_EQ(n->TermAtIndex(5), n->CurrentTerm());  // snapshot boundary sentinel

    // Persister has the snapshot.
    std::string b;
    LogIdx idx = 0;
    Term t = 0;
    ASSERT_TRUE(persister->LoadSnapshot(&b, &idx, &t));
    EXPECT_EQ(b, "snap-bytes");
    EXPECT_EQ(idx, 5U);
    EXPECT_EQ(t, n->CurrentTerm());
}

TEST(RaftNodeTest, LeaderSendsInstallSnapshotWhenPeerBehind) {
    auto persister = MakeMemoryPersister();
    auto n = RaftNode::Create({.self = "L",
                               .peers = {"F"},
                               .election_timeout_min_ticks = 2,
                               .election_timeout_max_ticks = 2,
                               .heartbeat_interval_ticks = 100,  // huge: don't auto-heartbeat
                               .rng_seed = 1,
                               .persister = persister});
    // Become leader.
    for (int i = 0; i < 2; ++i) {
        n->Tick();
    }
    n->Step(
        Envelope{.from = "F", .to = "L", .msg = RequestVoteResp{.term = 1, .vote_granted = true}});
    ASSERT_EQ(n->CurrentRole(), Role::kLeader);

    // Build leader's log + take a snapshot covering it.
    for (int i = 0; i < 10; ++i) {
        (void)n->Submit(EntryType::kCommand, "x");
    }
    n->MaybeCreateSnapshot(10, n->CurrentTerm(), "the-snapshot");
    (void)n->TakeOutgoing();  // drain any pending sends

    // Submit triggers a broadcast. Because log_start_index is now 11 and
    // F's next_index is whatever it was before (some value <= 10), the
    // broadcast must send InstallSnapshot to F.
    (void)n->Submit(EntryType::kCommand, "y");
    const auto out = n->TakeOutgoing();
    bool saw_install = false;
    for (const auto& env : out) {
        if (env.to == "F" && std::holds_alternative<InstallSnapshotReq>(env.msg)) {
            const auto& m = std::get<InstallSnapshotReq>(env.msg);
            EXPECT_EQ(m.last_included_index, 10U);
            EXPECT_EQ(m.data, "the-snapshot");
            saw_install = true;
        }
    }
    EXPECT_TRUE(saw_install) << "expected InstallSnapshotReq to F";
}

TEST(RaftNodeTest, FollowerAppliesInstallSnapshotInvokesCallback) {
    auto persister = MakeMemoryPersister();
    std::string captured;
    auto n = RaftNode::Create(
        {.self = "F",
         .peers = {"L"},
         .rng_seed = 1,
         .persister = persister,
         .apply_snapshot_callback = [&](std::string_view b) { captured = std::string(b); }});

    n->Step(Envelope{.from = "L",
                     .to = "F",
                     .msg = InstallSnapshotReq{.term = 3,
                                               .leader_id = "L",
                                               .last_included_index = 42,
                                               .last_included_term = 2,
                                               .data = "snap-payload"}});

    EXPECT_EQ(captured, "snap-payload");
    EXPECT_EQ(n->LogStartIndex(), 43U);
    EXPECT_EQ(n->CommitIndex(), 42U);

    // Persister has the snapshot.
    std::string b;
    LogIdx idx = 0;
    Term t = 0;
    ASSERT_TRUE(persister->LoadSnapshot(&b, &idx, &t));
    EXPECT_EQ(b, "snap-payload");
    EXPECT_EQ(idx, 42U);
    EXPECT_EQ(t, 2U);

    // Follower replied.
    const auto out = n->TakeOutgoing();
    bool saw_resp = false;
    for (const auto& env : out) {
        if (env.to == "L" && std::holds_alternative<InstallSnapshotResp>(env.msg)) {
            saw_resp = true;
        }
    }
    EXPECT_TRUE(saw_resp);
}

TEST(RaftNodeTest, RecoversFromSnapshotOnCreate) {
    auto persister = MakeMemoryPersister();
    persister->SaveSnapshot("recovered-bytes", 99U, 7U);

    std::string captured;
    auto n = RaftNode::Create(
        {.self = "n",
         .peers = {},
         .rng_seed = 1,
         .persister = persister,
         .apply_snapshot_callback = [&](std::string_view b) { captured = std::string(b); }});

    EXPECT_EQ(captured, "recovered-bytes");
    EXPECT_EQ(n->LogStartIndex(), 100U);
    EXPECT_EQ(n->CommitIndex(), 99U);
}

TEST(RaftNodeTest, RecoversTermAndLogFromPersister) {
    auto persister = MakeMemoryPersister();
    LogEntry e1{.term = 3, .index = 1, .type = EntryType::kCommand, .payload = "x"};
    LogEntry e2{.term = 3, .index = 2, .type = EntryType::kCommand, .payload = "y"};
    persister->SaveState(3U, std::nullopt);
    persister->AppendLog(e1);
    persister->AppendLog(e2);

    auto n = RaftNode::Create({.self = "n", .peers = {"a"}, .rng_seed = 1, .persister = persister});
    EXPECT_EQ(n->CurrentTerm(), 3U);
    EXPECT_EQ(n->LastLogIndex(), 2U);
}

TEST(RaftNodeTest, PersistsTermAndVoteOnElection) {
    auto persister = MakeMemoryPersister();
    auto n = RaftNode::Create({.self = "a",
                               .peers = {"b"},
                               .election_timeout_min_ticks = 2,
                               .election_timeout_max_ticks = 2,
                               .rng_seed = 1,
                               .persister = persister});
    for (int i = 0; i < 2; ++i) {
        n->Tick();
    }
    PersistentState s;
    std::vector<LogEntry> es;
    ASSERT_TRUE(persister->Load(&s, &es));
    EXPECT_EQ(s.current_term, 1U);
    ASSERT_TRUE(s.voted_for.has_value());
    EXPECT_EQ(*s.voted_for, "a");
}

TEST(RaftNodeTest, PersistsLogOnLeaderSubmit) {
    auto persister = MakeMemoryPersister();
    auto n = RaftNode::Create({.self = "a",
                               .peers = {"b"},
                               .election_timeout_min_ticks = 2,
                               .election_timeout_max_ticks = 2,
                               .rng_seed = 1,
                               .persister = persister});
    for (int i = 0; i < 2; ++i) {
        n->Tick();
    }
    n->Step(
        Envelope{.from = "b", .to = "a", .msg = RequestVoteResp{.term = 1, .vote_granted = true}});
    (void)n->TakeOutgoing();

    (void)n->Submit(EntryType::kCommand, "hi");

    PersistentState s;
    std::vector<LogEntry> es;
    ASSERT_TRUE(persister->Load(&s, &es));
    ASSERT_EQ(es.size(), 1U);
    EXPECT_EQ(es[0].payload, "hi");
}

TEST(RaftNodeIntegrationTest, ThreeNodeRestartsKeepsCommittedEntry) {
    const std::vector<NodeId> all = {"n1", "n2", "n3"};
    std::vector<std::shared_ptr<Persister>> persisters;
    persisters.reserve(all.size());
    for (std::size_t i = 0; i < all.size(); ++i) {
        persisters.push_back(MakeMemoryPersister());
    }

    auto build_nodes = [&]() {
        std::vector<std::unique_ptr<RaftNode>> nodes;
        std::uint64_t seed = 100;
        for (std::size_t i = 0; i < all.size(); ++i) {
            std::vector<NodeId> peers;
            for (const auto& other : all) {
                if (other != all[i]) {
                    peers.push_back(other);
                }
            }
            nodes.push_back(RaftNode::Create({.self = all[i],
                                              .peers = peers,
                                              .election_timeout_min_ticks = 5,
                                              .election_timeout_max_ticks = 15,
                                              .heartbeat_interval_ticks = 2,
                                              .rng_seed = seed++,
                                              .persister = persisters[i]}));
        }
        return nodes;
    };

    // === First incarnation: elect a leader, Submit, commit, then teardown. ===
    {
        InMemoryBus bus;
        auto nodes = build_nodes();
        std::vector<std::unique_ptr<Transport>> transports;
        transports.reserve(all.size());
        for (const auto& id : all) {
            transports.push_back(bus.Connect(id));
        }

        for (int t = 0; t < 60; ++t) {
            for (std::size_t i = 0; i < nodes.size(); ++i) {
                StepWorld(*nodes[i], *transports[i]);
            }
        }

        RaftNode* leader = nullptr;
        for (const auto& n : nodes) {
            if (n->CurrentRole() == Role::kLeader) {
                leader = n.get();
                break;
            }
        }
        ASSERT_NE(leader, nullptr);
        const auto idx = leader->Submit(EntryType::kCommand, "durable");
        ASSERT_TRUE(idx.has_value());

        for (int t = 0; t < 30; ++t) {
            for (std::size_t i = 0; i < nodes.size(); ++i) {
                StepWorld(*nodes[i], *transports[i]);
            }
        }
    }  // first incarnation destroyed here; persisters survive in `persisters`.

    // === Recovery: rebuild nodes with the same persisters, fresh bus. ===
    {
        InMemoryBus bus2;
        auto nodes = build_nodes();
        std::vector<std::unique_ptr<Transport>> transports;
        transports.reserve(all.size());
        for (const auto& id : all) {
            transports.push_back(bus2.Connect(id));
        }

        // Every recovered node should already see the entry in its log.
        for (const auto& n : nodes) {
            EXPECT_GE(n->LastLogIndex(), 1U) << "node " << n->Self() << " lost its log on restart";
        }

        for (int t = 0; t < 100; ++t) {
            for (std::size_t i = 0; i < nodes.size(); ++i) {
                StepWorld(*nodes[i], *transports[i]);
            }
        }

        bool saw_durable = false;
        for (const auto& n : nodes) {
            auto applied = n->TakeCommitted();
            for (const auto& e : applied) {
                if (e.payload == "durable") {
                    saw_durable = true;
                }
            }
        }
        EXPECT_TRUE(saw_durable) << "no node surfaced the durable entry after restart";
    }
}
