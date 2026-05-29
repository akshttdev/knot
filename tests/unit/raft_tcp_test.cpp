// Knot — TcpTransport + RaftNode integration test (single process, real TCP).

#include <chrono>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <knot/raft/node.h>
#include <knot/raft/persister.h>
#include <knot/raft/tcp_transport.h>

#include <boost/asio.hpp>

using namespace knot::raft;
using namespace std::chrono_literals;

namespace {

std::pair<std::uint16_t, std::uint16_t> PickTwoFreePorts() {
    boost::asio::io_context io;
    boost::asio::ip::tcp::acceptor a1(io);
    a1.open(boost::asio::ip::tcp::v4());
    a1.bind({boost::asio::ip::tcp::v4(), 0});
    const auto p1 = a1.local_endpoint().port();
    a1.close();

    boost::asio::ip::tcp::acceptor a2(io);
    a2.open(boost::asio::ip::tcp::v4());
    a2.bind({boost::asio::ip::tcp::v4(), 0});
    const auto p2 = a2.local_endpoint().port();
    a2.close();

    return {p1, p2};
}

void DrivePair(RaftNode& na, Transport& ta, RaftNode& nb, Transport& tb) {
    na.Tick();
    for (auto& env : ta.Drain()) {
        na.Step(env);
    }
    for (auto& env : na.TakeOutgoing()) {
        ta.Send(std::move(env));
    }

    nb.Tick();
    for (auto& env : tb.Drain()) {
        nb.Step(env);
    }
    for (auto& env : nb.TakeOutgoing()) {
        tb.Send(std::move(env));
    }
}

}  // namespace

TEST(RaftTcpIntegrationTest, TwoNodesElectAndReplicateOverTcp) {
    const auto [port_a, port_b] = PickTwoFreePorts();

    auto persister_a = MakeMemoryPersister();
    auto persister_b = MakeMemoryPersister();

    // Bring up node b first so its accept socket is bound before a tries
    // to connect (a's outbound retries via backoff, but we start clean
    // for predictability).
    auto transport_b = MakeTcpTransport("b", "127.0.0.1", port_b,
                                        {{.id = "a", .host = "127.0.0.1", .port = port_a}});
    std::this_thread::sleep_for(50ms);

    auto transport_a = MakeTcpTransport("a", "127.0.0.1", port_a,
                                        {{.id = "b", .host = "127.0.0.1", .port = port_b}});

    auto node_a = RaftNode::Create({.self = "a",
                                    .peers = {"b"},
                                    .election_timeout_min_ticks = 15,
                                    .election_timeout_max_ticks = 30,
                                    .heartbeat_interval_ticks = 5,
                                    .rng_seed = 1,
                                    .persister = persister_a});
    auto node_b = RaftNode::Create({.self = "b",
                                    .peers = {"a"},
                                    .election_timeout_min_ticks = 15,
                                    .election_timeout_max_ticks = 30,
                                    .heartbeat_interval_ticks = 5,
                                    .rng_seed = 2,
                                    .persister = persister_b});

    // Give TCP handshakes a moment.
    std::this_thread::sleep_for(150ms);

    // Spin until a leader emerges or 5s deadline.
    const auto leader_deadline = std::chrono::steady_clock::now() + 5s;
    RaftNode* leader = nullptr;
    while (std::chrono::steady_clock::now() < leader_deadline && !leader) {
        DrivePair(*node_a, *transport_a, *node_b, *transport_b);
        std::this_thread::sleep_for(10ms);
        if (node_a->CurrentRole() == Role::kLeader) {
            leader = node_a.get();
        } else if (node_b->CurrentRole() == Role::kLeader) {
            leader = node_b.get();
        }
    }
    ASSERT_NE(leader, nullptr) << "no leader emerged within 5s";

    const auto submitted = leader->Submit(EntryType::kCommand, "tcp-hi");
    ASSERT_TRUE(submitted.has_value());

    // Drive until both nodes see the entry via TakeCommitted, 5s budget.
    const auto commit_deadline = std::chrono::steady_clock::now() + 5s;
    bool a_saw = false;
    bool b_saw = false;
    while (std::chrono::steady_clock::now() < commit_deadline && !(a_saw && b_saw)) {
        DrivePair(*node_a, *transport_a, *node_b, *transport_b);
        std::this_thread::sleep_for(10ms);
        for (auto& e : node_a->TakeCommitted()) {
            if (e.payload == "tcp-hi") {
                a_saw = true;
            }
        }
        for (auto& e : node_b->TakeCommitted()) {
            if (e.payload == "tcp-hi") {
                b_saw = true;
            }
        }
    }
    EXPECT_TRUE(a_saw) << "node a never surfaced the entry";
    EXPECT_TRUE(b_saw) << "node b never surfaced the entry";
}
