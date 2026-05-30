// Knot — End-to-end test: 3 nodes + 3 ClientServers + 1 client.

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <knot/raft/command.h>
#include <knot/raft/node.h>
#include <knot/raft/persister.h>
#include <knot/raft/tcp_transport.h>
#include <knot/raft/types.h>
#include <knot/server/client_server.h>
#include <knot/storage/engine.h>

#include <boost/asio.hpp>

#include "knot.pb.h"

using namespace knot::raft;
using namespace knot::server;
using namespace std::chrono_literals;

namespace asio = boost::asio;

namespace {

constexpr std::uint8_t kPutReq = 0x10;
constexpr std::uint8_t kPutResp = 0x11;
constexpr std::uint8_t kGetReq = 0x12;
constexpr std::uint8_t kGetResp = 0x13;

void PutBE32(std::vector<std::uint8_t>& out, std::uint32_t v) {
    out.push_back(static_cast<std::uint8_t>((v >> 24U) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((v >> 16U) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((v >> 8U) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>(v & 0xFFU));
}

std::vector<std::uint8_t> Frame(std::uint8_t tag, const std::string& payload) {
    const auto body_len = static_cast<std::uint32_t>(1U + payload.size());
    std::vector<std::uint8_t> out;
    out.reserve(4U + body_len);
    PutBE32(out, body_len);
    out.push_back(tag);
    out.insert(out.end(), payload.begin(), payload.end());
    return out;
}

struct RpcResult {
    std::uint8_t tag = 0;
    std::string payload;
};

std::optional<RpcResult> SyncRpc(std::uint16_t port, std::uint8_t req_tag,
                                 const std::string& req_payload) {
    asio::io_context io;
    asio::ip::tcp::socket sock(io);
    boost::system::error_code ec;
    asio::ip::tcp::endpoint ep(asio::ip::make_address("127.0.0.1"), port);
    sock.connect(ep, ec);  // NOLINT(bugprone-unused-return-value,cert-err33-c)
    if (ec) {
        return std::nullopt;
    }
    const auto frame = Frame(req_tag, req_payload);
    asio::write(sock, asio::buffer(frame), ec);
    if (ec) {
        return std::nullopt;
    }
    std::vector<std::uint8_t> len_buf(4);
    asio::read(sock, asio::buffer(len_buf), ec);
    if (ec) {
        return std::nullopt;
    }
    const std::uint32_t body_len = (static_cast<std::uint32_t>(len_buf[0]) << 24U) |
                                   (static_cast<std::uint32_t>(len_buf[1]) << 16U) |
                                   (static_cast<std::uint32_t>(len_buf[2]) << 8U) |
                                   static_cast<std::uint32_t>(len_buf[3]);
    if (body_len == 0U) {
        return std::nullopt;
    }
    std::vector<std::uint8_t> body(body_len);
    asio::read(sock, asio::buffer(body), ec);
    if (ec) {
        return std::nullopt;
    }
    RpcResult result;
    result.tag = body[0];
    result.payload = std::string(body.begin() + 1, body.end());
    return result;
}

// Pick N free ports on 127.0.0.1.
std::vector<std::uint16_t> PickFreePorts(std::size_t n) {
    std::vector<std::uint16_t> ports;
    asio::io_context io;
    std::vector<asio::ip::tcp::acceptor> hold;
    for (std::size_t i = 0; i < n; ++i) {
        asio::ip::tcp::acceptor a(io);
        a.open(asio::ip::tcp::v4());
        a.bind({asio::ip::tcp::v4(), 0});
        ports.push_back(a.local_endpoint().port());
        hold.push_back(std::move(a));
    }
    // hold goes out of scope; ports released — callers re-bind.
    return ports;
}

}  // namespace

TEST(RaftEndToEndTest, ThreeNodePutAndGetOverClientRpc) {
    // 3 inter-node ports + 3 client ports.
    auto ports = PickFreePorts(6);
    const std::array<std::uint16_t, 3> p_node = {ports[0], ports[1], ports[2]};
    const std::array<std::uint16_t, 3> p_cli = {ports[3], ports[4], ports[5]};

    const std::vector<NodeId> ids = {"n1", "n2", "n3"};

    struct NodeBundle {
        std::shared_ptr<Persister> persister;
        std::unique_ptr<RaftNode> node;
        std::unique_ptr<Transport> transport;
        std::unique_ptr<knot::storage::StorageEngine> engine;
        std::mutex node_mu;
        std::mutex engine_mu;
        std::unique_ptr<ClientServer> cs;
        std::atomic<bool> stop{false};
        std::thread driver;
    };

    std::vector<std::unique_ptr<NodeBundle>> bundles;
    bundles.reserve(3);
    for (std::size_t i = 0; i < 3; ++i) {
        bundles.push_back(std::make_unique<NodeBundle>());
    }

    // Build all 3.
    for (std::size_t i = 0; i < 3; ++i) {
        auto& b = *bundles[i];
        b.persister = MakeMemoryPersister();

        std::vector<PeerEndpoint> peers;
        std::vector<NodeId> peer_ids;
        for (std::size_t j = 0; j < 3; ++j) {
            if (j == i) {
                continue;
            }
            peers.push_back({.id = ids[j], .host = "127.0.0.1", .port = p_node[j]});
            peer_ids.push_back(ids[j]);
        }

        b.node = RaftNode::Create({.self = ids[i],
                                   .peers = peer_ids,
                                   .election_timeout_min_ticks = 5,
                                   .election_timeout_max_ticks = 15,
                                   .heartbeat_interval_ticks = 2,
                                   .rng_seed = 100U + i,
                                   .persister = b.persister});

        b.transport = MakeTcpTransport(ids[i], "127.0.0.1", p_node[i], peers);

        // Temp storage dir per node.
        const auto dir =
            std::filesystem::temp_directory_path() /
            ("knot_e2e_" + std::to_string(i) + "_" +
             std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        b.engine = knot::storage::StorageEngine::Open({.data_dir = dir});

        b.cs = MakeClientServer("127.0.0.1", p_cli[i], b.node.get(), b.engine.get(), &b.node_mu,
                                &b.engine_mu);

        b.driver = std::thread([&b]() {
            while (!b.stop.load()) {
                {
                    std::lock_guard<std::mutex> lk(b.node_mu);
                    b.node->Tick();
                    for (auto& env : b.transport->Drain()) {
                        b.node->Step(env);
                    }
                    for (auto& env : b.node->TakeOutgoing()) {
                        b.transport->Send(std::move(env));
                    }
                    auto committed = b.node->TakeCommitted();
                    if (!committed.empty()) {
                        std::lock_guard<std::mutex> elk(b.engine_mu);
                        for (const auto& entry : committed) {
                            if (entry.type != EntryType::kCommand) {
                                continue;
                            }
                            auto cmd = DecodeCommand(entry.payload);
                            if (!cmd.has_value()) {
                                continue;
                            }
                            if (std::holds_alternative<PutCmd>(*cmd)) {
                                const auto& p = std::get<PutCmd>(*cmd);
                                b.engine->Put(p.key, p.value);
                            } else {
                                const auto& d = std::get<DeleteCmd>(*cmd);
                                b.engine->Delete(d.key);
                            }
                        }
                    }
                }
                std::this_thread::sleep_for(10ms);
            }
        });
    }

    // Wait for a leader to emerge (up to 5s).
    int leader_idx = -1;
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (std::chrono::steady_clock::now() < deadline && leader_idx < 0) {
        for (std::size_t i = 0; i < 3; ++i) {
            std::lock_guard<std::mutex> lk(bundles[i]->node_mu);
            if (bundles[i]->node->CurrentRole() == Role::kLeader) {
                leader_idx = static_cast<int>(i);
                break;
            }
        }
        if (leader_idx < 0) {
            std::this_thread::sleep_for(50ms);
        }
    }
    ASSERT_GE(leader_idx, 0) << "no leader emerged in 5s";

    // Send a Put to the leader directly.
    {
        knot::rpc::ClientPutRequest req;
        req.set_client_id("test");
        req.set_seq_no(1);
        req.set_key("hello");
        req.set_value("world");
        std::string buf;
        req.SerializeToString(&buf);
        auto r = SyncRpc(p_cli[leader_idx], kPutReq, buf);
        ASSERT_TRUE(r.has_value());
        ASSERT_EQ(r->tag, kPutResp);
        knot::rpc::ClientPutResponse resp;
        ASSERT_TRUE(resp.ParseFromString(r->payload));
        EXPECT_TRUE(resp.success()) << "Put failed: " << resp.error();
    }

    // Allow some ticks for replication.
    std::this_thread::sleep_for(300ms);

    // Send a Get to the leader.
    {
        knot::rpc::ClientGetRequest req;
        req.set_client_id("test");
        req.set_seq_no(2);
        req.set_key("hello");
        std::string buf;
        req.SerializeToString(&buf);
        auto r = SyncRpc(p_cli[leader_idx], kGetReq, buf);
        ASSERT_TRUE(r.has_value());
        ASSERT_EQ(r->tag, kGetResp);
        knot::rpc::ClientGetResponse resp;
        ASSERT_TRUE(resp.ParseFromString(r->payload));
        EXPECT_TRUE(resp.success());
        EXPECT_TRUE(resp.found());
        EXPECT_EQ(resp.value(), "world");
    }

    // Shutdown all bundles cleanly.
    for (auto& b : bundles) {
        b->stop.store(true);
        if (b->driver.joinable()) {
            b->driver.join();
        }
        b->cs.reset();
    }
}

TEST(RaftEndToEndTest, SnapshotPersistsAcrossClusterRestart) {
    const std::vector<NodeId> ids = {"n1", "n2", "n3"};

    // Pre-create temp data dirs that survive across "restarts".
    std::vector<std::filesystem::path> data_dirs;
    data_dirs.reserve(3);
    for (std::size_t i = 0; i < 3; ++i) {
        const auto d =
            std::filesystem::temp_directory_path() /
            ("knot_snap_e2e_" + std::to_string(i) + "_" +
             std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(d);
        data_dirs.push_back(d);
    }

    // === Phase 1: bring up cluster, Put 50 keys, snapshot, tear down. ===
    constexpr int kNumKeys = 50;
    int leader_phase1 = -1;

    {
        auto ports = PickFreePorts(6);
        const std::array<std::uint16_t, 3> p_node = {ports[0], ports[1], ports[2]};
        const std::array<std::uint16_t, 3> p_cli = {ports[3], ports[4], ports[5]};

        struct Bundle {
            std::shared_ptr<knot::raft::Persister> persister;
            std::unique_ptr<knot::storage::StorageEngine> engine;
            std::unique_ptr<RaftNode> node;
            std::unique_ptr<Transport> transport;
            std::mutex node_mu;
            std::mutex engine_mu;
            std::unique_ptr<ClientServer> cs;
            std::atomic<bool> stop{false};
            std::thread driver;
        };

        std::vector<std::unique_ptr<Bundle>> bundles;
        bundles.reserve(3);
        for (std::size_t i = 0; i < 3; ++i) {
            bundles.push_back(std::make_unique<Bundle>());
        }

        for (std::size_t i = 0; i < 3; ++i) {
            auto& b = *bundles[i];
            b.persister = knot::raft::MakeFilePersister(data_dirs[i]);
            b.engine = knot::storage::StorageEngine::Open({.data_dir = data_dirs[i] / "storage"});

            std::vector<PeerEndpoint> peers;
            std::vector<NodeId> peer_ids;
            for (std::size_t j = 0; j < 3; ++j) {
                if (j == i) {
                    continue;
                }
                peers.push_back({.id = ids[j], .host = "127.0.0.1", .port = p_node[j]});
                peer_ids.push_back(ids[j]);
            }

            knot::raft::RaftNode::Config cfg{
                .self = ids[i],
                .peers = peer_ids,
                .election_timeout_min_ticks = 5,
                .election_timeout_max_ticks = 15,
                .heartbeat_interval_ticks = 2,
                .rng_seed = 100U + i,
                .persister = b.persister,
            };
            auto* engine_ptr = b.engine.get();
            auto* engine_mu_ptr = &b.engine_mu;
            cfg.apply_snapshot_callback = [engine_ptr, engine_mu_ptr](std::string_view bytes) {
                std::lock_guard<std::mutex> lk(*engine_mu_ptr);
                engine_ptr->ApplySnapshot(bytes);
            };
            b.node = RaftNode::Create(cfg);
            b.transport = MakeTcpTransport(ids[i], "127.0.0.1", p_node[i], peers);
            b.cs = MakeClientServer("127.0.0.1", p_cli[i], b.node.get(), b.engine.get(), &b.node_mu,
                                    &b.engine_mu);

            b.driver = std::thread([&b]() {
                while (!b.stop.load()) {
                    {
                        std::lock_guard<std::mutex> lk(b.node_mu);
                        b.node->Tick();
                        for (auto& env : b.transport->Drain()) {
                            b.node->Step(env);
                        }
                        for (auto& env : b.node->TakeOutgoing()) {
                            b.transport->Send(std::move(env));
                        }
                        auto committed = b.node->TakeCommitted();
                        if (!committed.empty()) {
                            std::lock_guard<std::mutex> elk(b.engine_mu);
                            for (const auto& entry : committed) {
                                if (entry.type != EntryType::kCommand) {
                                    continue;
                                }
                                auto cmd = knot::raft::DecodeCommand(entry.payload);
                                if (!cmd.has_value()) {
                                    continue;
                                }
                                if (std::holds_alternative<knot::raft::PutCmd>(*cmd)) {
                                    const auto& p = std::get<knot::raft::PutCmd>(*cmd);
                                    b.engine->Put(p.key, p.value);
                                } else {
                                    const auto& d = std::get<knot::raft::DeleteCmd>(*cmd);
                                    b.engine->Delete(d.key);
                                }
                            }
                        }
                    }
                    std::this_thread::sleep_for(10ms);
                }
            });
        }

        // Wait for a leader.
        const auto leader_deadline = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < leader_deadline && leader_phase1 < 0) {
            for (std::size_t i = 0; i < 3; ++i) {
                std::lock_guard<std::mutex> lk(bundles[i]->node_mu);
                if (bundles[i]->node->CurrentRole() == Role::kLeader) {
                    leader_phase1 = static_cast<int>(i);
                    break;
                }
            }
            if (leader_phase1 < 0) {
                std::this_thread::sleep_for(50ms);
            }
        }
        ASSERT_GE(leader_phase1, 0) << "no leader in 5s";

        // Put 50 keys via client RPC to the leader.
        for (int k = 0; k < kNumKeys; ++k) {
            knot::rpc::ClientPutRequest req;
            req.set_client_id("snap-test");
            req.set_seq_no(static_cast<std::uint64_t>(k) + 1U);
            req.set_key("k" + std::to_string(k));
            req.set_value("v" + std::to_string(k));
            std::string buf;
            req.SerializeToString(&buf);
            auto r = SyncRpc(p_cli[leader_phase1], kPutReq, buf);
            ASSERT_TRUE(r.has_value()) << "put " << k << " failed";
        }

        // Wait for the cluster to fully apply.
        std::this_thread::sleep_for(300ms);

        // Manually snapshot the leader.
        {
            auto& lead = *bundles[leader_phase1];
            std::string snap;
            {
                std::lock_guard<std::mutex> elk(lead.engine_mu);
                snap = lead.engine->Snapshot();
            }
            std::lock_guard<std::mutex> lk(lead.node_mu);
            const auto applied = lead.node->CommitIndex();
            const auto term = lead.node->TermAtIndex(applied);
            lead.node->MaybeCreateSnapshot(applied, term, snap);
        }

        // Drive a few more ticks so followers receive any subsequent AE/IS.
        std::this_thread::sleep_for(300ms);

        // Tear down.
        for (auto& b : bundles) {
            b->stop.store(true);
            if (b->driver.joinable()) {
                b->driver.join();
            }
        }
    }  // bundles destroyed; persisters' data on disk survives.

    // === Phase 2: bring up 3 fresh nodes from the same data dirs. ===
    int leader_phase2 = -1;
    {
        auto ports = PickFreePorts(6);
        const std::array<std::uint16_t, 3> p_node = {ports[0], ports[1], ports[2]};
        const std::array<std::uint16_t, 3> p_cli = {ports[3], ports[4], ports[5]};

        struct Bundle {
            std::shared_ptr<knot::raft::Persister> persister;
            std::unique_ptr<knot::storage::StorageEngine> engine;
            std::unique_ptr<RaftNode> node;
            std::unique_ptr<Transport> transport;
            std::mutex node_mu;
            std::mutex engine_mu;
            std::unique_ptr<ClientServer> cs;
            std::atomic<bool> stop{false};
            std::thread driver;
        };

        std::vector<std::unique_ptr<Bundle>> bundles;
        bundles.reserve(3);
        for (std::size_t i = 0; i < 3; ++i) {
            bundles.push_back(std::make_unique<Bundle>());
        }

        for (std::size_t i = 0; i < 3; ++i) {
            auto& b = *bundles[i];
            b.persister = knot::raft::MakeFilePersister(data_dirs[i]);
            b.engine = knot::storage::StorageEngine::Open({.data_dir = data_dirs[i] / "storage"});

            std::vector<PeerEndpoint> peers;
            std::vector<NodeId> peer_ids;
            for (std::size_t j = 0; j < 3; ++j) {
                if (j == i) {
                    continue;
                }
                peers.push_back({.id = ids[j], .host = "127.0.0.1", .port = p_node[j]});
                peer_ids.push_back(ids[j]);
            }

            knot::raft::RaftNode::Config cfg{
                .self = ids[i],
                .peers = peer_ids,
                .election_timeout_min_ticks = 5,
                .election_timeout_max_ticks = 15,
                .heartbeat_interval_ticks = 2,
                .rng_seed = 200U + i,
                .persister = b.persister,
            };
            auto* engine_ptr = b.engine.get();
            auto* engine_mu_ptr = &b.engine_mu;
            cfg.apply_snapshot_callback = [engine_ptr, engine_mu_ptr](std::string_view bytes) {
                std::lock_guard<std::mutex> lk(*engine_mu_ptr);
                engine_ptr->ApplySnapshot(bytes);
            };
            b.node = RaftNode::Create(cfg);
            b.transport = MakeTcpTransport(ids[i], "127.0.0.1", p_node[i], peers);
            b.cs = MakeClientServer("127.0.0.1", p_cli[i], b.node.get(), b.engine.get(), &b.node_mu,
                                    &b.engine_mu);

            b.driver = std::thread([&b]() {
                while (!b.stop.load()) {
                    {
                        std::lock_guard<std::mutex> lk(b.node_mu);
                        b.node->Tick();
                        for (auto& env : b.transport->Drain()) {
                            b.node->Step(env);
                        }
                        for (auto& env : b.node->TakeOutgoing()) {
                            b.transport->Send(std::move(env));
                        }
                        auto committed = b.node->TakeCommitted();
                        if (!committed.empty()) {
                            std::lock_guard<std::mutex> elk(b.engine_mu);
                            for (const auto& entry : committed) {
                                if (entry.type != EntryType::kCommand) {
                                    continue;
                                }
                                auto cmd = knot::raft::DecodeCommand(entry.payload);
                                if (!cmd.has_value()) {
                                    continue;
                                }
                                if (std::holds_alternative<knot::raft::PutCmd>(*cmd)) {
                                    const auto& p = std::get<knot::raft::PutCmd>(*cmd);
                                    b.engine->Put(p.key, p.value);
                                } else {
                                    const auto& d = std::get<knot::raft::DeleteCmd>(*cmd);
                                    b.engine->Delete(d.key);
                                }
                            }
                        }
                    }
                    std::this_thread::sleep_for(10ms);
                }
            });
        }

        // Wait for a leader.
        const auto leader_deadline = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < leader_deadline && leader_phase2 < 0) {
            for (std::size_t i = 0; i < 3; ++i) {
                std::lock_guard<std::mutex> lk(bundles[i]->node_mu);
                if (bundles[i]->node->CurrentRole() == Role::kLeader) {
                    leader_phase2 = static_cast<int>(i);
                    break;
                }
            }
            if (leader_phase2 < 0) {
                std::this_thread::sleep_for(50ms);
            }
        }
        ASSERT_GE(leader_phase2, 0) << "no leader in phase 2 within 5s";

        // Allow extra time for the cluster to stabilize after restart.
        std::this_thread::sleep_for(500ms);

        // Verify all 50 keys are still readable via the leader.
        for (int k = 0; k < kNumKeys; ++k) {
            knot::rpc::ClientGetRequest req;
            req.set_client_id("verify");
            req.set_seq_no(static_cast<std::uint64_t>(k) + 1U);
            req.set_key("k" + std::to_string(k));
            std::string buf;
            req.SerializeToString(&buf);
            auto r = SyncRpc(p_cli[leader_phase2], kGetReq, buf);
            ASSERT_TRUE(r.has_value()) << "get " << k << " failed (no response)";
            ASSERT_EQ(r->tag, kGetResp) << "get " << k << " unexpected tag";
            knot::rpc::ClientGetResponse resp;
            ASSERT_TRUE(resp.ParseFromString(r->payload));
            EXPECT_TRUE(resp.success()) << "get " << k << " response not success";
            EXPECT_TRUE(resp.found()) << "key k" << k << " not found after restart";
            EXPECT_EQ(resp.value(), "v" + std::to_string(k)) << "wrong value for k" << k;
        }

        // Tear down.
        for (auto& b : bundles) {
            b->stop.store(true);
            if (b->driver.joinable()) {
                b->driver.join();
            }
        }
    }

    // Cleanup temp dirs.
    for (const auto& d : data_dirs) {
        std::error_code ec;
        std::filesystem::remove_all(d, ec);
    }
}
