// Knot — ClientServer integration tests.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <knot/raft/command.h>
#include <knot/raft/node.h>
#include <knot/raft/persister.h>
#include <knot/raft/types.h>
#include <knot/server/client_server.h>
#include <knot/storage/engine.h>

#include <boost/asio.hpp>

#include "knot.pb.h"

using namespace knot::raft;
using namespace knot::server;

namespace asio = boost::asio;

// -------------------------------------------------------------------------
// Wire helpers (mirrors client_server.cpp's private helpers)
// -------------------------------------------------------------------------

namespace {

constexpr std::uint8_t kPutReq = 0x10;
constexpr std::uint8_t kPutResp = 0x11;
constexpr std::uint8_t kGetReq = 0x12;
constexpr std::uint8_t kGetResp = 0x13;

void PutBE32Helper(std::vector<std::uint8_t>& out, std::uint32_t v) {
    out.push_back(static_cast<std::uint8_t>((v >> 24U) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((v >> 16U) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((v >> 8U) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>(v & 0xFFU));
}

std::vector<std::uint8_t> BuildFrame(std::uint8_t tag, const std::string& payload) {
    const auto body_len = static_cast<std::uint32_t>(1U + payload.size());
    std::vector<std::uint8_t> out;
    out.reserve(4U + body_len);
    PutBE32Helper(out, body_len);
    out.push_back(tag);
    out.insert(out.end(), payload.begin(), payload.end());
    return out;
}

// Synchronous TCP client: connect, send one framed request, read one
// framed response, and return [tag, payload].
// Returns std::nullopt on any error.
struct RpcResult {
    std::uint8_t tag = 0;
    std::string payload;
};

std::optional<RpcResult> SyncRpc(std::uint16_t port, std::uint8_t req_tag,
                                 const std::string& req_payload) {
    asio::io_context io;
    asio::ip::tcp::socket sock(io);
    asio::ip::tcp::endpoint ep(asio::ip::make_address("127.0.0.1"), port);
    boost::system::error_code ec;
    sock.connect(ep, ec);  // NOLINT(bugprone-unused-return-value,cert-err33-c)
    if (ec) {
        return std::nullopt;
    }

    // Send request.
    auto frame = BuildFrame(req_tag, req_payload);
    asio::write(sock, asio::buffer(frame), ec);
    if (ec) {
        return std::nullopt;
    }

    // Set a read deadline via a deadline timer approach: just do sync reads with
    // timeouts set via socket option. We rely on the test's overall time budget.
    // Read 4-byte length prefix.
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

    std::vector<std::uint8_t> body_buf(body_len);
    asio::read(sock, asio::buffer(body_buf), ec);
    if (ec) {
        return std::nullopt;
    }

    RpcResult result;
    result.tag = body_buf[0];
    result.payload = std::string(body_buf.begin() + 1, body_buf.end());
    return result;
}

// -------------------------------------------------------------------------
// TestFixture — single-node always-leader setup.
//
// Creates a RaftNode with no peers (so it self-elects on first Tick),
// opens a ClientServer on port 0 (OS-assigned), runs a driver thread
// that ticks the node every 5ms and applies committed entries to the engine.
// -------------------------------------------------------------------------

struct SingleNodeFixture {
    SingleNodeFixture(const SingleNodeFixture&) = delete;
    SingleNodeFixture& operator=(const SingleNodeFixture&) = delete;
    SingleNodeFixture(SingleNodeFixture&&) = delete;
    SingleNodeFixture& operator=(SingleNodeFixture&&) = delete;

    std::shared_ptr<Persister> persister;
    std::unique_ptr<RaftNode> node;
    std::unique_ptr<knot::storage::StorageEngine> engine;
    std::mutex node_mu;
    std::mutex engine_mu;
    std::unique_ptr<ClientServer> server;
    std::uint16_t port = 0;

    std::atomic<bool> stop_flag{false};
    std::thread driver_thread;

    // NOLINTNEXTLINE(readability-function-cognitive-complexity)
    explicit SingleNodeFixture(bool with_engine = true) {
        persister = MakeMemoryPersister();
        node = RaftNode::Create({
            .self = "s",
            .peers = {},
            .election_timeout_min_ticks = 2,
            .election_timeout_max_ticks = 4,
            .heartbeat_interval_ticks = 1,
            .persister = persister,
        });

        if (with_engine) {
            // Use a temporary in-memory-like directory.
            const auto tmp_dir =
                std::filesystem::temp_directory_path() /
                ("knot_client_test_" +
                 std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
            engine = knot::storage::StorageEngine::Open({.data_dir = tmp_dir});
        }

        // Bind on port 0 — OS assigns a free port.
        // We discover the assigned port after binding.
        // Use a temporary acceptor to get the port.
        {
            asio::io_context probe_io;
            asio::ip::tcp::acceptor probe_acc(probe_io);
            asio::ip::tcp::endpoint ep(asio::ip::make_address("127.0.0.1"), 0);
            probe_acc.open(ep.protocol());
            probe_acc.set_option(asio::socket_base::reuse_address(true));
            probe_acc.bind(ep);
            port = probe_acc.local_endpoint().port();
            // probe_acc destructor releases the port; ClientServer re-binds it.
        }

        server =
            MakeClientServer("127.0.0.1", port, node.get(), engine.get(), &node_mu, &engine_mu);

        // Driver thread: tick node, drain outgoing (no peers so nothing to send),
        // apply committed entries to engine.
        driver_thread = std::thread([this]() {
            while (!stop_flag.load()) {
                {
                    std::lock_guard<std::mutex> lk(node_mu);
                    node->Tick();
                    // Drain outgoing messages (no peers, so they are discarded).
                    (void)node->TakeOutgoing();
                    // Apply committed entries to engine.
                    auto committed = node->TakeCommitted();
                    if (!committed.empty() && engine != nullptr) {
                        std::lock_guard<std::mutex> elk(engine_mu);
                        for (const auto& entry : committed) {
                            if (entry.type != EntryType::kCommand) {
                                continue;
                            }
                            auto cmd = DecodeCommand(entry.payload);
                            if (!cmd.has_value()) {
                                continue;
                            }
                            if (std::holds_alternative<PutCmd>(*cmd)) {
                                const auto& put = std::get<PutCmd>(*cmd);
                                engine->Put(put.key, put.value);
                            } else if (std::holds_alternative<DeleteCmd>(*cmd)) {
                                const auto& del = std::get<DeleteCmd>(*cmd);
                                engine->Delete(del.key);
                            }
                        }
                    }
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
        });

        // Wait for the node to become leader (up to 2s).
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (std::chrono::steady_clock::now() < deadline) {
            std::lock_guard<std::mutex> lk(node_mu);
            if (node->CurrentRole() == Role::kLeader) {
                break;
            }
        }
    }

    ~SingleNodeFixture() {
        stop_flag.store(true);
        if (driver_thread.joinable()) {
            driver_thread.join();
        }
        server.reset();
    }
};

// Same fixture but node has a peer so it stays Follower.
struct FollowerNodeFixture {
    FollowerNodeFixture(const FollowerNodeFixture&) = delete;
    FollowerNodeFixture& operator=(const FollowerNodeFixture&) = delete;
    FollowerNodeFixture(FollowerNodeFixture&&) = delete;
    FollowerNodeFixture& operator=(FollowerNodeFixture&&) = delete;

    std::shared_ptr<Persister> persister;
    std::unique_ptr<RaftNode> node;
    std::mutex node_mu;
    std::mutex engine_mu;
    std::unique_ptr<ClientServer> server;
    std::uint16_t port = 0;

    explicit FollowerNodeFixture() {
        persister = MakeMemoryPersister();
        node = RaftNode::Create({
            .self = "a",
            .peers = {"b"},  // b never connects; quorum impossible → stays Follower
            .election_timeout_min_ticks = 1000,  // very long timeout so we don't become candidate
            .election_timeout_max_ticks = 2000,
            .heartbeat_interval_ticks = 1,
            .persister = persister,
        });

        {
            asio::io_context probe_io;
            asio::ip::tcp::acceptor probe_acc(probe_io);
            asio::ip::tcp::endpoint ep(asio::ip::make_address("127.0.0.1"), 0);
            probe_acc.open(ep.protocol());
            probe_acc.set_option(asio::socket_base::reuse_address(true));
            probe_acc.bind(ep);
            port = probe_acc.local_endpoint().port();
        }

        server = MakeClientServer("127.0.0.1", port, node.get(),
                                  /*engine=*/nullptr, &node_mu, &engine_mu);
    }

    ~FollowerNodeFixture() { server.reset(); }
};

}  // namespace

// -------------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------------

TEST(ClientServerSmokeTest, ConstructAndDestructDoesNotCrash) {
    auto persister = MakeMemoryPersister();
    auto node = RaftNode::Create({.self = "s", .peers = {}, .persister = persister});
    std::mutex node_mu;
    std::mutex engine_mu;

    auto cs = MakeClientServer("127.0.0.1", /*listen_port=*/0, node.get(),
                               /*engine=*/nullptr, &node_mu, &engine_mu);
    ASSERT_NE(cs, nullptr);
    // cs goes out of scope; destructor stops cleanly.
}

TEST(ClientServerTest, PutThenGetReturnsValue) {
    SingleNodeFixture fix;

    // Confirm we have a leader.
    {
        std::lock_guard<std::mutex> lk(fix.node_mu);
        ASSERT_EQ(fix.node->CurrentRole(), Role::kLeader);
    }

    // --- Put request ---
    knot::rpc::ClientPutRequest put_req;
    put_req.set_client_id("cli");
    put_req.set_seq_no(1U);
    put_req.set_key("k");
    put_req.set_value("v");
    std::string put_payload;
    put_req.SerializeToString(&put_payload);

    auto put_result = SyncRpc(fix.port, kPutReq, put_payload);
    ASSERT_TRUE(put_result.has_value()) << "Put RPC timed out or failed";
    EXPECT_EQ(put_result->tag, kPutResp);

    knot::rpc::ClientPutResponse put_resp;
    ASSERT_TRUE(put_resp.ParseFromString(put_result->payload));
    EXPECT_TRUE(put_resp.success()) << "Put failed: " << put_resp.error();

    // --- Get request ---
    knot::rpc::ClientGetRequest get_req;
    get_req.set_client_id("cli");
    get_req.set_seq_no(2U);
    get_req.set_key("k");
    std::string get_payload;
    get_req.SerializeToString(&get_payload);

    auto get_result = SyncRpc(fix.port, kGetReq, get_payload);
    ASSERT_TRUE(get_result.has_value()) << "Get RPC timed out or failed";
    EXPECT_EQ(get_result->tag, kGetResp);

    knot::rpc::ClientGetResponse get_resp;
    ASSERT_TRUE(get_resp.ParseFromString(get_result->payload));
    EXPECT_TRUE(get_resp.success()) << "Get failed: " << get_resp.error();
    EXPECT_TRUE(get_resp.found());
    EXPECT_EQ(get_resp.value(), "v");
}

TEST(ClientServerTest, IdempotentPutOnlyAppliesOnce) {
    SingleNodeFixture fix;

    // Confirm we have a leader.
    {
        std::lock_guard<std::mutex> lk(fix.node_mu);
        ASSERT_EQ(fix.node->CurrentRole(), Role::kLeader);
    }

    // First Put: client_id="cli", seq=1, key="k", value="v1" → should succeed.
    auto send_put = [&](const std::string& key, const std::string& value,
                        std::uint64_t seq) -> knot::rpc::ClientPutResponse {
        knot::rpc::ClientPutRequest req;
        req.set_client_id("cli");
        req.set_seq_no(seq);
        req.set_key(key);
        req.set_value(value);
        std::string payload;
        req.SerializeToString(&payload);
        auto result = SyncRpc(fix.port, kPutReq, payload);
        knot::rpc::ClientPutResponse resp;
        if (result.has_value()) {
            resp.ParseFromString(result->payload);
        }
        return resp;
    };

    auto resp1 = send_put("k", "v1", 1U);
    ASSERT_TRUE(resp1.success()) << "First put failed: " << resp1.error();

    // Duplicate Put: same (client_id="cli", seq=1) but different value="v2".
    // Should return cached response (success=true) but NOT overwrite "v1".
    auto resp2 = send_put("k", "v2", 1U);
    EXPECT_TRUE(resp2.success()) << "Duplicate put should return cached success";

    // Get: should still return "v1" (second put was deduplicated).
    knot::rpc::ClientGetRequest get_req;
    get_req.set_client_id("cli");
    get_req.set_seq_no(3U);
    get_req.set_key("k");
    std::string get_payload;
    get_req.SerializeToString(&get_payload);

    auto get_result = SyncRpc(fix.port, kGetReq, get_payload);
    ASSERT_TRUE(get_result.has_value()) << "Get RPC timed out";
    knot::rpc::ClientGetResponse get_resp;
    ASSERT_TRUE(get_resp.ParseFromString(get_result->payload));
    EXPECT_TRUE(get_resp.success());
    EXPECT_TRUE(get_resp.found());
    EXPECT_EQ(get_resp.value(), "v1") << "Expected v1 but got: " << get_resp.value();
}

TEST(ClientServerTest, GetOnFollowerReturnsNotLeader) {
    FollowerNodeFixture fix;

    // The node has a peer it can never reach, so it never wins an election
    // within the test timeframe (election timeout is very long).
    {
        std::lock_guard<std::mutex> lk(fix.node_mu);
        ASSERT_NE(fix.node->CurrentRole(), Role::kLeader);
    }

    knot::rpc::ClientGetRequest get_req;
    get_req.set_client_id("cli");
    get_req.set_seq_no(1U);
    get_req.set_key("anything");
    std::string get_payload;
    get_req.SerializeToString(&get_payload);

    auto result = SyncRpc(fix.port, kGetReq, get_payload);
    ASSERT_TRUE(result.has_value()) << "Get RPC on follower timed out";
    EXPECT_EQ(result->tag, kGetResp);

    knot::rpc::ClientGetResponse resp;
    ASSERT_TRUE(resp.ParseFromString(result->payload));
    EXPECT_FALSE(resp.success()) << "Expected success=false from non-leader";
}
