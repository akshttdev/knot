// Knot — wire codec tests.

#include <chrono>
#include <thread>
#include <variant>

#include <gtest/gtest.h>
#include <knot/raft/tcp_transport.h>
#include <knot/raft/wire.h>

#include <boost/asio.hpp>

using namespace knot::raft;

namespace {

Envelope RoundTrip(const Envelope& in, const NodeId& self_id) {
    const auto bytes = Encode(in);
    const auto res = DecodeOne(bytes.data(), bytes.size(), self_id);
    EXPECT_GT(res.bytes_consumed, 0U);
    EXPECT_TRUE(res.envelope.has_value());
    return *res.envelope;
}

}  // namespace

TEST(WireCodecTest, EncodeDecodeRequestVoteReq) {
    Envelope in{.from = "a",
                .to = "b",
                .msg = RequestVoteReq{
                    .term = 5, .candidate_id = "a", .last_log_index = 10, .last_log_term = 4}};
    const auto out = RoundTrip(in, "b");
    ASSERT_TRUE(std::holds_alternative<RequestVoteReq>(out.msg));
    const auto& m = std::get<RequestVoteReq>(out.msg);
    EXPECT_EQ(m.term, 5U);
    EXPECT_EQ(m.candidate_id, "a");
    EXPECT_EQ(m.last_log_index, 10U);
    EXPECT_EQ(m.last_log_term, 4U);
    EXPECT_EQ(out.from, "a");
    EXPECT_EQ(out.to, "b");
}

TEST(WireCodecTest, EncodeDecodeRequestVoteResp) {
    Envelope in{.from = "b", .to = "a", .msg = RequestVoteResp{.term = 7, .vote_granted = true}};
    const auto out = RoundTrip(in, "a");
    ASSERT_TRUE(std::holds_alternative<RequestVoteResp>(out.msg));
    const auto& m = std::get<RequestVoteResp>(out.msg);
    EXPECT_EQ(m.term, 7U);
    EXPECT_TRUE(m.vote_granted);
}

TEST(WireCodecTest, EncodeDecodeAppendEntriesReqWithEntries) {
    LogEntry e1{.term = 3, .index = 1, .type = EntryType::kCommand, .payload = "hello"};
    LogEntry e2{.term = 3, .index = 2, .type = EntryType::kCommand, .payload = "world"};
    Envelope in{.from = "leader",
                .to = "f",
                .msg = AppendEntriesReq{.term = 3,
                                        .leader_id = "leader",
                                        .prev_log_index = 0,
                                        .prev_log_term = 0,
                                        .entries = {e1, e2},
                                        .leader_commit = 2}};
    const auto out = RoundTrip(in, "f");
    ASSERT_TRUE(std::holds_alternative<AppendEntriesReq>(out.msg));
    const auto& m = std::get<AppendEntriesReq>(out.msg);
    EXPECT_EQ(m.term, 3U);
    EXPECT_EQ(m.leader_id, "leader");
    ASSERT_EQ(m.entries.size(), 2U);
    EXPECT_EQ(m.entries[0].payload, "hello");
    EXPECT_EQ(m.entries[1].payload, "world");
    EXPECT_EQ(m.leader_commit, 2U);
    EXPECT_EQ(out.from, "leader");
}

TEST(WireCodecTest, EncodeDecodeAppendEntriesRespWithConflictFields) {
    Envelope in{.from = "f",
                .to = "leader",
                .msg = AppendEntriesResp{.term = 4,
                                         .success = false,
                                         .match_index = 0,
                                         .conflict_index = 3,
                                         .conflict_term = 2}};
    const auto out = RoundTrip(in, "leader");
    ASSERT_TRUE(std::holds_alternative<AppendEntriesResp>(out.msg));
    const auto& m = std::get<AppendEntriesResp>(out.msg);
    EXPECT_EQ(m.term, 4U);
    EXPECT_FALSE(m.success);
    EXPECT_EQ(m.conflict_index, 3U);
    EXPECT_EQ(m.conflict_term, 2U);
}

TEST(WireCodecTest, DecodePartialBufferReturnsNothing) {
    Envelope in{.from = "a",
                .to = "b",
                .msg = RequestVoteReq{
                    .term = 1, .candidate_id = "a", .last_log_index = 0, .last_log_term = 0}};
    const auto bytes = Encode(in);
    ASSERT_GT(bytes.size(), 3U);
    const auto res = DecodeOne(bytes.data(), 3, "b");
    EXPECT_EQ(res.bytes_consumed, 0U);
    EXPECT_FALSE(res.envelope.has_value());
    EXPECT_FALSE(res.handshake_sender.has_value());
}

TEST(WireCodecTest, EncodeDecodeHandshakeYieldsSenderId) {
    const auto bytes = EncodeHandshake("n1");
    const auto res = DecodeOne(bytes.data(), bytes.size(), "self");
    EXPECT_GT(res.bytes_consumed, 0U);
    EXPECT_FALSE(res.envelope.has_value());
    ASSERT_TRUE(res.handshake_sender.has_value());
    EXPECT_EQ(*res.handshake_sender, "n1");
}

TEST(WireCodecTest, EncodeDecodeInstallSnapshotReq) {
    Envelope in{.from = "L",
                .to = "F",
                .msg = InstallSnapshotReq{.term = 5,
                                          .leader_id = "L",
                                          .last_included_index = 42,
                                          .last_included_term = 3,
                                          .data = "snap-bytes"}};
    const auto bytes = Encode(in);
    const auto r = DecodeOne(bytes.data(), bytes.size(), "F");
    ASSERT_TRUE(r.envelope.has_value());
    ASSERT_TRUE(std::holds_alternative<InstallSnapshotReq>(r.envelope->msg));
    const auto& m = std::get<InstallSnapshotReq>(r.envelope->msg);
    EXPECT_EQ(m.term, 5U);
    EXPECT_EQ(m.leader_id, "L");
    EXPECT_EQ(m.last_included_index, 42U);
    EXPECT_EQ(m.last_included_term, 3U);
    EXPECT_EQ(m.data, "snap-bytes");
    EXPECT_EQ(r.envelope->from, "L");
}

TEST(WireCodecTest, EncodeDecodeInstallSnapshotResp) {
    Envelope in{.from = "F", .to = "L", .msg = InstallSnapshotResp{.term = 9}};
    const auto bytes = Encode(in);
    const auto r = DecodeOne(bytes.data(), bytes.size(), "L");
    ASSERT_TRUE(r.envelope.has_value());
    ASSERT_TRUE(std::holds_alternative<InstallSnapshotResp>(r.envelope->msg));
    const auto& m = std::get<InstallSnapshotResp>(r.envelope->msg);
    EXPECT_EQ(m.term, 9U);
}

TEST(TcpTransportSmokeTest, ConstructAndDestructDoesNotCrash) {
    auto t = MakeTcpTransport(
        /*self_id=*/"s", /*listen_host=*/"127.0.0.1", /*listen_port=*/0, /*peers=*/{});
    ASSERT_NE(t, nullptr);
    EXPECT_TRUE(t->Drain().empty());
    // t goes out of scope; destructor stops the io_context cleanly.
}

TEST(TcpTransportTest, SendsAndReceivesOneEnvelopeOverLocalhost) {
    using namespace std::chrono_literals;

    // Pick a free port by binding port=0 and reading back.
    boost::asio::io_context tmp_io;
    boost::asio::ip::tcp::acceptor probe(tmp_io);
    probe.open(boost::asio::ip::tcp::v4());
    probe.bind({boost::asio::ip::tcp::v4(), 0});
    const auto port_b = probe.local_endpoint().port();
    probe.close();

    boost::asio::ip::tcp::acceptor probe_a(tmp_io);
    probe_a.open(boost::asio::ip::tcp::v4());
    probe_a.bind({boost::asio::ip::tcp::v4(), 0});
    const auto port_a = probe_a.local_endpoint().port();
    probe_a.close();

    // Create t_b first so its accept socket is ready before t_a tries to
    // connect.  t_a's outbound conn to port_b must succeed for Send to work.
    auto t_b = MakeTcpTransport("b", "127.0.0.1", port_b,
                                {{.id = "a", .host = "127.0.0.1", .port = port_a}});
    // Brief pause so t_b's acceptor is fully bound.
    std::this_thread::sleep_for(20ms);
    auto t_a = MakeTcpTransport("a", "127.0.0.1", port_a,
                                {{.id = "b", .host = "127.0.0.1", .port = port_b}});

    // Give them a moment to handshake.
    std::this_thread::sleep_for(200ms);

    Envelope env{.from = "a",
                 .to = "b",
                 .msg = RequestVoteReq{
                     .term = 1, .candidate_id = "a", .last_log_index = 0, .last_log_term = 0}};
    t_a->Send(env);

    // Poll t_b's inbox for up to 1 second.
    const auto deadline = std::chrono::steady_clock::now() + 1s;
    std::vector<Envelope> received;
    while (std::chrono::steady_clock::now() < deadline && received.empty()) {
        received = t_b->Drain();
        if (received.empty()) {
            std::this_thread::sleep_for(20ms);
        }
    }
    ASSERT_EQ(received.size(), 1U);
    EXPECT_EQ(received[0].from, "a");
    EXPECT_EQ(received[0].to, "b");
    ASSERT_TRUE(std::holds_alternative<RequestVoteReq>(received[0].msg));
    EXPECT_EQ(std::get<RequestVoteReq>(received[0].msg).term, 1U);
}

TEST(TcpTransportTest, SendSurvivesBriefResponderRestart) {
    using namespace std::chrono_literals;

    boost::asio::io_context tmp_io;
    boost::asio::ip::tcp::acceptor probe(tmp_io);
    probe.open(boost::asio::ip::tcp::v4());
    probe.bind({boost::asio::ip::tcp::v4(), 0});
    const auto port_b = probe.local_endpoint().port();
    probe.close();

    boost::asio::ip::tcp::acceptor probe_a(tmp_io);
    probe_a.open(boost::asio::ip::tcp::v4());
    probe_a.bind({boost::asio::ip::tcp::v4(), 0});
    const auto port_a = probe_a.local_endpoint().port();
    probe_a.close();

    auto t_a = MakeTcpTransport("a", "127.0.0.1", port_a,
                                {{.id = "b", .host = "127.0.0.1", .port = port_b}});

    // First t_b instance.
    {
        auto t_b = MakeTcpTransport("b", "127.0.0.1", port_b,
                                    {{.id = "a", .host = "127.0.0.1", .port = port_a}});
        std::this_thread::sleep_for(200ms);

        Envelope env{.from = "a",
                     .to = "b",
                     .msg = RequestVoteReq{
                         .term = 1, .candidate_id = "a", .last_log_index = 0, .last_log_term = 0}};
        t_a->Send(env);
        std::this_thread::sleep_for(200ms);
        EXPECT_FALSE(t_b->Drain().empty());
    }  // t_b dies — t_a's outbound conn breaks.

    // Wait a bit, then bring up a fresh t_b on the same port.
    std::this_thread::sleep_for(300ms);

    auto t_b = MakeTcpTransport("b", "127.0.0.1", port_b,
                                {{.id = "a", .host = "127.0.0.1", .port = port_a}});

    // Give t_a's reconnect logic time to reach the new t_b.
    // Backoff starts at 100ms; after a few failures it may be at 400-800ms;
    // give a generous budget to ensure reconnect completes.
    std::this_thread::sleep_for(3000ms);

    Envelope env2{.from = "a",
                  .to = "b",
                  .msg = RequestVoteReq{
                      .term = 2, .candidate_id = "a", .last_log_index = 0, .last_log_term = 0}};
    t_a->Send(env2);

    const auto deadline = std::chrono::steady_clock::now() + 2s;
    std::vector<Envelope> received;
    while (std::chrono::steady_clock::now() < deadline && received.empty()) {
        received = t_b->Drain();
        if (received.empty()) {
            std::this_thread::sleep_for(20ms);
        }
    }
    ASSERT_FALSE(received.empty()) << "t_a's reconnect didn't deliver after responder came back";
    EXPECT_EQ(std::get<RequestVoteReq>(received[0].msg).term, 2U);
}
