#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <knot/raft/command.h>
#include <knot/raft/node.h>
#include <knot/raft/types.h>
#include <knot/server/client_server.h>
#include <knot/storage/engine.h>

#include <boost/asio.hpp>

#include "knot.pb.h"

namespace knot::server {

namespace {

namespace asio = boost::asio;

// -------------------------------------------------------------------------
// Tags for client RPCs over the wire.
// -------------------------------------------------------------------------

namespace tags {
constexpr std::uint8_t kPutReq = 0x10;
constexpr std::uint8_t kPutResp = 0x11;
constexpr std::uint8_t kGetReq = 0x12;
constexpr std::uint8_t kGetResp = 0x13;
constexpr std::uint8_t kDeleteReq = 0x14;
constexpr std::uint8_t kDeleteResp = 0x15;
}  // namespace tags

// -------------------------------------------------------------------------
// Wire helpers
// -------------------------------------------------------------------------

void PutBE32(std::vector<std::uint8_t>& out, std::uint32_t v) {
    out.push_back(static_cast<std::uint8_t>((v >> 24U) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((v >> 16U) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((v >> 8U) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>(v & 0xFFU));
}

std::uint32_t GetBE32(const std::uint8_t* p) {
    return (static_cast<std::uint32_t>(p[0]) << 24U) | (static_cast<std::uint32_t>(p[1]) << 16U) |
           (static_cast<std::uint32_t>(p[2]) << 8U) | static_cast<std::uint32_t>(p[3]);
}

// Frame a payload string with the given tag.
// Wire format: [length: 4B BE][tag: 1B][protobuf payload]
// where length = 1 + payload.size()
std::vector<std::uint8_t> FrameOne(std::uint8_t tag, const std::string& payload) {
    const auto body_len = static_cast<std::uint32_t>(1U + payload.size());
    std::vector<std::uint8_t> out;
    out.reserve(4U + body_len);
    PutBE32(out, body_len);
    out.push_back(tag);
    out.insert(out.end(), payload.begin(), payload.end());
    return out;
}

// -------------------------------------------------------------------------
// DedupCache — prevents duplicate (client_id, seq_no) from re-applying.
// -------------------------------------------------------------------------

struct DedupCache {
    struct Entry {
        std::uint64_t last_seq = 0;
        std::vector<std::uint8_t> last_response;
    };
    std::mutex mu;
    std::unordered_map<std::string, Entry> by_client;
    static constexpr std::size_t kMaxClients = 10000;

    // Returns true if the (client_id, seq_no) is a duplicate and fills
    // out_cached with the previously framed response bytes.
    bool LookupDup(const std::string& client_id, std::uint64_t seq_no,
                   std::vector<std::uint8_t>* out_cached) {
        std::lock_guard<std::mutex> lk(mu);
        auto it = by_client.find(client_id);
        if (it == by_client.end()) {
            return false;
        }
        if (seq_no <= it->second.last_seq && !it->second.last_response.empty()) {
            *out_cached = it->second.last_response;
            return true;
        }
        return false;
    }

    void Insert(const std::string& client_id, std::uint64_t seq_no,
                std::vector<std::uint8_t> framed_response) {
        std::lock_guard<std::mutex> lk(mu);
        if (by_client.size() >= kMaxClients) {
            by_client.clear();
        }
        auto& e = by_client[client_id];
        e.last_seq = seq_no;
        e.last_response = std::move(framed_response);
    }
};

// -------------------------------------------------------------------------
// ConnHandler — handles one client TCP connection for its full lifetime.
// -------------------------------------------------------------------------

class ConnHandler : public std::enable_shared_from_this<ConnHandler> {
public:
    ConnHandler(asio::ip::tcp::socket socket, knot::raft::RaftNode* node,
                knot::storage::StorageEngine* engine, std::mutex* node_mu, std::mutex* engine_mu,
                DedupCache* dedup, asio::io_context* /*io*/)
        : socket_(std::move(socket)),
          node_(node),
          engine_(engine),
          node_mu_(node_mu),
          engine_mu_(engine_mu),
          dedup_(dedup) {}

    void Start() { ReadLengthPrefix(); }

private:
    void ReadLengthPrefix() {
        auto self = shared_from_this();
        len_buf_.resize(4);
        asio::async_read(socket_, asio::buffer(len_buf_),
                         [self, this](const boost::system::error_code& ec, std::size_t) {
                             if (ec) {
                                 return;
                             }
                             const std::uint32_t body_len = GetBE32(len_buf_.data());
                             ReadBody(body_len);
                         });
    }

    void ReadBody(std::uint32_t body_len) {
        auto self = shared_from_this();
        body_buf_.resize(body_len);
        asio::async_read(socket_, asio::buffer(body_buf_),
                         [self, this](const boost::system::error_code& ec, std::size_t) {
                             if (ec) {
                                 return;
                             }
                             if (body_buf_.empty()) {
                                 ReadLengthPrefix();
                                 return;
                             }
                             const std::uint8_t tag = body_buf_[0];
                             // Payload is everything after the 1-byte tag.
                             const std::string payload(body_buf_.begin() + 1, body_buf_.end());
                             Dispatch(tag, payload);
                         });
    }

    void Dispatch(std::uint8_t tag, const std::string& payload) {
        switch (tag) {
            case tags::kPutReq: {
                HandlePut(payload);
                break;
            }
            case tags::kGetReq: {
                HandleGet(payload);
                break;
            }
            case tags::kDeleteReq: {
                HandleDelete(payload);
                break;
            }
            default: {
                // Unknown tag — skip and keep reading.
                ReadLengthPrefix();
                break;
            }
        }
    }

    void HandlePut(const std::string& payload) {
        knot::rpc::ClientPutRequest req;
        if (!req.ParseFromString(payload)) {
            return;
        }

        knot::rpc::ClientPutResponse resp;

        // 1. Check if we are the leader.
        bool is_leader = false;
        {
            std::lock_guard<std::mutex> lk(*node_mu_);
            is_leader = (node_->CurrentRole() == knot::raft::Role::kLeader);
        }
        if (!is_leader) {
            resp.set_success(false);
            resp.set_leader_hint("");
            std::string buf;
            resp.SerializeToString(&buf);
            SendResponse(FrameOne(tags::kPutResp, buf));
            return;
        }

        // 2. Check dedup cache.
        std::vector<std::uint8_t> cached;
        if (dedup_->LookupDup(req.client_id(), req.seq_no(), &cached)) {
            SendResponse(std::move(cached));
            return;
        }

        // 3. Submit the command to Raft.
        knot::raft::LogIdx assigned_idx = 0;
        {
            std::lock_guard<std::mutex> lk(*node_mu_);
            const auto cmd_bytes = knot::raft::EncodePut(req.key(), req.value());
            std::string cmd_str(cmd_bytes.begin(), cmd_bytes.end());
            auto idx = node_->Submit(knot::raft::EntryType::kCommand, std::move(cmd_str));
            if (!idx.has_value()) {
                resp.set_success(false);
                resp.set_error("not leader (lost during submit)");
                std::string buf;
                resp.SerializeToString(&buf);
                SendResponse(FrameOne(tags::kPutResp, buf));
                return;
            }
            assigned_idx = *idx;
        }

        // 4. Poll until commit_index >= assigned_idx or 5s timeout.
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (std::chrono::steady_clock::now() < deadline) {
            knot::raft::LogIdx ci = 0;
            {
                std::lock_guard<std::mutex> lk(*node_mu_);
                ci = node_->CommitIndex();
            }
            if (ci >= assigned_idx) {
                resp.set_success(true);
                std::string buf;
                resp.SerializeToString(&buf);
                auto framed = FrameOne(tags::kPutResp, buf);
                dedup_->Insert(req.client_id(), req.seq_no(), framed);
                SendResponse(std::move(framed));
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }

        resp.set_success(false);
        resp.set_error("timeout");
        std::string buf;
        resp.SerializeToString(&buf);
        SendResponse(FrameOne(tags::kPutResp, buf));
    }

    void HandleDelete(const std::string& payload) {
        knot::rpc::ClientDeleteRequest req;
        if (!req.ParseFromString(payload)) {
            return;
        }

        knot::rpc::ClientDeleteResponse resp;

        // 1. Check if we are the leader.
        bool is_leader = false;
        {
            std::lock_guard<std::mutex> lk(*node_mu_);
            is_leader = (node_->CurrentRole() == knot::raft::Role::kLeader);
        }
        if (!is_leader) {
            resp.set_success(false);
            resp.set_leader_hint("");
            std::string buf;
            resp.SerializeToString(&buf);
            SendResponse(FrameOne(tags::kDeleteResp, buf));
            return;
        }

        // 2. Check dedup cache.
        std::vector<std::uint8_t> cached;
        if (dedup_->LookupDup(req.client_id(), req.seq_no(), &cached)) {
            SendResponse(std::move(cached));
            return;
        }

        // 3. Submit the command to Raft.
        knot::raft::LogIdx assigned_idx = 0;
        {
            std::lock_guard<std::mutex> lk(*node_mu_);
            const auto cmd_bytes = knot::raft::EncodeDelete(req.key());
            std::string cmd_str(cmd_bytes.begin(), cmd_bytes.end());
            auto idx = node_->Submit(knot::raft::EntryType::kCommand, std::move(cmd_str));
            if (!idx.has_value()) {
                resp.set_success(false);
                resp.set_error("not leader (lost during submit)");
                std::string buf;
                resp.SerializeToString(&buf);
                SendResponse(FrameOne(tags::kDeleteResp, buf));
                return;
            }
            assigned_idx = *idx;
        }

        // 4. Poll until commit_index >= assigned_idx or 5s timeout.
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (std::chrono::steady_clock::now() < deadline) {
            knot::raft::LogIdx ci = 0;
            {
                std::lock_guard<std::mutex> lk(*node_mu_);
                ci = node_->CommitIndex();
            }
            if (ci >= assigned_idx) {
                resp.set_success(true);
                std::string buf;
                resp.SerializeToString(&buf);
                auto framed = FrameOne(tags::kDeleteResp, buf);
                dedup_->Insert(req.client_id(), req.seq_no(), framed);
                SendResponse(std::move(framed));
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }

        resp.set_success(false);
        resp.set_error("timeout");
        std::string buf;
        resp.SerializeToString(&buf);
        SendResponse(FrameOne(tags::kDeleteResp, buf));
    }

    // Drain any committed-but-not-yet-applied entries and apply them to the
    // storage engine.  Must NOT be called while either mutex is held.
    void ApplyCommittedToEngine() {
        if (engine_ == nullptr) {
            return;
        }
        std::vector<knot::raft::LogEntry> committed;
        {
            std::lock_guard<std::mutex> lk(*node_mu_);
            committed = node_->TakeCommitted();
        }
        if (committed.empty()) {
            return;
        }
        std::lock_guard<std::mutex> elk(*engine_mu_);
        for (const auto& entry : committed) {
            if (entry.type != knot::raft::EntryType::kCommand) {
                continue;
            }
            auto cmd = knot::raft::DecodeCommand(entry.payload);
            if (!cmd.has_value()) {
                continue;
            }
            if (std::holds_alternative<knot::raft::PutCmd>(*cmd)) {
                const auto& put = std::get<knot::raft::PutCmd>(*cmd);
                engine_->Put(put.key, put.value);
            } else if (std::holds_alternative<knot::raft::DeleteCmd>(*cmd)) {
                const auto& del = std::get<knot::raft::DeleteCmd>(*cmd);
                engine_->Delete(del.key);
            }
        }
    }

    void HandleGet(const std::string& payload) {
        knot::rpc::ClientGetRequest req;
        if (!req.ParseFromString(payload)) {
            return;
        }

        knot::rpc::ClientGetResponse resp;

        // Only leaders serve reads (linearizable reads).
        bool is_leader = false;
        {
            std::lock_guard<std::mutex> lk(*node_mu_);
            is_leader = (node_->CurrentRole() == knot::raft::Role::kLeader);
        }
        if (!is_leader) {
            resp.set_success(false);
            std::string buf;
            resp.SerializeToString(&buf);
            SendResponse(FrameOne(tags::kGetResp, buf));
            return;
        }

        if (engine_ == nullptr) {
            resp.set_success(true);
            resp.set_found(false);
            std::string buf;
            resp.SerializeToString(&buf);
            SendResponse(FrameOne(tags::kGetResp, buf));
            return;
        }

        // Ensure committed entries are applied to engine before we read.
        ApplyCommittedToEngine();

        std::string value;
        bool found = false;
        {
            std::lock_guard<std::mutex> lk(*engine_mu_);
            const auto r = engine_->Get(req.key(), &value);
            found = (r == knot::storage::StorageEngine::GetResult::kFound);
        }
        resp.set_success(true);
        resp.set_found(found);
        if (found) {
            resp.set_value(value);
        }
        std::string buf;
        resp.SerializeToString(&buf);
        SendResponse(FrameOne(tags::kGetResp, buf));
    }

    void SendResponse(std::vector<std::uint8_t> framed) {
        auto self = shared_from_this();
        asio::async_write(socket_, asio::buffer(framed),
                          [self, this, framed](const boost::system::error_code& ec, std::size_t) {
                              (void)framed;
                              if (ec) {
                                  return;
                              }
                              ReadLengthPrefix();  // keep connection open for more requests
                          });
    }

    asio::ip::tcp::socket socket_;
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
    knot::raft::RaftNode* node_;
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
    knot::storage::StorageEngine* engine_;
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
    std::mutex* node_mu_;
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
    std::mutex* engine_mu_;
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
    DedupCache* dedup_;
    std::vector<std::uint8_t> len_buf_;
    std::vector<std::uint8_t> body_buf_;
};

// -------------------------------------------------------------------------
// ClientServerImpl
// -------------------------------------------------------------------------

class ClientServerImpl final : public ClientServer {
public:
    ClientServerImpl(const std::string& listen_host, std::uint16_t listen_port,
                     knot::raft::RaftNode* node, knot::storage::StorageEngine* engine,
                     std::mutex* node_mu, std::mutex* engine_mu)
        : node_(node),
          engine_(engine),
          node_mu_(node_mu),
          engine_mu_(engine_mu),
          acceptor_(io_),
          work_guard_(asio::make_work_guard(io_)) {
        const auto address = asio::ip::make_address(listen_host);
        asio::ip::tcp::endpoint endpoint(address, listen_port);
        acceptor_.open(endpoint.protocol());
        acceptor_.set_option(asio::socket_base::reuse_address(true));
        acceptor_.bind(endpoint);
        acceptor_.listen();
        StartAccept();
        worker_ = std::thread([this]() { io_.run(); });
    }

    ~ClientServerImpl() override {
        work_guard_.reset();
        boost::system::error_code ec;
        acceptor_.close(ec);  // NOLINT(bugprone-unused-return-value,cert-err33-c)
        io_.stop();
        if (worker_.joinable()) {
            worker_.join();
        }
    }

    ClientServerImpl(const ClientServerImpl&) = delete;
    ClientServerImpl& operator=(const ClientServerImpl&) = delete;
    ClientServerImpl(ClientServerImpl&&) = delete;
    ClientServerImpl& operator=(ClientServerImpl&&) = delete;

private:
    void StartAccept() {
        acceptor_.async_accept(
            [this](const boost::system::error_code& ec, asio::ip::tcp::socket socket) {
                if (!ec) {
                    auto handler = std::make_shared<ConnHandler>(
                        std::move(socket), node_, engine_, node_mu_, engine_mu_, &dedup_, &io_);
                    handler->Start();
                    StartAccept();
                } else if (ec != asio::error::operation_aborted) {
                    StartAccept();
                }
            });
    }

    knot::raft::RaftNode* node_;
    knot::storage::StorageEngine* engine_;
    std::mutex* node_mu_;
    std::mutex* engine_mu_;
    DedupCache dedup_;

    asio::io_context io_;
    asio::ip::tcp::acceptor acceptor_;
    asio::executor_work_guard<asio::io_context::executor_type> work_guard_;
    std::thread worker_;
};

}  // namespace

std::unique_ptr<ClientServer> MakeClientServer(const std::string& listen_host,
                                               std::uint16_t listen_port,
                                               knot::raft::RaftNode* node,
                                               knot::storage::StorageEngine* engine,
                                               std::mutex* node_mu, std::mutex* engine_mu) {
    return std::make_unique<ClientServerImpl>(listen_host, listen_port, node, engine, node_mu,
                                              engine_mu);
}

}  // namespace knot::server
