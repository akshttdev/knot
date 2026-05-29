#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <knot/raft/tcp_transport.h>
#include <knot/raft/wire.h>

#include <boost/asio.hpp>

namespace knot::raft {

namespace {

namespace asio = boost::asio;

class InboundConn : public std::enable_shared_from_this<InboundConn> {
public:
    InboundConn(asio::ip::tcp::socket socket, NodeId self_id, std::mutex& inbox_mu,
                std::vector<Envelope>& inbox)
        : socket_(std::move(socket)),
          self_id_(std::move(self_id)),
          inbox_mu_(inbox_mu),
          inbox_(inbox) {}

    void Start() { ReadLengthPrefix(); }

private:
    void ReadLengthPrefix() {
        auto self = shared_from_this();
        len_buf_.resize(4);
        asio::async_read(socket_, asio::buffer(len_buf_),
                         [self, this](const boost::system::error_code& ec, std::size_t) {
                             if (ec) {
                                 return;  // connection closed / error; drop
                             }
                             const std::uint32_t body_len =
                                 (static_cast<std::uint32_t>(len_buf_[0]) << 24U) |
                                 (static_cast<std::uint32_t>(len_buf_[1]) << 16U) |
                                 (static_cast<std::uint32_t>(len_buf_[2]) << 8U) |
                                 static_cast<std::uint32_t>(len_buf_[3]);
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
                             std::vector<std::uint8_t> full;
                             full.reserve(len_buf_.size() + body_buf_.size());
                             full.insert(full.end(), len_buf_.begin(), len_buf_.end());
                             full.insert(full.end(), body_buf_.begin(), body_buf_.end());

                             const auto r = DecodeOne(full.data(), full.size(), self_id_);
                             if (r.handshake_sender.has_value()) {
                                 peer_id_ = *r.handshake_sender;
                             } else if (r.envelope.has_value()) {
                                 Envelope env = *r.envelope;
                                 if (env.from.empty()) {
                                     env.from = peer_id_;  // stamp response messages
                                 }
                                 std::lock_guard<std::mutex> lk(inbox_mu_);
                                 inbox_.push_back(std::move(env));
                             }
                             ReadLengthPrefix();
                         });
    }

    asio::ip::tcp::socket socket_;
    NodeId self_id_;
    NodeId peer_id_;
    std::vector<std::uint8_t> len_buf_;
    std::vector<std::uint8_t> body_buf_;
    // InboundConn is owned exclusively by shared_ptr and only outlives the
    // parent TcpTransport via in-flight async ops that the destructor drains.
    // Holding the parent's mutex + inbox by reference is intentional.
    std::mutex& inbox_mu_;          // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    std::vector<Envelope>& inbox_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
};

class OutboundConn : public std::enable_shared_from_this<OutboundConn> {
public:
    OutboundConn(asio::io_context& io, NodeId self_id, PeerEndpoint peer)
        : io_(io), socket_(io), self_id_(std::move(self_id)), peer_(std::move(peer)) {}

    void Start() { Connect(); }

    void Enqueue(std::vector<std::uint8_t> bytes) {
        // Post to the io_context to keep all socket ops single-threaded.
        auto self = shared_from_this();
        asio::post(io_, [self, this, bytes = std::move(bytes)]() mutable {
            outq_.push_back(std::move(bytes));
            if (connected_ && !writing_) {
                DoWrite();
            }
        });
    }

private:
    void Connect() {
        auto self = shared_from_this();
        asio::ip::tcp::resolver resolver(io_);
        boost::system::error_code rec;
        const auto endpoints = resolver.resolve(peer_.host, std::to_string(peer_.port), rec);
        if (rec) {
            ScheduleReconnect();
            return;
        }
        asio::async_connect(
            socket_, endpoints,
            [self, this](const boost::system::error_code& ec, const asio::ip::tcp::endpoint&) {
                if (ec) {
                    ScheduleReconnect();
                    return;
                }
                connected_ = true;
                current_backoff_ = std::chrono::milliseconds(100);  // reset on success
                // Send handshake first so server learns our id.
                outq_.insert(outq_.begin(), EncodeHandshake(self_id_));
                if (!writing_) {
                    DoWrite();
                }
                // Start a read loop so we detect EOF/RST from the server
                // promptly without waiting for an outbound write to fail.
                MonitorConnection();
            });
    }

    void DoWrite() {
        if (outq_.empty()) {
            writing_ = false;
            return;
        }
        writing_ = true;
        auto self = shared_from_this();
        // Move next bytes into the lambda capture so they stay alive
        // until async_write completes.
        auto bytes = std::move(outq_.front());
        outq_.erase(outq_.begin());
        asio::async_write(socket_, asio::buffer(bytes),
                          [self, this, bytes = std::move(bytes)](
                              const boost::system::error_code& ec, std::size_t) mutable {
                              if (ec) {
                                  // Put the failed bytes back at the front so they are
                                  // retried after reconnect (queue preservation).
                                  outq_.insert(outq_.begin(), std::move(bytes));
                                  writing_ = false;
                                  connected_ = false;
                                  ScheduleReconnect();
                                  return;
                              }
                              if (!connected_) {
                                  // MonitorConnection detected disconnect while we were
                                  // writing; stop the write loop and schedule reconnect.
                                  writing_ = false;
                                  ScheduleReconnect();
                                  return;
                              }
                              DoWrite();
                          });
    }

    void MonitorConnection() {
        // Drain any bytes the server sends (shouldn't happen, but discard them).
        // The main purpose is to detect EOF/RST so we can reconnect promptly
        // without waiting for a write to fail.
        auto self = shared_from_this();
        monitor_buf_.resize(64);
        socket_.async_read_some(asio::buffer(monitor_buf_),
                                [self, this](const boost::system::error_code& ec, std::size_t) {
                                    if (!ec) {
                                        // Unexpected data from server — just keep reading.
                                        MonitorConnection();
                                        return;
                                    }
                                    if (ec == asio::error::operation_aborted) {
                                        return;  // socket was closed by our own ScheduleReconnect
                                    }
                                    if (!connected_) {
                                        return;  // write error already triggered reconnect
                                    }
                                    // EOF or error: the server went away.
                                    connected_ = false;
                                    if (!writing_) {
                                        // No write in flight — schedule reconnect directly.
                                        ScheduleReconnect();
                                    }
                                    // If writing_ is true, DoWrite's completion (error or
                                    // success-then-!connected_ check) will call ScheduleReconnect.
                                });
    }

    void ScheduleReconnect() {
        // Compute next delay with ±10% jitter; cap at kMaxBackoff.
        const auto base = current_backoff_;
        const auto jitter_range_ms = std::max<std::int64_t>(base.count() / 10, 1);
        std::uniform_int_distribution<std::int64_t> dist(-jitter_range_ms, jitter_range_ms);
        const auto delay = base + std::chrono::milliseconds(dist(jitter_rng_));

        // Double backoff for next time, capped at kMaxBackoff.
        const auto next = current_backoff_ * 2;
        current_backoff_ = (next < kMaxBackoff) ? next : kMaxBackoff;

        auto self = shared_from_this();
        auto timer = std::make_shared<asio::steady_timer>(io_, delay);
        timer->async_wait([self, this, timer](const boost::system::error_code& ec) {
            if (ec) {
                return;  // timer cancelled (likely shutdown)
            }
            // Reset socket for a fresh connection attempt.
            boost::system::error_code close_ec;
            socket_.close(close_ec);  // NOLINT(bugprone-unused-return-value,cert-err33-c)
            Connect();
        });
    }

    asio::io_context& io_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    asio::ip::tcp::socket socket_;
    NodeId self_id_;
    PeerEndpoint peer_;
    bool connected_ = false;
    bool writing_ = false;
    std::vector<std::vector<std::uint8_t>> outq_;
    std::vector<std::uint8_t> monitor_buf_;

    std::chrono::milliseconds current_backoff_{100};
    static constexpr std::chrono::milliseconds kMaxBackoff{5000};
    std::mt19937 jitter_rng_{std::random_device{}()};
};

class TcpTransport final : public Transport {
public:
    TcpTransport(NodeId self_id, const std::string& listen_host, std::uint16_t listen_port,
                 std::vector<PeerEndpoint> peers)
        : self_id_(std::move(self_id)),
          peers_(std::move(peers)),
          acceptor_(io_),
          work_guard_(asio::make_work_guard(io_)) {
        const auto address = asio::ip::make_address(listen_host);
        asio::ip::tcp::endpoint endpoint(address, listen_port);
        acceptor_.open(endpoint.protocol());
        acceptor_.set_option(asio::socket_base::reuse_address(true));
        acceptor_.bind(endpoint);
        acceptor_.listen();
        StartAccept();

        for (const auto& peer : peers_) {
            auto conn = std::make_shared<OutboundConn>(io_, self_id_, peer);
            outbound_[peer.id] = conn;
            conn->Start();
        }

        worker_ = std::thread([this]() { io_.run(); });
    }

    ~TcpTransport() override {
        work_guard_.reset();
        // Destructor must not throw; any close error is intentionally swallowed.
        boost::system::error_code ec;
        acceptor_.close(ec);  // NOLINT(bugprone-unused-return-value,cert-err33-c)
        io_.stop();
        if (worker_.joinable()) {
            worker_.join();
        }
    }

    TcpTransport(const TcpTransport&) = delete;
    TcpTransport& operator=(const TcpTransport&) = delete;
    TcpTransport(TcpTransport&&) = delete;
    TcpTransport& operator=(TcpTransport&&) = delete;

    void Send(Envelope env) override {
        auto it = outbound_.find(env.to);
        if (it == outbound_.end()) {
            return;  // unknown destination — silent drop (Raft tolerates this)
        }
        it->second->Enqueue(Encode(env));
    }

    [[nodiscard]] std::vector<Envelope> Drain() override {
        std::lock_guard<std::mutex> lk(inbox_mu_);
        std::vector<Envelope> out;
        out.swap(inbox_);
        return out;
    }

private:
    void StartAccept() {
        acceptor_.async_accept([this](const boost::system::error_code& ec,
                                      asio::ip::tcp::socket socket) {
            if (!ec) {
                auto conn =
                    std::make_shared<InboundConn>(std::move(socket), self_id_, inbox_mu_, inbox_);
                conn->Start();
                StartAccept();
            } else if (ec != asio::error::operation_aborted) {
                // Non-fatal accept error: retry.
                StartAccept();
            }
            // operation_aborted → shutting down; stop the chain.
        });
    }

    NodeId self_id_;
    std::vector<PeerEndpoint> peers_;

    // inbox_mu_ and inbox_ must be declared BEFORE io_ so they are
    // destroyed AFTER io_.  The io_context destructor tears down pending
    // InboundConn handlers that hold references to these members; if
    // inbox_mu_/inbox_ were destroyed first we would crash.
    std::mutex inbox_mu_;
    std::vector<Envelope> inbox_;

    // io_ is declared after inbox_mu_/inbox_ so its destructor (which
    // drops pending handlers) runs before those fields are gone.
    asio::io_context io_;
    asio::ip::tcp::acceptor acceptor_;
    asio::executor_work_guard<asio::io_context::executor_type> work_guard_;
    std::thread worker_;

    std::unordered_map<NodeId, std::shared_ptr<OutboundConn>> outbound_;
};

}  // namespace

std::unique_ptr<Transport> MakeTcpTransport(NodeId self_id, const std::string& listen_host,
                                            std::uint16_t listen_port,
                                            std::vector<PeerEndpoint> peers) {
    return std::make_unique<TcpTransport>(std::move(self_id), listen_host, listen_port,
                                          std::move(peers));
}

}  // namespace knot::raft
