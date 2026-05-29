// Knot — knotctl CLI client.
//
// Sends Put/Get/Delete RPCs to a knotd cluster. Follows leader_hint
// redirects up to 3 hops, round-robins through --servers if leader
// is unknown.
//
// Usage:
//   knotctl --servers=<id@host:port>[,<id@host:port>...] <cmd> [args]
//   cmds: put <key> <value>
//         get <key>
//         delete <key>
//         status

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <boost/asio.hpp>

#include "knot.pb.h"

namespace asio = boost::asio;

namespace {

constexpr std::uint8_t kPutReq = 0x10;
constexpr std::uint8_t kPutResp = 0x11;
constexpr std::uint8_t kGetReq = 0x12;
constexpr std::uint8_t kGetResp = 0x13;
constexpr std::uint8_t kDeleteReq = 0x14;
constexpr std::uint8_t kDeleteResp = 0x15;

struct ServerEntry {
    std::string id;
    std::string host;
    std::uint16_t port = 0;
};

void PrintUsage() {
    std::cerr << "Usage: knotctl --servers=<id@host:port>[,<id@host:port>...] <cmd> [args]\n"
                 "Commands:\n"
                 "  put <key> <value>   - submit a Put\n"
                 "  get <key>           - submit a Get\n"
                 "  delete <key>        - submit a Delete\n"
                 "  status              - check leader liveness\n";
}

std::string RandomClientId() {
    std::random_device rd;
    std::uniform_int_distribution<std::uint32_t> dist(0, 0xFFFFFFFFU);
    std::ostringstream os;
    os << std::hex << dist(rd) << dist(rd);
    return os.str();
}

bool ParseHostPort(std::string_view s, std::string* host_out, std::uint16_t* port_out) {
    const auto colon = s.find(':');
    if (colon == std::string_view::npos) {
        return false;
    }
    *host_out = std::string(s.substr(0, colon));
    try {
        const int parsed = std::stoi(std::string(s.substr(colon + 1)));
        if (parsed < 0 || parsed > 65535) {
            return false;
        }
        *port_out = static_cast<std::uint16_t>(parsed);
    } catch (...) {
        return false;
    }
    return true;
}

// Parse "id@host:port,id@host:port,..." into a list.
bool ParseServers(std::string_view s, std::vector<ServerEntry>* out) {
    std::size_t start = 0;
    while (start < s.size()) {
        const auto comma = s.find(',', start);
        const auto piece = s.substr(start, comma - start);
        const auto at_pos = piece.find('@');
        if (at_pos == std::string_view::npos) {
            return false;
        }
        ServerEntry e;
        e.id = std::string(piece.substr(0, at_pos));
        if (!ParseHostPort(piece.substr(at_pos + 1), &e.host, &e.port)) {
            return false;
        }
        out->push_back(e);
        if (comma == std::string_view::npos) {
            break;
        }
        start = comma + 1;
    }
    return !out->empty();
}

// Frame helpers (BE32 + tag + payload).
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

std::optional<RpcResult> SyncRpc(const ServerEntry& server, std::uint8_t req_tag,
                                 const std::string& req_payload,
                                 std::chrono::milliseconds timeout) {
    asio::io_context io;
    asio::ip::tcp::socket sock(io);

    boost::system::error_code ec;
    const auto address = asio::ip::make_address(server.host, ec);
    if (ec) {
        return std::nullopt;
    }
    asio::ip::tcp::endpoint ep(address, server.port);

    sock.connect(ep, ec);  // NOLINT(bugprone-unused-return-value,cert-err33-c)
    if (ec) {
        return std::nullopt;
    }

    // (For a production client, we'd use async + asio::steady_timer for
    // hard timeouts. For knotctl v1, the connect/read either succeeds
    // or fails; we don't enforce timeout granularly.)
    (void)timeout;

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

// Try one RPC against `target`. Returns (success, leader_hint, response_payload).
// Caller handles the parse + retry loop.
struct AttemptOutcome {
    bool rpc_ok = false;  // RPC roundtrip completed (not TCP error)
    bool app_success = false;
    std::string leader_hint;
    std::string response_payload;
    std::uint8_t resp_tag = 0;
};

AttemptOutcome AttemptRpc(const ServerEntry& server, std::uint8_t req_tag,
                          const std::string& req_payload) {
    AttemptOutcome out;
    auto r = SyncRpc(server, req_tag, req_payload, std::chrono::seconds(5));
    if (!r.has_value()) {
        return out;
    }
    out.rpc_ok = true;
    out.resp_tag = r->tag;
    out.response_payload = r->payload;

    // For each known response type, peek at success + leader_hint.
    if (r->tag == kPutResp) {
        knot::rpc::ClientPutResponse resp;
        if (resp.ParseFromString(r->payload)) {
            out.app_success = resp.success();
            out.leader_hint = resp.leader_hint();
        }
    } else if (r->tag == kGetResp) {
        knot::rpc::ClientGetResponse resp;
        if (resp.ParseFromString(r->payload)) {
            out.app_success = resp.success();
            out.leader_hint = resp.leader_hint();
        }
    } else if (r->tag == kDeleteResp) {
        knot::rpc::ClientDeleteResponse resp;
        if (resp.ParseFromString(r->payload)) {
            out.app_success = resp.success();
            out.leader_hint = resp.leader_hint();
        }
    }
    return out;
}

// Run an RPC against the list of servers, following leader_hint redirects
// up to max_hops. Returns the final (parsed) response_payload of the
// LAST attempted response on success, or empty string on failure.
struct LoopResult {
    bool ok = false;
    std::uint8_t resp_tag = 0;
    std::string payload;
};

LoopResult RunWithRedirects(const std::vector<ServerEntry>& servers, std::uint8_t req_tag,
                            const std::string& req_payload, std::size_t max_hops = 3) {
    // Build id→entry map for leader_hint resolution.
    std::unordered_map<std::string, ServerEntry> by_id;
    for (const auto& s : servers) {
        by_id[s.id] = s;
    }

    std::size_t cursor = 0;  // index into servers for round-robin
    for (std::size_t hop = 0; hop < max_hops; ++hop) {
        const ServerEntry target = servers[cursor];
        const auto outcome = AttemptRpc(target, req_tag, req_payload);
        if (outcome.rpc_ok && outcome.app_success) {
            LoopResult ok;
            ok.ok = true;
            ok.resp_tag = outcome.resp_tag;
            ok.payload = outcome.response_payload;
            return ok;
        }
        // Decide where to go next.
        if (!outcome.leader_hint.empty()) {
            auto it = by_id.find(outcome.leader_hint);
            if (it != by_id.end()) {
                // Set cursor to that server's index.
                for (std::size_t i = 0; i < servers.size(); ++i) {
                    if (servers[i].id == it->second.id) {
                        cursor = i;
                        break;
                    }
                }
                continue;
            }
        }
        // Otherwise round-robin.
        cursor = (cursor + 1) % servers.size();
    }
    LoopResult fail;
    return fail;
}

int DoPut(const std::vector<ServerEntry>& servers, const std::string& client_id,
          const std::string& key, const std::string& value) {
    knot::rpc::ClientPutRequest req;
    req.set_client_id(client_id);
    req.set_seq_no(1);
    req.set_key(key);
    req.set_value(value);
    std::string buf;
    req.SerializeToString(&buf);

    const auto r = RunWithRedirects(servers, kPutReq, buf);
    if (!r.ok) {
        std::cerr << "knotctl: put failed\n";
        return 1;
    }
    std::cout << "ok\n";
    return 0;
}

int DoGet(const std::vector<ServerEntry>& servers, const std::string& client_id,
          const std::string& key) {
    knot::rpc::ClientGetRequest req;
    req.set_client_id(client_id);
    req.set_seq_no(1);
    req.set_key(key);
    std::string buf;
    req.SerializeToString(&buf);

    const auto r = RunWithRedirects(servers, kGetReq, buf);
    if (!r.ok || r.resp_tag != kGetResp) {
        std::cerr << "knotctl: get failed\n";
        return 1;
    }
    knot::rpc::ClientGetResponse resp;
    if (!resp.ParseFromString(r.payload)) {
        std::cerr << "knotctl: bad response\n";
        return 1;
    }
    if (!resp.found()) {
        std::cerr << "knotctl: not found\n";
        return 1;
    }
    std::cout.write(resp.value().data(), resp.value().size());
    std::cout << "\n";
    return 0;
}

int DoDelete(const std::vector<ServerEntry>& servers, const std::string& client_id,
             const std::string& key) {
    knot::rpc::ClientDeleteRequest req;
    req.set_client_id(client_id);
    req.set_seq_no(1);
    req.set_key(key);
    std::string buf;
    req.SerializeToString(&buf);

    const auto r = RunWithRedirects(servers, kDeleteReq, buf);
    if (!r.ok) {
        std::cerr << "knotctl: delete failed\n";
        return 1;
    }
    std::cout << "ok\n";
    return 0;
}

int DoStatus(const std::vector<ServerEntry>& servers, const std::string& client_id) {
    // Use a Get for a zero-byte key as a liveness probe (the server
    // returns success=true, found=false for an empty store).
    return DoGet(servers, client_id, "");
}

}  // namespace

int main(int argc, char** argv) {
    std::vector<ServerEntry> servers;
    std::vector<std::string> positionals;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a == "--help" || a == "-h") {
            PrintUsage();
            return 1;
        }
        if (a.starts_with("--servers=")) {
            if (!ParseServers(a.substr(10), &servers)) {
                std::cerr << "knotctl: bad --servers: " << a << "\n";
                return 1;
            }
        } else {
            positionals.push_back(a);
        }
    }
    if (servers.empty() || positionals.empty()) {
        PrintUsage();
        return 1;
    }

    const std::string client_id = RandomClientId();
    const std::string& cmd = positionals[0];

    if (cmd == "put" && positionals.size() == 3) {
        return DoPut(servers, client_id, positionals[1], positionals[2]);
    }
    if (cmd == "get" && positionals.size() == 2) {
        return DoGet(servers, client_id, positionals[1]);
    }
    if (cmd == "delete" && positionals.size() == 2) {
        return DoDelete(servers, client_id, positionals[1]);
    }
    if (cmd == "status" && positionals.size() == 1) {
        return DoStatus(servers, client_id);
    }
    PrintUsage();
    return 1;
}
