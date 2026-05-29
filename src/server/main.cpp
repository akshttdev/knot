// Knot — knotd daemon.
//
// Runs one Raft node over real TCP. Driver loop ticks every 10 ms.
// Clean shutdown on SIGINT / SIGTERM.
//
// Usage:
//   knotd --id=<id> --listen=<host:port> [--peer=<id@host:port>]... \
//         --data-dir=<path>
//
// Example (3-node cluster, run each in a separate terminal):
//   knotd --id=n1 --listen=127.0.0.1:7001 \
//         --peer=n2@127.0.0.1:7002 --peer=n3@127.0.0.1:7003 \
//         --data-dir=./data/n1

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <knot/raft/node.h>
#include <knot/raft/persister.h>
#include <knot/raft/tcp_transport.h>
#include <spdlog/spdlog.h>

namespace {

// Required to be a global so signal handlers can reach it.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<bool> g_shutdown_requested{false};

void HandleSignal(int /*sig*/) {
    g_shutdown_requested = true;
}

struct CliArgs {
    std::string id;
    std::string listen_host;
    std::uint16_t listen_port = 0;
    std::vector<knot::raft::PeerEndpoint> peers;
    std::string data_dir;
};

void PrintUsage() {
    std::cerr << "Usage: knotd --id=<id> --listen=<host:port> "
                 "[--peer=<id@host:port>]... --data-dir=<path>\n"
                 "\n"
                 "Example (3-node cluster, run each in a separate terminal):\n"
                 "  knotd --id=n1 --listen=127.0.0.1:7001 \\\n"
                 "        --peer=n2@127.0.0.1:7002 --peer=n3@127.0.0.1:7003 \\\n"
                 "        --data-dir=./data/n1\n";
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

bool ParseArgs(int argc, char** argv, CliArgs* out) {
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a == "--help" || a == "-h") {
            return false;
        }
        if (a.starts_with("--id=")) {
            out->id = a.substr(5);
        } else if (a.starts_with("--listen=")) {
            if (!ParseHostPort(a.substr(9), &out->listen_host, &out->listen_port)) {
                std::cerr << "knotd: bad --listen: " << a << "\n";
                return false;
            }
        } else if (a.starts_with("--peer=")) {
            const std::string p = a.substr(7);
            const auto at_pos = p.find('@');
            if (at_pos == std::string::npos) {
                std::cerr << "knotd: --peer must be id@host:port: " << a << "\n";
                return false;
            }
            knot::raft::PeerEndpoint pe;
            pe.id = p.substr(0, at_pos);
            if (!ParseHostPort(std::string_view(p).substr(at_pos + 1), &pe.host, &pe.port)) {
                std::cerr << "knotd: bad --peer: " << a << "\n";
                return false;
            }
            out->peers.push_back(pe);
        } else if (a.starts_with("--data-dir=")) {
            out->data_dir = a.substr(11);
        } else {
            std::cerr << "knotd: unknown arg: " << a << "\n";
            return false;
        }
    }
    return !out->id.empty() && !out->listen_host.empty();
}

}  // namespace

int main(int argc, char** argv) {
    CliArgs args;
    if (!ParseArgs(argc, argv, &args)) {
        PrintUsage();
        return 1;
    }

    // Old signal handlers discarded — we don't need to chain.
    (void)std::signal(SIGINT, HandleSignal);
    (void)std::signal(SIGTERM, HandleSignal);

    spdlog::info("knotd id={} listen={}:{} peers={} data_dir={}", args.id, args.listen_host,
                 args.listen_port, args.peers.size(), args.data_dir);

    auto persister = args.data_dir.empty() ? knot::raft::MakeMemoryPersister()
                                           : knot::raft::MakeFilePersister(args.data_dir);

    std::vector<knot::raft::NodeId> peer_ids;
    peer_ids.reserve(args.peers.size());
    for (const auto& p : args.peers) {
        peer_ids.push_back(p.id);
    }

    auto node =
        knot::raft::RaftNode::Create({.self = args.id, .peers = peer_ids, .persister = persister});

    auto transport =
        knot::raft::MakeTcpTransport(args.id, args.listen_host, args.listen_port, args.peers);

    spdlog::info("knotd up; entering driver loop");

    while (!g_shutdown_requested.load()) {
        node->Tick();
        for (auto& env : transport->Drain()) {
            node->Step(env);
        }
        for (auto& env : node->TakeOutgoing()) {
            transport->Send(std::move(env));
        }
        // Day 15: drain node->TakeCommitted() into the LSM engine.
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    spdlog::info("knotd shutting down");
    return 0;
}
