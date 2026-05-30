// Knot — knotd daemon.
//
// Runs one Raft node over real TCP. Driver loop ticks every 10 ms.
// Clean shutdown on SIGINT / SIGTERM.
//
// Usage:
//   knotd --id=<id> --listen=<host:port> [--peer=<id@host:port>]... \
//         --data-dir=<path> [--client-listen=<host:port>]
//
// Example (3-node cluster, run each in a separate terminal):
//   knotd --id=n1 --listen=127.0.0.1:7001 \
//         --peer=n2@127.0.0.1:7002 --peer=n3@127.0.0.1:7003 \
//         --client-listen=127.0.0.1:8001 \
//         --data-dir=./data/n1

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <vector>

#include <knot/raft/command.h>
#include <knot/raft/node.h>
#include <knot/raft/persister.h>
#include <knot/raft/tcp_transport.h>
#include <knot/server/client_server.h>
#include <knot/storage/engine.h>
#include <spdlog/spdlog.h>

namespace {

// Required to be a global so signal handlers can reach it.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<bool> g_shutdown_requested{false};

// Day 17: compact the Raft log once more than this many entries accumulate.
constexpr knot::raft::LogIdx kSnapshotThreshold = 1000;

void HandleSignal(int /*sig*/) {
    g_shutdown_requested = true;
}

struct CliArgs {
    std::string id;
    std::string listen_host;
    std::uint16_t listen_port = 0;
    std::vector<knot::raft::PeerEndpoint> peers;
    std::string data_dir;

    // Day 16 additions:
    std::string client_listen_host;
    std::uint16_t client_listen_port = 0;
};

void PrintUsage() {
    std::cerr << "Usage: knotd --id=<id> --listen=<host:port> "
                 "[--peer=<id@host:port>]... --data-dir=<path> "
                 "[--client-listen=<host:port>]\n"
                 "\n"
                 "Example (3-node cluster, run each in a separate terminal):\n"
                 "  knotd --id=n1 --listen=127.0.0.1:7001 \\\n"
                 "        --peer=n2@127.0.0.1:7002 --peer=n3@127.0.0.1:7003 \\\n"
                 "        --client-listen=127.0.0.1:8001 \\\n"
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
        } else if (a.starts_with("--client-listen=")) {
            if (!ParseHostPort(a.substr(16), &out->client_listen_host, &out->client_listen_port)) {
                std::cerr << "knotd: bad --client-listen: " << a << "\n";
                return false;
            }
        } else {
            std::cerr << "knotd: unknown arg: " << a << "\n";
            return false;
        }
    }
    return !out->id.empty() && !out->listen_host.empty();
}

void ApplyCommittedEntries(knot::raft::RaftNode* node, knot::storage::StorageEngine* engine,
                           std::mutex* engine_mu) {
    for (auto& entry : node->TakeCommitted()) {
        if (entry.type != knot::raft::EntryType::kCommand) {
            continue;
        }
        const auto cmd = knot::raft::DecodeCommand(entry.payload);
        if (!cmd.has_value()) {
            spdlog::warn("knotd: decode failed at idx={}", entry.index);
            continue;
        }
        std::lock_guard<std::mutex> lk(*engine_mu);
        std::visit(
            [&](auto&& c) {
                using T = std::decay_t<decltype(c)>;
                if constexpr (std::is_same_v<T, knot::raft::PutCmd>) {
                    engine->Put(c.key, c.value);
                } else {
                    engine->Delete(c.key);
                }
            },
            *cmd);
    }
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

    // Open engine FIRST so apply_snapshot_callback can reference it.
    std::unique_ptr<knot::storage::StorageEngine> engine;
    if (!args.data_dir.empty()) {
        const auto storage_dir = std::filesystem::path(args.data_dir) / "storage";
        engine = knot::storage::StorageEngine::Open({.data_dir = storage_dir});
    }

    std::mutex engine_mu;
    std::mutex node_mu;  // guards all RaftNode access from any thread

    // Build Config with apply_snapshot_callback baked in BEFORE Create().
    knot::raft::RaftNode::Config cfg{
        .self = args.id,
        .peers = peer_ids,
        .persister = persister,
    };
    if (engine) {
        cfg.apply_snapshot_callback = [engine_ptr = engine.get(),
                                       engine_mu_ptr = &engine_mu](std::string_view bytes) {
            std::lock_guard<std::mutex> lk(*engine_mu_ptr);
            engine_ptr->ApplySnapshot(bytes);
        };
    }

    // Create node — its recovery path will invoke apply_snapshot_callback if needed.
    auto node = knot::raft::RaftNode::Create(std::move(cfg));

    auto transport =
        knot::raft::MakeTcpTransport(args.id, args.listen_host, args.listen_port, args.peers);

    std::unique_ptr<knot::server::ClientServer> client_server;
    if (!args.client_listen_host.empty()) {
        client_server =
            knot::server::MakeClientServer(args.client_listen_host, args.client_listen_port,
                                           node.get(), engine.get(), &node_mu, &engine_mu);
        spdlog::info("knotd client RPC listening on {}:{}", args.client_listen_host,
                     args.client_listen_port);
    }

    spdlog::info("knotd up; entering driver loop");

    while (!g_shutdown_requested.load()) {
        {
            std::lock_guard<std::mutex> lk(node_mu);
            node->Tick();
            for (auto& env : transport->Drain()) {
                node->Step(env);
            }
            for (auto& env : node->TakeOutgoing()) {
                transport->Send(std::move(env));
            }
            if (engine) {
                ApplyCommittedEntries(node.get(), engine.get(), &engine_mu);

                // Day 17: snapshot when log grows past threshold.
                const auto applied_idx = node->CommitIndex();
                const auto start_idx = node->LogStartIndex();
                if (applied_idx >= start_idx && applied_idx - start_idx + 1 > kSnapshotThreshold) {
                    std::string snap_bytes;
                    {
                        std::lock_guard<std::mutex> elk(engine_mu);
                        snap_bytes = engine->Snapshot();
                    }
                    knot::raft::Term applied_term = 0;
                    try {
                        applied_term = node->TermAtIndex(applied_idx);
                    } catch (...) {
                        // Already snapshotted past this index — skip.
                        applied_term = 0;
                    }
                    if (applied_term > 0) {
                        node->MaybeCreateSnapshot(applied_idx, applied_term, snap_bytes);
                    }
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    spdlog::info("knotd shutting down");
    return 0;
}
