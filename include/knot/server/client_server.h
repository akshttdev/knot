// Knot — client RPC server (Day 16).
//
// Binds a TCP port for client requests (Put/Get/Delete). Each incoming
// connection is handled by ConnHandler which reads framed protobuf
// requests, dispatches to RaftNode (mutating ops) or StorageEngine
// (reads), and returns framed responses.
//
// Created via MakeClientServer(...). The returned object owns one
// Asio io_context + one worker thread. Destruction joins cleanly.

#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

#include <knot/raft/node.h>
#include <knot/storage/engine.h>

namespace knot::server {

class ClientServer {
public:
    virtual ~ClientServer() = default;

    ClientServer(const ClientServer&) = delete;
    ClientServer& operator=(const ClientServer&) = delete;
    ClientServer(ClientServer&&) = delete;
    ClientServer& operator=(ClientServer&&) = delete;

protected:
    ClientServer() = default;
};

[[nodiscard]] std::unique_ptr<ClientServer> MakeClientServer(
    const std::string& listen_host, std::uint16_t listen_port, knot::raft::RaftNode* node,
    knot::storage::StorageEngine* engine, std::mutex* node_mu, std::mutex* engine_mu);

}  // namespace knot::server
