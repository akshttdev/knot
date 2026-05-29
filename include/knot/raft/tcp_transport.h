// Knot — Raft TCP transport.
//
// Implements the Transport interface over Boost.Asio with length-prefixed
// protobuf framing (see wire.h).
//
// MakeTcpTransport(self_id, listen_host, listen_port, peers) binds the
// listen socket, starts a worker thread, kicks off async accepts +
// per-peer async connects. The Raft core doesn't know it's TCP — the
// returned object is just a Transport.

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <knot/raft/transport.h>
#include <knot/raft/types.h>

namespace knot::raft {

struct PeerEndpoint {
    NodeId id;
    std::string host;
    std::uint16_t port;
};

[[nodiscard]] std::unique_ptr<Transport> MakeTcpTransport(NodeId self_id,
                                                          const std::string& listen_host,
                                                          std::uint16_t listen_port,
                                                          std::vector<PeerEndpoint> peers);

}  // namespace knot::raft
