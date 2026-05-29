// Knot — Raft wire codec.
//
// Encodes/decodes Envelopes to length-prefixed protobuf-framed bytes.
//
// Frame format:
//   [length: 4 bytes BE][type_tag: 1 byte][payload: protobuf bytes]
// where length = 1 + payload.size() (covers tag + payload).

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

#include <knot/raft/messages.h>
#include <knot/raft/types.h>

namespace knot::raft {

// Frame type tags. Frozen for life — never reuse.
enum class WireTag : std::uint8_t {
    kHandshake = 0x00,
    kRequestVoteReq = 0x01,
    kRequestVoteResp = 0x02,
    kAppendEntriesReq = 0x03,
    kAppendEntriesResp = 0x04,
};

[[nodiscard]] std::vector<std::uint8_t> Encode(const Envelope& env);

[[nodiscard]] std::vector<std::uint8_t> EncodeHandshake(const NodeId& sender_id);

struct DecodeResult {
    // Set if a complete RPC frame was parsed.
    std::optional<Envelope> envelope;
    // Set if the parsed frame was a handshake (carries the sender's id).
    std::optional<NodeId> handshake_sender;
    // Bytes consumed from the input buffer (0 if frame was incomplete).
    std::size_t bytes_consumed = 0;
};

// Decode one frame from `buf`. The receiver's NodeId (`self_id`) is
// stamped into the returned Envelope's `to` field. For response messages
// (RVResp/AEResp), the `from` field is empty — the transport must
// overwrite it with the connection's peer-id.
[[nodiscard]] DecodeResult DecodeOne(const std::uint8_t* buf, std::size_t len,
                                     const NodeId& self_id);

}  // namespace knot::raft
