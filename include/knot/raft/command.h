// Knot — Raft command codec.
//
// Encodes Put/Delete commands as bytes for LogEntry.payload.
// Format:
//   [tag: 1B][key_len: 4B BE][key bytes][value_len: 4B BE][value bytes]
// Tags: 0x01 = Put (carries value), 0x02 = Delete (omits value section)

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace knot::raft {

struct PutCmd {
    std::string key;
    std::string value;
};

struct DeleteCmd {
    std::string key;
};

using Command = std::variant<PutCmd, DeleteCmd>;

[[nodiscard]] std::vector<std::uint8_t> EncodePut(const std::string& key, const std::string& value);

[[nodiscard]] std::vector<std::uint8_t> EncodeDelete(const std::string& key);

// Decode raw payload bytes. Accepts any byte buffer (std::string,
// std::string_view, etc.) implicitly convertible to string_view.
[[nodiscard]] std::optional<Command> DecodeCommand(std::string_view bytes);

}  // namespace knot::raft
