#include <utility>

#include <knot/raft/command.h>

namespace knot::raft {

namespace {

constexpr std::uint8_t kTagPut = 0x01;
constexpr std::uint8_t kTagDelete = 0x02;

void PutBE32(std::vector<std::uint8_t>& out, std::uint32_t v) {
    out.push_back(static_cast<std::uint8_t>((v >> 24) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((v >> 16) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((v >> 8) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>(v & 0xFFU));
}

std::uint32_t GetBE32(const char* p) {
    return (static_cast<std::uint32_t>(static_cast<std::uint8_t>(p[0])) << 24) |
           (static_cast<std::uint32_t>(static_cast<std::uint8_t>(p[1])) << 16) |
           (static_cast<std::uint32_t>(static_cast<std::uint8_t>(p[2])) << 8) |
           static_cast<std::uint32_t>(static_cast<std::uint8_t>(p[3]));
}

}  // namespace

std::vector<std::uint8_t> EncodePut(const std::string& key, const std::string& value) {
    std::vector<std::uint8_t> out;
    out.reserve(1 + 4 + key.size() + 4 + value.size());
    out.push_back(kTagPut);
    PutBE32(out, static_cast<std::uint32_t>(key.size()));
    out.insert(out.end(), key.begin(), key.end());
    PutBE32(out, static_cast<std::uint32_t>(value.size()));
    out.insert(out.end(), value.begin(), value.end());
    return out;
}

std::vector<std::uint8_t> EncodeDelete(const std::string& key) {
    std::vector<std::uint8_t> out;
    out.reserve(1 + 4 + key.size());
    out.push_back(kTagDelete);
    PutBE32(out, static_cast<std::uint32_t>(key.size()));
    out.insert(out.end(), key.begin(), key.end());
    return out;
}

std::optional<Command> DecodeCommand(std::string_view bytes) {
    const std::size_t len = bytes.size();
    if (len < 1 + 4) {
        return std::nullopt;
    }
    const auto tag = static_cast<std::uint8_t>(bytes[0]);
    std::size_t off = 1;

    const std::uint32_t key_len = GetBE32(bytes.data() + off);
    off += 4;
    if (off + key_len > len) {
        return std::nullopt;
    }
    std::string key(bytes.data() + off, bytes.data() + off + key_len);
    off += key_len;

    if (tag == kTagPut) {
        if (off + 4 > len) {
            return std::nullopt;
        }
        const std::uint32_t val_len = GetBE32(bytes.data() + off);
        off += 4;
        if (off + val_len > len) {
            return std::nullopt;
        }
        std::string value(bytes.data() + off, bytes.data() + off + val_len);
        return Command{PutCmd{.key = std::move(key), .value = std::move(value)}};
    }
    if (tag == kTagDelete) {
        return Command{DeleteCmd{.key = std::move(key)}};
    }
    return std::nullopt;
}

}  // namespace knot::raft
