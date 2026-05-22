#include <array>

#include <knot/common/crc32.h>

namespace knot::common {

namespace {

// Standard IEEE 802.3 polynomial (reversed/normal form: 0xEDB88320).
// Same one used by Ethernet, ZIP, PNG, and most disk formats.
constexpr std::uint32_t kPoly = 0xEDB88320U;

// Build the lookup table at compile time using a constexpr constructor.
// Each entry encodes "what the CRC of a single byte value would be."
// At runtime, we just XOR + shift + table-lookup — no divisions needed.
struct CrcTable {
    std::array<std::uint32_t, 256> entries{};

    constexpr CrcTable() {
        for (std::uint32_t i = 0; i < 256; ++i) {
            std::uint32_t c = i;
            for (int k = 0; k < 8; ++k) {
                c = (c & 1U) ? (kPoly ^ (c >> 1)) : (c >> 1);
            }
            entries[i] = c;
        }
    }
};

constexpr CrcTable kCrcTable{};

}  // namespace

std::uint32_t Crc32(std::uint32_t crc, const void* data, std::size_t len) {
    const auto* bytes = static_cast<const std::uint8_t*>(data);
    crc = ~crc;  // pre-invert (CRC convention)
    for (std::size_t i = 0; i < len; ++i) {
        crc = kCrcTable.entries[(crc ^ bytes[i]) & 0xFFU] ^ (crc >> 8);
    }
    return ~crc;  // post-invert
}

}  // namespace knot::common
