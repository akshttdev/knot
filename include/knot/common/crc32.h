// Knot — CRC-32 (IEEE 802.3 polynomial).
//
// Used by the WAL and SSTables to detect on-disk corruption. NOT a
// cryptographic hash — only protects against accidental damage
// (truncated writes, bit flips on disk, etc.).

#pragma once

#include <cstddef>
#include <cstdint>

namespace knot::common {

// Compute CRC-32 over [data..data+len).
// Pass 0 for `crc` to start a fresh computation; pass a previous return
// value to continue from a prior chunk (so multi-part records work).
//
// Example:
//   uint32_t c = 0;
//   c = Crc32(c, header, sizeof(header));
//   c = Crc32(c, payload, payload_len);
//   // c is now the CRC of [header | payload]
std::uint32_t Crc32(std::uint32_t crc, const void* data, std::size_t len);

}  // namespace knot::common
