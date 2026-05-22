// Knot — Write-Ahead Log.
//
// The WAL is the durability primitive of the storage engine. Every
// mutation is appended here BEFORE being applied anywhere else; on
// crash, the WAL is replayed to reconstruct in-memory state.
//
// Record format on disk:
//
//   ┌──────────┬──────────┬──────────┬──────────────┐
//   │ CRC32(4) │  Seq(8)  │  Len(4)  │  Payload (N) │
//   └──────────┴──────────┴──────────┴──────────────┘
//
// CRC covers [Seq | Len | Payload]. Header is 16 bytes.
//
// Records are stored in 64 MB segment files within a directory:
//
//   <dir>/000001.wal
//   <dir>/000002.wal
//   ...
//
// Sequence numbers are assigned monotonically by the writer, starting
// at 1. (0 is reserved to mean "no record".)

#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace knot::storage {

// ---------------------------------------------------------------------
// WalWriter — append-only writer.
//
// Writes records into the active segment. When the active segment
// reaches `segment_size_bytes`, it is closed and a new segment is
// opened. fsync is explicit: callers decide when to make appends
// durable via Sync().
// ---------------------------------------------------------------------
class WalWriter {
public:
    static constexpr std::size_t kDefaultSegmentSize = 64ULL << 20;  // 64 MB

    // Open or create a WAL writer in `dir`. If `dir` already contains
    // segment files, continues numbering from the highest existing one.
    // Throws std::runtime_error if the directory cannot be opened.
    static std::unique_ptr<WalWriter> Open(const std::filesystem::path& dir,
                                           std::size_t segment_size_bytes = kDefaultSegmentSize);

    ~WalWriter();

    // Owns a file descriptor — non-copyable, non-movable.
    WalWriter(const WalWriter&) = delete;
    WalWriter& operator=(const WalWriter&) = delete;
    WalWriter(WalWriter&&) = delete;
    WalWriter& operator=(WalWriter&&) = delete;

    // Append `payload` as a new record. Returns the assigned sequence
    // number. The bytes are in the OS page cache — NOT durable until
    // Sync() is called.
    std::uint64_t Append(std::string_view payload);

    // Force everything appended so far to physical disk (fsync + on
    // macOS, F_FULLFSYNC). Returns after the OS confirms.
    void Sync();

    // Convenience: Append followed immediately by Sync().
    std::uint64_t AppendSync(std::string_view payload);

    // Sequence number of the most recently appended record (0 if none).
    std::uint64_t LastSequence() const;

private:
    WalWriter(std::filesystem::path dir, std::size_t segment_size);
    void RotateSegment();
    void OpenSegment(std::uint64_t segment_num);

    std::filesystem::path dir_;
    std::size_t segment_size_;
    std::uint64_t next_seq_ = 1;
    std::uint64_t current_segment_num_ = 0;
    std::size_t current_segment_bytes_ = 0;
    int fd_ = -1;
};

// ---------------------------------------------------------------------
// WalReader — sequential reader over all segments in a directory.
//
// Used at startup for crash recovery: opens all segments in order and
// yields records one at a time. Returns std::nullopt at clean EOF.
// Throws on corruption (truncated record / CRC mismatch).
// ---------------------------------------------------------------------
class WalReader {
public:
    struct Record {
        std::uint64_t sequence;
        std::string payload;
    };

    static std::unique_ptr<WalReader> Open(const std::filesystem::path& dir);

    ~WalReader();

    WalReader(const WalReader&) = delete;
    WalReader& operator=(const WalReader&) = delete;
    WalReader(WalReader&&) = delete;
    WalReader& operator=(WalReader&&) = delete;

    // Read the next record. Returns std::nullopt at the end of the
    // last segment. Throws std::runtime_error if a record is
    // truncated or its CRC does not match.
    std::optional<Record> Next();

private:
    explicit WalReader(std::vector<std::filesystem::path> segments);
    bool OpenNextSegment();
    void CloseCurrentSegment();

    std::vector<std::filesystem::path> segments_;
    std::size_t current_segment_idx_ = 0;
    int fd_ = -1;
};

}  // namespace knot::storage
