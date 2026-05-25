// Knot — StorageEngine.
//
// The integration layer that ties WAL + MemTable + SSTable into a single
// crash-safe key-value engine.
//
// Write path:
//   Put(k, v)
//     1. Encode + append to WAL (fsynced) — durability
//     2. Insert into active MemTable
//     3. If MemTable exceeds size threshold, flush to a new SSTable
//
// Read path:
//   Get(k)
//     1. Check active MemTable
//     2. Check SSTables, newest to oldest
//     3. First hit wins (tombstones shadow older versions)
//
// On-disk layout (under data_dir):
//   data_dir/
//   ├── MANIFEST              # binary: list of live SSTables + last seq
//   ├── wal/
//   │   └── 000001.wal        # WAL segments
//   └── sst/
//       └── sst-00000001.sst  # SSTables (filename id-zero-padded)
//
// On Open():
//   1. Read MANIFEST (default to empty state if missing)
//   2. Open every SSTable listed in MANIFEST for reading
//   3. Open WAL for writing (continues from highest segment)
//   4. Replay WAL records with sequence > MANIFEST.last_seq into MemTable

#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

namespace knot::storage {

class WalWriter;
class MemTable;
class SSTableReader;

class StorageEngine {
public:
    struct Options {
        std::filesystem::path data_dir;
        std::size_t memtable_size_threshold = 4 * 1024 * 1024;  // 4 MB
        std::size_t wal_segment_size = 64 * 1024 * 1024;        // 64 MB
        std::size_t sstable_block_size = 4096;                  // 4 KB
    };

    enum class GetResult { kFound, kNotFound };

    static std::unique_ptr<StorageEngine> Open(Options options);

    ~StorageEngine();

    StorageEngine(const StorageEngine&) = delete;
    StorageEngine& operator=(const StorageEngine&) = delete;
    StorageEngine(StorageEngine&&) = delete;
    StorageEngine& operator=(StorageEngine&&) = delete;

    void Put(std::string_view key, std::string_view value);
    void Delete(std::string_view key);
    GetResult Get(std::string_view key, std::string* value_out) const;

    // Force-flush the active MemTable to a new SSTable, even if it
    // hasn't reached the size threshold. Useful for tests and manual control.
    void Flush();

    // Diagnostics.
    std::uint64_t SequenceNumber() const;
    std::size_t SSTableCount() const;
    std::size_t MemTableSize() const;

private:
    explicit StorageEngine(Options options);
    void Recover();
    void FlushLocked();
    void RewriteManifest();

    Options options_;

    mutable std::mutex mutex_;
    std::uint64_t next_seq_ = 1;
    std::uint64_t next_sst_id_ = 1;
    std::uint64_t last_seq_in_sstables_ = 0;
    std::unique_ptr<WalWriter> wal_;
    std::unique_ptr<MemTable> memtable_;

    // SSTables, parallel arrays in newest-first order (front = newest).
    struct LiveSSTable {
        std::uint64_t id;
        std::unique_ptr<SSTableReader> reader;
    };
    std::vector<LiveSSTable> sstables_;
};

}  // namespace knot::storage
