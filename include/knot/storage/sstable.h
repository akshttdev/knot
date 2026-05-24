// Knot — SSTable (Sorted String Table).
//
// Immutable, sorted, block-structured file on disk. Created by flushing
// a MemTable; read on every Get that misses the MemTable.
//
// File layout:
//
//   ┌────────────────────────────────────────────────┐
//   │ Data block 1   (sorted KV entries + CRC trailer)│
//   │ Data block 2                                     │
//   │ ...                                              │
//   │ Data block N                                     │
//   ├────────────────────────────────────────────────┤
//   │ Index block (one entry per data block) + CRC    │
//   ├────────────────────────────────────────────────┤
//   │ Footer (24 bytes, fixed):                       │
//   │   magic(8) | index_offset(8) | index_length(4)  │
//   │   | footer_crc(4)                                │
//   └────────────────────────────────────────────────┘
//
// Each data-block entry:
//   key_len(4) | value_len(4) | type(1) | sequence(8) | key... | value...
//   type: 0 = value present, 1 = tombstone (value_len = 0)
//
// Each index entry:
//   key_len(4) | block_offset(8) | block_size(4) | first_key...

#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace knot::storage {

class SSTableWriter {
public:
    static std::unique_ptr<SSTableWriter> Open(const std::filesystem::path& path,
                                               std::size_t target_block_size = 4096);

    ~SSTableWriter();

    SSTableWriter(const SSTableWriter&) = delete;
    SSTableWriter& operator=(const SSTableWriter&) = delete;
    SSTableWriter(SSTableWriter&&) = delete;
    SSTableWriter& operator=(SSTableWriter&&) = delete;

    // Add a value. Keys must be strictly increasing and non-empty.
    void Add(std::uint64_t seq, std::string_view key, std::string_view value);

    // Add a tombstone (deletion marker). Same ordering rules.
    void AddTombstone(std::uint64_t seq, std::string_view key);

    // Flush remaining block, write index + footer, fsync, close.
    // Must be called before the file can be read.
    void Finish();

    std::size_t EntryCount() const;

private:
    SSTableWriter(std::filesystem::path path, std::size_t target_block_size);
    void AppendEntry(std::uint64_t seq, std::string_view key, std::string_view value,
                     std::uint8_t type);
    void FlushBlock();
    void WriteIndexAndFooter();

    struct IndexEntry {
        std::string first_key;
        std::uint64_t offset;
        std::uint32_t size;
    };

    std::filesystem::path path_;
    std::size_t target_block_size_;
    int fd_ = -1;
    std::uint64_t current_offset_ = 0;
    std::string current_block_;
    std::string pending_first_key_;
    bool block_has_entries_ = false;
    std::string previous_key_;
    bool has_previous_key_ = false;
    std::vector<IndexEntry> index_;
    std::size_t entry_count_ = 0;
    bool finished_ = false;
};

class SSTableReader {
public:
    enum class LookupResult { kFound, kNotFound, kDeleted };
    class Iterator;

    static std::unique_ptr<SSTableReader> Open(const std::filesystem::path& path);

    ~SSTableReader();

    SSTableReader(const SSTableReader&) = delete;
    SSTableReader& operator=(const SSTableReader&) = delete;
    SSTableReader(SSTableReader&&) = delete;
    SSTableReader& operator=(SSTableReader&&) = delete;

    LookupResult Get(std::string_view key, std::string* value_out) const;

    std::unique_ptr<Iterator> NewIterator() const;

    std::size_t BlockCount() const;

private:
    struct IndexEntry {
        std::string first_key;
        std::uint64_t offset;
        std::uint32_t size;
    };

    SSTableReader() = default;
    std::string ReadBlock(std::uint64_t offset, std::uint32_t size) const;

    int fd_ = -1;
    std::vector<IndexEntry> index_;
};

class SSTableReader::Iterator {
public:
    Iterator() = default;
    ~Iterator() = default;

    Iterator(const Iterator&) = delete;
    Iterator& operator=(const Iterator&) = delete;
    Iterator(Iterator&&) = default;
    Iterator& operator=(Iterator&&) = default;

    bool Valid() const;
    void Next();

    std::string_view Key() const;
    std::string_view Value() const;
    bool IsTombstone() const;
    std::uint64_t Sequence() const;

private:
    friend class SSTableReader;

    void LoadBlock(std::size_t block_idx);
    void ParseCurrentEntry();

    const SSTableReader* reader_ = nullptr;
    std::size_t block_idx_ = 0;
    std::string block_data_;
    std::size_t pos_ = 0;

    std::string_view key_view_;
    std::string_view value_view_;
    std::uint64_t seq_ = 0;
    bool is_tombstone_ = false;
    bool valid_ = false;
};

}  // namespace knot::storage
