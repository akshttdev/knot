// Knot — In-memory sorted KV table (MemTable).
//
// Holds writes after they've been durably logged to the WAL. Reads
// (Get) check here first before falling back to SSTables on disk.
// When the MemTable's ApproximateSize exceeds the engine's threshold,
// the engine flushes it to an SSTable and starts a fresh empty one.
//
// Tombstones:
//   Delete(k) inserts a tombstone — an entry with `is_tombstone = true`
//   and an empty value. Tombstones shadow older versions in SSTables
//   until they're physically removed during compaction.
//
// Threading:
//   - Put/Delete take an exclusive lock.
//   - Get / Empty / EntryCount / ApproximateSize / LastSequence take a
//     shared lock.
//   - NewIterator() snapshots data under a shared lock; the returned
//     iterator owns its snapshot and is safe to use while other threads
//     continue writing.

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace knot::storage {

class MemTable {
public:
    enum class LookupResult { kFound, kNotFound, kDeleted };

    class Iterator;

    MemTable() = default;
    ~MemTable() = default;

    MemTable(const MemTable&) = delete;
    MemTable& operator=(const MemTable&) = delete;
    MemTable(MemTable&&) = delete;
    MemTable& operator=(MemTable&&) = delete;

    // Insert or overwrite. `seq` should be > any previously seen seq.
    void Put(std::uint64_t seq, std::string_view key, std::string_view value);

    // Insert a tombstone for `key`.
    void Delete(std::uint64_t seq, std::string_view key);

    // Look up the latest version of `key`. If found, copies the value
    // into *value_out (when value_out != nullptr).
    LookupResult Get(std::string_view key, std::string* value_out) const;

    // Approximate bytes used by entries (keys + values + per-entry overhead).
    std::size_t ApproximateSize() const;

    std::size_t EntryCount() const;
    bool Empty() const;

    // Highest sequence number seen so far (0 if MemTable is empty).
    std::uint64_t LastSequence() const;

    // Sorted iterator over all entries (including tombstones).
    // Snapshots data at construction — safe to iterate while writes
    // continue elsewhere.
    std::unique_ptr<Iterator> NewIterator() const;

private:
    struct Entry {
        std::uint64_t sequence;
        std::string value;
        bool is_tombstone;
    };

    mutable std::shared_mutex mutex_;
    // std::less<> makes the map heterogeneous-comparable, so we can
    // look up with a std::string_view without constructing a string.
    std::map<std::string, Entry, std::less<>> table_;
    std::size_t approx_size_ = 0;
    std::uint64_t last_seq_ = 0;
};

// Sorted iterator over a snapshot of MemTable entries.
class MemTable::Iterator {
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
    friend class MemTable;
    std::vector<std::pair<std::string, Entry>> snapshot_;
    std::size_t pos_ = 0;
};

}  // namespace knot::storage
