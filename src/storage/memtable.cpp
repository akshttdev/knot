#include <stdexcept>
#include <utility>

#include <knot/storage/memtable.h>

namespace knot::storage {

namespace {

// Per-entry overhead estimate (map node + std::string control blocks +
// padding). Used to make ApproximateSize roughly track real RAM use.
constexpr std::size_t kEntryOverhead = 32;

}  // namespace

void MemTable::Put(std::uint64_t seq, std::string_view key, std::string_view value) {
    std::unique_lock lock(mutex_);

    // try_emplace inserts if absent, returns existing iterator if present.
    auto [it, inserted] = table_.try_emplace(std::string(key));

    if (inserted) {
        approx_size_ += key.size() + kEntryOverhead;
    } else {
        // Subtract old value size; we'll add the new size below.
        approx_size_ -= it->second.value.size();
    }

    it->second.sequence = seq;
    it->second.value = std::string(value);
    it->second.is_tombstone = false;
    approx_size_ += value.size();

    if (seq > last_seq_) {
        last_seq_ = seq;
    }
}

void MemTable::Delete(std::uint64_t seq, std::string_view key) {
    std::unique_lock lock(mutex_);

    auto [it, inserted] = table_.try_emplace(std::string(key));

    if (inserted) {
        approx_size_ += key.size() + kEntryOverhead;
    } else {
        approx_size_ -= it->second.value.size();
    }

    it->second.sequence = seq;
    it->second.value.clear();
    it->second.is_tombstone = true;

    if (seq > last_seq_) {
        last_seq_ = seq;
    }
}

MemTable::LookupResult MemTable::Get(std::string_view key, std::string* value_out) const {
    std::shared_lock lock(mutex_);

    auto it = table_.find(key);
    if (it == table_.end()) {
        return LookupResult::kNotFound;
    }
    if (it->second.is_tombstone) {
        return LookupResult::kDeleted;
    }
    if (value_out != nullptr) {
        *value_out = it->second.value;
    }
    return LookupResult::kFound;
}

std::size_t MemTable::ApproximateSize() const {
    std::shared_lock lock(mutex_);
    return approx_size_;
}

std::size_t MemTable::EntryCount() const {
    std::shared_lock lock(mutex_);
    return table_.size();
}

bool MemTable::Empty() const {
    std::shared_lock lock(mutex_);
    return table_.empty();
}

std::uint64_t MemTable::LastSequence() const {
    std::shared_lock lock(mutex_);
    return last_seq_;
}

std::unique_ptr<MemTable::Iterator> MemTable::NewIterator() const {
    std::shared_lock lock(mutex_);
    auto iter = std::make_unique<Iterator>();
    iter->snapshot_.reserve(table_.size());
    for (const auto& [key, entry] : table_) {
        iter->snapshot_.emplace_back(key, entry);
    }
    return iter;
}

// =====================================================================
// Iterator
// =====================================================================

bool MemTable::Iterator::Valid() const {
    return pos_ < snapshot_.size();
}

void MemTable::Iterator::Next() {
    ++pos_;
}

std::string_view MemTable::Iterator::Key() const {
    return snapshot_[pos_].first;
}

std::string_view MemTable::Iterator::Value() const {
    return snapshot_[pos_].second.value;
}

bool MemTable::Iterator::IsTombstone() const {
    return snapshot_[pos_].second.is_tombstone;
}

std::uint64_t MemTable::Iterator::Sequence() const {
    return snapshot_[pos_].second.sequence;
}

}  // namespace knot::storage
