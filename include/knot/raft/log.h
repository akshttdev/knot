// Knot — Raft log abstraction.
//
// Day 8: backed by std::vector<LogEntry>. Day 13 swaps internals to
// segment files; this public interface stays unchanged.
//
// Indexing: 1-indexed externally (matches Raft paper). Internally
// stored 0-indexed in entries_; offset = index - 1. LastIndex()==0
// means the log is empty.

#pragma once

#include <cstddef>
#include <vector>

#include <knot/raft/types.h>

namespace knot::raft {

class RaftLog {
public:
    RaftLog();

    // Leader-side append. Auto-assigns index = LastIndex() + 1.
    void Append(Term term, EntryType type, std::string payload);

    // Follower-side append. Entries must be index-contiguous with the
    // existing log; throws std::invalid_argument on a gap.
    void AppendBatch(const std::vector<LogEntry>& entries);

    // 1-indexed access. Throws std::out_of_range if index == 0 or
    // index > LastIndex().
    [[nodiscard]] const LogEntry& At(LogIdx index) const;

    // Half-open [from, to). Empty if from >= to or from > LastIndex().
    // Clamps to LastIndex() at the high end.
    [[nodiscard]] std::vector<LogEntry> Slice(LogIdx from, LogIdx to) const;

    // Drop the suffix starting at `from`. No-op if from > LastIndex().
    void TruncateFrom(LogIdx from);

    [[nodiscard]] LogIdx LastIndex() const;
    [[nodiscard]] Term LastTerm() const;

    // True iff the log contains an entry at `index` with `term`.
    [[nodiscard]] bool Matches(LogIdx index, Term term) const;

    [[nodiscard]] std::size_t Size() const;

private:
    std::vector<LogEntry> entries_;  // entries_[i] is logical index i+1
};

}  // namespace knot::raft
