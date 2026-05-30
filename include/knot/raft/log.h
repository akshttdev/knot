// Knot — Raft log abstraction.
//
// Day 8: backed by std::vector<LogEntry>. Day 13 swaps internals to
// segment files; this public interface stays unchanged.
//
// Indexing: 1-indexed externally (matches Raft paper).
// After TruncatePrefix(through_idx, through_term), entries with index
// <= through_idx are gone.  log_start_index_ is the smallest index
// currently in entries_; log_start_term_ is the term of the entry at
// log_start_index_ - 1 (i.e. the last snapshotted entry).
// LastIndex() == log_start_index_ - 1 when entries_ is empty.

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

    // 1-indexed access. Throws std::out_of_range if index < log_start_index_
    // (snapshotted) or index > LastIndex().
    [[nodiscard]] const LogEntry& At(LogIdx index) const;

    // Half-open [from, to). Empty if from == 0, from >= to, or from > LastIndex().
    // Throws std::out_of_range if from is in the snapshot (from < log_start_index_
    // and from != 0).  Clamps to LastIndex() at the high end.
    [[nodiscard]] std::vector<LogEntry> Slice(LogIdx from, LogIdx to) const;

    // Drop the suffix starting at `from`. No-op if from > LastIndex().
    void TruncateFrom(LogIdx from);

    // Drop entries with index <= through_idx.  After this,
    // log_start_index_ becomes through_idx + 1 and log_start_term_
    // becomes through_term.  If through_idx >= LastIndex(), entries_ is
    // cleared entirely.
    void TruncatePrefix(LogIdx through_idx, Term through_term);

    [[nodiscard]] LogIdx LastIndex() const;
    [[nodiscard]] Term LastTerm() const;

    // True iff the log contains an entry at `index` with `term`.
    [[nodiscard]] bool Matches(LogIdx index, Term term) const;

    [[nodiscard]] std::size_t Size() const;

    [[nodiscard]] LogIdx LogStartIndex() const;
    [[nodiscard]] Term LogStartTerm() const;

private:
    std::vector<LogEntry> entries_;
    LogIdx log_start_index_ = 1;
    Term log_start_term_ = 0;
};

}  // namespace knot::raft
