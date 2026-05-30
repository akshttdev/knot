#include <algorithm>
#include <stdexcept>
#include <utility>

#include <knot/raft/log.h>

namespace knot::raft {

RaftLog::RaftLog() = default;

void RaftLog::Append(Term term, EntryType type, std::string payload) {
    LogEntry e;
    e.term = term;
    e.index = LastIndex() + 1;
    e.type = type;
    e.payload = std::move(payload);
    entries_.push_back(std::move(e));
}

void RaftLog::AppendBatch(const std::vector<LogEntry>& entries) {
    if (entries.empty()) {
        return;
    }
    LogIdx expected = LastIndex() + 1;
    if (entries.front().index != expected) {
        throw std::invalid_argument(
            "RaftLog::AppendBatch: first entry index does not continue log");
    }
    for (const auto& e : entries) {
        entries_.push_back(e);
    }
}

const LogEntry& RaftLog::At(LogIdx index) const {
    if (index < log_start_index_) {
        throw std::out_of_range("RaftLog::At: index in snapshot");
    }
    const auto off = static_cast<std::size_t>(index - log_start_index_);
    if (off >= entries_.size()) {
        throw std::out_of_range("RaftLog::At: index out of range");
    }
    return entries_[off];
}

std::vector<LogEntry> RaftLog::Slice(LogIdx from, LogIdx to) const {
    if (from == 0) {
        return {};
    }
    if (from < log_start_index_) {
        throw std::out_of_range(
            "RaftLog::Slice: from is in snapshot; caller must send InstallSnapshot");
    }
    if (from >= to) {
        return {};
    }
    const auto last = LastIndex();
    if (from > last) {
        return {};
    }
    const auto end = (to > last + 1) ? (last + 1) : to;
    std::vector<LogEntry> out;
    out.reserve(static_cast<std::size_t>(end - from));
    for (LogIdx i = from; i < end; ++i) {
        out.push_back(entries_[static_cast<std::size_t>(i - log_start_index_)]);
    }
    return out;
}

void RaftLog::TruncateFrom(LogIdx from) {
    from = std::max(from, log_start_index_);
    if (from > LastIndex()) {
        return;
    }
    const auto keep = static_cast<std::size_t>(from - log_start_index_);
    entries_.resize(keep);
}

void RaftLog::TruncatePrefix(LogIdx through_idx, Term through_term) {
    if (through_idx + 1 <= log_start_index_) {
        return;  // already snapshotted past this point
    }
    if (through_idx >= LastIndex()) {
        entries_.clear();
        log_start_index_ = through_idx + 1;
        log_start_term_ = through_term;
        return;
    }
    const auto drop = static_cast<std::size_t>(through_idx - log_start_index_ + 1);
    entries_.erase(entries_.begin(), entries_.begin() + static_cast<std::ptrdiff_t>(drop));
    log_start_index_ = through_idx + 1;
    log_start_term_ = through_term;
}

LogIdx RaftLog::LastIndex() const {
    if (entries_.empty()) {
        return log_start_index_ - 1;
    }
    return entries_.back().index;
}

Term RaftLog::LastTerm() const {
    if (entries_.empty()) {
        return log_start_term_;
    }
    return entries_.back().term;
}

bool RaftLog::Matches(LogIdx index, Term term) const {
    if (index == 0) {
        return term == 0;
    }
    if (index == log_start_index_ - 1) {
        return term == log_start_term_;
    }
    if (index < log_start_index_ || index > LastIndex()) {
        return false;
    }
    return entries_[static_cast<std::size_t>(index - log_start_index_)].term == term;
}

std::size_t RaftLog::Size() const {
    return entries_.size();
}

LogIdx RaftLog::LogStartIndex() const {
    return log_start_index_;
}
Term RaftLog::LogStartTerm() const {
    return log_start_term_;
}

}  // namespace knot::raft
