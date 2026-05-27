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
    if (index == 0 || index > entries_.size()) {
        throw std::out_of_range("RaftLog::At index out of range");
    }
    return entries_[index - 1];
}

std::vector<LogEntry> RaftLog::Slice(LogIdx from, LogIdx to) const {
    if (from == 0) {
        return {};
    }
    if (from >= to) {
        return {};
    }
    LogIdx last = LastIndex();
    if (from > last) {
        return {};
    }
    LogIdx end = (to - 1 > last) ? last : (to - 1);  // inclusive on disk

    std::vector<LogEntry> out;
    out.reserve(end - from + 1);
    for (LogIdx i = from; i <= end; ++i) {
        out.push_back(entries_[i - 1]);
    }
    return out;
}

void RaftLog::TruncateFrom(LogIdx from) {
    if (from == 0 || from > entries_.size()) {
        return;
    }
    entries_.resize(from - 1);
}

LogIdx RaftLog::LastIndex() const {
    return entries_.empty() ? 0 : entries_.back().index;
}

Term RaftLog::LastTerm() const {
    return entries_.empty() ? 0 : entries_.back().term;
}

bool RaftLog::Matches(LogIdx index, Term term) const {
    if (index == 0) {
        return term == 0;
    }
    if (index > entries_.size()) {
        return false;
    }
    return entries_[index - 1].term == term;
}

std::size_t RaftLog::Size() const {
    return entries_.size();
}

}  // namespace knot::raft
