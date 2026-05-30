// Knot — Raft persistence interface.
//
// FilePersister: durable on-disk (state.bin + log.bin under data_dir/).
// MemoryPersister: test fake — round-trip-able across Create() instances
// when the same shared_ptr is passed in.

#pragma once

#include <filesystem>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

#include <knot/raft/state.h>
#include <knot/raft/types.h>

namespace knot::raft {

class Persister {
public:
    virtual ~Persister() = default;

    Persister(const Persister&) = delete;
    Persister& operator=(const Persister&) = delete;
    Persister(Persister&&) = delete;
    Persister& operator=(Persister&&) = delete;

    virtual void SaveState(Term current_term, const std::optional<NodeId>& voted_for) = 0;

    virtual void AppendLog(const LogEntry& entry) = 0;

    virtual void TruncateLog(LogIdx from) = 0;

    [[nodiscard]] virtual bool Load(PersistentState* out_state,
                                    std::vector<LogEntry>* out_entries) = 0;

    // Save the snapshot atomically. last_included_index is the last log
    // index covered by this snapshot; last_included_term is its term.
    virtual void SaveSnapshot(std::string_view bytes, LogIdx last_included_index,
                              Term last_included_term) = 0;

    // Load snapshot if one exists. Returns false on missing/corrupt.
    [[nodiscard]] virtual bool LoadSnapshot(std::string* out_bytes, LogIdx* out_index,
                                            Term* out_term) = 0;

protected:
    Persister() = default;
};

[[nodiscard]] std::shared_ptr<Persister> MakeFilePersister(const std::filesystem::path& dir);

[[nodiscard]] std::shared_ptr<Persister> MakeMemoryPersister();

}  // namespace knot::raft
