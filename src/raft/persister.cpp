#include <cerrno>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <system_error>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <knot/raft/persister.h>
#include <unistd.h>

namespace knot::raft {

namespace {

class MemoryPersister final : public Persister {
public:
    void SaveState(Term current_term, const std::optional<NodeId>& voted_for) override {
        state_.current_term = current_term;
        state_.voted_for = voted_for;
        has_state_ = true;
    }

    void AppendLog(const LogEntry& entry) override { entries_.push_back(entry); }

    void TruncateLog(LogIdx from) override {
        const auto removed =
            std::ranges::remove_if(entries_, [from](const LogEntry& e) { return e.index >= from; });
        entries_.erase(removed.begin(), removed.end());
    }

    bool Load(PersistentState* out_state, std::vector<LogEntry>* out_entries) override {
        if (!has_state_ && entries_.empty()) {
            return false;
        }
        if (out_state) {
            *out_state = state_;
        }
        if (out_entries) {
            *out_entries = entries_;
        }
        return true;
    }

private:
    PersistentState state_;
    bool has_state_ = false;
    std::vector<LogEntry> entries_;
};

constexpr std::uint32_t kStateMagic = 0x4B4E4F54U;      // "KNOT"
constexpr std::uint32_t kLogRecordMagic = 0x4B4C4F47U;  // "KLOG"

// Table-less CRC32 (IEEE 802.3). Slower than a table-driven impl, but
// matches the polynomial used in src/common/crc32.cpp and adds no deps.
// Takes void* so any byte buffer (char*, uint8_t*) flows in cleanly.
std::uint32_t Crc32(const void* vdata, std::size_t n) {
    const auto* data = static_cast<const std::uint8_t*>(vdata);
    std::uint32_t crc = 0xFFFFFFFFU;
    for (std::size_t i = 0; i < n; ++i) {
        crc ^= data[i];
        for (int b = 0; b < 8; ++b) {
            const std::uint32_t mask = -(crc & 1U);
            crc = (crc >> 1) ^ (0xEDB88320U & mask);
        }
    }
    return ~crc;
}

void FullSync(int fd) {
#ifdef __APPLE__
    if (::fcntl(fd, F_FULLFSYNC) < 0) {
        if (::fsync(fd) < 0) {
            throw std::system_error(errno, std::system_category(), "F_FULLFSYNC/fsync");
        }
    }
#else
    if (::fsync(fd) < 0) {
        throw std::system_error(errno, std::system_category(), "fsync");
    }
#endif
}

class FilePersister final : public Persister {
public:
    explicit FilePersister(std::filesystem::path dir) : dir_(std::move(dir)) {
        std::filesystem::create_directories(dir_);
    }

    void SaveState(Term current_term, const std::optional<NodeId>& voted_for) override {
        const auto tmp_path = dir_ / "state.bin.tmp";
        const auto final_path = dir_ / "state.bin";

        const std::string vf = voted_for.value_or(std::string{});
        const auto vf_len = static_cast<std::uint32_t>(vf.size());

        std::vector<std::uint8_t> buf;
        buf.reserve(4 + 8 + 4 + vf.size());
        auto put32 = [&](std::uint32_t v) {
            for (int i = 0; i < 4; ++i) {
                buf.push_back(static_cast<std::uint8_t>(v >> (8 * i)));
            }
        };
        auto put64 = [&](std::uint64_t v) {
            for (int i = 0; i < 8; ++i) {
                buf.push_back(static_cast<std::uint8_t>(v >> (8 * i)));
            }
        };
        put32(kStateMagic);
        put64(current_term);
        put32(vf_len);
        buf.insert(buf.end(), vf.begin(), vf.end());

        const int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) {
            throw std::system_error(errno, std::system_category(), "FilePersister::SaveState open");
        }
        const ssize_t n = ::write(fd, buf.data(), buf.size());
        if (n != static_cast<ssize_t>(buf.size())) {
            const int saved_errno = errno;
            ::close(fd);
            throw std::system_error(saved_errno, std::system_category(),
                                    "FilePersister::SaveState write");
        }
        FullSync(fd);
        ::close(fd);
        std::filesystem::rename(tmp_path, final_path);
    }

    void AppendLog(const LogEntry& entry) override {
        const auto log_path = dir_ / "log.bin";

        const auto payload_len = static_cast<std::uint32_t>(entry.payload.size());

        std::vector<std::uint8_t> body;
        body.reserve(4 + 8 + 8 + 1 + 4 + entry.payload.size());

        auto put32 = [&](std::uint32_t v) {
            for (int i = 0; i < 4; ++i) {
                body.push_back(static_cast<std::uint8_t>(v >> (8 * i)));
            }
        };
        auto put64 = [&](std::uint64_t v) {
            for (int i = 0; i < 8; ++i) {
                body.push_back(static_cast<std::uint8_t>(v >> (8 * i)));
            }
        };

        put32(kLogRecordMagic);
        put64(entry.term);
        put64(entry.index);
        body.push_back(static_cast<std::uint8_t>(entry.type));
        put32(payload_len);
        body.insert(body.end(), entry.payload.begin(), entry.payload.end());

        const std::uint32_t crc = Crc32(body.data(), body.size());

        std::vector<std::uint8_t> framed;
        framed.reserve(4 + body.size());
        for (int i = 0; i < 4; ++i) {
            framed.push_back(static_cast<std::uint8_t>(crc >> (8 * i)));
        }
        framed.insert(framed.end(), body.begin(), body.end());

        const int fd = ::open(log_path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) {
            throw std::system_error(errno, std::system_category(), "FilePersister::AppendLog open");
        }
        const ssize_t n = ::write(fd, framed.data(), framed.size());
        if (n != static_cast<ssize_t>(framed.size())) {
            const int saved_errno = errno;
            ::close(fd);
            throw std::system_error(saved_errno, std::system_category(),
                                    "FilePersister::AppendLog write");
        }
        FullSync(fd);
        ::close(fd);
    }

    void TruncateLog(LogIdx from) override {
        std::vector<LogEntry> kept;
        (void)LoadLogOnly(&kept);
        const auto removed =
            std::ranges::remove_if(kept, [from](const LogEntry& e) { return e.index >= from; });
        kept.erase(removed.begin(), removed.end());

        const auto log_path = dir_ / "log.bin";
        const auto tmp_path = dir_ / "log.bin.tmp";

        // Wipe the on-disk log; we rebuild it from kept[].
        const int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) {
            throw std::system_error(errno, std::system_category(),
                                    "FilePersister::TruncateLog open");
        }
        FullSync(fd);
        ::close(fd);
        std::filesystem::rename(tmp_path, log_path);

        for (const auto& e : kept) {
            AppendLog(e);
        }
    }

    bool Load(PersistentState* out_state, std::vector<LogEntry>* out_entries) override {
        const auto state_path = dir_ / "state.bin";
        const bool has_state = std::filesystem::exists(state_path);

        if (has_state && out_state) {
            std::ifstream f(state_path, std::ios::binary);
            if (!f) {
                return false;
            }
            const std::vector<char> raw((std::istreambuf_iterator<char>(f)),
                                        std::istreambuf_iterator<char>());
            if (raw.size() < 4 + 8 + 4) {
                return false;
            }
            auto get32 = [&](std::size_t off) {
                std::uint32_t v = 0;
                for (int i = 0; i < 4; ++i) {
                    v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(raw[off + i]))
                         << (8 * i);
                }
                return v;
            };
            auto get64 = [&](std::size_t off) {
                std::uint64_t v = 0;
                for (int i = 0; i < 8; ++i) {
                    v |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(raw[off + i]))
                         << (8 * i);
                }
                return v;
            };

            if (get32(0) != kStateMagic) {
                return false;
            }
            const Term term = get64(4);
            const std::uint32_t lv = get32(12);
            if (raw.size() < 16U + lv) {
                return false;
            }

            out_state->current_term = term;
            if (lv > 0) {
                out_state->voted_for = std::string(raw.data() + 16, lv);
            } else {
                out_state->voted_for.reset();
            }
        }

        if (out_entries) {
            out_entries->clear();
            (void)LoadLogOnly(out_entries);
        }

        return has_state || std::filesystem::exists(dir_ / "log.bin");
    }

private:
    // Read all framed records from log.bin. Returns true if file existed.
    bool LoadLogOnly(std::vector<LogEntry>* out_entries) {
        const auto log_path = dir_ / "log.bin";
        if (!std::filesystem::exists(log_path)) {
            return false;
        }
        std::ifstream f(log_path, std::ios::binary);
        if (!f) {
            return false;
        }
        const std::vector<char> raw((std::istreambuf_iterator<char>(f)),
                                    std::istreambuf_iterator<char>());

        auto get32 = [&](std::size_t off) {
            std::uint32_t v = 0;
            for (int i = 0; i < 4; ++i) {
                v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(raw[off + i])) << (8 * i);
            }
            return v;
        };
        auto get64 = [&](std::size_t off) {
            std::uint64_t v = 0;
            for (int i = 0; i < 8; ++i) {
                v |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(raw[off + i])) << (8 * i);
            }
            return v;
        };

        std::size_t off = 0;
        const std::size_t header_size = 4 + 4 + 8 + 8 + 1 + 4;  // crc+magic+term+idx+type+plen
        while (off + header_size <= raw.size()) {
            const std::uint32_t stored_crc = get32(off);
            const std::size_t body_off = off + 4;

            if (get32(body_off) != kLogRecordMagic) {
                return false;
            }
            const Term term = get64(body_off + 4);
            const LogIdx idx = get64(body_off + 12);
            const auto type = static_cast<EntryType>(static_cast<std::uint8_t>(raw[body_off + 20]));
            const std::uint32_t pl = get32(body_off + 21);

            const std::size_t body_size = 4 + 8 + 8 + 1 + 4 + pl;
            if (body_off + body_size > raw.size()) {
                return false;
            }

            const std::uint32_t computed = Crc32(raw.data() + body_off, body_size);
            if (computed != stored_crc) {
                return false;
            }

            LogEntry e{.term = term,
                       .index = idx,
                       .type = type,
                       .payload = std::string(raw.data() + body_off + 25, pl)};
            out_entries->push_back(std::move(e));
            off = body_off + body_size;
        }
        return true;
    }

    std::filesystem::path dir_;
};

}  // namespace

std::shared_ptr<Persister> MakeMemoryPersister() {
    return std::make_shared<MemoryPersister>();
}

std::shared_ptr<Persister> MakeFilePersister(const std::filesystem::path& dir) {
    return std::make_shared<FilePersister>(dir);
}

}  // namespace knot::raft
