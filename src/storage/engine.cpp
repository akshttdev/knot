#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include <fcntl.h>
#include <knot/common/crc32.h>
#include <knot/storage/engine.h>
#include <knot/storage/memtable.h>
#include <knot/storage/sstable.h>
#include <knot/storage/wal.h>
#include <unistd.h>

namespace knot::storage {

namespace {

constexpr std::uint64_t kManifestMagic = 0xBABE5757DA7AB1EFULL;
constexpr std::uint32_t kManifestVersion = 1;
constexpr std::uint8_t kOpPut = 0;
constexpr std::uint8_t kOpDelete = 1;

// --------- I/O helpers ---------

[[noreturn]] void ThrowErrno(const std::string& what) {
    throw std::system_error(errno, std::system_category(), what);
}

void AppendU32(std::string* out, std::uint32_t v) {
    char buf[4];
    std::memcpy(buf, &v, 4);
    out->append(buf, 4);
}

void AppendU64(std::string* out, std::uint64_t v) {
    char buf[8];
    std::memcpy(buf, &v, 8);
    out->append(buf, 8);
}

std::uint32_t ReadU32(const char* p) {
    std::uint32_t v = 0;
    std::memcpy(&v, p, 4);
    return v;
}

std::uint64_t ReadU64(const char* p) {
    std::uint64_t v = 0;
    std::memcpy(&v, p, 8);
    return v;
}

void WriteFileAtomic(const std::filesystem::path& final_path, std::string_view data) {
    auto tmp = final_path;
    tmp += ".tmp";
    int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        ThrowErrno("open " + tmp.string());
    }
    std::size_t written = 0;
    while (written < data.size()) {
        ssize_t w = ::write(fd, data.data() + written, data.size() - written);
        if (w < 0) {
            if (errno == EINTR) {
                continue;
            }
            ::close(fd);
            ThrowErrno("write manifest");
        }
        written += static_cast<std::size_t>(w);
    }
#ifdef __APPLE__
    ::fcntl(fd, F_FULLFSYNC);
#else
    ::fsync(fd);
#endif
    ::close(fd);
    if (::rename(tmp.c_str(), final_path.c_str()) != 0) {
        ThrowErrno("rename manifest");
    }
}

std::string ReadEntireFile(const std::filesystem::path& path) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        ThrowErrno("open " + path.string());
    }
    std::string out;
    char buf[4096];
    while (true) {
        ssize_t n = ::read(fd, buf, sizeof(buf));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            ::close(fd);
            ThrowErrno("read " + path.string());
        }
        if (n == 0) {
            break;
        }
        out.append(buf, static_cast<std::size_t>(n));
    }
    ::close(fd);
    return out;
}

// --------- MANIFEST encode/decode ---------

struct ManifestState {
    std::uint64_t last_seq = 0;  // highest engine seq known to be in SSTables
    std::uint64_t next_sst_id = 1;
    std::vector<std::uint64_t> sst_ids;  // newest-first (highest id first)
};

std::string EncodeManifest(const ManifestState& state) {
    std::string body;
    AppendU64(&body, kManifestMagic);
    AppendU32(&body, kManifestVersion);
    AppendU64(&body, state.last_seq);
    AppendU64(&body, state.next_sst_id);
    AppendU32(&body, static_cast<std::uint32_t>(state.sst_ids.size()));
    for (auto id : state.sst_ids) {
        AppendU64(&body, id);
    }
    auto crc = common::Crc32(0, body.data(), body.size());
    AppendU32(&body, crc);
    return body;
}

ManifestState DecodeManifest(std::string_view data) {
    constexpr std::size_t kFixedHeaderSize = 8 + 4 + 8 + 8 + 4;  // 32 bytes
    if (data.size() < kFixedHeaderSize + 4) {
        throw std::runtime_error("MANIFEST: too small");
    }
    auto stored_crc = ReadU32(data.data() + data.size() - 4);
    auto computed_crc = common::Crc32(0, data.data(), data.size() - 4);
    if (stored_crc != computed_crc) {
        throw std::runtime_error("MANIFEST: CRC mismatch");
    }
    auto magic = ReadU64(data.data());
    if (magic != kManifestMagic) {
        throw std::runtime_error("MANIFEST: bad magic");
    }
    auto version = ReadU32(data.data() + 8);
    if (version != kManifestVersion) {
        throw std::runtime_error("MANIFEST: unknown version");
    }
    ManifestState state;
    state.last_seq = ReadU64(data.data() + 12);
    state.next_sst_id = ReadU64(data.data() + 20);
    auto num_ssts = ReadU32(data.data() + 28);
    if (data.size() < kFixedHeaderSize + num_ssts * 8U + 4U) {
        throw std::runtime_error("MANIFEST: truncated sst list");
    }
    state.sst_ids.reserve(num_ssts);
    for (std::uint32_t i = 0; i < num_ssts; ++i) {
        state.sst_ids.push_back(ReadU64(data.data() + kFixedHeaderSize + i * 8));
    }
    return state;
}

// --------- WAL payload encode/decode ---------
// Format: [op(1)] [seq(8)] [key_len(4)] [key] [value_len(4)] [value]
// For DELETE: value_len = 0 (no value bytes).

std::string EncodeWalPayload(std::uint8_t op, std::uint64_t seq, std::string_view key,
                             std::string_view value) {
    std::string out;
    out.reserve(17 + key.size() + value.size());
    out.push_back(static_cast<char>(op));
    AppendU64(&out, seq);
    AppendU32(&out, static_cast<std::uint32_t>(key.size()));
    out.append(key);
    AppendU32(&out, static_cast<std::uint32_t>(value.size()));
    out.append(value);
    return out;
}

bool DecodeWalPayload(std::string_view payload, std::uint8_t* op_out, std::uint64_t* seq_out,
                      std::string* key_out, std::string* value_out) {
    if (payload.size() < 17) {
        return false;
    }
    const char* p = payload.data();
    *op_out = static_cast<std::uint8_t>(p[0]);
    *seq_out = ReadU64(p + 1);
    auto klen = ReadU32(p + 9);
    if (payload.size() < 13 + klen) {
        return false;
    }
    key_out->assign(p + 13, klen);
    auto vlen = ReadU32(p + 13 + klen);
    if (payload.size() < 17 + klen + vlen) {
        return false;
    }
    value_out->assign(p + 17 + klen, vlen);
    return true;
}

// --------- Path helpers ---------

std::filesystem::path ManifestPath(const std::filesystem::path& dir) {
    return dir / "MANIFEST";
}

std::filesystem::path WalDir(const std::filesystem::path& dir) {
    return dir / "wal";
}

std::filesystem::path SstDir(const std::filesystem::path& dir) {
    return dir / "sst";
}

std::string SstFileName(std::uint64_t sst_id) {
    char buf[32];
    (void)std::snprintf(buf, sizeof(buf), "sst-%08llu.sst",
                        static_cast<unsigned long long>(sst_id));
    return buf;
}

std::filesystem::path SstPath(const std::filesystem::path& dir, std::uint64_t sst_id) {
    return SstDir(dir) / SstFileName(sst_id);
}

}  // namespace

// =====================================================================
// StorageEngine
// =====================================================================

std::unique_ptr<StorageEngine> StorageEngine::Open(Options options) {
    std::filesystem::create_directories(options.data_dir);
    std::filesystem::create_directories(WalDir(options.data_dir));
    std::filesystem::create_directories(SstDir(options.data_dir));

    std::unique_ptr<StorageEngine> engine(new StorageEngine(std::move(options)));
    engine->Recover();
    return engine;
}

StorageEngine::StorageEngine(Options options) : options_(std::move(options)) {}

StorageEngine::~StorageEngine() = default;

void StorageEngine::Recover() {
    // 1. Load MANIFEST (or default if missing).
    ManifestState manifest;
    auto mpath = ManifestPath(options_.data_dir);
    if (std::filesystem::exists(mpath)) {
        manifest = DecodeManifest(ReadEntireFile(mpath));
    }
    last_seq_in_sstables_ = manifest.last_seq;
    next_sst_id_ = manifest.next_sst_id;
    next_seq_ = manifest.last_seq + 1;

    // 2. Open SSTables in newest-first order (manifest already in that order).
    for (auto sst_id : manifest.sst_ids) {
        sstables_.push_back({sst_id, SSTableReader::Open(SstPath(options_.data_dir, sst_id))});
    }

    // 3. Open WAL writer for ongoing appends.
    wal_ = WalWriter::Open(WalDir(options_.data_dir), options_.wal_segment_size);

    // 4. Replay WAL records that aren't yet in SSTables.
    memtable_ = std::make_unique<MemTable>();
    auto wal_reader = WalReader::Open(WalDir(options_.data_dir));
    while (auto rec = wal_reader->Next()) {
        std::uint8_t op = 0;
        std::uint64_t engine_seq = 0;
        std::string key;
        std::string value;
        if (!DecodeWalPayload(rec->payload, &op, &engine_seq, &key, &value)) {
            throw std::runtime_error("Engine: malformed WAL record");
        }
        if (engine_seq <= last_seq_in_sstables_) {
            continue;  // already flushed to an SSTable
        }
        if (op == kOpPut) {
            memtable_->Put(engine_seq, key, value);
        } else if (op == kOpDelete) {
            memtable_->Delete(engine_seq, key);
        } else {
            throw std::runtime_error("Engine: unknown WAL op");
        }
        if (engine_seq >= next_seq_) {
            next_seq_ = engine_seq + 1;
        }
    }
}

void StorageEngine::Put(std::string_view key, std::string_view value) {
    std::lock_guard lock(mutex_);
    auto seq = next_seq_++;
    auto payload = EncodeWalPayload(kOpPut, seq, key, value);
    wal_->AppendSync(payload);
    memtable_->Put(seq, key, value);
    if (memtable_->ApproximateSize() >= options_.memtable_size_threshold) {
        FlushLocked();
    }
}

void StorageEngine::Delete(std::string_view key) {
    std::lock_guard lock(mutex_);
    auto seq = next_seq_++;
    auto payload = EncodeWalPayload(kOpDelete, seq, key, {});
    wal_->AppendSync(payload);
    memtable_->Delete(seq, key);
    if (memtable_->ApproximateSize() >= options_.memtable_size_threshold) {
        FlushLocked();
    }
}

StorageEngine::GetResult StorageEngine::Get(std::string_view key, std::string* value_out) const {
    std::lock_guard lock(mutex_);
    // 1. Active MemTable.
    auto mt = memtable_->Get(key, value_out);
    if (mt == MemTable::LookupResult::kFound) {
        return GetResult::kFound;
    }
    if (mt == MemTable::LookupResult::kDeleted) {
        return GetResult::kNotFound;
    }
    // 2. SSTables, newest first.
    for (const auto& sst : sstables_) {
        auto r = sst.reader->Get(key, value_out);
        if (r == SSTableReader::LookupResult::kFound) {
            return GetResult::kFound;
        }
        if (r == SSTableReader::LookupResult::kDeleted) {
            return GetResult::kNotFound;
        }
    }
    return GetResult::kNotFound;
}

void StorageEngine::Flush() {
    std::lock_guard lock(mutex_);
    if (memtable_->Empty()) {
        return;
    }
    FlushLocked();
}

void StorageEngine::FlushLocked() {
    auto sst_id = next_sst_id_++;
    auto sst_path = SstPath(options_.data_dir, sst_id);

    auto highest_seq = memtable_->LastSequence();

    auto writer = SSTableWriter::Open(sst_path, options_.sstable_block_size);
    auto iter = memtable_->NewIterator();
    while (iter->Valid()) {
        if (iter->IsTombstone()) {
            writer->AddTombstone(iter->Sequence(), iter->Key());
        } else {
            writer->Add(iter->Sequence(), iter->Key(), iter->Value());
        }
        iter->Next();
    }
    writer->Finish();

    // Fresh MemTable, new SSTable at the front (newest).
    memtable_ = std::make_unique<MemTable>();
    sstables_.insert(sstables_.begin(), {sst_id, SSTableReader::Open(sst_path)});
    if (highest_seq > last_seq_in_sstables_) {
        last_seq_in_sstables_ = highest_seq;
    }

    RewriteManifest();
}

void StorageEngine::RewriteManifest() {
    ManifestState state;
    state.last_seq = last_seq_in_sstables_;
    state.next_sst_id = next_sst_id_;
    state.sst_ids.reserve(sstables_.size());
    for (const auto& sst : sstables_) {
        state.sst_ids.push_back(sst.id);
    }
    auto encoded = EncodeManifest(state);
    WriteFileAtomic(ManifestPath(options_.data_dir), encoded);
}

std::uint64_t StorageEngine::SequenceNumber() const {
    std::lock_guard lock(mutex_);
    return next_seq_ - 1;
}

std::size_t StorageEngine::SSTableCount() const {
    std::lock_guard lock(mutex_);
    return sstables_.size();
}

std::size_t StorageEngine::MemTableSize() const {
    std::lock_guard lock(mutex_);
    return memtable_->ApproximateSize();
}

}  // namespace knot::storage
