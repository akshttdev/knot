#include <algorithm>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include <fcntl.h>
#include <knot/common/crc32.h>
#include <knot/storage/sstable.h>
#include <sys/stat.h>
#include <unistd.h>

namespace knot::storage {

namespace {

constexpr std::uint64_t kMagic = 0xDB57AB1E12345678ULL;
constexpr std::size_t kFooterSize = 24;
constexpr std::size_t kEntryHeaderSize = 17;  // key_len(4)+value_len(4)+type(1)+seq(8)

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

ssize_t PreadFull(int fd, void* buf, std::size_t n, std::uint64_t offset) {
    auto* p = static_cast<std::uint8_t*>(buf);
    std::size_t got = 0;
    while (got < n) {
        ssize_t r = ::pread(fd, p + got, n - got, static_cast<off_t>(offset + got));
        if (r < 0) {
            if (errno == EINTR) {
                continue;
            }
            ThrowErrno("pread");
        }
        if (r == 0) {
            break;
        }
        got += static_cast<std::size_t>(r);
    }
    return static_cast<ssize_t>(got);
}

void WriteFull(int fd, const void* data, std::size_t n) {
    const auto* p = static_cast<const std::uint8_t*>(data);
    std::size_t written = 0;
    while (written < n) {
        ssize_t w = ::write(fd, p + written, n - written);
        if (w < 0) {
            if (errno == EINTR) {
                continue;
            }
            ThrowErrno("write");
        }
        written += static_cast<std::size_t>(w);
    }
}

}  // namespace

// =====================================================================
// SSTableWriter
// =====================================================================

std::unique_ptr<SSTableWriter> SSTableWriter::Open(const std::filesystem::path& path,
                                                   std::size_t target_block_size) {
    std::unique_ptr<SSTableWriter> w(new SSTableWriter(path, target_block_size));
    w->fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (w->fd_ < 0) {
        ThrowErrno("SSTableWriter::Open(" + path.string() + ")");
    }
    return w;
}

SSTableWriter::SSTableWriter(std::filesystem::path path, std::size_t target_block_size)
    : path_(std::move(path)), target_block_size_(target_block_size) {}

SSTableWriter::~SSTableWriter() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

void SSTableWriter::Add(std::uint64_t seq, std::string_view key, std::string_view value) {
    AppendEntry(seq, key, value, 0);
}

void SSTableWriter::AddTombstone(std::uint64_t seq, std::string_view key) {
    AppendEntry(seq, key, {}, 1);
}

void SSTableWriter::AppendEntry(std::uint64_t seq, std::string_view key, std::string_view value,
                                std::uint8_t type) {
    if (finished_) {
        throw std::runtime_error("SSTable: Add after Finish");
    }
    if (key.empty()) {
        throw std::runtime_error("SSTable: key must be non-empty");
    }
    if (has_previous_key_ && key <= previous_key_) {
        throw std::runtime_error("SSTable: keys must be strictly increasing");
    }
    previous_key_ = std::string(key);
    has_previous_key_ = true;

    if (!block_has_entries_) {
        pending_first_key_ = std::string(key);
        block_has_entries_ = true;
    }

    auto key_len = static_cast<std::uint32_t>(key.size());
    auto value_len = static_cast<std::uint32_t>(value.size());

    AppendU32(&current_block_, key_len);
    AppendU32(&current_block_, value_len);
    current_block_.push_back(static_cast<char>(type));
    AppendU64(&current_block_, seq);
    current_block_.append(key.data(), key.size());
    current_block_.append(value.data(), value.size());

    ++entry_count_;

    if (current_block_.size() >= target_block_size_) {
        FlushBlock();
    }
}

void SSTableWriter::FlushBlock() {
    if (!block_has_entries_) {
        return;
    }

    auto crc = common::Crc32(0, current_block_.data(), current_block_.size());
    AppendU32(&current_block_, crc);

    auto block_size = static_cast<std::uint32_t>(current_block_.size());
    WriteFull(fd_, current_block_.data(), block_size);

    index_.push_back({std::move(pending_first_key_), current_offset_, block_size});
    current_offset_ += block_size;

    current_block_.clear();
    pending_first_key_.clear();
    block_has_entries_ = false;
}

void SSTableWriter::WriteIndexAndFooter() {
    std::string index_block;
    for (const auto& entry : index_) {
        AppendU32(&index_block, static_cast<std::uint32_t>(entry.first_key.size()));
        AppendU64(&index_block, entry.offset);
        AppendU32(&index_block, entry.size);
        index_block.append(entry.first_key);
    }
    auto index_crc = common::Crc32(0, index_block.data(), index_block.size());
    AppendU32(&index_block, index_crc);

    auto index_offset = current_offset_;
    auto index_length = static_cast<std::uint32_t>(index_block.size());

    WriteFull(fd_, index_block.data(), index_block.size());
    current_offset_ += index_block.size();

    std::string footer;
    AppendU64(&footer, kMagic);
    AppendU64(&footer, index_offset);
    AppendU32(&footer, index_length);
    auto footer_crc = common::Crc32(0, footer.data(), footer.size());
    AppendU32(&footer, footer_crc);

    WriteFull(fd_, footer.data(), footer.size());
}

void SSTableWriter::Finish() {
    if (finished_) {
        return;
    }
    FlushBlock();
    WriteIndexAndFooter();
    if (fd_ >= 0) {
#ifdef __APPLE__
        ::fcntl(fd_, F_FULLFSYNC);
#else
        ::fsync(fd_);
#endif
        ::close(fd_);
        fd_ = -1;
    }
    finished_ = true;
}

std::size_t SSTableWriter::EntryCount() const {
    return entry_count_;
}

// =====================================================================
// SSTableReader
// =====================================================================

std::unique_ptr<SSTableReader> SSTableReader::Open(const std::filesystem::path& path) {
    auto reader = std::unique_ptr<SSTableReader>(new SSTableReader());
    reader->fd_ = ::open(path.c_str(), O_RDONLY);
    if (reader->fd_ < 0) {
        ThrowErrno("SSTableReader::Open(" + path.string() + ")");
    }

    struct stat st = {};
    if (::fstat(reader->fd_, &st) < 0) {
        ThrowErrno("fstat");
    }
    auto file_size = static_cast<std::uint64_t>(st.st_size);
    if (file_size < kFooterSize) {
        throw std::runtime_error("SSTable: file too small to contain footer");
    }

    // Read footer
    std::string footer(kFooterSize, '\0');
    if (PreadFull(reader->fd_, footer.data(), kFooterSize, file_size - kFooterSize) !=
        static_cast<ssize_t>(kFooterSize)) {
        throw std::runtime_error("SSTable: truncated footer");
    }

    auto magic = ReadU64(footer.data());
    if (magic != kMagic) {
        throw std::runtime_error("SSTable: bad magic number");
    }
    auto index_offset = ReadU64(footer.data() + 8);
    auto index_length = ReadU32(footer.data() + 16);
    auto stored_footer_crc = ReadU32(footer.data() + 20);

    auto computed_footer_crc = common::Crc32(0, footer.data(), 20);
    if (stored_footer_crc != computed_footer_crc) {
        throw std::runtime_error("SSTable: footer CRC mismatch");
    }

    if (index_offset + index_length > file_size - kFooterSize) {
        throw std::runtime_error("SSTable: invalid index offset/length");
    }

    std::string index_block(index_length, '\0');
    if (PreadFull(reader->fd_, index_block.data(), index_length, index_offset) !=
        static_cast<ssize_t>(index_length)) {
        throw std::runtime_error("SSTable: truncated index");
    }

    auto stored_index_crc = ReadU32(index_block.data() + index_length - 4);
    auto computed_index_crc = common::Crc32(0, index_block.data(), index_length - 4);
    if (stored_index_crc != computed_index_crc) {
        throw std::runtime_error("SSTable: index CRC mismatch");
    }

    // Parse index entries
    const char* p = index_block.data();
    const char* end = p + index_length - 4;  // exclude CRC
    while (p < end) {
        if (end - p < 16) {
            throw std::runtime_error("SSTable: truncated index entry");
        }
        IndexEntry e;
        auto key_len = ReadU32(p);
        e.offset = ReadU64(p + 4);
        e.size = ReadU32(p + 12);
        p += 16;
        if (end - p < static_cast<std::ptrdiff_t>(key_len)) {
            throw std::runtime_error("SSTable: truncated index key");
        }
        e.first_key.assign(p, key_len);
        p += key_len;
        reader->index_.push_back(std::move(e));
    }

    return reader;
}

SSTableReader::~SSTableReader() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

std::string SSTableReader::ReadBlock(std::uint64_t offset, std::uint32_t size) const {
    std::string block(size, '\0');
    if (PreadFull(fd_, block.data(), size, offset) != static_cast<ssize_t>(size)) {
        throw std::runtime_error("SSTable: truncated data block");
    }
    auto stored_crc = ReadU32(block.data() + size - 4);
    auto computed_crc = common::Crc32(0, block.data(), size - 4);
    if (stored_crc != computed_crc) {
        throw std::runtime_error("SSTable: data block CRC mismatch");
    }
    block.resize(size - 4);
    return block;
}

SSTableReader::LookupResult SSTableReader::Get(std::string_view key, std::string* value_out) const {
    if (index_.empty()) {
        return LookupResult::kNotFound;
    }

    // Find the rightmost block whose first_key <= key
    auto it =
        std::upper_bound(index_.begin(), index_.end(), key,
                         [](std::string_view k, const IndexEntry& e) { return k < e.first_key; });
    if (it == index_.begin()) {
        return LookupResult::kNotFound;
    }
    --it;

    auto block = ReadBlock(it->offset, it->size);

    const char* p = block.data();
    const char* end = p + block.size();
    while (p < end) {
        if (end - p < static_cast<std::ptrdiff_t>(kEntryHeaderSize)) {
            throw std::runtime_error("SSTable: truncated entry header");
        }
        auto klen = ReadU32(p);
        auto vlen = ReadU32(p + 4);
        auto type = static_cast<std::uint8_t>(p[8]);
        p += kEntryHeaderSize;
        if (end - p < static_cast<std::ptrdiff_t>(klen + vlen)) {
            throw std::runtime_error("SSTable: truncated entry payload");
        }
        std::string_view this_key(p, klen);
        std::string_view this_value(p + klen, vlen);
        p += klen + vlen;

        if (this_key == key) {
            if (type == 1) {
                return LookupResult::kDeleted;
            }
            if (value_out != nullptr) {
                value_out->assign(this_value);
            }
            return LookupResult::kFound;
        }
        if (this_key > key) {
            break;  // keys are sorted; key isn't in this block
        }
    }
    return LookupResult::kNotFound;
}

std::size_t SSTableReader::BlockCount() const {
    return index_.size();
}

std::unique_ptr<SSTableReader::Iterator> SSTableReader::NewIterator() const {
    auto iter = std::make_unique<Iterator>();
    iter->reader_ = this;
    if (!index_.empty()) {
        iter->LoadBlock(0);
    }
    return iter;
}

// =====================================================================
// SSTableReader::Iterator
// =====================================================================

void SSTableReader::Iterator::LoadBlock(std::size_t block_idx) {
    if (block_idx >= reader_->index_.size()) {
        valid_ = false;
        return;
    }
    const auto& entry = reader_->index_[block_idx];
    block_data_ = reader_->ReadBlock(entry.offset, entry.size);
    block_idx_ = block_idx;
    pos_ = 0;
    ParseCurrentEntry();
}

void SSTableReader::Iterator::ParseCurrentEntry() {
    if (pos_ >= block_data_.size()) {
        valid_ = false;
        return;
    }
    const char* p = block_data_.data() + pos_;
    auto klen = ReadU32(p);
    auto vlen = ReadU32(p + 4);
    is_tombstone_ = (p[8] == 1);
    seq_ = ReadU64(p + 9);
    p += kEntryHeaderSize;
    key_view_ = std::string_view(p, klen);
    value_view_ = std::string_view(p + klen, vlen);
    valid_ = true;
}

bool SSTableReader::Iterator::Valid() const {
    return valid_;
}

void SSTableReader::Iterator::Next() {
    if (!valid_) {
        return;
    }
    pos_ += kEntryHeaderSize + key_view_.size() + value_view_.size();
    if (pos_ >= block_data_.size()) {
        LoadBlock(block_idx_ + 1);
    } else {
        ParseCurrentEntry();
    }
}

std::string_view SSTableReader::Iterator::Key() const {
    return key_view_;
}
std::string_view SSTableReader::Iterator::Value() const {
    return value_view_;
}
bool SSTableReader::Iterator::IsTombstone() const {
    return is_tombstone_;
}
std::uint64_t SSTableReader::Iterator::Sequence() const {
    return seq_;
}

}  // namespace knot::storage
