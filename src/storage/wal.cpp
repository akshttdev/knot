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
#include <knot/storage/wal.h>
#include <unistd.h>

namespace knot::storage {

namespace {

constexpr std::size_t kHeaderSize = 16;  // CRC(4) + Seq(8) + Len(4)

std::string SegmentFileName(std::uint64_t num) {
    char buf[16];
    std::snprintf(buf, sizeof(buf), "%06llu.wal", static_cast<unsigned long long>(num));
    return buf;
}

std::uint64_t HighestSegmentNumber(const std::filesystem::path& dir) {
    std::uint64_t highest = 0;
    if (!std::filesystem::exists(dir)) {
        return 0;
    }
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        if (entry.path().extension() != ".wal") {
            continue;
        }
        try {
            auto num = std::stoull(entry.path().stem().string());
            highest = std::max(highest, static_cast<std::uint64_t>(num));
        } catch (...) {
            // not a numeric stem — ignore
        }
    }
    return highest;
}

[[noreturn]] void ThrowErrno(const std::string& what) {
    throw std::system_error(errno, std::system_category(), what);
}

// Read exactly `n` bytes into `buf` from `fd`. Returns:
//   n              — full read
//   0              — clean EOF before any bytes
//   anything else  — short read (caller decides: EOF vs truncated)
ssize_t ReadFull(int fd, void* buf, std::size_t n) {
    auto* p = static_cast<std::uint8_t*>(buf);
    std::size_t got = 0;
    while (got < n) {
        ssize_t r = ::read(fd, p + got, n - got);
        if (r < 0) {
            if (errno == EINTR) {
                continue;
            }
            ThrowErrno("read");
        }
        if (r == 0) {
            break;
        }
        got += static_cast<std::size_t>(r);
    }
    return static_cast<ssize_t>(got);
}

}  // namespace

// =====================================================================
// WalWriter — implementation
// =====================================================================

std::unique_ptr<WalWriter> WalWriter::Open(const std::filesystem::path& dir,
                                           std::size_t segment_size_bytes) {
    std::filesystem::create_directories(dir);
    std::unique_ptr<WalWriter> writer(new WalWriter(dir, segment_size_bytes));
    auto highest = HighestSegmentNumber(dir);
    writer->OpenSegment(highest + 1);
    return writer;
}

WalWriter::WalWriter(std::filesystem::path dir, std::size_t segment_size)
    : dir_(std::move(dir)), segment_size_(segment_size) {}

WalWriter::~WalWriter() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

void WalWriter::OpenSegment(std::uint64_t segment_num) {
    auto path = dir_ / SegmentFileName(segment_num);
    int new_fd = ::open(path.c_str(), O_WRONLY | O_APPEND | O_CREAT, 0644);
    if (new_fd < 0) {
        ThrowErrno("WalWriter::OpenSegment(" + path.string() + ")");
    }
    if (fd_ >= 0) {
        ::close(fd_);
    }
    fd_ = new_fd;
    current_segment_num_ = segment_num;
    current_segment_bytes_ = 0;
}

void WalWriter::RotateSegment() {
    OpenSegment(current_segment_num_ + 1);
}

std::uint64_t WalWriter::Append(std::string_view payload) {
    // Record on disk: [CRC32(4)] [Seq(8)] [Len(4)] [Payload(N)]
    // CRC covers [Seq | Len | Payload].

    std::uint64_t seq = next_seq_;
    auto len = static_cast<std::uint32_t>(payload.size());
    std::size_t record_size = kHeaderSize + payload.size();

    // Rotate if this record won't fit and the segment isn't empty.
    if (current_segment_bytes_ > 0 && current_segment_bytes_ + record_size > segment_size_) {
        RotateSegment();
    }

    // Encode seq and len into byte buffers so we can CRC them.
    std::uint8_t seq_bytes[8];
    std::uint8_t len_bytes[4];
    std::memcpy(seq_bytes, &seq, sizeof(seq));
    std::memcpy(len_bytes, &len, sizeof(len));

    std::uint32_t crc = 0;
    crc = common::Crc32(crc, seq_bytes, sizeof(seq_bytes));
    crc = common::Crc32(crc, len_bytes, sizeof(len_bytes));
    crc = common::Crc32(crc, payload.data(), payload.size());

    // Build header [CRC | Seq | Len].
    std::uint8_t header[kHeaderSize];
    std::memcpy(header, &crc, sizeof(crc));
    std::memcpy(header + 4, seq_bytes, sizeof(seq_bytes));
    std::memcpy(header + 12, len_bytes, sizeof(len_bytes));

    // Write header.
    ssize_t n = ::write(fd_, header, kHeaderSize);
    if (n != static_cast<ssize_t>(kHeaderSize)) {
        ThrowErrno("WalWriter::Append (header)");
    }

    // Write payload.
    if (!payload.empty()) {
        n = ::write(fd_, payload.data(), payload.size());
        if (n != static_cast<ssize_t>(payload.size())) {
            ThrowErrno("WalWriter::Append (payload)");
        }
    }

    current_segment_bytes_ += record_size;
    ++next_seq_;
    return seq;
}

void WalWriter::Sync() {
    if (fd_ < 0) {
        return;
    }
#ifdef __APPLE__
    if (::fcntl(fd_, F_FULLFSYNC) < 0) {
        ThrowErrno("WalWriter::Sync (F_FULLFSYNC)");
    }
#else
    if (::fsync(fd_) < 0) {
        ThrowErrno("WalWriter::Sync (fsync)");
    }
#endif
}

std::uint64_t WalWriter::AppendSync(std::string_view payload) {
    auto seq = Append(payload);
    Sync();
    return seq;
}

std::uint64_t WalWriter::LastSequence() const {
    return next_seq_ - 1;
}

// =====================================================================
// WalReader — implementation
// =====================================================================

std::unique_ptr<WalReader> WalReader::Open(const std::filesystem::path& dir) {
    std::vector<std::filesystem::path> segments;
    if (std::filesystem::exists(dir)) {
        for (const auto& entry : std::filesystem::directory_iterator(dir)) {
            if (entry.path().extension() == ".wal") {
                segments.push_back(entry.path());
            }
        }
        std::sort(segments.begin(), segments.end());
    }
    return std::unique_ptr<WalReader>(new WalReader(std::move(segments)));
}

WalReader::WalReader(std::vector<std::filesystem::path> segments)
    : segments_(std::move(segments)) {}

WalReader::~WalReader() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

bool WalReader::OpenNextSegment() {
    CloseCurrentSegment();
    if (current_segment_idx_ >= segments_.size()) {
        return false;
    }
    const auto& path = segments_[current_segment_idx_];
    int new_fd = ::open(path.c_str(), O_RDONLY);
    if (new_fd < 0) {
        ThrowErrno("WalReader::OpenNextSegment(" + path.string() + ")");
    }
    fd_ = new_fd;
    ++current_segment_idx_;
    return true;
}

void WalReader::CloseCurrentSegment() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

std::optional<WalReader::Record> WalReader::Next() {
    while (true) {
        // Ensure we have a segment open.
        if (fd_ < 0) {
            if (!OpenNextSegment()) {
                return std::nullopt;
            }
        }

        // Read header.
        std::uint8_t header[kHeaderSize];
        ssize_t got = ReadFull(fd_, header, kHeaderSize);
        if (got == 0) {
            // Clean EOF — try next segment.
            CloseCurrentSegment();
            continue;
        }
        if (got < static_cast<ssize_t>(kHeaderSize)) {
            throw std::runtime_error("WAL: truncated header");
        }

        // Decode header.
        std::uint32_t stored_crc = 0;
        std::uint64_t seq = 0;
        std::uint32_t len = 0;
        std::memcpy(&stored_crc, header, sizeof(stored_crc));
        std::memcpy(&seq, header + 4, sizeof(seq));
        std::memcpy(&len, header + 12, sizeof(len));

        // Read payload.
        std::string payload(len, '\0');
        if (len > 0) {
            got = ReadFull(fd_, payload.data(), len);
            if (got < static_cast<ssize_t>(len)) {
                throw std::runtime_error("WAL: truncated payload");
            }
        }

        // Verify CRC over [Seq | Len | Payload].
        std::uint32_t computed = 0;
        computed = common::Crc32(computed, header + 4, 12);  // seq + len bytes
        computed = common::Crc32(computed, payload.data(), payload.size());
        if (computed != stored_crc) {
            throw std::runtime_error("WAL: CRC mismatch");
        }

        return Record{seq, std::move(payload)};
    }
}

}  // namespace knot::storage
