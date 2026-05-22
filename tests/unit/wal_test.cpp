// Knot — WAL unit tests.

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <random>
#include <stdexcept>
#include <string>

#include <gtest/gtest.h>
#include <knot/storage/wal.h>

namespace fs = std::filesystem;
using namespace knot::storage;

// =====================================================================
// Test fixture — creates a temp dir per test, cleans up on teardown.
// =====================================================================
class WalTest : public ::testing::Test {
protected:
    fs::path dir;

    void SetUp() override {
        std::random_device rd;
        dir = fs::temp_directory_path() / ("knot_wal_test_" + std::to_string(rd()));
        fs::create_directories(dir);
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(dir, ec);
    }

    int CountSegments() const {
        int n = 0;
        for (const auto& entry : fs::directory_iterator(dir)) {
            if (entry.path().extension() == ".wal") {
                ++n;
            }
        }
        return n;
    }
};

// ---------------------------------------------------------------------
// Basic roundtrip
// ---------------------------------------------------------------------
TEST_F(WalTest, RoundtripSingleRecord) {
    {
        auto w = WalWriter::Open(dir);
        EXPECT_EQ(w->AppendSync("hello world"), 1U);
    }
    auto r = WalReader::Open(dir);
    auto rec = r->Next();
    ASSERT_TRUE(rec.has_value());
    EXPECT_EQ(rec->sequence, 1U);
    EXPECT_EQ(rec->payload, "hello world");
    EXPECT_FALSE(r->Next().has_value());
}

TEST_F(WalTest, RoundtripManyRecords) {
    constexpr int kN = 1000;
    {
        auto w = WalWriter::Open(dir);
        for (int i = 0; i < kN; ++i) {
            EXPECT_EQ(w->Append("record-" + std::to_string(i)), static_cast<std::uint64_t>(i + 1));
        }
        w->Sync();
    }
    auto r = WalReader::Open(dir);
    for (int i = 0; i < kN; ++i) {
        auto rec = r->Next();
        ASSERT_TRUE(rec.has_value()) << "missing record " << i;
        EXPECT_EQ(rec->sequence, static_cast<std::uint64_t>(i + 1));
        EXPECT_EQ(rec->payload, "record-" + std::to_string(i));
    }
    EXPECT_FALSE(r->Next().has_value());
}

TEST_F(WalTest, EmptyPayload) {
    {
        auto w = WalWriter::Open(dir);
        w->AppendSync("");
    }
    auto r = WalReader::Open(dir);
    auto rec = r->Next();
    ASSERT_TRUE(rec.has_value());
    EXPECT_EQ(rec->payload, "");
    EXPECT_FALSE(r->Next().has_value());
}

// ---------------------------------------------------------------------
// Segment rotation
// ---------------------------------------------------------------------
TEST_F(WalTest, SegmentRotation) {
    constexpr std::size_t kSmallSegment = 256;
    {
        auto w = WalWriter::Open(dir, kSmallSegment);
        for (int i = 0; i < 50; ++i) {
            w->Append(std::string(50, 'a'));  // 50-byte payload + 16-byte header
        }
        w->Sync();
    }
    EXPECT_GT(CountSegments(), 1) << "should have rotated to multiple segments";

    auto r = WalReader::Open(dir);
    int n = 0;
    while (auto rec = r->Next()) {
        EXPECT_EQ(rec->payload, std::string(50, 'a'));
        EXPECT_EQ(rec->sequence, static_cast<std::uint64_t>(n + 1));
        ++n;
    }
    EXPECT_EQ(n, 50);
}

// ---------------------------------------------------------------------
// Reopen — each writer instance starts a fresh segment
// ---------------------------------------------------------------------
TEST_F(WalTest, ReopenCreatesNewSegment) {
    {
        auto w = WalWriter::Open(dir);
        w->AppendSync("first");
    }
    {
        auto w = WalWriter::Open(dir);
        w->AppendSync("second");
    }
    EXPECT_EQ(CountSegments(), 2);

    auto r = WalReader::Open(dir);
    auto a = r->Next();
    ASSERT_TRUE(a);
    EXPECT_EQ(a->payload, "first");
    auto b = r->Next();
    ASSERT_TRUE(b);
    EXPECT_EQ(b->payload, "second");
    EXPECT_FALSE(r->Next().has_value());
}

// ---------------------------------------------------------------------
// Empty directory — reader yields nothing, no error
// ---------------------------------------------------------------------
TEST_F(WalTest, EmptyDirectory) {
    auto r = WalReader::Open(dir);
    EXPECT_FALSE(r->Next().has_value());
}

// ---------------------------------------------------------------------
// Corruption — flipping a byte in the payload trips CRC
// ---------------------------------------------------------------------
TEST_F(WalTest, CrcMismatchDetected) {
    {
        auto w = WalWriter::Open(dir);
        w->AppendSync("abcdefgh");
    }
    auto seg = dir / "000001.wal";
    std::fstream f(seg, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(f.is_open());
    f.seekp(18);  // somewhere inside the payload (after 16-byte header)
    f.put('Z');
    f.close();

    auto r = WalReader::Open(dir);
    EXPECT_THROW(r->Next(), std::runtime_error);
}

// ---------------------------------------------------------------------
// Truncation — last N bytes cut off => detected
// ---------------------------------------------------------------------
TEST_F(WalTest, TruncatedRecordDetected) {
    {
        auto w = WalWriter::Open(dir);
        w->AppendSync("hello world");
    }
    auto seg = dir / "000001.wal";
    auto size = fs::file_size(seg);
    fs::resize_file(seg, size - 3);

    auto r = WalReader::Open(dir);
    EXPECT_THROW(r->Next(), std::runtime_error);
}

// ---------------------------------------------------------------------
// Bytes survive across writer destruction (the durability claim)
// ---------------------------------------------------------------------
TEST_F(WalTest, RecordsSurviveProcessClose) {
    // First "process": write 3 records then sync.
    {
        auto w = WalWriter::Open(dir);
        w->Append("alpha");
        w->Append("beta");
        w->Append("gamma");
        w->Sync();
        // Writer goes out of scope → ::close(fd) runs in dtor.
    }
    // Second "process": open fresh reader; should see all three.
    auto r = WalReader::Open(dir);
    auto a = r->Next();
    ASSERT_TRUE(a);
    EXPECT_EQ(a->payload, "alpha");
    auto b = r->Next();
    ASSERT_TRUE(b);
    EXPECT_EQ(b->payload, "beta");
    auto c = r->Next();
    ASSERT_TRUE(c);
    EXPECT_EQ(c->payload, "gamma");
    EXPECT_FALSE(r->Next().has_value());
}
