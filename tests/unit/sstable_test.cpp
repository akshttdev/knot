// Knot — SSTable unit tests.

#include <cstdio>
#include <filesystem>
#include <random>
#include <stdexcept>
#include <string>

#include <fcntl.h>
#include <gtest/gtest.h>
#include <knot/storage/sstable.h>
#include <unistd.h>

namespace ks = knot::storage;
namespace fs = std::filesystem;
using Result = ks::SSTableReader::LookupResult;

class SSTableTest : public ::testing::Test {
protected:
    fs::path file_path;

    void SetUp() override {
        std::random_device rd;
        file_path = fs::temp_directory_path() / ("knot_sst_test_" + std::to_string(rd()) + ".sst");
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove(file_path, ec);
    }
};

// ---------------------------------------------------------------------
// Basics
// ---------------------------------------------------------------------
TEST_F(SSTableTest, EmptyFile) {
    {
        auto w = ks::SSTableWriter::Open(file_path);
        w->Finish();
    }
    auto r = ks::SSTableReader::Open(file_path);
    EXPECT_EQ(r->BlockCount(), 0U);
    std::string v;
    EXPECT_EQ(r->Get("anything", &v), Result::kNotFound);
    auto it = r->NewIterator();
    EXPECT_FALSE(it->Valid());
}

TEST_F(SSTableTest, SingleEntry) {
    {
        auto w = ks::SSTableWriter::Open(file_path);
        w->Add(1, "key", "value");
        w->Finish();
    }
    auto r = ks::SSTableReader::Open(file_path);
    EXPECT_EQ(r->BlockCount(), 1U);
    std::string v;
    EXPECT_EQ(r->Get("key", &v), Result::kFound);
    EXPECT_EQ(v, "value");
    EXPECT_EQ(r->Get("missing", &v), Result::kNotFound);
}

TEST_F(SSTableTest, ManyEntriesRoundtrip) {
    constexpr int kN = 1000;
    {
        auto w = ks::SSTableWriter::Open(file_path);
        for (int i = 0; i < kN; ++i) {
            char buf[32];
            std::snprintf(buf, sizeof(buf), "key-%06d", i);
            w->Add(static_cast<std::uint64_t>(i + 1), buf, "val-" + std::to_string(i));
        }
        w->Finish();
    }
    auto r = ks::SSTableReader::Open(file_path);
    std::string v;
    for (int i = 0; i < kN; ++i) {
        char buf[32];
        std::snprintf(buf, sizeof(buf), "key-%06d", i);
        ASSERT_EQ(r->Get(buf, &v), Result::kFound) << "miss at " << i;
        EXPECT_EQ(v, "val-" + std::to_string(i));
    }
}

// ---------------------------------------------------------------------
// Tombstones
// ---------------------------------------------------------------------
TEST_F(SSTableTest, Tombstone) {
    {
        auto w = ks::SSTableWriter::Open(file_path);
        w->Add(1, "alpha", "v1");
        w->AddTombstone(2, "bravo");
        w->Add(3, "charlie", "v3");
        w->Finish();
    }
    auto r = ks::SSTableReader::Open(file_path);
    std::string v;
    EXPECT_EQ(r->Get("alpha", &v), Result::kFound);
    EXPECT_EQ(v, "v1");
    EXPECT_EQ(r->Get("bravo", &v), Result::kDeleted);
    EXPECT_EQ(r->Get("charlie", &v), Result::kFound);
    EXPECT_EQ(v, "v3");
}

// ---------------------------------------------------------------------
// Multiple blocks
// ---------------------------------------------------------------------
TEST_F(SSTableTest, MultipleBlocks) {
    {
        // Tiny block size to force rotation
        auto w = ks::SSTableWriter::Open(file_path, /*target_block_size=*/128);
        for (int i = 0; i < 50; ++i) {
            char key[32];
            std::snprintf(key, sizeof(key), "key-%04d", i);
            w->Add(static_cast<std::uint64_t>(i + 1), key, "value-here");
        }
        w->Finish();
    }
    auto r = ks::SSTableReader::Open(file_path);
    EXPECT_GT(r->BlockCount(), 1U) << "should have multiple blocks";

    std::string v;
    for (int i = 0; i < 50; ++i) {
        char key[32];
        std::snprintf(key, sizeof(key), "key-%04d", i);
        ASSERT_EQ(r->Get(key, &v), Result::kFound);
        EXPECT_EQ(v, "value-here");
    }
}

// ---------------------------------------------------------------------
// Negative tests on writer
// ---------------------------------------------------------------------
TEST_F(SSTableTest, OutOfOrderKeysThrows) {
    auto w = ks::SSTableWriter::Open(file_path);
    w->Add(1, "b", "v");
    EXPECT_THROW(w->Add(2, "a", "v"), std::runtime_error);
}

TEST_F(SSTableTest, DuplicateKeysThrows) {
    auto w = ks::SSTableWriter::Open(file_path);
    w->Add(1, "a", "v");
    EXPECT_THROW(w->Add(2, "a", "v"), std::runtime_error);
}

TEST_F(SSTableTest, EmptyKeyThrows) {
    auto w = ks::SSTableWriter::Open(file_path);
    EXPECT_THROW(w->Add(1, "", "v"), std::runtime_error);
}

TEST_F(SSTableTest, AddAfterFinishThrows) {
    auto w = ks::SSTableWriter::Open(file_path);
    w->Add(1, "a", "v");
    w->Finish();
    EXPECT_THROW(w->Add(2, "b", "v"), std::runtime_error);
}

// ---------------------------------------------------------------------
// Iterator
// ---------------------------------------------------------------------
TEST_F(SSTableTest, IteratorYieldsSorted) {
    {
        auto w = ks::SSTableWriter::Open(file_path);
        w->Add(1, "alpha", "1");
        w->Add(2, "bravo", "2");
        w->Add(3, "charlie", "3");
        w->Finish();
    }
    auto r = ks::SSTableReader::Open(file_path);
    auto it = r->NewIterator();

    ASSERT_TRUE(it->Valid());
    EXPECT_EQ(it->Key(), "alpha");
    EXPECT_EQ(it->Value(), "1");
    EXPECT_EQ(it->Sequence(), 1U);
    EXPECT_FALSE(it->IsTombstone());

    it->Next();
    ASSERT_TRUE(it->Valid());
    EXPECT_EQ(it->Key(), "bravo");

    it->Next();
    ASSERT_TRUE(it->Valid());
    EXPECT_EQ(it->Key(), "charlie");

    it->Next();
    EXPECT_FALSE(it->Valid());
}

TEST_F(SSTableTest, IteratorIncludesTombstones) {
    {
        auto w = ks::SSTableWriter::Open(file_path);
        w->Add(1, "a", "v");
        w->AddTombstone(2, "b");
        w->Add(3, "c", "v");
        w->Finish();
    }
    auto r = ks::SSTableReader::Open(file_path);
    auto it = r->NewIterator();

    it->Next();  // skip 'a'
    ASSERT_TRUE(it->Valid());
    EXPECT_EQ(it->Key(), "b");
    EXPECT_TRUE(it->IsTombstone());
    EXPECT_EQ(it->Value(), "");
}

TEST_F(SSTableTest, IteratorCrossesBlocks) {
    {
        auto w = ks::SSTableWriter::Open(file_path, /*target_block_size=*/64);
        for (int i = 0; i < 20; ++i) {
            char key[16];
            std::snprintf(key, sizeof(key), "k%03d", i);
            w->Add(static_cast<std::uint64_t>(i + 1), key, "v");
        }
        w->Finish();
    }
    auto r = ks::SSTableReader::Open(file_path);
    EXPECT_GT(r->BlockCount(), 1U);

    auto it = r->NewIterator();
    int seen = 0;
    while (it->Valid()) {
        char expected[16];
        std::snprintf(expected, sizeof(expected), "k%03d", seen);
        EXPECT_EQ(it->Key(), expected);
        it->Next();
        ++seen;
    }
    EXPECT_EQ(seen, 20);
}

// ---------------------------------------------------------------------
// Get edge cases
// ---------------------------------------------------------------------
TEST_F(SSTableTest, GetReturnsNotFoundForGaps) {
    {
        auto w = ks::SSTableWriter::Open(file_path);
        w->Add(1, "key1", "v");
        w->Add(2, "key3", "v");
        w->Add(3, "key5", "v");
        w->Finish();
    }
    auto r = ks::SSTableReader::Open(file_path);
    std::string v;
    EXPECT_EQ(r->Get("key0", &v), Result::kNotFound);
    EXPECT_EQ(r->Get("key2", &v), Result::kNotFound);
    EXPECT_EQ(r->Get("key4", &v), Result::kNotFound);
    EXPECT_EQ(r->Get("key6", &v), Result::kNotFound);
    EXPECT_EQ(r->Get("key1", &v), Result::kFound);
    EXPECT_EQ(r->Get("key5", &v), Result::kFound);
}

// ---------------------------------------------------------------------
// Corruption detection
// ---------------------------------------------------------------------
TEST_F(SSTableTest, BadMagicDetected) {
    {
        auto w = ks::SSTableWriter::Open(file_path);
        w->Add(1, "k", "v");
        w->Finish();
    }
    auto size = fs::file_size(file_path);
    int fd = ::open(file_path.c_str(), O_WRONLY);
    ASSERT_GE(fd, 0);
    const char garbage[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    ::pwrite(fd, garbage, 8, static_cast<off_t>(size - 24));
    ::close(fd);

    EXPECT_THROW(ks::SSTableReader::Open(file_path), std::runtime_error);
}

TEST_F(SSTableTest, DataBlockCorruptionDetected) {
    {
        auto w = ks::SSTableWriter::Open(file_path);
        w->Add(1, "abcdefgh", "ijklmnop");
        w->Finish();
    }
    // Corrupt a byte in the first data block (at offset 20 — inside payload)
    int fd = ::open(file_path.c_str(), O_WRONLY);
    ASSERT_GE(fd, 0);
    char x = 'Z';
    ::pwrite(fd, &x, 1, 20);
    ::close(fd);

    auto r = ks::SSTableReader::Open(file_path);
    std::string v;
    EXPECT_THROW(r->Get("abcdefgh", &v), std::runtime_error);
}
