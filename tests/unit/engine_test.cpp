// Knot — StorageEngine unit tests.

#include <cstdio>
#include <filesystem>
#include <random>
#include <string>

#include <gtest/gtest.h>
#include <knot/storage/engine.h>

namespace ks = knot::storage;
namespace fs = std::filesystem;
using Result = ks::StorageEngine::GetResult;

class EngineTest : public ::testing::Test {
protected:
    fs::path data_dir;

    void SetUp() override {
        std::random_device rd;
        data_dir = fs::temp_directory_path() / ("knot_engine_test_" + std::to_string(rd()));
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(data_dir, ec);
    }

    ks::StorageEngine::Options Opts(std::size_t mt_threshold = 1024 * 1024) {
        ks::StorageEngine::Options o;
        o.data_dir = data_dir;
        o.memtable_size_threshold = mt_threshold;
        o.sstable_block_size = 256;  // small to exercise multi-block paths
        return o;
    }
};

// ---------------------------------------------------------------------
// Basics
// ---------------------------------------------------------------------
TEST_F(EngineTest, EmptyEngine) {
    auto e = ks::StorageEngine::Open(Opts());
    std::string v;
    EXPECT_EQ(e->Get("nope", &v), Result::kNotFound);
    EXPECT_EQ(e->SSTableCount(), 0U);
    EXPECT_EQ(e->SequenceNumber(), 0U);
}

TEST_F(EngineTest, PutGet) {
    auto e = ks::StorageEngine::Open(Opts());
    e->Put("alpha", "1");
    e->Put("bravo", "2");

    std::string v;
    EXPECT_EQ(e->Get("alpha", &v), Result::kFound);
    EXPECT_EQ(v, "1");
    EXPECT_EQ(e->Get("bravo", &v), Result::kFound);
    EXPECT_EQ(v, "2");
    EXPECT_EQ(e->Get("missing", &v), Result::kNotFound);
}

TEST_F(EngineTest, PutOverwrites) {
    auto e = ks::StorageEngine::Open(Opts());
    e->Put("k", "old");
    e->Put("k", "new");

    std::string v;
    EXPECT_EQ(e->Get("k", &v), Result::kFound);
    EXPECT_EQ(v, "new");
}

TEST_F(EngineTest, DeleteHidesValue) {
    auto e = ks::StorageEngine::Open(Opts());
    e->Put("k", "v");
    e->Delete("k");

    std::string v;
    EXPECT_EQ(e->Get("k", &v), Result::kNotFound);
}

TEST_F(EngineTest, PutAfterDelete) {
    auto e = ks::StorageEngine::Open(Opts());
    e->Put("k", "v1");
    e->Delete("k");
    e->Put("k", "v2");

    std::string v;
    EXPECT_EQ(e->Get("k", &v), Result::kFound);
    EXPECT_EQ(v, "v2");
}

// ---------------------------------------------------------------------
// Flush + SSTables
// ---------------------------------------------------------------------
TEST_F(EngineTest, ManualFlushCreatesSSTable) {
    auto e = ks::StorageEngine::Open(Opts());
    e->Put("a", "1");
    e->Put("b", "2");

    EXPECT_EQ(e->SSTableCount(), 0U);
    e->Flush();
    EXPECT_EQ(e->SSTableCount(), 1U);

    std::string v;
    EXPECT_EQ(e->Get("a", &v), Result::kFound);
    EXPECT_EQ(v, "1");
    EXPECT_EQ(e->Get("b", &v), Result::kFound);
    EXPECT_EQ(v, "2");
}

TEST_F(EngineTest, AutomaticFlushOnThreshold) {
    // Very small threshold so the engine flushes on its own.
    auto e = ks::StorageEngine::Open(Opts(/*mt_threshold=*/256));
    for (int i = 0; i < 100; ++i) {
        e->Put("key-" + std::to_string(i), std::string(50, 'x'));
    }
    EXPECT_GT(e->SSTableCount(), 0U);

    std::string v;
    for (int i = 0; i < 100; ++i) {
        ASSERT_EQ(e->Get("key-" + std::to_string(i), &v), Result::kFound);
        EXPECT_EQ(v, std::string(50, 'x'));
    }
}

TEST_F(EngineTest, ReadsFromMultipleLayers) {
    auto e = ks::StorageEngine::Open(Opts());
    // First flush — these go into SSTable #1
    e->Put("a", "v1");
    e->Put("b", "v1");
    e->Flush();

    // Second flush — overwrites "a", new "c"
    e->Put("a", "v2");
    e->Put("c", "v2");
    e->Flush();

    // In-MemTable — overwrites "b"
    e->Put("b", "v3");

    EXPECT_EQ(e->SSTableCount(), 2U);

    std::string v;
    EXPECT_EQ(e->Get("a", &v), Result::kFound);
    EXPECT_EQ(v, "v2") << "newer SSTable should shadow older";
    EXPECT_EQ(e->Get("b", &v), Result::kFound);
    EXPECT_EQ(v, "v3") << "MemTable should shadow SSTable";
    EXPECT_EQ(e->Get("c", &v), Result::kFound);
    EXPECT_EQ(v, "v2");
}

TEST_F(EngineTest, TombstoneShadowsOlderSSTable) {
    auto e = ks::StorageEngine::Open(Opts());
    e->Put("k", "v");
    e->Flush();

    e->Delete("k");
    EXPECT_GT(e->SSTableCount(), 0U);

    std::string v;
    EXPECT_EQ(e->Get("k", &v), Result::kNotFound);
}

// ---------------------------------------------------------------------
// Recovery
// ---------------------------------------------------------------------
TEST_F(EngineTest, RecoverFromWalOnly) {
    {
        auto e = ks::StorageEngine::Open(Opts());
        e->Put("alpha", "1");
        e->Put("bravo", "2");
        e->Delete("alpha");
        // No explicit flush — data is in WAL only.
    }
    // Reopen
    auto e = ks::StorageEngine::Open(Opts());
    std::string v;
    EXPECT_EQ(e->Get("alpha", &v), Result::kNotFound);  // delete preserved
    EXPECT_EQ(e->Get("bravo", &v), Result::kFound);
    EXPECT_EQ(v, "2");
}

TEST_F(EngineTest, RecoverFromSSTablesAndWal) {
    {
        auto e = ks::StorageEngine::Open(Opts());
        e->Put("a", "from-sst");
        e->Flush();
        e->Put("b", "from-wal");
        // 'b' is in WAL but not in any SSTable yet.
    }
    auto e = ks::StorageEngine::Open(Opts());
    EXPECT_EQ(e->SSTableCount(), 1U);
    std::string v;
    EXPECT_EQ(e->Get("a", &v), Result::kFound);
    EXPECT_EQ(v, "from-sst");
    EXPECT_EQ(e->Get("b", &v), Result::kFound);
    EXPECT_EQ(v, "from-wal");
}

TEST_F(EngineTest, RecoverPreservesSequenceContinuity) {
    {
        auto e = ks::StorageEngine::Open(Opts());
        for (int i = 0; i < 5; ++i) {
            e->Put("k" + std::to_string(i), "v");
        }
        EXPECT_EQ(e->SequenceNumber(), 5U);
    }
    auto e = ks::StorageEngine::Open(Opts());
    EXPECT_EQ(e->SequenceNumber(), 5U);
    e->Put("new", "v");
    EXPECT_EQ(e->SequenceNumber(), 6U);
}

TEST_F(EngineTest, RecoverFromMultipleFlushes) {
    {
        auto e = ks::StorageEngine::Open(Opts());
        e->Put("a", "1");
        e->Flush();
        e->Put("b", "1");
        e->Flush();
        e->Put("c", "1");
        e->Flush();
    }
    auto e = ks::StorageEngine::Open(Opts());
    EXPECT_EQ(e->SSTableCount(), 3U);
    std::string v;
    EXPECT_EQ(e->Get("a", &v), Result::kFound);
    EXPECT_EQ(e->Get("b", &v), Result::kFound);
    EXPECT_EQ(e->Get("c", &v), Result::kFound);
}

// ---------------------------------------------------------------------
// Snapshot API
// ---------------------------------------------------------------------

TEST_F(EngineTest, ForEachLiveSurfacesNewestValues) {
    auto e = ks::StorageEngine::Open({.data_dir = data_dir / "fea"});
    e->Put("k", "v1");
    e->Put("k", "v2");
    std::vector<std::pair<std::string, std::string>> seen;
    e->ForEachLive([&](std::string_view k, std::string_view v) {
        seen.emplace_back(std::string(k), std::string(v));
    });
    ASSERT_EQ(seen.size(), 1U);
    EXPECT_EQ(seen[0].first, "k");
    EXPECT_EQ(seen[0].second, "v2");
}

TEST_F(EngineTest, SnapshotApplySnapshotRoundtrip) {
    auto src = ks::StorageEngine::Open({.data_dir = data_dir / "src"});
    src->Put("a", "1");
    src->Put("b", "2");
    src->Put("c", "3");
    src->Delete("b");
    const auto snap = src->Snapshot();

    auto dst = ks::StorageEngine::Open({.data_dir = data_dir / "dst"});
    dst->ApplySnapshot(snap);
    std::string v;
    EXPECT_EQ(dst->Get("a", &v), Result::kFound);
    EXPECT_EQ(v, "1");
    EXPECT_EQ(dst->Get("c", &v), Result::kFound);
    EXPECT_EQ(v, "3");
    EXPECT_EQ(dst->Get("b", &v), Result::kNotFound);
}

TEST_F(EngineTest, ApplyEmptySnapshotClearsExistingData) {
    auto e = ks::StorageEngine::Open({.data_dir = data_dir / "clr"});
    e->Put("k", "v");
    e->ApplySnapshot("");
    std::string v;
    EXPECT_EQ(e->Get("k", &v), Result::kNotFound);
}

// ---------------------------------------------------------------------
// Volume
// ---------------------------------------------------------------------
TEST_F(EngineTest, ManyKeysRoundtrip) {
    constexpr int kN = 1000;
    auto e = ks::StorageEngine::Open(Opts(/*mt_threshold=*/4096));
    for (int i = 0; i < kN; ++i) {
        char buf[16];
        std::snprintf(buf, sizeof(buf), "k%06d", i);
        e->Put(buf, "v");
    }
    std::string v;
    for (int i = 0; i < kN; ++i) {
        char buf[16];
        std::snprintf(buf, sizeof(buf), "k%06d", i);
        ASSERT_EQ(e->Get(buf, &v), Result::kFound) << "miss at " << i;
        EXPECT_EQ(v, "v");
    }
}
