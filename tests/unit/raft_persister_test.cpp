// Knot — Persister tests.

#include <filesystem>
#include <optional>
#include <random>
#include <string>

#include <gtest/gtest.h>
#include <knot/raft/persister.h>

using namespace knot::raft;

TEST(MemoryPersisterTest, RoundtripPreservesStateAndEntries) {
    auto p = MakeMemoryPersister();
    p->SaveState(7U, std::string("node-x"));

    LogEntry e1{.term = 1, .index = 1, .type = EntryType::kCommand, .payload = "a"};
    LogEntry e2{.term = 2, .index = 2, .type = EntryType::kCommand, .payload = "b"};
    p->AppendLog(e1);
    p->AppendLog(e2);

    PersistentState s;
    std::vector<LogEntry> es;
    ASSERT_TRUE(p->Load(&s, &es));
    EXPECT_EQ(s.current_term, 7U);
    ASSERT_TRUE(s.voted_for.has_value());
    EXPECT_EQ(*s.voted_for, "node-x");
    ASSERT_EQ(es.size(), 2U);
    EXPECT_EQ(es[0].payload, "a");
    EXPECT_EQ(es[1].payload, "b");
}

TEST(MemoryPersisterTest, LoadEmptyReturnsFalse) {
    auto p = MakeMemoryPersister();
    PersistentState s;
    std::vector<LogEntry> es;
    EXPECT_FALSE(p->Load(&s, &es));
}

TEST(MemoryPersisterTest, TruncateDropsSuffixOnly) {
    auto p = MakeMemoryPersister();
    for (LogIdx i = 1; i <= 5; ++i) {
        LogEntry e{.term = 1,
                   .index = i,
                   .type = EntryType::kCommand,
                   .payload = std::string("v") + std::to_string(i)};
        p->AppendLog(e);
    }
    p->TruncateLog(3);

    PersistentState s;
    std::vector<LogEntry> es;
    ASSERT_TRUE(p->Load(&s, &es));
    ASSERT_EQ(es.size(), 2U);
    EXPECT_EQ(es[0].index, 1U);
    EXPECT_EQ(es[1].index, 2U);
}

namespace {
std::filesystem::path UniqueTempDir(const std::string& prefix) {
    std::random_device rd;
    return std::filesystem::temp_directory_path() / (prefix + "_" + std::to_string(rd()));
}
}  // namespace

TEST(FilePersisterTest, RoundtripStatePersistsAcrossInstances) {
    const auto dir = UniqueTempDir("knot_filepers");

    {
        auto p = MakeFilePersister(dir);
        ASSERT_NE(p, nullptr);
        p->SaveState(42U, std::string("leader-x"));
    }  // first persister destroyed; data on disk

    {
        auto p = MakeFilePersister(dir);
        ASSERT_NE(p, nullptr);
        PersistentState s;
        std::vector<LogEntry> es;
        ASSERT_TRUE(p->Load(&s, &es));
        EXPECT_EQ(s.current_term, 42U);
        ASSERT_TRUE(s.voted_for.has_value());
        EXPECT_EQ(*s.voted_for, "leader-x");
        EXPECT_TRUE(es.empty());  // log methods come in Task C
    }

    std::filesystem::remove_all(dir);
}

TEST(FilePersisterTest, LoadEmptyDirReturnsFalse) {
    const auto dir = UniqueTempDir("knot_filepers_empty");
    std::filesystem::create_directories(dir);

    auto p = MakeFilePersister(dir);
    ASSERT_NE(p, nullptr);

    PersistentState s;
    std::vector<LogEntry> es;
    EXPECT_FALSE(p->Load(&s, &es));

    std::filesystem::remove_all(dir);
}

TEST(FilePersisterTest, LogRoundtripPersistsAcrossInstances) {
    const auto dir = UniqueTempDir("knot_filepers_log");

    LogEntry e1{.term = 1, .index = 1, .type = EntryType::kCommand, .payload = "a"};
    LogEntry e2{.term = 2, .index = 2, .type = EntryType::kCommand, .payload = "bb"};
    LogEntry e3{.term = 2, .index = 3, .type = EntryType::kCommand, .payload = "ccc"};

    {
        auto p = MakeFilePersister(dir);
        p->SaveState(2U, std::nullopt);
        p->AppendLog(e1);
        p->AppendLog(e2);
        p->AppendLog(e3);
    }

    {
        auto p = MakeFilePersister(dir);
        PersistentState s;
        std::vector<LogEntry> es;
        ASSERT_TRUE(p->Load(&s, &es));
        EXPECT_EQ(s.current_term, 2U);
        EXPECT_FALSE(s.voted_for.has_value());
        ASSERT_EQ(es.size(), 3U);
        EXPECT_EQ(es[0].index, 1U);
        EXPECT_EQ(es[0].payload, "a");
        EXPECT_EQ(es[1].index, 2U);
        EXPECT_EQ(es[1].payload, "bb");
        EXPECT_EQ(es[2].index, 3U);
        EXPECT_EQ(es[2].payload, "ccc");
    }

    std::filesystem::remove_all(dir);
}

TEST(MemoryPersisterTest, SnapshotRoundtrip) {
    auto p = MakeMemoryPersister();
    p->SaveSnapshot("hello-snap", 100U, 7U);

    std::string b;
    LogIdx i = 0;
    Term t = 0;
    ASSERT_TRUE(p->LoadSnapshot(&b, &i, &t));
    EXPECT_EQ(b, "hello-snap");
    EXPECT_EQ(i, 100U);
    EXPECT_EQ(t, 7U);
}

TEST(FilePersisterTest, SnapshotPersistsAcrossInstances) {
    const auto dir = UniqueTempDir("knot_filepers_snap");

    {
        auto p = MakeFilePersister(dir);
        p->SaveSnapshot("snap-data", 42U, 5U);
    }

    {
        auto p = MakeFilePersister(dir);
        std::string b;
        LogIdx i = 0;
        Term t = 0;
        ASSERT_TRUE(p->LoadSnapshot(&b, &i, &t));
        EXPECT_EQ(b, "snap-data");
        EXPECT_EQ(i, 42U);
        EXPECT_EQ(t, 5U);
    }

    std::filesystem::remove_all(dir);
}

TEST(FilePersisterTest, TruncateDropsSuffixOnDisk) {
    const auto dir = UniqueTempDir("knot_filepers_trunc");

    {
        auto p = MakeFilePersister(dir);
        p->SaveState(1U, std::nullopt);
        for (LogIdx i = 1; i <= 5; ++i) {
            LogEntry e{.term = 1,
                       .index = i,
                       .type = EntryType::kCommand,
                       .payload = std::string("v") + std::to_string(i)};
            p->AppendLog(e);
        }
        p->TruncateLog(3);
    }

    {
        auto p = MakeFilePersister(dir);
        PersistentState s;
        std::vector<LogEntry> es;
        ASSERT_TRUE(p->Load(&s, &es));
        ASSERT_EQ(es.size(), 2U);
        EXPECT_EQ(es[0].index, 1U);
        EXPECT_EQ(es[1].index, 2U);
    }

    std::filesystem::remove_all(dir);
}
