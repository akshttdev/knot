// Knot — RaftLog unit tests.

#include <stdexcept>
#include <string>

#include <gtest/gtest.h>
#include <knot/raft/log.h>

using namespace knot::raft;

TEST(RaftLogTest, EmptyLogHasZeroIndexAndTerm) {
    RaftLog log;
    EXPECT_EQ(log.LastIndex(), 0U);
    EXPECT_EQ(log.LastTerm(), 0U);
    EXPECT_EQ(log.Size(), 0U);
}

TEST(RaftLogTest, AppendAssignsSequentialIndicesStartingAtOne) {
    RaftLog log;
    log.Append(1, EntryType::kCommand, "a");
    log.Append(1, EntryType::kCommand, "b");
    log.Append(2, EntryType::kCommand, "c");

    EXPECT_EQ(log.Size(), 3U);
    EXPECT_EQ(log.LastIndex(), 3U);
    EXPECT_EQ(log.LastTerm(), 2U);
}

TEST(RaftLogTest, AtReturnsEntryByIndex) {
    RaftLog log;
    log.Append(5, EntryType::kCommand, "hello");

    const LogEntry& e = log.At(1);
    EXPECT_EQ(e.index, 1U);
    EXPECT_EQ(e.term, 5U);
    EXPECT_EQ(e.type, EntryType::kCommand);
    EXPECT_EQ(e.payload, "hello");
}

TEST(RaftLogTest, AtThrowsOnZeroIndex) {
    RaftLog log;
    log.Append(1, EntryType::kCommand, "a");
    EXPECT_THROW((void)log.At(0), std::out_of_range);
}

TEST(RaftLogTest, AtThrowsOnIndexBeyondEnd) {
    RaftLog log;
    log.Append(1, EntryType::kCommand, "a");
    EXPECT_THROW((void)log.At(2), std::out_of_range);
}

TEST(RaftLogTest, SliceReturnsHalfOpenRange) {
    RaftLog log;
    for (int i = 0; i < 5; ++i) {
        log.Append(1, EntryType::kCommand, std::string("v") + std::to_string(i));
    }

    auto out = log.Slice(2, 4);
    ASSERT_EQ(out.size(), 2U);
    EXPECT_EQ(out[0].index, 2U);
    EXPECT_EQ(out[0].payload, "v1");
    EXPECT_EQ(out[1].index, 3U);
    EXPECT_EQ(out[1].payload, "v2");
}

TEST(RaftLogTest, SliceEmptyWhenFromEqualsTo) {
    RaftLog log;
    log.Append(1, EntryType::kCommand, "x");
    EXPECT_TRUE(log.Slice(1, 1).empty());
}

TEST(RaftLogTest, SliceClampsToLastIndex) {
    RaftLog log;
    log.Append(1, EntryType::kCommand, "a");
    log.Append(1, EntryType::kCommand, "b");
    log.Append(1, EntryType::kCommand, "c");

    auto out = log.Slice(1, 100);
    EXPECT_EQ(out.size(), 3U);
}

TEST(RaftLogTest, SliceFromZeroReturnsEmpty) {
    RaftLog log;
    log.Append(1, EntryType::kCommand, "a");
    EXPECT_TRUE(log.Slice(0, 1).empty());
}

TEST(RaftLogTest, TruncateFromDropsSuffix) {
    RaftLog log;
    for (int i = 0; i < 5; ++i) {
        log.Append(1, EntryType::kCommand, "x");
    }

    log.TruncateFrom(3);
    EXPECT_EQ(log.Size(), 2U);
    EXPECT_EQ(log.LastIndex(), 2U);
}

TEST(RaftLogTest, TruncateFromBeyondEndIsNoOp) {
    RaftLog log;
    log.Append(1, EntryType::kCommand, "a");
    log.Append(1, EntryType::kCommand, "b");

    log.TruncateFrom(99);
    EXPECT_EQ(log.Size(), 2U);
    EXPECT_EQ(log.LastIndex(), 2U);
}

TEST(RaftLogTest, TruncateFromOneClearsLog) {
    RaftLog log;
    log.Append(1, EntryType::kCommand, "a");
    log.Append(1, EntryType::kCommand, "b");

    log.TruncateFrom(1);
    EXPECT_EQ(log.Size(), 0U);
    EXPECT_EQ(log.LastIndex(), 0U);
}

TEST(RaftLogTest, MatchesTrueForExistingEntry) {
    RaftLog log;
    log.Append(7, EntryType::kCommand, "x");
    EXPECT_TRUE(log.Matches(1, 7));
}

TEST(RaftLogTest, MatchesFalseForWrongTerm) {
    RaftLog log;
    log.Append(7, EntryType::kCommand, "x");
    EXPECT_FALSE(log.Matches(1, 8));
}

TEST(RaftLogTest, MatchesIndexZeroTermZeroIsTrueForEmptyLog) {
    // Convention: (0, 0) matches the implicit "before-the-log" pointer.
    // The Raft paper uses prev_log_index=0, prev_log_term=0 for the
    // very first AppendEntries.
    RaftLog log;
    EXPECT_TRUE(log.Matches(0, 0));
}

TEST(RaftLogTest, AppendBatchAcceptsContiguousEntries) {
    RaftLog log;
    log.Append(1, EntryType::kCommand, "a");  // idx 1

    LogEntry e2{.term = 1, .index = 2, .type = EntryType::kCommand, .payload = "b"};
    LogEntry e3{.term = 1, .index = 3, .type = EntryType::kCommand, .payload = "c"};
    log.AppendBatch({e2, e3});

    EXPECT_EQ(log.Size(), 3U);
    EXPECT_EQ(log.At(2).payload, "b");
    EXPECT_EQ(log.At(3).payload, "c");
}

TEST(RaftLogTest, AppendBatchRejectsIndexGap) {
    RaftLog log;
    log.Append(1, EntryType::kCommand, "a");  // idx 1

    LogEntry bad{.term = 1, .index = 5, .type = EntryType::kCommand, .payload = "z"};
    EXPECT_THROW(log.AppendBatch({bad}), std::invalid_argument);
}

TEST(RaftLogTest, AppendBatchEmptyIsNoOp) {
    RaftLog log;
    log.Append(1, EntryType::kCommand, "a");
    log.AppendBatch({});
    EXPECT_EQ(log.Size(), 1U);
}
