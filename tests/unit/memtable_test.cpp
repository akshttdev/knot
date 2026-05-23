// Knot — MemTable unit tests.

#include <string>

#include <gtest/gtest.h>
#include <knot/storage/memtable.h>

namespace ks = knot::storage;
using Result = ks::MemTable::LookupResult;

// ---------------------------------------------------------------------
// Empty state
// ---------------------------------------------------------------------
TEST(MemTable, EmptyByDefault) {
    ks::MemTable m;
    EXPECT_TRUE(m.Empty());
    EXPECT_EQ(m.EntryCount(), 0U);
    EXPECT_EQ(m.ApproximateSize(), 0U);
    EXPECT_EQ(m.LastSequence(), 0U);

    std::string v;
    EXPECT_EQ(m.Get("missing", &v), Result::kNotFound);
}

// ---------------------------------------------------------------------
// Basic Put/Get
// ---------------------------------------------------------------------
TEST(MemTable, PutGet) {
    ks::MemTable m;
    m.Put(1, "key", "value");

    std::string v;
    EXPECT_EQ(m.Get("key", &v), Result::kFound);
    EXPECT_EQ(v, "value");
    EXPECT_EQ(m.EntryCount(), 1U);
    EXPECT_FALSE(m.Empty());
}

TEST(MemTable, PutOverwrites) {
    ks::MemTable m;
    m.Put(1, "k", "old");
    m.Put(2, "k", "new");

    std::string v;
    EXPECT_EQ(m.Get("k", &v), Result::kFound);
    EXPECT_EQ(v, "new");
    EXPECT_EQ(m.EntryCount(), 1U);  // still one entry
    EXPECT_EQ(m.LastSequence(), 2U);
}

TEST(MemTable, EmptyValueWorks) {
    ks::MemTable m;
    m.Put(1, "k", "");

    std::string v = "preexisting";
    EXPECT_EQ(m.Get("k", &v), Result::kFound);
    EXPECT_EQ(v, "");
}

TEST(MemTable, GetNotFound) {
    ks::MemTable m;
    m.Put(1, "alpha", "1");

    std::string v;
    EXPECT_EQ(m.Get("beta", &v), Result::kNotFound);
}

// ---------------------------------------------------------------------
// Delete + tombstones
// ---------------------------------------------------------------------
TEST(MemTable, DeleteCreatesTombstone) {
    ks::MemTable m;
    m.Put(1, "key", "value");
    m.Delete(2, "key");

    std::string v;
    EXPECT_EQ(m.Get("key", &v), Result::kDeleted);
    EXPECT_EQ(m.EntryCount(), 1U);  // tombstone still occupies an entry
}

TEST(MemTable, DeleteOnNeverWrittenKey) {
    ks::MemTable m;
    m.Delete(1, "ghost");

    std::string v;
    EXPECT_EQ(m.Get("ghost", &v), Result::kDeleted);
}

TEST(MemTable, PutAfterDelete) {
    ks::MemTable m;
    m.Put(1, "k", "v1");
    m.Delete(2, "k");
    m.Put(3, "k", "v2");

    std::string v;
    EXPECT_EQ(m.Get("k", &v), Result::kFound);
    EXPECT_EQ(v, "v2");
    EXPECT_EQ(m.LastSequence(), 3U);
}

// ---------------------------------------------------------------------
// Size tracking
// ---------------------------------------------------------------------
TEST(MemTable, SizeGrowsWithEntries) {
    ks::MemTable m;
    auto s0 = m.ApproximateSize();
    m.Put(1, "k", "v");
    auto s1 = m.ApproximateSize();
    EXPECT_GT(s1, s0);

    m.Put(2, "k2", "v2");
    EXPECT_GT(m.ApproximateSize(), s1);
}

TEST(MemTable, SizeAccountsForValueReplacement) {
    ks::MemTable m;
    m.Put(1, "k", "short");
    auto s_short = m.ApproximateSize();

    m.Put(2, "k", "much-longer-value");
    EXPECT_GT(m.ApproximateSize(), s_short);

    m.Put(3, "k", "x");
    EXPECT_LT(m.ApproximateSize(), s_short + 5);
}

// ---------------------------------------------------------------------
// Iterator — sorted, includes tombstones
// ---------------------------------------------------------------------
TEST(MemTable, IteratorSortedOrder) {
    ks::MemTable m;
    m.Put(1, "charlie", "3");
    m.Put(2, "alpha", "1");
    m.Put(3, "bravo", "2");

    auto it = m.NewIterator();
    ASSERT_TRUE(it->Valid());
    EXPECT_EQ(it->Key(), "alpha");
    EXPECT_EQ(it->Value(), "1");
    EXPECT_EQ(it->Sequence(), 2U);
    it->Next();

    ASSERT_TRUE(it->Valid());
    EXPECT_EQ(it->Key(), "bravo");
    EXPECT_EQ(it->Value(), "2");
    it->Next();

    ASSERT_TRUE(it->Valid());
    EXPECT_EQ(it->Key(), "charlie");
    EXPECT_EQ(it->Value(), "3");
    it->Next();

    EXPECT_FALSE(it->Valid());
}

TEST(MemTable, IteratorShowsTombstones) {
    ks::MemTable m;
    m.Put(1, "a", "1");
    m.Delete(2, "b");
    m.Put(3, "c", "3");

    auto it = m.NewIterator();
    ASSERT_TRUE(it->Valid());
    EXPECT_EQ(it->Key(), "a");
    EXPECT_FALSE(it->IsTombstone());

    it->Next();
    EXPECT_EQ(it->Key(), "b");
    EXPECT_TRUE(it->IsTombstone());

    it->Next();
    EXPECT_EQ(it->Key(), "c");
    EXPECT_FALSE(it->IsTombstone());
}

TEST(MemTable, IteratorEmptyTable) {
    ks::MemTable m;
    auto it = m.NewIterator();
    EXPECT_FALSE(it->Valid());
}

TEST(MemTable, IteratorSnapshotIsolation) {
    ks::MemTable m;
    m.Put(1, "k", "v1");

    auto it = m.NewIterator();
    // After iterator construction, modify the table.
    m.Put(2, "k", "v2");
    m.Put(3, "new", "x");

    // Iterator sees the snapshot, not later writes.
    ASSERT_TRUE(it->Valid());
    EXPECT_EQ(it->Key(), "k");
    EXPECT_EQ(it->Value(), "v1");
    it->Next();
    EXPECT_FALSE(it->Valid());  // no "new" key in the snapshot
}

// ---------------------------------------------------------------------
// LastSequence tracking
// ---------------------------------------------------------------------
TEST(MemTable, LastSequenceTracking) {
    ks::MemTable m;
    EXPECT_EQ(m.LastSequence(), 0U);

    m.Put(5, "k", "v");
    EXPECT_EQ(m.LastSequence(), 5U);

    m.Delete(7, "x");
    EXPECT_EQ(m.LastSequence(), 7U);

    // Earlier sequence does NOT decrease LastSequence.
    m.Put(3, "y", "v");
    EXPECT_EQ(m.LastSequence(), 7U);
}

// ---------------------------------------------------------------------
// Many entries roundtrip
// ---------------------------------------------------------------------
TEST(MemTable, ManyEntriesRoundtrip) {
    ks::MemTable m;
    constexpr int kN = 1000;
    for (int i = 0; i < kN; ++i) {
        m.Put(i + 1, "key-" + std::to_string(i), "val-" + std::to_string(i));
    }
    EXPECT_EQ(m.EntryCount(), static_cast<std::size_t>(kN));

    std::string v;
    for (int i = 0; i < kN; ++i) {
        ASSERT_EQ(m.Get("key-" + std::to_string(i), &v), Result::kFound);
        EXPECT_EQ(v, "val-" + std::to_string(i));
    }
}
