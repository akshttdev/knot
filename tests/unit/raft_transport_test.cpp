// Knot — InMemoryBus / Transport unit tests.

#include <variant>

#include <gtest/gtest.h>
#include <knot/raft/messages.h>
#include <knot/raft/transport.h>

using namespace knot::raft;

namespace {
Envelope MakeRV(const NodeId& from, const NodeId& to, Term term) {
    return Envelope{
        .from = from,
        .to = to,
        .msg = RequestVoteReq{
            .term = term, .candidate_id = from, .last_log_index = 0, .last_log_term = 0}};
}
}  // namespace

TEST(InMemoryBusTest, RoundtripDeliversMessage) {
    InMemoryBus bus;
    auto a = bus.Connect("A");
    auto b = bus.Connect("B");

    a->Send(MakeRV("A", "B", 1));

    auto inbox = b->Drain();
    ASSERT_EQ(inbox.size(), 1U);
    EXPECT_EQ(inbox[0].from, "A");
    EXPECT_EQ(inbox[0].to, "B");
    EXPECT_TRUE(std::holds_alternative<RequestVoteReq>(inbox[0].msg));

    EXPECT_TRUE(b->Drain().empty());  // second drain is empty
}

TEST(InMemoryBusTest, EmptyDrainReturnsEmpty) {
    InMemoryBus bus;
    auto a = bus.Connect("A");
    EXPECT_TRUE(a->Drain().empty());
}

TEST(InMemoryBusTest, UnknownDestinationSilentlyDropped) {
    InMemoryBus bus;
    auto a = bus.Connect("A");
    // "ghost" is never connected. Send must not throw.
    a->Send(MakeRV("A", "ghost", 1));
    EXPECT_TRUE(a->Drain().empty());  // sender's own mailbox empty too
}

TEST(InMemoryBusTest, PartitionDropsBothDirectionsHealRestores) {
    InMemoryBus bus;
    auto a = bus.Connect("A");
    auto b = bus.Connect("B");

    bus.Partition("A", "B");
    a->Send(MakeRV("A", "B", 1));
    b->Send(MakeRV("B", "A", 1));
    EXPECT_TRUE(a->Drain().empty());
    EXPECT_TRUE(b->Drain().empty());

    bus.Heal("A", "B");
    a->Send(MakeRV("A", "B", 2));
    EXPECT_EQ(b->Drain().size(), 1U);
}

TEST(InMemoryBusTest, MultipleMessagesPreserveFifoOrder) {
    InMemoryBus bus;
    auto a = bus.Connect("A");
    auto b = bus.Connect("B");

    for (int i = 1; i <= 5; ++i) {
        a->Send(MakeRV("A", "B", static_cast<Term>(i)));
    }

    auto inbox = b->Drain();
    ASSERT_EQ(inbox.size(), 5U);
    for (int i = 0; i < 5; ++i) {
        const auto& m = std::get<RequestVoteReq>(inbox[i].msg);
        EXPECT_EQ(m.term, static_cast<Term>(i + 1));
    }
}
