// Knot — Command codec tests.

#include <string>
#include <string_view>
#include <variant>

#include <gtest/gtest.h>
#include <knot/raft/command.h>

using namespace knot::raft;

namespace {
// Helper: build a string_view over the raw bytes of an Encode* result.
std::string_view AsView(const std::vector<std::uint8_t>& v) {
    // char and uint8_t are layout-compatible for string_view's purposes;
    // construct via a temporary string to avoid any cast lint.
    static thread_local std::string buf;
    buf.assign(v.begin(), v.end());
    return buf;
}
}  // namespace

TEST(CommandCodecTest, EncodeDecodePutRoundtrip) {
    const auto bytes = EncodePut("hello", "world");
    const auto cmd = DecodeCommand(AsView(bytes));
    ASSERT_TRUE(cmd.has_value());
    ASSERT_TRUE(std::holds_alternative<PutCmd>(*cmd));
    const auto& p = std::get<PutCmd>(*cmd);
    EXPECT_EQ(p.key, "hello");
    EXPECT_EQ(p.value, "world");
}

TEST(CommandCodecTest, EncodeDecodeDeleteRoundtrip) {
    const auto bytes = EncodeDelete("hello");
    const auto cmd = DecodeCommand(AsView(bytes));
    ASSERT_TRUE(cmd.has_value());
    ASSERT_TRUE(std::holds_alternative<DeleteCmd>(*cmd));
    EXPECT_EQ(std::get<DeleteCmd>(*cmd).key, "hello");
}

TEST(CommandCodecTest, DecodeBadTagReturnsNullopt) {
    const std::string bad{static_cast<char>(0xFF), 0, 0, 0, 0};
    EXPECT_FALSE(DecodeCommand(bad).has_value());
}

TEST(CommandCodecTest, EncodePutKeyAndValueWithBinaryBytes) {
    const std::string key{'\0', 'k', 'e', '\x80', 'y'};
    const std::string val{'\xFF', '\0', 'v', '\x01'};
    const auto bytes = EncodePut(key, val);
    const auto cmd = DecodeCommand(AsView(bytes));
    ASSERT_TRUE(cmd.has_value());
    ASSERT_TRUE(std::holds_alternative<PutCmd>(*cmd));
    EXPECT_EQ(std::get<PutCmd>(*cmd).key, key);
    EXPECT_EQ(std::get<PutCmd>(*cmd).value, val);
}
