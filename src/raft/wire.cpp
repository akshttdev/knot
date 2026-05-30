#include <string>
#include <type_traits>
#include <variant>

#include <knot/raft/wire.h>

#include "knot.pb.h"

namespace knot::raft {

namespace {

constexpr std::size_t kLenPrefixSize = 4;
constexpr std::size_t kTagSize = 1;

void PutBE32(std::vector<std::uint8_t>& out, std::uint32_t v) {
    out.push_back(static_cast<std::uint8_t>((v >> 24) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((v >> 16) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((v >> 8) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>(v & 0xFFU));
}

std::uint32_t GetBE32(const std::uint8_t* p) {
    return (static_cast<std::uint32_t>(p[0]) << 24) | (static_cast<std::uint32_t>(p[1]) << 16) |
           (static_cast<std::uint32_t>(p[2]) << 8) | static_cast<std::uint32_t>(p[3]);
}

std::vector<std::uint8_t> FrameOne(WireTag tag, const std::string& payload) {
    const auto body_len = static_cast<std::uint32_t>(kTagSize + payload.size());
    std::vector<std::uint8_t> out;
    out.reserve(kLenPrefixSize + body_len);
    PutBE32(out, body_len);
    out.push_back(static_cast<std::uint8_t>(tag));
    out.insert(out.end(), payload.begin(), payload.end());
    return out;
}

knot::rpc::LogEntryProto ToProto(const LogEntry& e) {
    knot::rpc::LogEntryProto out;
    out.set_term(e.term);
    out.set_index(e.index);
    out.set_type(static_cast<std::uint32_t>(e.type));
    out.set_payload(e.payload);
    return out;
}

LogEntry FromProto(const knot::rpc::LogEntryProto& p) {
    return LogEntry{.term = p.term(),
                    .index = p.index(),
                    .type = static_cast<EntryType>(p.type()),
                    .payload = p.payload()};
}

}  // namespace

std::vector<std::uint8_t> EncodeHandshake(const NodeId& sender_id) {
    knot::rpc::RaftHandshake hs;
    hs.set_sender_id(sender_id);
    std::string buf;
    hs.SerializeToString(&buf);
    return FrameOne(WireTag::kHandshake, buf);
}

std::vector<std::uint8_t> Encode(const Envelope& env) {
    return std::visit(
        [](auto&& msg) -> std::vector<std::uint8_t> {
            using T = std::decay_t<decltype(msg)>;
            std::string buf;
            if constexpr (std::is_same_v<T, RequestVoteReq>) {
                knot::rpc::RequestVoteRequest p;
                p.set_term(msg.term);
                p.set_candidate_id(msg.candidate_id);
                p.set_last_log_index(msg.last_log_index);
                p.set_last_log_term(msg.last_log_term);
                p.SerializeToString(&buf);
                return FrameOne(WireTag::kRequestVoteReq, buf);
            } else if constexpr (std::is_same_v<T, RequestVoteResp>) {
                knot::rpc::RequestVoteResponse p;
                p.set_term(msg.term);
                p.set_vote_granted(msg.vote_granted);
                p.SerializeToString(&buf);
                return FrameOne(WireTag::kRequestVoteResp, buf);
            } else if constexpr (std::is_same_v<T, AppendEntriesReq>) {
                knot::rpc::AppendEntriesRequest p;
                p.set_term(msg.term);
                p.set_leader_id(msg.leader_id);
                p.set_prev_log_index(msg.prev_log_index);
                p.set_prev_log_term(msg.prev_log_term);
                for (const auto& e : msg.entries) {
                    *p.add_entries() = ToProto(e);
                }
                p.set_leader_commit(msg.leader_commit);
                p.SerializeToString(&buf);
                return FrameOne(WireTag::kAppendEntriesReq, buf);
            } else if constexpr (std::is_same_v<T, AppendEntriesResp>) {
                knot::rpc::AppendEntriesResponse p;
                p.set_term(msg.term);
                p.set_success(msg.success);
                p.set_match_index(msg.match_index);
                p.set_conflict_index(msg.conflict_index);
                p.set_conflict_term(msg.conflict_term);
                p.SerializeToString(&buf);
                return FrameOne(WireTag::kAppendEntriesResp, buf);
            } else if constexpr (std::is_same_v<T, InstallSnapshotReq>) {
                knot::rpc::InstallSnapshotRequest p;
                p.set_term(msg.term);
                p.set_leader_id(msg.leader_id);
                p.set_last_included_index(msg.last_included_index);
                p.set_last_included_term(msg.last_included_term);
                p.set_data(msg.data);
                p.SerializeToString(&buf);
                return FrameOne(WireTag::kInstallSnapshotReq, buf);
            } else {
                // InstallSnapshotResp
                knot::rpc::InstallSnapshotResponse p;
                p.set_term(msg.term);
                p.SerializeToString(&buf);
                return FrameOne(WireTag::kInstallSnapshotResp, buf);
            }
        },
        env.msg);
}

DecodeResult DecodeOne(const std::uint8_t* buf, std::size_t len, const NodeId& self_id) {
    DecodeResult result;
    if (len < kLenPrefixSize) {
        return result;
    }
    const std::uint32_t body_len = GetBE32(buf);
    if (len < kLenPrefixSize + body_len) {
        return result;
    }
    if (body_len < kTagSize) {
        return result;
    }
    const auto tag = static_cast<WireTag>(buf[kLenPrefixSize]);
    const auto* payload = buf + kLenPrefixSize + kTagSize;
    const auto payload_n = static_cast<std::size_t>(body_len) - kTagSize;

    // Construct payload string from the byte buffer. Build from a
    // (begin, end) range of std::uint8_t* — no casts needed.
    const std::string payload_str(payload, payload + payload_n);
    result.bytes_consumed = kLenPrefixSize + body_len;

    switch (tag) {
        case WireTag::kHandshake: {
            knot::rpc::RaftHandshake p;
            if (!p.ParseFromString(payload_str)) {
                result.bytes_consumed = 0;
                return result;
            }
            result.handshake_sender = p.sender_id();
            return result;
        }
        case WireTag::kRequestVoteReq: {
            knot::rpc::RequestVoteRequest p;
            if (!p.ParseFromString(payload_str)) {
                result.bytes_consumed = 0;
                return result;
            }
            RequestVoteReq m;
            m.term = p.term();
            m.candidate_id = p.candidate_id();
            m.last_log_index = p.last_log_index();
            m.last_log_term = p.last_log_term();
            result.envelope = Envelope{.from = m.candidate_id, .to = self_id, .msg = m};
            return result;
        }
        case WireTag::kRequestVoteResp: {
            knot::rpc::RequestVoteResponse p;
            if (!p.ParseFromString(payload_str)) {
                result.bytes_consumed = 0;
                return result;
            }
            RequestVoteResp m{.term = p.term(), .vote_granted = p.vote_granted()};
            result.envelope = Envelope{.from = "", .to = self_id, .msg = m};
            return result;
        }
        case WireTag::kAppendEntriesReq: {
            knot::rpc::AppendEntriesRequest p;
            if (!p.ParseFromString(payload_str)) {
                result.bytes_consumed = 0;
                return result;
            }
            AppendEntriesReq m;
            m.term = p.term();
            m.leader_id = p.leader_id();
            m.prev_log_index = p.prev_log_index();
            m.prev_log_term = p.prev_log_term();
            m.entries.reserve(static_cast<std::size_t>(p.entries_size()));
            for (const auto& e : p.entries()) {
                m.entries.push_back(FromProto(e));
            }
            m.leader_commit = p.leader_commit();
            result.envelope = Envelope{.from = m.leader_id, .to = self_id, .msg = m};
            return result;
        }
        case WireTag::kAppendEntriesResp: {
            knot::rpc::AppendEntriesResponse p;
            if (!p.ParseFromString(payload_str)) {
                result.bytes_consumed = 0;
                return result;
            }
            AppendEntriesResp m;
            m.term = p.term();
            m.success = p.success();
            m.match_index = p.match_index();
            m.conflict_index = p.conflict_index();
            m.conflict_term = p.conflict_term();
            result.envelope = Envelope{.from = "", .to = self_id, .msg = m};
            return result;
        }
        case WireTag::kInstallSnapshotReq: {
            knot::rpc::InstallSnapshotRequest p;
            if (!p.ParseFromString(payload_str)) {
                result.bytes_consumed = 0;
                return result;
            }
            InstallSnapshotReq m;
            m.term = p.term();
            m.leader_id = p.leader_id();
            m.last_included_index = p.last_included_index();
            m.last_included_term = p.last_included_term();
            m.data = p.data();
            result.envelope = Envelope{.from = m.leader_id, .to = self_id, .msg = m};
            return result;
        }
        case WireTag::kInstallSnapshotResp: {
            knot::rpc::InstallSnapshotResponse p;
            if (!p.ParseFromString(payload_str)) {
                result.bytes_consumed = 0;
                return result;
            }
            InstallSnapshotResp m{.term = p.term()};
            result.envelope = Envelope{.from = "", .to = self_id, .msg = m};
            return result;
        }
    }
    result.bytes_consumed = 0;  // unknown tag
    return result;
}

}  // namespace knot::raft
