#include <mutex>
#include <queue>
#include <set>
#include <unordered_map>
#include <utility>

#include <knot/raft/transport.h>

namespace knot::raft {

namespace {

std::pair<NodeId, NodeId> UndirectedKey(const NodeId& a, const NodeId& b) {
    if (a < b) {
        return std::make_pair(a, b);
    }
    return std::make_pair(b, a);
}

}  // namespace

struct InMemoryBus::Impl {
    std::mutex mu;
    std::unordered_map<NodeId, std::queue<Envelope>> mailboxes;
    std::set<std::pair<NodeId, NodeId>> partitions;

    void Register(const NodeId& id) {
        std::lock_guard<std::mutex> lk(mu);
        mailboxes[id];  // default-construct an empty queue if absent
    }

    void Deliver(Envelope env) {
        std::lock_guard<std::mutex> lk(mu);
        if (partitions.contains(UndirectedKey(env.from, env.to))) {
            return;
        }
        auto it = mailboxes.find(env.to);
        if (it == mailboxes.end()) {
            return;  // unknown destination -> silent drop
        }
        it->second.push(std::move(env));
    }

    std::vector<Envelope> DrainFor(const NodeId& id) {
        std::lock_guard<std::mutex> lk(mu);
        std::vector<Envelope> out;
        auto it = mailboxes.find(id);
        if (it == mailboxes.end()) {
            return out;
        }
        auto& q = it->second;
        while (!q.empty()) {
            out.push_back(std::move(q.front()));
            q.pop();
        }
        return out;
    }

    void Partition(const NodeId& a, const NodeId& b) {
        std::lock_guard<std::mutex> lk(mu);
        partitions.insert(UndirectedKey(a, b));
    }

    void Heal(const NodeId& a, const NodeId& b) {
        std::lock_guard<std::mutex> lk(mu);
        partitions.erase(UndirectedKey(a, b));
    }

    void DropAll() {
        std::lock_guard<std::mutex> lk(mu);
        for (auto& kv : mailboxes) {
            std::queue<Envelope> empty;
            std::swap(kv.second, empty);
        }
    }
};

namespace {

class InMemoryTransport : public Transport {
public:
    InMemoryTransport(NodeId self, InMemoryBus::Impl* bus) : self_(std::move(self)), bus_(bus) {}

    void Send(Envelope env) override { bus_->Deliver(std::move(env)); }
    std::vector<Envelope> Drain() override { return bus_->DrainFor(self_); }

private:
    NodeId self_;
    InMemoryBus::Impl* bus_;  // non-owning
};

}  // namespace

InMemoryBus::InMemoryBus() : impl_(std::make_unique<Impl>()) {}
InMemoryBus::~InMemoryBus() = default;

std::unique_ptr<Transport> InMemoryBus::Connect(NodeId id) {
    impl_->Register(id);
    return std::make_unique<InMemoryTransport>(std::move(id), impl_.get());
}

void InMemoryBus::Partition(const NodeId& a, const NodeId& b) {
    impl_->Partition(a, b);
}
void InMemoryBus::Heal(const NodeId& a, const NodeId& b) {
    impl_->Heal(a, b);
}
void InMemoryBus::DropAll() {
    impl_->DropAll();
}

}  // namespace knot::raft
