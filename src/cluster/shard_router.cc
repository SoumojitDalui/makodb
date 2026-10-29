module;
#include <atomic>
#include <string>
module cluster;
import :shard_router;

namespace mako {
namespace {
std::atomic<ShardRedirectFn> shard_redirect{nullptr};
} // namespace

// @safe - legacy engine table-slot addressing only; native governed routing
// never dispatches through this function. Invalid physical IDs fail closed.
int compute_shard_for_key(int table_id, const std::string&) {
    const int owner = table_id > 0 ? (table_id - 1) / SHARD_ROUTER_NUM_TABLES_PER_SHARD : -1;
    const ShardRedirectFn redirect = shard_redirect.load(std::memory_order_acquire);
    return owner >= 0 && redirect != nullptr ? redirect(owner) : owner;
}

void set_shard_redirect(ShardRedirectFn redirect) {
    shard_redirect.store(redirect, std::memory_order_release);
}
} // namespace mako
