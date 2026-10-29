module;
#include <string>

export module cluster:shard_router;

export namespace mako {
// Physical slot geometry retained ONLY for disabled/static engine tables.
// Governed native accesses use mako_sharding_route's complete owner+epoch.
constexpr int SHARD_ROUTER_NUM_TABLES_PER_SHARD = 200;

// @safe - legacy physical-table addressing, with no policy/cache fallback.
// Callers select this only when native sharding is disabled or the catalog
// explicitly identifies a replicated/static table. `key` does not affect it.
int compute_shard_for_key(int table_id, const std::string& key);
// Dead-shard redirect for the static addressing above. The config table
// (MAKO_CONFIG_TABLE=1) sets it, so the requests for a shard killed with
// kill_shard go to the shard recorded as its replacement. Null, the default,
// leaves every owner as computed.
using ShardRedirectFn = int (*)(int shard);
void set_shard_redirect(ShardRedirectFn redirect);
} // namespace mako
