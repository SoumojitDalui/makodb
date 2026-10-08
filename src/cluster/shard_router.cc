/**
 * @file shard_router.cc
 * @brief Implementation of shard routing functions.
 *
 * This file is compiled as part of txlog library to have access to
 * deptran headers (ShardingPolicyCache, etc.).
 *
 * NOTE: We avoid including mako headers that pull in lib/common.h or rpc.h
 * to prevent symbol conflicts between mako and deptran.
 *
 * Authored in the inline-Rust DSL (docs/storage-interface.md): the four
 * routing functions are the `#if RUSTYCPP_RUST` block below (regenerate
 * with scripts/regen_storage_dsl.sh). They are free `pub fn`s, so the
 * generated definitions land here in the .cc — compiled exactly once,
 * matching the declarations in shard_router.h — rather than in a header
 * (where non-inline free fns would be an ODR hazard). The bodies only do
 * control flow + method calls into ClusterConfig / ShardingPolicyCache /
 * the table registry, so no C++ kernels are needed; the header stays a
 * minimal set of declarations.
 */

module;
#include <string>
#include <cstdint>
#include "mako/lib/table_registry.h"   // plain header: mako::get_table_registry()
#include <rusty/slice.hpp>             // deref_if_pointer_like (generated bodies)
#include <rusty/option.hpp>            // get_table_name() -> rusty::Option<std::string>
module cluster;
import :shard_router;            // this partition's decls + SHARD_ROUTER_NUM_TABLES_PER_SHARD
import :cluster_config;          // janus::get_cluster_config()
import :sharding_policy_cache;   // janus::get_sharding_policy_cache()

namespace mako {

#if RUSTYCPP_RUST
// Placement first: a key goes where its data was loaded, which today is
// decided per table (ShardingPolicyCache for policy tables such as TPC-C's
// warehouse-partitioned ones, else the table-ID home shard). Once the
// process-global ClusterConfig is populated (ConfigWatcher has loaded shard
// 0's __mako_config__), it refines that in two ways only: a table with its
// own policy in the config routes by that policy, and every result is
// passed through the config's dead-shard replacement pointers. The config's
// hash-mod default is deliberately NOT applied to tables without a policy:
// Mako does not place data by key hash, so hashing would send requests to
// shards that do not hold the table.
pub fn compute_shard_for_key(table_id: i32, key: &std::string) -> i32 {
    let cc_populated: bool = janus::get_cluster_config().get_shard_count() > 0;
    let cache_on: bool = janus::get_sharding_policy_cache().is_initialized();
    let mut table_name: std::string = std::string();
    let mut has_name: bool = false;
    if cc_populated || cache_on {
        let name_opt: rusty::Option<std::string> =
            get_table_registry().get_table_name(table_id);
        if name_opt.is_some() {
            table_name = name_opt.as_ref().unwrap();
            has_name = true;
        }
    }

    if cc_populated && has_name
        && janus::get_cluster_config().has_table_policy(&table_name) {
        // Policy shard, then any dead-shard replacement.
        return janus::get_cluster_config().get_shard_for_key(&table_name, key) as i32;
    }

    // Placement: ShardingPolicyCache, else the table-ID home shard.
    let mut placed: i32 = (table_id - 1) / SHARD_ROUTER_NUM_TABLES_PER_SHARD;
    if cache_on && has_name && !key.empty()
        && janus::get_sharding_policy_cache().has_policy_for_table(&table_name) {
        // First 8 key bytes as a big-endian int64 sharding field.
        let mut key_value: i64 = 0;
        let n: usize = if key.size() < 8 { key.size() } else { 8 };
        let mut i: usize = 0;
        while i < n {
            key_value = (key_value << 8) | ((key[i] as u8) as i64);
            i = i + 1;
        }
        let shard: i32 = janus::get_sharding_policy_cache()
            .get_shard_for_key(&table_name, key_value);
        if shard >= 0 {
            placed = shard;
        }
    }

    if cc_populated && placed >= 0 {
        return janus::get_cluster_config().resolve_live_shard(placed as u32) as i32;
    }
    placed
}

// key_value already extracted (e.g. warehouse_id): policy lookup, then the
// table-ID fallback.
pub fn compute_shard_for_key_value(table_id: i32, table_name: &std::string,
                                   key_value: i64) -> i32 {
    if janus::get_sharding_policy_cache().is_initialized()
        && janus::get_sharding_policy_cache().has_policy_for_table(table_name) {
        let shard: i32 = janus::get_sharding_policy_cache()
            .get_shard_for_key(table_name, key_value);
        if shard >= 0 {
            return shard;
        }
    }
    (table_id - 1) / SHARD_ROUTER_NUM_TABLES_PER_SHARD
}

pub fn has_policy_routing(table_name: &std::string) -> bool {
    janus::get_sharding_policy_cache().is_initialized()
        && janus::get_sharding_policy_cache().has_policy_for_table(table_name)
}

pub fn get_policy_num_shards() -> i32 {
    if janus::get_sharding_policy_cache().is_initialized() {
        return janus::get_sharding_policy_cache().get_num_shards();
    }
    0
}
#endif
/*RUSTYCPP:GEN-BEGIN id=shard_router.1 version=1 rust_sha256=31da3ab31a9a9544142b70e75dd3574667f20c018e3e000215ae3560d20ba757*/
int32_t compute_shard_for_key(int32_t table_id, const std::string& key);
int32_t compute_shard_for_key_value(int32_t table_id, const std::string& table_name, int64_t key_value);
bool has_policy_routing(const std::string& table_name);
int32_t get_policy_num_shards();

int32_t compute_shard_for_key(int32_t table_id, const std::string& key) {
    const bool cc_populated = janus::get_cluster_config().get_shard_count() > 0;
    const bool cache_on = janus::get_sharding_policy_cache().is_initialized();
    std::string table_name = std::string();
    bool has_name = false;
    if (rusty::detail::deref_if_pointer_like(cc_populated) || rusty::detail::deref_if_pointer_like(cache_on)) {
        const rusty::Option<std::string> name_opt = get_table_registry().get_table_name(std::move(table_id));
        if (name_opt.is_some()) {
            table_name = name_opt.as_ref().unwrap();
            has_name = true;
        }
    }
    if ((rusty::detail::deref_if_pointer_like(cc_populated) && rusty::detail::deref_if_pointer_like(has_name)) && janus::get_cluster_config().has_table_policy(table_name)) {
        return static_cast<int32_t>(janus::get_cluster_config().get_shard_for_key(table_name, key));
    }
    int32_t placed = ((rusty::detail::deref_if_pointer_like(table_id) - static_cast<int32_t>(1))) / rusty::detail::deref_if_pointer_like(SHARD_ROUTER_NUM_TABLES_PER_SHARD);
    if (((rusty::detail::deref_if_pointer_like(cache_on) && rusty::detail::deref_if_pointer_like(has_name)) && rusty::detail::rust_not(key.empty())) && janus::get_sharding_policy_cache().has_policy_for_table(table_name)) {
        int64_t key_value = static_cast<int64_t>(0);
        const size_t n = (key.size() < 8 ? key.size() : static_cast<size_t>(8));
        size_t i = static_cast<size_t>(0);
        while (rusty::detail::deref_if_pointer_like(i) < rusty::detail::deref_if_pointer_like(n)) {
            key_value = ((rusty::detail::deref_if_pointer_like(key_value) << 8)) | ((static_cast<int64_t>((static_cast<uint8_t>(key[i])))));
            i = rusty::detail::deref_if_pointer_like(i) + static_cast<size_t>(1);
        }
        int32_t shard = janus::get_sharding_policy_cache().get_shard_for_key(table_name, std::move(key_value));
        if (rusty::detail::deref_if_pointer_like(shard) >= 0) {
            placed = std::move(shard);
        }
    }
    if (rusty::detail::deref_if_pointer_like(cc_populated) && (rusty::detail::deref_if_pointer_like(placed) >= 0)) {
        return static_cast<int32_t>(janus::get_cluster_config().resolve_live_shard(static_cast<uint32_t>(placed)));
    }
    return std::move(placed);
}

int32_t compute_shard_for_key_value(int32_t table_id, const std::string& table_name, int64_t key_value) {
    if (janus::get_sharding_policy_cache().is_initialized() && janus::get_sharding_policy_cache().has_policy_for_table(table_name)) {
        int32_t shard = janus::get_sharding_policy_cache().get_shard_for_key(table_name, std::move(key_value));
        if (rusty::detail::deref_if_pointer_like(shard) >= 0) {
            return std::move(shard);
        }
    }
    return ((rusty::detail::deref_if_pointer_like(table_id) - static_cast<int32_t>(1))) / rusty::detail::deref_if_pointer_like(SHARD_ROUTER_NUM_TABLES_PER_SHARD);
}

bool has_policy_routing(const std::string& table_name) {
    return janus::get_sharding_policy_cache().is_initialized() && janus::get_sharding_policy_cache().has_policy_for_table(table_name);
}

int32_t get_policy_num_shards() {
    if (janus::get_sharding_policy_cache().is_initialized()) {
        return janus::get_sharding_policy_cache().get_num_shards();
    }
    return static_cast<int32_t>(0);
}
/*RUSTYCPP:GEN-END id=shard_router.1*/

}  // namespace mako
