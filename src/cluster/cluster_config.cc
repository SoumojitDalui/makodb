// ClusterConfig is authored in the inline-Rust DSL in cluster_config.h
// (struct + methods generate there, plus the get_cluster_config() singleton).
// This is a module IMPLEMENTATION unit of `cluster`: it holds the DSL bodies of
// the routing free fns (cc_hash_key / cc_extract_key_value /
// cc_follow_replacement / cc_route), whose declarations are exported by the
// cluster:cluster_config partition, plus the one true kernel cc_load_from_cm
// (it reads through a *complete* ConfigManager). Under modules there is no ODR
// hazard: the definitions are owned once by module `cluster`.
module;
#include <string>
#include <vector>
#include <cstdint>
#include <rusty/slice.hpp>           // deref_if_pointer_like (generated DSL bodies)
#include <rusty/move.hpp>            // rusty::clone (enum-literal comparisons in the GEN block)
module cluster;
import btree_port.btree.map;   // c529cd3d: btree_port is now a C++20 module (retired the .hpp header)
import :sharding_policy;   // KeyExtractor(Type) used by cc_extract_key_value / cc_route
import :cluster_config;    // ClusterConfigState / ShardInfo / cc_* declarations
import :config_manager;    // ConfigManager (cc_load_from_cm reads through it)

namespace btree_port { using btree::map::BTreeMap; }  // compat: flat name the DSL/GEN expect

namespace janus {

#if RUSTYCPP_RUST
// FNV-1a over the raw key bytes.
pub fn cc_hash_key(key: &std::string) -> u32 {
    let mut hash: u32 = 2166136261;
    let mut i: usize = 0;
    while i < key.size() {
        hash = hash ^ ((key[i] as u8) as u32);
        hash = hash * 16777619;
        i = i + 1;
    }
    hash
}
// Chase the dead-shard replacement pointer; cycle-guarded by shard_count + 1.
pub fn cc_follow_replacement(s: &ClusterConfigState, sid: u32) -> u32 {
    let max_hops: u32 = s.shard_count + 1;
    let mut cur: u32 = sid;
    let mut hop: u32 = 0;
    while hop < max_hops {
        if !s.shards.contains_key(cur) {
            return cur;
        }
        let info: &ShardInfo = s.shards.get(cur).unwrap();
        if info.status != std::string("dead") {
            return cur;
        }
        cur = info.replacement;
        hop = hop + 1;
    }
    cur
}
// Big-endian int64 decode mirroring shard_router.cc's convention. Extractor
// by value (a small copyable aggregate) so the call site needs no borrow.
pub fn cc_extract_key_value(ext: KeyExtractor, key: &std::string) -> i64 {
    if key.empty() {
        return 0;
    }
    let mut nbytes: usize = 0;
    if ext.kind == KeyExtractorType::FIELD_INDEX {
        nbytes = 8;
    } else if ext.kind == KeyExtractorType::PREFIX_BYTES {
        nbytes = ext.prefix_length as usize;
        if nbytes == 0 {
            nbytes = 8;
        }
    } else {
        return 0;
    }
    if nbytes > key.size() {
        nbytes = key.size();
    }
    let mut value: i64 = 0;
    let mut i: usize = 0;
    while i < nbytes {
        value = (value << 8) | ((key[i] as u8) as i64);
        i = i + 1;
    }
    value
}
// Full routing: per-table policy -> range shard, else hash-mod; then chase
// any dead-shard replacement. Returns 0 when there are no shards.
// The shard a table's own policy gives the key, before dead-shard
// replacement; -1 when the table has no policy or its ranges do not
// cover the key.
pub fn cc_policy_shard(s: &ClusterConfigState, table: &std::string, key: &std::string) -> i32 {
    if table.empty() || !s.table_policies.contains_key(table) {
        return -1;
    }
    let kv: i64 = janus::cc_extract_key_value(s.table_policies.get(table).unwrap().key_extractor, key);
    s.table_policies.get(table).unwrap().get_shard(kv)
}

pub fn cc_route(s: &ClusterConfigState, table: &std::string, key: &std::string) -> u32 {
    if s.shard_count == 0 {
        return 0;
    }
    let sid: i32 = janus::cc_policy_shard(s, table, key);
    if sid >= 0 {
        return janus::cc_follow_replacement(s, sid as u32);
    }
    janus::cc_follow_replacement(s, janus::cc_hash_key(key) % s.shard_count)
}
#endif
/*RUSTYCPP:GEN-BEGIN id=cluster_config.1 version=1 rust_sha256=9d9b3aa88dc12180adca87360d505752e0534f79ba56ef9c34eedb77d90c8ab2*/
uint32_t cc_hash_key(const std::string& key);

uint32_t cc_hash_key(const std::string& key) {
    uint32_t hash = static_cast<uint32_t>(2166136261);
    size_t i = static_cast<size_t>(0);
    while (rusty::detail::deref_if_pointer_like(i) < key.size()) {
        hash = rusty::detail::deref_if_pointer_like(hash) ^ ((static_cast<uint32_t>((static_cast<uint8_t>(key[i])))));
        hash = rusty::detail::deref_if_pointer_like(hash) * static_cast<uint32_t>(16777619);
        i = rusty::detail::deref_if_pointer_like(i) + static_cast<size_t>(1);
    }
    return std::move(hash);
}

uint32_t cc_follow_replacement(const ClusterConfigState& s, uint32_t sid) {
    const uint32_t max_hops = rusty::detail::deref_if_pointer_like(s.shard_count) + static_cast<uint32_t>(1);
    uint32_t cur = sid;
    uint32_t hop = static_cast<uint32_t>(0);
    while (rusty::detail::deref_if_pointer_like(hop) < rusty::detail::deref_if_pointer_like(max_hops)) {
        if (rusty::detail::rust_not(s.shards.contains_key(std::move(cur)))) {
            return std::move(cur);
        }
        const ShardInfo& info = s.shards.get(std::move(cur)).unwrap();
        if (rusty::detail::deref_if_pointer_like(info.status) != std::string("dead")) {
            return std::move(cur);
        }
        cur = info.replacement;
        hop = rusty::detail::deref_if_pointer_like(hop) + static_cast<uint32_t>(1);
    }
    return std::move(cur);
}

int64_t cc_extract_key_value(KeyExtractor ext, const std::string& key) {
    if (key.empty()) {
        return static_cast<int64_t>(0);
    }
    size_t nbytes = static_cast<size_t>(0);
    if (rusty::detail::deref_if_pointer_like(ext.kind) == rusty::clone(KeyExtractorType::FIELD_INDEX)) {
        nbytes = static_cast<size_t>(8);
    } else if (rusty::detail::deref_if_pointer_like(ext.kind) == rusty::clone(KeyExtractorType::PREFIX_BYTES)) {
        nbytes = static_cast<size_t>(ext.prefix_length);
        if (rusty::detail::deref_if_pointer_like(nbytes) == static_cast<size_t>(0)) {
            nbytes = static_cast<size_t>(8);
        }
    } else {
        return static_cast<int64_t>(0);
    }
    if (rusty::detail::deref_if_pointer_like(nbytes) > key.size()) {
        nbytes = key.size();
    }
    int64_t value = static_cast<int64_t>(0);
    size_t i = static_cast<size_t>(0);
    while (rusty::detail::deref_if_pointer_like(i) < rusty::detail::deref_if_pointer_like(nbytes)) {
        value = ((rusty::detail::deref_if_pointer_like(value) << 8)) | ((static_cast<int64_t>((static_cast<uint8_t>(key[i])))));
        i = rusty::detail::deref_if_pointer_like(i) + static_cast<size_t>(1);
    }
    return std::move(value);
}

int32_t cc_policy_shard(const ClusterConfigState& s, const std::string& table, const std::string& key) {
    if (table.empty() || rusty::detail::rust_not(s.table_policies.contains_key(table))) {
        return -1;
    }
    const int64_t kv = janus::cc_extract_key_value(s.table_policies.get(table).unwrap().key_extractor, key);
    return s.table_policies.get(table).unwrap().get_shard(std::move(kv));
}

uint32_t cc_route(const ClusterConfigState& s, const std::string& table, const std::string& key) {
    if (rusty::detail::deref_if_pointer_like(s.shard_count) == 0) {
        return static_cast<uint32_t>(0);
    }
    const int32_t sid = janus::cc_policy_shard(s, table, key);
    if (rusty::detail::deref_if_pointer_like(sid) >= 0) {
        return janus::cc_follow_replacement(s, static_cast<uint32_t>(sid));
    }
    return janus::cc_follow_replacement(s, janus::cc_hash_key(key) % rusty::detail::deref_if_pointer_like(s.shard_count));
}
/*RUSTYCPP:GEN-END id=cluster_config.1*/

// @unsafe - reads through ConfigManager (storage engine / RemoteKvStore
// RPC) and rebuilds the topology maps. Runs under the caller's held guard.
bool cc_load_from_cm(ClusterConfigState& s, ConfigManager* cm) {
    if (cm == nullptr) return false;
    // Over RPC (RemoteKvStore) an unreachable store reads as empty, every key
    // absent, and a connection lost partway through reads as part config,
    // part empty. A store that holds a config has a nonzero __version__, and
    // every change rewrites it last, so read it before and after the rest: if
    // it is zero or moved in between, there is no consistent snapshot and the
    // loaded config stays as it was. (A change still in flight shows up as a
    // new version on the next poll.)
    //
    // A replica that takes over shard 0 moves the version to a newer term:
    // the shard-0 bootstrap puts partition 0's replication term in a
    // version's high 32 bits (term << 32 | (replica position + 1) << 24 |
    // count), and each change bumps it. So a version from an older term
    // comes from a replica that has lost shard 0 but not stepped down yet,
    // and loading it would undo changes this node already has. Two replicas
    // can claim the same term (Mako's Raft can let that happen); their
    // versions are told apart by position, not ordered, so one of the same
    // term is loaded even when it is lower, or nodes that saw the other
    // first would never take the replica that stays leader.
    const uint64_t ver = cm->get_version();
    if (ver == 0 || (ver >> 32) < (s.version >> 32)) return false;
    uint32_t count = cm->get_shard_count();
    if (count == 0) return false;   // no topology, or shard_count read failed
    uint64_t ep = cm->get_epoch();
    // c529cd3d: BTreeMap has no default ctor; construct explicitly.
    btree_port::BTreeMap<uint32_t, ShardInfo> new_shards =
        btree_port::BTreeMap<uint32_t, ShardInfo>::new_();
    for (uint32_t i = 0; i < count; i++) {
        ShardInfo info;
        info.id = i;
        info.replicas = cm->get_shard_replicas(i);
        info.leader = cm->get_shard_leader(i);
        info.status = cm->get_shard_status(i);
        info.replacement = cm->get_shard_replacement(i);
        cc_shards_insert(new_shards, i, std::move(info));
    }
    if (cm->get_version() != ver) return false;
    s.shard_count = count;
    s.version = ver;
    s.epoch = ep;
    s.shards = std::move(new_shards);
    return true;
}

}  // namespace janus
