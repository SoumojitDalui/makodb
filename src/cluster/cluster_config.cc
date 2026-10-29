// ClusterConfig is authored in the inline-Rust DSL in cluster_config.h
// (struct + methods generate there, plus the get_cluster_config() singleton).
// This is a module IMPLEMENTATION unit of `config_table`: it holds the DSL body
// of cc_follow_replacement, whose declaration is exported by the
// config_table:cluster_config partition, plus the one true kernel
// cc_load_from_cm (it reads through a *complete* ConfigManager).
module;
#include <string>
#include <vector>
#include <cstdint>
#include <rusty/slice.hpp>           // deref_if_pointer_like (generated DSL bodies)
#include <rusty/move.hpp>            // rusty::clone (generated DSL bodies)
module config_table;
import btree_port.btree.map;   // c529cd3d: btree_port is now a C++20 module (retired the .hpp header)
import cluster;            // ConfigManager (cc_load_from_cm reads through it)

namespace btree_port { using btree::map::BTreeMap; }  // compat: flat name the DSL/GEN expect

namespace janus {

#if RUSTYCPP_RUST
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
#endif
/*RUSTYCPP:GEN-BEGIN id=cluster_config.1 version=1 rust_sha256=e7e59d1c80c9e6053926bbf88c60b2fc4daa77de1e31afd5f372a2ff59298608*/
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
    // Versions only grow: each change bumps the version, and a replica that
    // takes over shard 0 continues above every version it has seen and
    // above a floor from its replication term. So a lower version comes
    // from a replica that has lost shard 0 but not stepped down yet, and
    // loading it would undo changes this node already has.
    const uint64_t ver = cm->get_version();
    if (ver == 0 || ver < s.version) return false;
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
