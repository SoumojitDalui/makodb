module;
#include <string>
#include <vector>
#include <map>
#include <cstdint>

#include <rusty/mutex.hpp>
#include <rusty/sync/atomic.hpp>   // AtomicU32 redirect hint
#include <rusty/slice.hpp>   // deref_if_pointer_like (guard bodies)

export module config_table:cluster_config;
import btree_port.btree.map;   // c529cd3d: btree_port is now a C++20 module (retired the .hpp header)
import cluster;   // ConfigManager (load_from_config_manager / cc_load_from_cm)

/*RUSTYCPP:GEN-DISPATCH-BEGIN*/
namespace rusty { namespace detail {
RUSTY_METHOD_DISPATCH(unwrap)
} } // namespace rusty::detail (issue #31 deref_call dispatch)
/*RUSTYCPP:GEN-DISPATCH-END*/

namespace btree_port { using btree::map::BTreeMap; }  // compat: flat name the DSL/GEN expect

export namespace janus {

struct ShardInfo {
    uint32_t id = 0;
    std::vector<std::string> replicas;
    std::string leader;
    std::string status;  // "active", "draining", "dead", etc.
    // For status == "dead": taker shard whose Raft group inherits requests
    // that would hash to this shard. 0 means unset; chased transitively.
    uint32_t replacement = 0;
};

/**
 * ClusterConfig - In-memory read-only cache of the config table: each
 * shard's replicas, leader, status and dead-shard replacement, plus the
 * version and epoch. Placement (which shard owns a key) is not here; native
 * sharding (src/cluster/*.rs) or its static addressing owns it.
 *
 * Every node holds a local copy, updated by ConfigWatcher /
 * load_from_config_manager.
 *
 * Authored in the inline-Rust DSL (docs/storage-interface.md): the
 * `#if RUSTYCPP_RUST` block is the source of truth; regenerate with
 * scripts/regen_storage_dsl.sh. All mutable state lives behind a single
 * rusty::Mutex<ClusterConfigState> (Rust's "the mutex guards the data"
 * shape, replacing the old bare std::mutex + separate fields). Each
 * method acquires the guard in the DSL and hands the guarded state to a
 * cc_* C++ kernel for the std::map iterator surgery (find / it->second /
 * insert); the pure scalar accessors are inline. The dead-shard
 * replacement chase (with a cycle guard) lives in the kernels too.
 *
 * Next to the mutex sits a lock-free summary of the state, the hints
 * (CC_HINT_*), republished under the guard by every method that changes
 * them, so a caller can tell with one atomic load whether any shard is
 * dead before it takes the guard.
 */
// ConfigManager is named (as a pointer) below; imported from module
// cluster above — modules forbid forward-declaring a foreign module's
// entity.

// The guarded state (bare struct; the Mutex is the ClusterConfig field).
struct ClusterConfigState {
    uint32_t shard_count = 0;
    uint64_t version = 0;
    uint64_t epoch = 0;
    btree_port::BTreeMap<uint32_t, ShardInfo> shards;
};

// ---- kernels: run under an already-held guard; own the map/routing ----
inline std::vector<std::string> cc_shard_replicas(const ClusterConfigState& s, uint32_t id) {
    auto found = s.shards.get(id);
    return found.is_some() ? found.unwrap().replicas : std::vector<std::string>{};
}
inline std::string cc_shard_leader(const ClusterConfigState& s, uint32_t id) {
    auto found = s.shards.get(id);
    return found.is_some() ? found.unwrap().leader : std::string();
}
inline std::string cc_shard_status(const ClusterConfigState& s, uint32_t id) {
    auto found = s.shards.get(id);
    return found.is_some() ? found.unwrap().status : std::string();
}
// The dead-shard replacement chase — authored in the inline-Rust DSL in
// cluster_config.cc. Declared here so the DSL methods below can call it.
uint32_t cc_follow_replacement(const ClusterConfigState& s, uint32_t sid);
// Rebuild the topology from a ConfigManager (declared; defined in the .cc
// where ConfigManager is a complete type).
bool cc_load_from_cm(ClusterConfigState& s, ConfigManager* cm);

// @unsafe - Workaround for a clang C++20-module defect (clang 21 & 22): the ONE
// btree MUTATION in the cluster_config.cc impl unit (cc_load_from_cm's insert)
// makes clang emit the module's reachable BTreeMap insert/split/remove generic
// lambdas (incl. <string,string> from Shard::data) IN that impl unit, which trips
// an Itanium-mangler / lambda-ODR bug. Routing the insert through this concrete,
// non-inline helper DEFINED in the interface partition keeps insert_fit's
// instantiation on the interface side (where it already occurs + compiles), so
// the impl unit emits only a call. Do NOT mark inline. See
// docs/dev/clang22-mangler-crash.md + issue shuaimu/rusty-cpp#31.
void cc_shards_insert(btree_port::BTreeMap<uint32_t, ShardInfo>& shards,
                      uint32_t id, ShardInfo info) {
    shards.insert(std::move(id), std::move(info));
}

// Hints: a lock-free summary of the state.
constexpr uint32_t CC_HINT_POPULATED = 1;        // shard_count > 0
constexpr uint32_t CC_HINT_REDIRECTS = 4;        // some shard is "dead" (may redirect)
// Runs under the caller's held guard.
inline uint32_t cc_routing_hints(const ClusterConfigState& s) {
    uint32_t hints = 0;
    if (s.shard_count > 0) hints |= CC_HINT_POPULATED;
    auto it = s.shards.iter();
    while (true) {
        auto kv = it.next();
        if (kv.is_none()) break;
        if (std::get<1>(kv.unwrap()).status == "dead") {
            hints |= CC_HINT_REDIRECTS;
            break;
        }
    }
    return hints;
}

#if RUSTYCPP_RUST
pub struct ClusterConfig {
    state: rusty::Mutex<ClusterConfigState>,
    // CC_HINT_* summary of `state`; written only under the state guard.
    hints: rusty::sync::atomic::AtomicU32,
}
impl ClusterConfig {
    fn new() -> ClusterConfig {
        ClusterConfig {
            state: rusty::Mutex::<ClusterConfigState>::new_(ClusterConfigState {
                shard_count: 0,
                version: 0,
                epoch: 0,
                shards: btree_port::BTreeMap::<u32, ShardInfo>::new_(),
            }),
            hints: rusty::sync::atomic::AtomicU32::new_(0),
        }
    }
    fn load_from_config_manager(&mut self, cm: *mut ConfigManager) -> bool {
        let mut g = (*self).state.lock().unwrap();
        let ok: bool = unsafe { cc_load_from_cm((*g), cm) };
        let h: u32 = unsafe { cc_routing_hints((*g)) };
        (*self).hints.store(h);
        ok
    }
    // The shard that serves `shard_id` now: itself, or the end of its
    // dead-shard replacement chain.
    fn resolve_live_shard(&self, shard_id: u32) -> u32 {
        let g = (*self).state.lock().unwrap();
        unsafe { cc_follow_replacement((*g), shard_id) }
    }
    // The CC_HINT_* bits: one atomic load, no lock.
    fn routing_hints(&self) -> u32 {
        (*self).hints.load()
    }
    fn get_shard_count(&self) -> u32 {
        let g = (*self).state.lock().unwrap();
        (*g).shard_count
    }
    fn get_shard_replicas(&self, shard_id: u32) -> std::vector<std::string> {
        let g = (*self).state.lock().unwrap();
        unsafe { cc_shard_replicas((*g), shard_id) }
    }
    fn get_shard_leader(&self, shard_id: u32) -> std::string {
        let g = (*self).state.lock().unwrap();
        unsafe { cc_shard_leader((*g), shard_id) }
    }
    fn get_shard_status(&self, shard_id: u32) -> std::string {
        let g = (*self).state.lock().unwrap();
        unsafe { cc_shard_status((*g), shard_id) }
    }
    fn get_version(&self) -> u64 {
        let g = (*self).state.lock().unwrap();
        (*g).version
    }
    fn get_epoch(&self) -> u64 {
        let g = (*self).state.lock().unwrap();
        (*g).epoch
    }
    fn update_shard(&mut self, id: u32, info: &ShardInfo) {
        let mut g = (*self).state.lock().unwrap();
        (*g).shards.insert(id, info);
        let h: u32 = unsafe { cc_routing_hints((*g)) };
        (*self).hints.store(h);
    }
    fn set_shard_count(&mut self, count: u32) {
        let mut g = (*self).state.lock().unwrap();
        (*g).shard_count = count;
        let h: u32 = unsafe { cc_routing_hints((*g)) };
        (*self).hints.store(h);
    }
    fn set_version(&mut self, version: u64) {
        let mut g = (*self).state.lock().unwrap();
        (*g).version = version;
    }
    fn set_epoch(&mut self, epoch: u64) {
        let mut g = (*self).state.lock().unwrap();
        (*g).epoch = epoch;
    }
}
#endif
/*RUSTYCPP:GEN-BEGIN id=cluster_config.1 version=1 rust_sha256=4d1e9e8363e8d3d6c52ee29a972d0af848adac5eb3b607663552a3955e2e60eb*/
struct ClusterConfig;

struct ClusterConfig {
    rusty::Mutex<ClusterConfigState> state;
    rusty::sync::atomic::AtomicU32 hints;

    static ClusterConfig new_();
    bool load_from_config_manager(ConfigManager* cm);
    uint32_t resolve_live_shard(uint32_t shard_id) const;
    uint32_t routing_hints() const;
    uint32_t get_shard_count() const;
    std::vector<std::string> get_shard_replicas(uint32_t shard_id) const;
    std::string get_shard_leader(uint32_t shard_id) const;
    std::string get_shard_status(uint32_t shard_id) const;
    uint64_t get_version() const;
    uint64_t get_epoch() const;
    void update_shard(uint32_t id, const ShardInfo& info);
    void set_shard_count(uint32_t count);
    void set_version(uint64_t version);
    void set_epoch(uint64_t epoch);
};

ClusterConfig ClusterConfig::new_() {
    return ClusterConfig{.state = rusty::Mutex<ClusterConfigState>::new_(ClusterConfigState{.shard_count = 0, .version = 0, .epoch = 0, .shards = btree_port::BTreeMap<uint32_t, ShardInfo>::new_()}), .hints = rusty::sync::atomic::AtomicU32::new_(0)};
}

bool ClusterConfig::load_from_config_manager(ConfigManager* cm) {
    auto&& g = rusty::deref_call(((*this)).state.lock(), rusty::detail::__mdisp_unwrap{});
    bool ok = cc_load_from_cm((rusty::detail::deref_if_pointer_like(g)), cm);
    const uint32_t h = cc_routing_hints((rusty::detail::deref_if_pointer_like(g)));
    ((*this)).hints.store(std::move(h));
    return std::move(ok);
}

uint32_t ClusterConfig::resolve_live_shard(uint32_t shard_id) const {
    const auto&& g = rusty::deref_call(((*this)).state.lock(), rusty::detail::__mdisp_unwrap{});
    // @unsafe
    {
        return cc_follow_replacement((rusty::detail::deref_if_pointer_like(g)), std::move(shard_id));
    }
}

uint32_t ClusterConfig::routing_hints() const {
    return ((*this)).hints.load();
}

uint32_t ClusterConfig::get_shard_count() const {
    const auto&& g = rusty::deref_call(((*this)).state.lock(), rusty::detail::__mdisp_unwrap{});
    return (rusty::detail::deref_if_pointer_like(g)).shard_count;
}

std::vector<std::string> ClusterConfig::get_shard_replicas(uint32_t shard_id) const {
    const auto&& g = rusty::deref_call(((*this)).state.lock(), rusty::detail::__mdisp_unwrap{});
    // @unsafe
    {
        return cc_shard_replicas((rusty::detail::deref_if_pointer_like(g)), std::move(shard_id));
    }
}

std::string ClusterConfig::get_shard_leader(uint32_t shard_id) const {
    const auto&& g = rusty::deref_call(((*this)).state.lock(), rusty::detail::__mdisp_unwrap{});
    // @unsafe
    {
        return cc_shard_leader((rusty::detail::deref_if_pointer_like(g)), std::move(shard_id));
    }
}

std::string ClusterConfig::get_shard_status(uint32_t shard_id) const {
    const auto&& g = rusty::deref_call(((*this)).state.lock(), rusty::detail::__mdisp_unwrap{});
    // @unsafe
    {
        return cc_shard_status((rusty::detail::deref_if_pointer_like(g)), std::move(shard_id));
    }
}

uint64_t ClusterConfig::get_version() const {
    const auto&& g = rusty::deref_call(((*this)).state.lock(), rusty::detail::__mdisp_unwrap{});
    return (rusty::detail::deref_if_pointer_like(g)).version;
}

uint64_t ClusterConfig::get_epoch() const {
    const auto&& g = rusty::deref_call(((*this)).state.lock(), rusty::detail::__mdisp_unwrap{});
    return (rusty::detail::deref_if_pointer_like(g)).epoch;
}

void ClusterConfig::update_shard(uint32_t id, const ShardInfo& info) {
    auto&& g = rusty::deref_call(((*this)).state.lock(), rusty::detail::__mdisp_unwrap{});
    (rusty::detail::deref_if_pointer_like(g)).shards.insert(std::move(id), std::move(info));
    const uint32_t h = cc_routing_hints((rusty::detail::deref_if_pointer_like(g)));
    ((*this)).hints.store(std::move(h));
}

void ClusterConfig::set_shard_count(uint32_t count) {
    auto&& g = rusty::deref_call(((*this)).state.lock(), rusty::detail::__mdisp_unwrap{});
    (rusty::detail::deref_if_pointer_like(g)).shard_count = std::move(count);
    const uint32_t h = cc_routing_hints((rusty::detail::deref_if_pointer_like(g)));
    ((*this)).hints.store(std::move(h));
}

void ClusterConfig::set_version(uint64_t version) {
    auto&& g = rusty::deref_call(((*this)).state.lock(), rusty::detail::__mdisp_unwrap{});
    (rusty::detail::deref_if_pointer_like(g)).version = std::move(version);
}

void ClusterConfig::set_epoch(uint64_t epoch) {
    auto&& g = rusty::deref_call(((*this)).state.lock(), rusty::detail::__mdisp_unwrap{});
    (rusty::detail::deref_if_pointer_like(g)).epoch = std::move(epoch);
}
/*RUSTYCPP:GEN-END id=cluster_config.1*/

// Process-global ClusterConfig — this node's copy of the config table,
// populated by the ConfigWatcher.
// @safe - function-local static
inline ClusterConfig& get_cluster_config() {
    static ClusterConfig instance = ClusterConfig::new_();
    return instance;
}

}  // namespace janus
