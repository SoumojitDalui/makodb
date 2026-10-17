#include "cluster_bootstrap.h"

#include <stdlib.h>  // getenv, strtol
#include <string.h>  // strcmp
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "storage/abstract_db.h"           // abstract_db, abstract_ordered_index
#include "ordered_index_kv_store.h"        // OrderedIndexKvStore
#include "benchmarks/benchmark_config.h"   // BenchmarkConfig, transport::Configuration

import cluster;   // config/sharding metadata module (was #include "cluster/...")

#include "deptran/config_kv_service.h"     // ConfigKvServiceImpl, ConfigKvServiceProxy

#include "srpc/srpc.hpp"                      // srpc::Server / Client / PollThread
#include "srpc_log.h"

namespace janus {
namespace {

// Where shard 0's config service listens. Every process derives it the
// same way from the shared shard config (--shard-config, which lists every
// shard's address per role), so non-shard-0 nodes find shard 0 instead of
// their own shard. The port is MAKO_CLUSTER_CONFIG_PORT when set, otherwise
// kConfigKvPortOffset above shard 0's preferred-leader base port. Shard
// configs space shards 100 ports apart and a shard's own servers use base +
// [0, warehouses + rpc servers + 5), so the offset sits above those and
// below the next shard. (The previous derivation, the Paxos site port +
// 20000, exceeds 65535 for the standard Paxos range starting at 45001, and
// used the local shard's Paxos config, so other shards resolved their own
// leader.)
constexpr int kConfigKvPortOffset = 90;

// How often a node re-polls shard 0 for a config-version change.
constexpr uint64_t kConfigPollIntervalMs = 1000;

// How long a node waits between attempts to reach shard 0's config service.
constexpr auto kConfigReconnectInterval = std::chrono::seconds(1);

// Shard 0's config store as the leader uses it: every write goes to the
// __mako_config__ Mako index (the record) and to an in-memory mirror, and
// every read is served from the mirror. The readers are the ConfigWatcher
// poll thread and the ConfigKvService handler thread, which are not
// transaction-engine threads, and an index op needs engine thread state (an
// STO thread id and a Masstree threadinfo). Registering those two threads
// with the engine works, but on the two-shard TPC-C bench it cost about 19%
// of throughput on both shards while they sat idle between reads, so they
// never touch the index. Writes must come from an engine-registered thread;
// today the only writer is the bootstrap thread, which seeds the store
// before any reader starts.
// @unsafe - writes through to a Mako index
class MirroredConfigStore : public KvStore {
public:
    explicit MirroredConfigStore(KvStore* record) : record_(record) {}
    ~MirroredConfigStore() noexcept override {}

    rusty::Option<std::string> get(const std::string& key) override {
        std::lock_guard<std::mutex> lock(mu_);
        return mirror_.get(key);
    }

    void put(const std::string& key, const std::string& value) override {
        record_->put(key, value);
        std::lock_guard<std::mutex> lock(mu_);
        mirror_.put(key, value);
    }

    void remove(const std::string& key) override {
        record_->remove(key);
        std::lock_guard<std::mutex> lock(mu_);
        mirror_.remove(key);
    }

private:
    KvStore* record_;   // non-owning: the OrderedIndexKvStore over the index
    std::mutex mu_;
    InMemoryKvStore mirror_;
};

// ---- Process-lifetime singletons (this wiring runs once per node) ------
// File scope, mirroring config_node_init.cc's g_config_* pattern. The
// Boxes give stable addresses for the raw-pointer cross-references below
// (ConfigManager borrows the KvStore; ConfigWatcher borrows both the
// ConfigManager and the routing cache).
rusty::Option<rusty::Arc<srpc::PollThread>> g_cfg_poll;
srpc::Server* g_cfg_server = nullptr;                          // shard-0 leader only
rusty::Option<rusty::Box<OrderedIndexKvStore>> g_cfg_kv_record;  // shard-0 leader
rusty::Option<rusty::Box<MirroredConfigStore>> g_cfg_kv_local;   // shard-0 leader
rusty::Option<rusty::Box<RemoteKvStore>> g_cfg_kv_remote;        // other nodes
rusty::Option<rusty::Box<ConfigManager>> g_cfg_cm;
rusty::Option<rusty::Box<ConfigWatcher>> g_cfg_watcher;

// @safe - env-var read
bool cluster_config_enabled() {
    const char* v = getenv("MAKO_CLUSTER_CONFIG");
    return v != nullptr && strcmp(v, "1") == 0;
}

struct ConfigEndpoint {
    std::string host;
    int port = 0;
};

// Shard 0's config-service endpoint, or false (with an error logged) when it
// cannot be determined safely. The feature then stays off on this node
// rather than running half-wired.
// @unsafe - env read, shared-config lookup
bool ResolveConfigEndpoint(ConfigEndpoint* out) {
    auto& bench = BenchmarkConfig::getInstance();
    transport::Configuration* tc = bench.getConfig();
    if (tc == nullptr || !tc->HasShard(0, mako::LOCALHOST_CENTER_INT)) {
        srpc::Log_error("BootstrapClusterConfig: shard config has no address for shard 0; "
                        "cluster config disabled");
        return false;
    }
    transport::ShardAddress leader0 = tc->shard(0, mako::LOCALHOST_CENTER_INT);
    out->host = leader0.host;

    const char* env = getenv("MAKO_CLUSTER_CONFIG_PORT");
    long port = 0;
    if (env != nullptr) {
        char* end = nullptr;
        port = strtol(env, &end, 10);
        if (end == env || *end != '\0') port = -1;
    } else {
        const long used = static_cast<long>(tc->warehouses)
                        + static_cast<long>(bench.getNumRpcServer()) + 5;
        if (used >= kConfigKvPortOffset) {
            srpc::Log_error("BootstrapClusterConfig: shard 0's servers may use up to base+{}, "
                            "past the default config-service offset +{}; set "
                            "MAKO_CLUSTER_CONFIG_PORT. Cluster config disabled",
                            used, kConfigKvPortOffset);
            return false;
        }
        port = atol(leader0.port.c_str()) + kConfigKvPortOffset;
    }
    if (port < 1 || port > 65535) {
        srpc::Log_error("BootstrapClusterConfig: config-service port {} is not a valid port "
                        "(MAKO_CLUSTER_CONFIG_PORT={}); cluster config disabled",
                        port, env != nullptr ? env : "unset");
        return false;
    }
    out->port = static_cast<int>(port);
    return true;
}

// Every replica of one shard as (site name, ip:port), preferred leader
// first, from the shared shard config. New-format configs carry globally
// unique site names; old-format configs only list addresses per role, so
// the name is shard-qualified ("shard1-p2") to stay unique across shards.
struct ReplicaAddr {
    std::string name;
    std::string addr;
};

// @unsafe - shared-config lookup
std::vector<ReplicaAddr> ShardReplicas(transport::Configuration* tc, uint32_t sid) {
    std::vector<ReplicaAddr> out;
    if (tc->is_new_format) {
        for (transport::SiteInfo* site : tc->GetReplicasForShard(static_cast<int>(sid))) {
            out.push_back({site->name, site->ip + ":" + std::to_string(site->port)});
        }
        return out;
    }
    static const struct { int role; const char* name; } kRoles[] = {
        {mako::LOCALHOST_CENTER_INT, "localhost"},   // preferred leader
        {mako::P1_CENTER_INT, "p1"},
        {mako::P2_CENTER_INT, "p2"},
        {mako::LEARNER_CENTER_INT, "learner"},
    };
    for (const auto& r : kRoles) {
        if (!tc->HasShard(static_cast<int>(sid), r.role)) continue;
        transport::ShardAddress a = tc->shard(static_cast<int>(sid), r.role);
        out.push_back({"shard" + std::to_string(sid) + "-" + r.name, a.host + ":" + a.port});
    }
    return out;
}

// Seed shard 0's config store from the shared shard config so other nodes
// have a complete snapshot to read on their first poll: per shard its
// replicas (leader first), leader and status, plus every replica's address.
// Blind puts; __version__ is bumped last (set_shard_count), so a reader that
// observes the new version sees every key of it. No per-table sharding
// policy is seeded, so routing keeps today's table/warehouse placement (see
// compute_shard_for_key).
// @unsafe - KvStore writes via ConfigManager
void SeedTopology(ConfigManager* cm, uint32_t nshards) {
    transport::Configuration* tc = BenchmarkConfig::getInstance().getConfig();
    for (uint32_t sid = 0; sid < nshards; ++sid) {
        std::vector<ReplicaAddr> replicas = ShardReplicas(tc, sid);
        std::vector<std::string> names;
        for (const auto& r : replicas) {
            names.push_back(r.name);
            cm->set_node_addr(r.name, r.addr);
            cm->set_node_status(r.name, "alive");
        }
        cm->set_shard_replicas(sid, names);
        cm->set_shard_leader(sid, names.empty() ? std::string() : names.front());
        cm->set_shard_status(sid, "active");
    }
    cm->set_sharding_mode("hash");
    cm->set_shard_count(nshards);  // version-bumping write, done last
}


// Connection to shard 0's config service that reconnects. A node can come
// up before shard 0's service is listening (shard 0's own followers start
// alongside its leader, which binds the service only after setup2), and the
// service can go away and come back, so a failed connect or read is retried
// on later polls instead of ending the watch. Only the ConfigWatcher's poll
// thread calls read(), so no locking is needed.
// @unsafe - RPC client lifecycle
class RemoteConfigConnection {
public:
    explicit RemoteConfigConnection(std::string addr)
        : addr_(std::move(addr)), poll_(srpc::PollThread::create()) {}

    rusty::Option<std::string> read(const std::string& key) {
        if (!proxy_ && !try_connect()) return rusty::None;
        ConfigKvServiceProxy::RpcReadConfigKeyRequest req;
        req.key = key;
        auto r = proxy_->ReadConfigKey(req);   // synchronous
        if (r.is_err()) {
            drop();
            return rusty::None;
        }
        if (!serving_) {
            srpc::Log_info("BootstrapClusterConfig: reading shard-0 config from {}", addr_.c_str());
            serving_ = true;
            waiting_logged_ = false;
        }
        auto resp = r.unwrap();
        if (!resp.found) return rusty::None;
        return rusty::Some(std::move(resp.value));
    }

private:
    bool try_connect() {
        const auto now = std::chrono::steady_clock::now();
        if (now < next_attempt_) return false;
        next_attempt_ = now + kConfigReconnectInterval;
        auto client = srpc::Client::create(poll_.clone());
        if (client->connect(reinterpret_cast<const int8_t*>(addr_.c_str()), false) != 0) {
            client->close();
            if (!waiting_logged_) {
                srpc::Log_warn("BootstrapClusterConfig: shard-0 config at {} not reachable yet; "
                               "retrying every second", addr_.c_str());
                waiting_logged_ = true;
            }
            return false;
        }
        client_ = rusty::Some(std::move(client));
        proxy_ = std::make_unique<ConfigKvServiceProxy>(
            const_cast<srpc::Client*>(client_.as_ref().unwrap().get()));
        return true;
    }

    // A successful connect only means the port accepted: a leader that has
    // crashed but is still writing its core keeps its listening socket open
    // and times out every read. So state is logged on read outcomes, once
    // per transition, not on each connect.
    void drop() {
        if (serving_) {
            srpc::Log_warn("BootstrapClusterConfig: lost shard-0 config at {}; will reconnect",
                           addr_.c_str());
            serving_ = false;
        }
        proxy_.reset();
        if (client_.is_some()) {
            client_.as_ref().unwrap()->close();
            client_ = rusty::None;
        }
    }

    std::string addr_;
    rusty::Arc<srpc::PollThread> poll_;
    rusty::Option<rusty::Arc<srpc::Client>> client_;
    std::unique_ptr<ConfigKvServiceProxy> proxy_;
    std::chrono::steady_clock::time_point next_attempt_{};
    bool waiting_logged_ = false;
    bool serving_ = false;   // last read RPC succeeded
};

RemoteConfigConnection* g_cfg_remote = nullptr;   // other nodes; lives for the process

// Shard 0's leader: owns the config store, serves reads, and keeps its
// own routing cache fresh from the local store (no self-RPC).
// @unsafe - storage index open, RPC server bind, background thread
void StartShard0Leader(abstract_db* db, uint32_t nshards, const ConfigEndpoint& ep) {
    abstract_ordered_index* idx = db->open_index("__mako_config__", /*shard_index=*/0);
    if (idx == nullptr) {
        srpc::Log_error("BootstrapClusterConfig: could not open __mako_config__ index; "
                        "cluster config disabled");
        return;
    }
    g_cfg_kv_record = rusty::Some(rusty::make_box<OrderedIndexKvStore>(idx));
    g_cfg_kv_local = rusty::Some(rusty::make_box<MirroredConfigStore>(
        g_cfg_kv_record.as_ref().unwrap().get()));
    KvStore* kv = g_cfg_kv_local.as_ref().unwrap().get();

    g_cfg_cm = rusty::Some(rusty::make_box<ConfigManager>(kv));
    ConfigManager* cm = g_cfg_cm.as_ref().unwrap().get();
    SeedTopology(cm, nshards);

    // Local routing cache first, so this node routes by the config even if
    // the service below fails to bind.
    g_cfg_watcher = rusty::Some(rusty::make_box<ConfigWatcher>(
        ConfigWatcher::new_(cm, &get_cluster_config(), kConfigPollIntervalMs)));
    g_cfg_watcher.as_ref().unwrap()->poll();   // prime the cache immediately
    g_cfg_watcher.as_ref().unwrap()->start();

    // Dedicated RPC server for config reads (config_node_init.cc pattern).
    std::string bind_addr = "0.0.0.0:" + std::to_string(ep.port);
    g_cfg_poll = rusty::Some(srpc::PollThread::create());
    g_cfg_server = new srpc::Server(srpc::Server::new_(rusty::Some(g_cfg_poll.as_ref().unwrap().clone())));
    g_cfg_server->reg_service_typed(rusty::make_box<ConfigKvServiceImpl>(kv));
    if (g_cfg_server->start(reinterpret_cast<const int8_t*>(bind_addr.c_str())) != 0) {
        srpc::Log_error("BootstrapClusterConfig: config server failed to bind {}; other nodes "
                        "cannot read the cluster config", bind_addr.c_str());
        return;
    }
    srpc::Log_info("BootstrapClusterConfig: shard-0 config service listening on {}",
                   bind_addr.c_str());
}

// Every other node (shard-0 followers + all non-zero shards): read shard
// 0's config over RPC and watch it for changes. The watcher starts even if
// shard 0 is not reachable yet; RemoteConfigConnection keeps retrying.
// @unsafe - RPC client, background thread
void StartRemoteWatcher(const ConfigEndpoint& ep) {
    const std::string addr = ep.host + ":" + std::to_string(ep.port);
    g_cfg_remote = new RemoteConfigConnection(addr);
    RemoteConfigConnection* conn = g_cfg_remote;

    RemoteKvStoreReadFn read_fn =
        [conn](const std::string& key) -> rusty::Option<std::string> {
            return conn->read(key);
        };
    g_cfg_kv_remote = rusty::Some(rusty::make_box<RemoteKvStore>(std::move(read_fn)));
    g_cfg_cm = rusty::Some(rusty::make_box<ConfigManager>(
        g_cfg_kv_remote.as_ref().unwrap().get()));
    g_cfg_watcher = rusty::Some(rusty::make_box<ConfigWatcher>(
        ConfigWatcher::new_(g_cfg_cm.as_ref().unwrap().get(), &get_cluster_config(), kConfigPollIntervalMs)));
    g_cfg_watcher.as_ref().unwrap()->start();
    srpc::Log_info("BootstrapClusterConfig: watching shard-0 config at {}", addr.c_str());
}

}  // namespace

// @unsafe - see per-branch helpers
void BootstrapClusterConfig(abstract_db* db) {
    static bool done = false;
    if (done) return;
    done = true;

    if (!cluster_config_enabled()) return;

    auto& bench = BenchmarkConfig::getInstance();
    const uint32_t nshards = static_cast<uint32_t>(bench.getNshards());
    if (nshards <= 1) return;  // sharding not in play; legacy routing stands

    ConfigEndpoint ep;
    if (!ResolveConfigEndpoint(&ep)) return;

    const bool is_shard0_leader =
        (bench.getShardIndex() == 0) && (bench.getLeaderConfig() != 0);

    if (is_shard0_leader) {
        StartShard0Leader(db, nshards, ep);
    } else {
        StartRemoteWatcher(ep);
    }
}

}  // namespace janus
