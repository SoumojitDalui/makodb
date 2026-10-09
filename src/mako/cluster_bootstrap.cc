#include "cluster_bootstrap.h"

#include <stdlib.h>  // getenv, strtol, atol
#include <string.h>  // strcmp
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "storage/abstract_db.h"           // abstract_db, abstract_ordered_index
#include "ordered_index_kv_store.h"        // OrderedIndexKvStore
#include "benchmarks/benchmark_config.h"   // BenchmarkConfig, transport::Configuration
#include "lib/common.h"                    // mako::CONFIG_TABLE_ID
#include "sto/function_pool.h"             // actual_directs (Masstree thread init), TThread

import cluster;   // config/sharding metadata module (was #include "cluster/...")

#include "deptran/config_kv_service.h"     // ConfigKvServiceImpl, ConfigKvServiceProxy

#include "srpc/srpc.hpp"                      // srpc::Server / Client / PollThread
#include "srpc_log.h"

namespace janus {
namespace {

// Where shard 0's config service listens. Every shard-0 replica has an
// endpoint, because whichever replica leads shard 0 serves the config: the
// preferred leader from startup, another replica after a failover. Every
// process derives the same list from the shared shard config
// (--shard-config, which lists every shard's address per role), so
// non-shard-0 nodes find shard 0 instead of their own shard. A replica's
// port is kConfigKvPortOffset above its own base port, or
// MAKO_CLUSTER_CONFIG_PORT plus its position in shard 0's replica list when
// that is set. Shard configs give each shard and role its own block of 100
// ports and a process's servers use base + [0, warehouses + rpc servers +
// 5), so the offset sits above those and below the next block. (The
// previous derivation, the Paxos site port + 20000, exceeds 65535 for the
// standard Paxos range starting at 45001, and used the local shard's Paxos
// config, so other shards resolved their own leader.)
constexpr int kConfigKvPortOffset = 90;

// How often a node re-polls shard 0 for a config-version change.
constexpr uint64_t kConfigPollIntervalMs = 1000;

// How long a node waits between passes over shard 0's endpoints when none
// of them answers.
constexpr auto kConfigReconnectInterval = std::chrono::seconds(1);

// The engine thread id the config-table thread runs as. On a follower,
// replay threads set their ids directly (0..nthreads-1), so an id from the
// shared counter in abstract_db::thread_init could collide with one; this
// one is reserved.
constexpr int kConfigThreadId = MAX_THREADS - 1;

// Shard 0's config store: the config table (__mako_config__, the reserved
// table id mako::CONFIG_TABLE_ID) owned by one thread registered with the
// transaction engine. An index op needs engine thread state (an STO thread
// id and a Masstree threadinfo), and the callers here are not engine
// threads (the bootstrap and promotion threads, the ConfigWatcher poll
// thread, the ConfigKvService handler thread), so they hand each op to the
// table thread and wait for it. Every write is a one-key transaction whose
// log entry the table thread pushes to shard 0's Paxos stream for partition
// 0 as soon as it commits, so shard 0's followers replay the table like
// any other and a replica promoted to leader already holds the config.
//
// A removal is written as kRemovedMarker instead of deleting the key:
// replay turns a deleted key into a tombstone value rather than removing
// it, while a marker reads the same on every replica.
// @unsafe - engine thread, Mako index ops
class ConfigTableStore : public KvStore {
public:
    explicit ConfigTableStore(abstract_ordered_index* idx)
        : record_(idx), thread_([this] { run(); }) {
        thread_.detach();   // serves the config for the life of the process
    }
    ~ConfigTableStore() noexcept override {}

    rusty::Option<std::string> get(const std::string& key) override {
        rusty::Option<std::string> out = rusty::None;
        call([&] {
            auto v = record_.get(key);
            if (v.is_some() && v.as_ref().unwrap() != kRemovedMarker) out = std::move(v);
        });
        return out;
    }

    void put(const std::string& key, const std::string& value) override {
        call([&] { record_.put(key, value); });
    }

    void remove(const std::string& key) override {
        call([&] { record_.put(key, kRemovedMarker); });
    }

private:
    struct Job {
        std::function<void()> op;
        bool done = false;
    };

    // Run op on the table thread and wait for it.
    void call(std::function<void()> op) {
        Job job{std::move(op)};
        std::unique_lock<std::mutex> lock(mu_);
        jobs_.push_back(&job);
        cv_.notify_all();
        cv_.wait(lock, [&] { return job.done; });
    }

    void run() {
        RegisterEngineThread();
        std::unique_lock<std::mutex> lock(mu_);
        for (;;) {
            cv_.wait(lock, [&] { return !jobs_.empty(); });
            Job* job = jobs_.front();
            jobs_.pop_front();
            lock.unlock();
            job->op();
            lock.lock();
            job->done = true;
            cv_.notify_all();
        }
    }

    // Engine thread state as replay threads get it (ThreadDBWrapperMbta),
    // with the reserved id, shard 0's Paxos partition 0 for its log
    // entries, and every commit pushed at once.
    static void RegisterEngineThread() {
        auto& bench = BenchmarkConfig::getInstance();
        TThread::set_id(kConfigThreadId);
        TThread::set_pid(0);
        TThread::set_mode(0);
        TThread::set_shard_index(bench.getShardIndex());
        TThread::set_nshards(bench.getNshards());
        TThread::set_warehouses(bench.getConfig()->warehouses);
        TThread::disable_multiversion();
        TThread::push_log_each_commit = true;
        actual_directs::thread_init();
    }

    static inline const std::string kRemovedMarker = std::string("\0mako-config-removed\0", 21);

    OrderedIndexKvStore record_;
    std::mutex mu_;
    std::condition_variable cv_;
    std::deque<Job*> jobs_;
    std::thread thread_;   // last: started once the members above exist
};

// One shard-0 replica's config-service endpoint. name matches the replica
// names SeedTopology writes (ShardReplicas).
struct ConfigEndpoint {
    std::string name;
    std::string host;
    int port = 0;

    std::string addr() const { return host + ":" + std::to_string(port); }
};

// ---- Process-lifetime singletons (this wiring runs once per node) ------
// File scope, mirroring config_node_init.cc's g_config_* pattern. The
// Boxes give stable addresses for the raw-pointer cross-references below
// (ConfigManager borrows the KvStore; ConfigWatcher borrows both the
// ConfigManager and the routing cache). g_cfg_mu serializes the bootstrap
// with a later promotion (PromoteClusterConfigLeader).
std::mutex g_cfg_mu;
bool g_cfg_active = false;    // the cluster-config wiring is up on this node
bool g_cfg_leading = false;   // this node is shard 0's config leader
uint32_t g_cfg_nshards = 0;
std::vector<ConfigEndpoint> g_cfg_endpoints;   // shard 0's replicas, preferred leader first
rusty::Option<rusty::Arc<srpc::PollThread>> g_cfg_poll;
srpc::Server* g_cfg_server = nullptr;                          // config leader only
abstract_db* g_cfg_db = nullptr;                               // holds the config table
ConfigTableStore* g_cfg_store = nullptr;                       // config leader; lives for the process
rusty::Option<rusty::Box<RemoteKvStore>> g_cfg_kv_remote;        // other nodes
rusty::Option<rusty::Box<ConfigManager>> g_cfg_cm;
rusty::Option<rusty::Box<ConfigWatcher>> g_cfg_watcher;

// @safe - env-var read
bool cluster_config_enabled() {
    const char* v = getenv("MAKO_CLUSTER_CONFIG");
    return v != nullptr && strcmp(v, "1") == 0;
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

// Every shard-0 replica's config-service endpoint, preferred leader first,
// or false (with an error logged) when they cannot be determined safely.
// The feature then stays off on this node rather than running half-wired.
// @unsafe - env read, shared-config lookup
bool ResolveConfigEndpoints(std::vector<ConfigEndpoint>* out) {
    auto& bench = BenchmarkConfig::getInstance();
    transport::Configuration* tc = bench.getConfig();
    std::vector<ReplicaAddr> replicas;
    if (tc != nullptr) replicas = ShardReplicas(tc, 0);
    if (replicas.empty()) {
        srpc::Log_error("BootstrapClusterConfig: shard config has no address for shard 0; "
                        "cluster config disabled");
        return false;
    }

    const char* env = getenv("MAKO_CLUSTER_CONFIG_PORT");
    long env_port = 0;
    if (env != nullptr) {
        char* end = nullptr;
        env_port = strtol(env, &end, 10);
        if (end == env || *end != '\0') env_port = -1;
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
    }

    out->clear();
    for (size_t i = 0; i < replicas.size(); ++i) {
        const std::string& addr = replicas[i].addr;
        const size_t colon = addr.rfind(':');
        long port = -1;
        if (colon != std::string::npos) {
            if (env == nullptr) {
                port = atol(addr.c_str() + colon + 1) + kConfigKvPortOffset;
            } else if (env_port > 0) {
                port = env_port + static_cast<long>(i);
            }
        }
        if (port < 1 || port > 65535) {
            srpc::Log_error("BootstrapClusterConfig: config-service port {} for {} is not a valid "
                            "port (MAKO_CLUSTER_CONFIG_PORT={}); cluster config disabled",
                            port, replicas[i].name.c_str(), env != nullptr ? env : "unset");
            return false;
        }
        ConfigEndpoint ep;
        ep.name = replicas[i].name;
        ep.host = addr.substr(0, colon);
        ep.port = static_cast<int>(port);
        out->push_back(std::move(ep));
    }
    return true;
}

// This process's name in its shard's replica list (see ShardReplicas): the
// site name (-P) for new-format configs, "shard<i>-<role>" otherwise.
// @safe - config read
std::string MyReplicaName() {
    auto& bench = BenchmarkConfig::getInstance();
    transport::Configuration* tc = bench.getConfig();
    if (tc != nullptr && tc->is_new_format) return bench.getPaxosProcName();
    return "shard" + std::to_string(bench.getShardIndex()) + "-" + bench.getCluster();
}

// @safe - lookup in the resolved endpoint list
const ConfigEndpoint* FindEndpoint(const std::string& name) {
    for (const auto& ep : g_cfg_endpoints) {
        if (ep.name == name) return &ep;
    }
    return nullptr;
}

// Seed shard 0's config store from the shared shard config so other nodes
// have a complete snapshot to read on their first poll: per shard its
// replicas (leader first), leader and status, plus every replica's address.
// shard0_leader overrides shard 0's leader (a promoted replica names
// itself); empty means the preferred leader. Blind puts; __version__ is
// bumped last (set_shard_count), so a reader that observes the new version
// sees every key of it. No per-table sharding policy is seeded, so routing
// keeps today's table/warehouse placement (see compute_shard_for_key).
// @unsafe - KvStore writes via ConfigManager
void SeedTopology(ConfigManager* cm, uint32_t nshards, const std::string& shard0_leader) {
    transport::Configuration* tc = BenchmarkConfig::getInstance().getConfig();
    for (uint32_t sid = 0; sid < nshards; ++sid) {
        std::vector<ReplicaAddr> replicas = ShardReplicas(tc, sid);
        std::vector<std::string> names;
        for (const auto& r : replicas) {
            names.push_back(r.name);
            cm->set_node_addr(r.name, r.addr);
            cm->set_node_status(r.name, "alive");
        }
        std::string leader = names.empty() ? std::string() : names.front();
        if (sid == 0 && !shard0_leader.empty()) leader = shard0_leader;
        cm->set_shard_replicas(sid, names);
        cm->set_shard_leader(sid, leader);
        cm->set_shard_status(sid, "active");
    }
    cm->set_sharding_mode("hash");
    cm->set_shard_count(nshards);  // version-bumping write, done last
}

// Connection to shard 0's config service that follows shard 0's leader. A
// node can come up before the service is listening (shard 0's own
// followers start alongside its leader, which binds the service only after
// setup2), the service can go away and come back, and after a failover a
// different shard-0 replica serves it. So the connection tries shard 0's
// replicas in turn, starting with the last one that answered, and a failed
// connect or read is retried on later polls instead of ending the watch.
// Only the ConfigWatcher's poll thread calls read(), so no locking is needed.
// @unsafe - RPC client lifecycle
class RemoteConfigConnection {
public:
    explicit RemoteConfigConnection(std::vector<std::string> addrs)
        : addrs_(std::move(addrs)), poll_(srpc::PollThread::create()) {}

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
            srpc::Log_info("BootstrapClusterConfig: reading shard-0 config from {}",
                           addrs_[cur_].c_str());
            serving_ = true;
            waiting_logged_ = false;
        }
        auto resp = r.unwrap();
        if (!resp.found) return rusty::None;
        return rusty::Some(std::move(resp.value));
    }

private:
    // One pass over the endpoints, starting at the current one, and at most
    // one pass per kConfigReconnectInterval while none answers. A replica
    // that does not lead shard 0 does not listen, so its connect fails fast.
    bool try_connect() {
        const auto now = std::chrono::steady_clock::now();
        if (now < next_attempt_) return false;
        for (size_t n = 0; n < addrs_.size(); ++n) {
            const size_t i = (cur_ + n) % addrs_.size();
            auto client = srpc::Client::create(poll_.clone());
            if (client->connect(reinterpret_cast<const int8_t*>(addrs_[i].c_str()), false) != 0) {
                client->close();
                continue;
            }
            cur_ = i;
            client_ = rusty::Some(std::move(client));
            proxy_ = std::make_unique<ConfigKvServiceProxy>(
                const_cast<srpc::Client*>(client_.as_ref().unwrap().get()));
            return true;
        }
        next_attempt_ = now + kConfigReconnectInterval;
        if (!waiting_logged_) {
            std::string all;
            for (const auto& a : addrs_) all += (all.empty() ? "" : ", ") + a;
            srpc::Log_warn("BootstrapClusterConfig: no shard-0 replica serves the cluster config "
                           "yet ({}); retrying every second", all.c_str());
            waiting_logged_ = true;
        }
        return false;
    }

    // A successful connect only means the port accepted: a leader that has
    // crashed but is still writing its core keeps its listening socket open
    // and times out every read. So state is logged on read outcomes, once
    // per transition, not on each connect. The next pass starts with the
    // next replica, where a newly promoted leader would be.
    void drop() {
        if (serving_) {
            srpc::Log_warn("BootstrapClusterConfig: lost shard-0 config at {}; trying shard 0's "
                           "replicas", addrs_[cur_].c_str());
            serving_ = false;
        }
        proxy_.reset();
        if (client_.is_some()) {
            client_.as_ref().unwrap()->close();
            client_ = rusty::None;
        }
        cur_ = (cur_ + 1) % addrs_.size();
    }

    std::vector<std::string> addrs_;   // shard 0's replicas, preferred leader first
    size_t cur_ = 0;                   // endpoint tried first
    rusty::Arc<srpc::PollThread> poll_;
    rusty::Option<rusty::Arc<srpc::Client>> client_;
    std::unique_ptr<ConfigKvServiceProxy> proxy_;
    std::chrono::steady_clock::time_point next_attempt_{};
    bool waiting_logged_ = false;
    bool serving_ = false;   // last read RPC succeeded
};

RemoteConfigConnection* g_cfg_remote = nullptr;   // other nodes; lives for the process

// The config leader's routing cache, fed from its own store (no self-RPC).
// @unsafe - background thread
void StartLocalWatcher(ConfigManager* cm) {
    g_cfg_watcher = rusty::Some(rusty::make_box<ConfigWatcher>(
        ConfigWatcher::new_(cm, &get_cluster_config(), kConfigPollIntervalMs)));
    g_cfg_watcher.as_ref().unwrap()->poll();   // prime the cache immediately
    g_cfg_watcher.as_ref().unwrap()->start();
}

// Dedicated RPC server for config reads (config_node_init.cc pattern).
// @unsafe - RPC server bind
void StartConfigService(KvStore* kv, int port) {
    std::string bind_addr = "0.0.0.0:" + std::to_string(port);
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

// The config table on this replica: the reserved id in the database the
// replication log replays into, which is also where the bootstrap opens it
// on the preferred leader.
// @unsafe - storage lookup
abstract_ordered_index* ConfigTable() {
    return g_cfg_db == nullptr ? nullptr : g_cfg_db->get_index_by_table_id(mako::CONFIG_TABLE_ID);
}

// Shard 0's preferred leader: seeds the config table, serves reads, and
// keeps its own routing cache fresh from it.
// @unsafe - storage index, RPC server bind, background threads
void StartShard0Leader(uint32_t nshards, const ConfigEndpoint& ep) {
    abstract_ordered_index* idx = ConfigTable();
    if (idx == nullptr) {
        srpc::Log_error("BootstrapClusterConfig: no config table (id {}); cluster config disabled",
                        mako::CONFIG_TABLE_ID);
        return;
    }
    g_cfg_store = new ConfigTableStore(idx);
    g_cfg_cm = rusty::Some(rusty::make_box<ConfigManager>(g_cfg_store));
    SeedTopology(g_cfg_cm.as_ref().unwrap().get(), nshards, std::string());

    // Local routing cache first, so this node routes by the config even if
    // the service below fails to bind.
    StartLocalWatcher(g_cfg_cm.as_ref().unwrap().get());
    StartConfigService(g_cfg_store, ep.port);
}

// Every other node (shard-0 followers + all non-zero shards): read shard
// 0's config over RPC from whichever shard-0 replica serves it, and watch
// it for changes. The watcher starts even if no replica serves yet;
// RemoteConfigConnection keeps retrying.
// @unsafe - RPC client, background thread
void StartRemoteWatcher(const std::vector<ConfigEndpoint>& eps) {
    std::vector<std::string> addrs;
    std::string all;
    for (const auto& ep : eps) {
        addrs.push_back(ep.addr());
        all += (all.empty() ? "" : ", ") + ep.addr();
    }
    g_cfg_remote = new RemoteConfigConnection(std::move(addrs));
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
    srpc::Log_info("BootstrapClusterConfig: watching shard-0 config at {}", all.c_str());
}

}  // namespace

// @unsafe - see per-branch helpers
void BootstrapClusterConfig(abstract_db* db) {
    std::lock_guard<std::mutex> lock(g_cfg_mu);
    static bool done = false;
    if (done) return;
    done = true;

    if (!cluster_config_enabled()) return;

    auto& bench = BenchmarkConfig::getInstance();
    const uint32_t nshards = static_cast<uint32_t>(bench.getNshards());
    if (nshards <= 1) return;  // sharding not in play; legacy routing stands

    if (!ResolveConfigEndpoints(&g_cfg_endpoints)) return;
    g_cfg_nshards = nshards;
    g_cfg_db = db;

    const bool is_shard0_leader =
        (bench.getShardIndex() == 0) && (bench.getLeaderConfig() != 0);

    if (is_shard0_leader) {
        const ConfigEndpoint* mine = FindEndpoint(MyReplicaName());
        if (mine == nullptr) {
            srpc::Log_warn("BootstrapClusterConfig: {} is not in shard 0's replica list; serving "
                           "the cluster config at the preferred leader's endpoint",
                           MyReplicaName().c_str());
            mine = &g_cfg_endpoints.front();
        }
        StartShard0Leader(nshards, *mine);
        g_cfg_leading = true;
    } else {
        StartRemoteWatcher(g_cfg_endpoints);
    }
    g_cfg_active = true;
}

// @unsafe - stops the remote watcher, storage index, RPC server bind, background threads
void PromoteClusterConfigLeader() {
    std::lock_guard<std::mutex> lock(g_cfg_mu);
    if (!g_cfg_active || g_cfg_leading) return;
    if (BenchmarkConfig::getInstance().getShardIndex() != 0) return;  // only shard 0 serves

    const std::string me = MyReplicaName();
    const ConfigEndpoint* mine = FindEndpoint(me);
    abstract_ordered_index* idx = ConfigTable();
    if (mine == nullptr || idx == nullptr) {
        srpc::Log_error("BootstrapClusterConfig: {} now leads shard 0 but {}, so it cannot serve "
                        "the cluster config", me.c_str(),
                        mine == nullptr ? "is not in shard 0's replica list" : "has no config table");
        return;
    }
    g_cfg_leading = true;

    // Stop following the old leader.
    if (g_cfg_watcher.is_some()) g_cfg_watcher.as_ref().unwrap()->stop();
    g_cfg_watcher = rusty::None;
    const uint64_t last_seen = get_cluster_config().get_version();

    g_cfg_store = new ConfigTableStore(idx);
    g_cfg_cm = rusty::Some(rusty::make_box<ConfigManager>(g_cfg_store));
    ConfigManager* cm = g_cfg_cm.as_ref().unwrap().get();

    // The old leader's writes reached this replica's table through the log.
    // shard_count is written last when seeding, so with it present the
    // table is complete; this replica then only names itself shard 0's
    // leader. Otherwise (promoted before the seed replicated) it rebuilds
    // the config from the shared shard config. Either way the version
    // continues above every one a watcher has seen, so all of them reload.
    const uint64_t replicated = cm->get_version();
    const bool complete = replicated > 0 && cm->get_shard_count() > 0;
    if (last_seen > replicated) g_cfg_store->put("__version__", std::to_string(last_seen));
    if (complete) {
        cm->set_shard_leader(0, me);
    } else {
        SeedTopology(cm, g_cfg_nshards, me);
    }
    StartLocalWatcher(cm);
    StartConfigService(g_cfg_store, mine->port);
    srpc::Log_info("BootstrapClusterConfig: {} now leads shard 0 and serves the {} cluster config "
                   "(version {}; replicated version {}, last seen {})", me.c_str(),
                   complete ? "replicated" : "re-seeded", get_cluster_config().get_version(),
                   replicated, last_seen);
}

}  // namespace janus
