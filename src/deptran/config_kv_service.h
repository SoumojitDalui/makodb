#pragma once

// ConfigKvService — reads and changes of shard 0's cluster config. A node
// reads a single key from shard 0's __mako_config__ system table through
// ReadConfigKey (the transport for RemoteKvStore's ReadFn), and an
// operator or another node changes the config through ApplyConfigChange.
// The server handler is backed by a KvStore (on the shard-0 replica that
// serves the config, the config table) and, for changes, a function that
// applies them there. Distinct from ConfigService, which serves the
// c-node's ConfigStore — this one is bound to shard 0's config index,
// resolving the c-node-vs-shard-0 split by giving shard-0 config its own
// focused service.

#include "rcc_rpc.h"                    // ConfigKvServiceService / Proxy
import cluster;   // config/sharding metadata module (was #include "cluster/...")

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace janus {

// Outcome of a config change (ApplyConfigChange's status).
enum ConfigChangeStatus : int32_t {
    kConfigChangeApplied = 0,       // committed and held by shard 0's replicas
    kConfigChangeUnconfirmed = 1,   // committed; replication not confirmed in time
    kConfigChangeRejected = 2,      // unknown op, bad arguments, or refused
    kConfigChangeNotLeader = 3,     // the replica asked does not serve the config
    kConfigChangeUnknown = 4,       // client side: lost the replica before it answered
};

struct ConfigChangeResult {
    int32_t status = kConfigChangeRejected;
    std::string message;   // why a change was rejected or is unconfirmed
    uint64_t version = 0;  // config version after the change
};

// How long a client waits for a change's answer. The serving replica waits
// up to 5 s for shard 0's replicas to hold the change before it answers,
// longer than an RPC's default one-second wait.
constexpr double kConfigChangeCallTimeoutSec = 15.0;

// Applies one change where the config is served (see ApplyConfigChangeTo).
using ConfigChangeFn =
    std::function<ConfigChangeResult(const std::string& op, const std::vector<std::string>& args)>;

namespace config_change_detail {

// @safe - string parse
inline bool ParseShardId(const std::string& s, uint32_t* out) {
    if (s.empty() || s.size() > 10) return false;
    uint64_t v = 0;
    for (char c : s) {
        if (c < '0' || c > '9') return false;
        v = v * 10 + static_cast<uint64_t>(c - '0');
    }
    if (v > UINT32_MAX) return false;
    *out = static_cast<uint32_t>(v);
    return true;
}

// @safe - result construction
inline ConfigChangeResult Rejected(std::string why) {
    ConfigChangeResult r;
    r.status = kConfigChangeRejected;
    r.message = std::move(why);
    return r;
}

}  // namespace config_change_detail

/**
 * Apply one config change through a ConfigManager, which writes each
 * change as one batch (one transaction on the config table). The changes
 * a running cluster can take:
 *
 *   kill_shard <dead> <taker>        mark a shard dead; routing sends its
 *                                    keys to <taker> on every node
 *   set_shard_status <shard> <status>   any status but "dead" (kill_shard
 *                                    also names who takes over)
 *   set_shard_leader <shard> <replica>  one of the shard's replicas
 *   set_shard_replicas <shard> <replica>...
 *   set_node_addr <site> <addr>
 *   set_node_status <site> <status>
 *   advance_epoch
 *
 * Shard 0's own entries are not changed this way: shard 0 holds the
 * config, its replicas come from the shard config, and the replica that
 * serves the config names itself leader when it takes over. Adding or
 * removing shards and per-table sharding policies are not offered: Mako
 * does not move data between shards, and nodes do not load policies from
 * the config.
 */
// @unsafe - ConfigManager writes through a raw pointer
inline ConfigChangeResult ApplyConfigChangeTo(ConfigManager* cm, const std::string& op,
                                              const std::vector<std::string>& args) {
    using config_change_detail::ParseShardId;
    using config_change_detail::Rejected;
    if (cm == nullptr) return Rejected("no config store");

    const uint32_t count = cm->get_shard_count();
    // Why args[i] is not a shard this way of changing may touch, or "".
    auto other_shard = [&](size_t i, uint32_t* id) -> std::string {
        if (!ParseShardId(args[i], id)) return "'" + args[i] + "' is not a shard id";
        if (*id >= count) {
            return "no shard " + args[i] + " (the config has " + std::to_string(count) +
                   " shards)";
        }
        if (*id == 0) {
            return "shard 0 holds the cluster config; its entries change only when another "
                   "shard-0 replica takes over";
        }
        return "";
    };
    auto usage = [](const std::string& form) { return Rejected("usage: " + form); };

    uint32_t shard = 0;
    std::string why;
    bool ok = false;
    if (op == "kill_shard") {
        if (args.size() != 2) return usage("kill_shard <dead shard> <taker shard>");
        if (!(why = other_shard(0, &shard)).empty()) return Rejected(why);
        uint32_t taker = 0;
        if (!ParseShardId(args[1], &taker) || taker >= count) {
            return Rejected("no shard " + args[1] + " to take over");
        }
        if (taker == shard) return Rejected("a shard cannot take over from itself");
        if (!cm->kill_shard(shard, taker)) {
            return Rejected("shard " + args[1] + " has no replicas to take over with");
        }
        ok = true;
    } else if (op == "set_shard_status") {
        if (args.size() != 2) return usage("set_shard_status <shard> <status>");
        if (!(why = other_shard(0, &shard)).empty()) return Rejected(why);
        if (args[1].empty()) return Rejected("empty status");
        if (args[1] == "dead") {
            return Rejected("use kill_shard, which also names the shard that takes over");
        }
        ok = cm->set_shard_status(shard, args[1]);
    } else if (op == "set_shard_leader") {
        if (args.size() != 2) return usage("set_shard_leader <shard> <replica>");
        if (!(why = other_shard(0, &shard)).empty()) return Rejected(why);
        bool member = false;
        for (const auto& r : cm->get_shard_replicas(shard)) member = member || r == args[1];
        if (!member) return Rejected(args[1] + " is not a replica of shard " + args[0]);
        ok = cm->set_shard_leader(shard, args[1]);
    } else if (op == "set_shard_replicas") {
        if (args.size() < 2) return usage("set_shard_replicas <shard> <replica>...");
        if (!(why = other_shard(0, &shard)).empty()) return Rejected(why);
        std::vector<std::string> replicas(args.begin() + 1, args.end());
        for (const auto& r : replicas) {
            if (r.empty() || r.find(',') != std::string::npos) {
                return Rejected("replica names must be non-empty and contain no comma");
            }
        }
        ok = cm->set_shard_replicas(shard, replicas);
    } else if (op == "set_node_addr" || op == "set_node_status") {
        if (args.size() != 2 || args[0].empty() || args[1].empty()) {
            return usage(op + (op == "set_node_addr" ? " <site> <host:port>" : " <site> <status>"));
        }
        ok = op == "set_node_addr" ? cm->set_node_addr(args[0], args[1])
                                   : cm->set_node_status(args[0], args[1]);
    } else if (op == "advance_epoch") {
        if (!args.empty()) return usage("advance_epoch");
        ok = cm->advance_epoch();
    } else {
        return Rejected("unknown change '" + op + "'; one of kill_shard, set_shard_status, "
                        "set_shard_leader, set_shard_replicas, set_node_addr, "
                        "set_node_status, advance_epoch");
    }
    if (!ok) return Rejected("the config store refused " + op);

    ConfigChangeResult r;
    r.status = kConfigChangeApplied;
    r.version = cm->get_version();
    return r;
}

/**
 * Server side. Serves ReadConfigKey from a KvStore and ApplyConfigChange
 * through an apply function. Both are injected: on the shard-0 replica that
 * serves the config, the config table and a function that applies a change
 * there; in tests an InMemoryKvStore. Without an apply function every
 * change is answered kConfigChangeNotLeader.
 */
// @safe - thin handler over an injected KvStore and apply function.
class ConfigKvServiceImpl : public ConfigKvServiceService {
public:
    explicit ConfigKvServiceImpl(KvStore* kv, ConfigChangeFn apply = nullptr)
        : kv_(kv), apply_(std::move(apply)) {}

    // Read logic, factored out of the RPC handler so it can be unit
    // tested without the RPC machinery (no DeferredReply / socket).
    // @unsafe - KvStore read (port returns Option; adapt to found + out)
    bool DoReadConfigKey(const std::string& key, std::string* value) const {
        if (kv_ == nullptr || value == nullptr) return false;
        auto found = kv_->get(key);
        if (found.is_none()) return false;
        *value = found.unwrap();
        return true;
    }

    // @unsafe - RPC handler; delegates to DoReadConfigKey then replies.
    void ReadConfigKey(const RpcReadConfigKeyRequest& req,
                       RpcReadConfigKeyResponse& resp,
                       srpc::DeferredReply defer) const override {
        std::string value;
        const bool found = DoReadConfigKey(req.key, &value);
        resp.found = found ? 1 : 0;
        if (found) resp.value = std::move(value);
        defer.reply();
    }

    // Change logic without the RPC machinery, for unit tests.
    // @unsafe - calls the injected apply function
    ConfigChangeResult DoApplyConfigChange(const std::string& op,
                                           const std::vector<std::string>& args) const {
        return RunChange(apply_, op, args);
    }

    // A change waits for shard 0's replicas to hold it, and this server's
    // poll thread also answers every node's reads, so the change runs on a
    // thread of its own while this request's fiber sleeps in short steps,
    // which lets the poll thread serve other requests in the meantime. The
    // thread holds its own copy of the apply function: a replica that stops
    // serving the config deletes this server, maybe mid-change.
    // @unsafe - RPC handler; thread hand-off and fiber sleep
    rusty::Result<RpcApplyConfigChangeResponse, srpc::i32> ApplyConfigChange(
            const RpcApplyConfigChangeRequest& req) const override {
        struct Pending {
            std::atomic<bool> done{false};
            ConfigChangeResult result;
        };
        auto pending = std::make_shared<Pending>();
        std::thread([apply = apply_, pending, op = req.op, args = req.args] {
            pending->result = RunChange(apply, op, args);
            pending->done.store(true, std::memory_order_release);
        }).detach();
        while (!pending->done.load(std::memory_order_acquire)) srpc::this_fiber::sleep_ms(1);

        RpcApplyConfigChangeResponse resp;
        resp.status = pending->result.status;
        resp.message = pending->result.message;
        resp.version = static_cast<srpc::i64>(pending->result.version);
        return rusty::Result<RpcApplyConfigChangeResponse, srpc::i32>::Ok(std::move(resp));
    }

private:
    // @unsafe - calls the apply function
    static ConfigChangeResult RunChange(const ConfigChangeFn& apply, const std::string& op,
                                        const std::vector<std::string>& args) {
        if (!apply) {
            ConfigChangeResult r;
            r.status = kConfigChangeNotLeader;
            r.message = "this replica does not serve the cluster config";
            return r;
        }
        return apply(op, args);
    }

    KvStore* kv_;            // non-owning; shard 0's config store via the port.
    ConfigChangeFn apply_;   // empty: this server takes no changes
};

/**
 * Client side. Build a RemoteKvStoreReadFn from a connected
 * ConfigKvServiceProxy. The proxy must outlive the returned function.
 * Production wires this on a non-shard-0 node, pointed at shard 0's
 * leader; the resulting ReadFn goes into a RemoteKvStore, which a
 * ConfigManager/ConfigWatcher reads through.
 */
// @unsafe - issues an RPC per get.
inline RemoteKvStoreReadFn make_config_read_fn(ConfigKvServiceProxy* proxy) {
    return [proxy](const std::string& key) -> rusty::Option<std::string> {
        if (proxy == nullptr) return rusty::None;
        ConfigKvServiceProxy::RpcReadConfigKeyRequest req;
        req.key = key;
        auto r = proxy->ReadConfigKey(req);   // synchronous
        if (r.is_err()) return rusty::None;
        auto resp = r.unwrap();
        if (!resp.found) return rusty::None;
        return rusty::Some(std::move(resp.value));
    };
}

/**
 * Client side of ApplyConfigChange. Only the shard-0 replica that serves
 * the config listens, so this tries shard 0's config endpoints (host:port)
 * in turn and applies the change at the first one that takes it. It moves
 * on only when the change surely did not apply there: the connect failed,
 * or the replica answered that it does not serve the config. A call that
 * fails after it was sent may have applied, so that is reported as
 * kConfigChangeUnknown instead of being retried at another replica, where
 * the change could apply twice.
 */
// @unsafe - RPC client lifecycle
inline ConfigChangeResult ApplyConfigChangeAt(const std::vector<std::string>& addrs,
                                              const std::string& op,
                                              const std::vector<std::string>& args) {
    ConfigChangeResult out;
    out.status = kConfigChangeNotLeader;
    out.message = "no shard-0 replica serves the cluster config";
    auto poll = srpc::PollThread::create();
    for (const auto& addr : addrs) {
        auto client = srpc::Client::create(poll.clone());
        if (client->connect(reinterpret_cast<const int8_t*>(addr.c_str()), false) != 0) {
            client->close();
            continue;
        }
        ConfigKvServiceProxy proxy(const_cast<srpc::Client*>(client.get()));
        ConfigKvServiceProxy::RpcApplyConfigChangeRequest req;
        req.op = op;
        req.args = args;
        auto sent = proxy.async_ApplyConfigChange(req);
        if (sent.is_err()) {   // never sent, so not applied there
            client->close();
            continue;
        }
        auto call = sent.unwrap();
        call.raw_future()->timed_wait(kConfigChangeCallTimeoutSec);
        auto r = call.resolve();
        client->close();
        if (r.is_err()) {
            out.status = kConfigChangeUnknown;
            out.message = "lost " + addr + " before it answered; the change may or may not "
                          "have applied";
            break;
        }
        auto resp = r.unwrap();
        out.status = resp.status;
        out.message = std::move(resp.message);
        out.version = static_cast<uint64_t>(resp.version);
        if (out.status != kConfigChangeNotLeader) break;
    }
    poll->shutdown();
    return out;
}

}  // namespace janus
