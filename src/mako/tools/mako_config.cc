// mako_config — read or change the cluster config (MAKO_CLUSTER_CONFIG=1)
// of a running cluster.
//
//   mako_config [--endpoints HOST:PORT,...] get KEY
//   mako_config [--endpoints HOST:PORT,...] CHANGE [ARG...]
//
// The endpoints are shard 0's config-service endpoints, one per shard-0
// replica. Every node logs them at startup ("watching shard-0 config at
// ..."); MAKO_CLUSTER_CONFIG_ENDPOINTS supplies them when --endpoints is
// not given. Only the replica that serves the config listens, so the tool
// tries them in turn. CHANGE is one of the changes ApplyConfigChangeTo in
// src/deptran/config_kv_service.h takes, for example
//
//   mako_config --endpoints 10.0.0.1:31090,10.0.0.2:31190 kill_shard 2 1
//   mako_config --endpoints 10.0.0.1:31090,10.0.0.2:31190 get shard/2/status
//
// Exit status: 0 the change is applied and held by shard 0's replicas, or
// the key was read; 1 bad usage, or no replica serves the config; 2 the
// change was rejected, or the key is absent; 3 the change is applied but
// its replication is unconfirmed; 4 the replica was lost before it
// answered, so the change may or may not have applied.

#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include "config_kv_service.h"

namespace {

// @safe - string split
std::vector<std::string> SplitCommas(const std::string& s) {
    std::vector<std::string> out;
    size_t start = 0;
    while (start <= s.size()) {
        const size_t comma = s.find(',', start);
        const size_t end = comma == std::string::npos ? s.size() : comma;
        if (end > start) out.push_back(s.substr(start, end - start));
        if (comma == std::string::npos) break;
        start = comma + 1;
    }
    return out;
}

// @safe - output
int Usage() {
    std::fprintf(stderr,
                 "usage: mako_config [--endpoints HOST:PORT,...] get KEY\n"
                 "       mako_config [--endpoints HOST:PORT,...] CHANGE [ARG...]\n"
                 "endpoints: shard 0's config-service endpoints (or set "
                 "MAKO_CLUSTER_CONFIG_ENDPOINTS)\n"
                 "changes: kill_shard <dead> <taker>, set_shard_status <shard> <status>,\n"
                 "         set_shard_leader <shard> <replica>, set_shard_replicas <shard> "
                 "<replica>...,\n"
                 "         set_node_addr <site> <host:port>, set_node_status <site> <status>,\n"
                 "         advance_epoch\n");
    return 1;
}

// Read one key from the first endpoint that answers.
// @unsafe - RPC client lifecycle
int Get(const std::vector<std::string>& addrs, const std::string& key) {
    auto poll = srpc::PollThread::create();
    int rc = 1;
    for (const auto& addr : addrs) {
        auto client = srpc::Client::create(poll.clone());
        if (client->connect(reinterpret_cast<const int8_t*>(addr.c_str()), false) != 0) {
            client->close();
            continue;
        }
        janus::ConfigKvServiceProxy proxy(const_cast<srpc::Client*>(client.get()));
        janus::ConfigKvServiceProxy::RpcReadConfigKeyRequest req;
        req.key = key;
        auto r = proxy.ReadConfigKey(req);   // synchronous
        client->close();
        if (r.is_err()) continue;
        auto resp = r.unwrap();
        if (resp.found) {
            std::fwrite(resp.value.data(), 1, resp.value.size(), stdout);
            std::fputc('\n', stdout);
            rc = 0;
        } else {
            std::fprintf(stderr, "%s is not set\n", key.c_str());
            rc = 2;
        }
        break;
    }
    if (rc == 1) std::fprintf(stderr, "no shard-0 replica serves the cluster config\n");
    poll->shutdown();
    return rc;
}

}  // namespace

// @unsafe - process entry; RPC client
int main(int argc, char** argv) {
    srpc::Log::set_level(srpc::Log::WARN);   // keep the RPC runtime's chatter off stdout
    std::string endpoints;
    if (const char* env = std::getenv("MAKO_CLUSTER_CONFIG_ENDPOINTS")) endpoints = env;
    int i = 1;
    if (i < argc && std::string(argv[i]) == "--endpoints") {
        if (i + 1 >= argc) return Usage();
        endpoints = argv[i + 1];
        i += 2;
    }
    const std::vector<std::string> addrs = SplitCommas(endpoints);
    if (addrs.empty() || i >= argc) return Usage();
    const std::string command = argv[i++];
    const std::vector<std::string> args(argv + i, argv + argc);

    if (command == "get") {
        if (args.size() != 1) return Usage();
        return Get(addrs, args[0]);
    }

    const janus::ConfigChangeResult r = janus::ApplyConfigChangeAt(addrs, command, args);
    switch (r.status) {
    case janus::kConfigChangeApplied:
        std::printf("applied: version %llu\n", static_cast<unsigned long long>(r.version));
        return 0;
    case janus::kConfigChangeUnconfirmed:
        std::printf("applied, version %llu, but not confirmed replicated: %s\n",
                    static_cast<unsigned long long>(r.version), r.message.c_str());
        return 3;
    case janus::kConfigChangeRejected:
        std::fprintf(stderr, "rejected: %s\n", r.message.c_str());
        return 2;
    case janus::kConfigChangeUnknown:
        std::fprintf(stderr, "%s\n", r.message.c_str());
        return 4;
    default:
        std::fprintf(stderr, "%s\n", r.message.c_str());
        return 1;
    }
}
