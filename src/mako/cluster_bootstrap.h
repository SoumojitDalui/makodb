#pragma once

// Cluster-config runtime bootstrap.
//
// Ties the read-side cluster-config components together at node startup.
// Everything below this call already exists and is unit-tested in
// isolation (OrderedIndexKvStore, RemoteKvStore, ConfigManager,
// ConfigKvServiceImpl, ConfigWatcher, and the ClusterConfig routing
// cache the shard router consults); this is the one place that
// constructs and connects them on a live node.
//
// Forward-declared dependency only, so this header stays free of the
// storage-engine and RPC includes (those live in the .cc).

class abstract_db;

namespace janus {

// Wire the cluster-config read path. Call ONCE from init_env(), after
// the shard's RPC servers are up (post setup2()).
//
// Gated twice, so it is a no-op on the common CI paths:
//   1. the MAKO_CLUSTER_CONFIG env var must be "1", and
//   2. the cluster must have more than one shard.
// Single-shard / unsharded runs keep the legacy routing path untouched.
// It is only reached in replicated mode: init_env() has no database to
// hand it otherwise.
//
// Every node finds shard 0's config service the same way, from the shared
// shard config (--shard-config): shard 0's preferred-leader host, at
// MAKO_CLUSTER_CONFIG_PORT or 90 above that leader's base port. If the
// endpoint cannot be determined safely the node logs an error and leaves
// the feature off rather than running half-wired.
//
// When active, branches on this node's identity:
//   - Shard 0's leader opens its __mako_config__ index, wraps it in an
//     OrderedIndexKvStore, seeds it from the shared shard config (shard
//     count + per-shard replicas, leader, status and replica addresses;
//     no per-table policy), primes and watches its own routing cache, and
//     stands up a dedicated ConfigKvService RPC server so other nodes can
//     read config keys.
//   - Every other node wraps a reconnecting RPC client to that service in
//     a RemoteKvStore and starts a ConfigWatcher. A node that starts before
//     shard 0 serves keeps retrying every second.
// Threads that touch the Mako-backed store without being workers (the
// leader's watcher thread, the service handler thread) are registered
// with the transaction engine on first use.
//
// Not covered here: the config table is written only on shard 0's leader
// and is neither replicated nor persisted, so a shard-0 failover or restart
// loses it until the next bootstrap; nothing serves config after a
// failover; and there is no runtime write path (shardmaster commands).
//
// @unsafe - RPC I/O, storage index open, background thread creation.
void BootstrapClusterConfig(abstract_db* db);

}  // namespace janus
