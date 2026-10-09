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
// Every node derives the same list of config-service endpoints from the
// shared shard config (--shard-config), one per shard-0 replica, preferred
// leader first: the replica's host, at 90 above its base port, or at
// MAKO_CLUSTER_CONFIG_PORT plus its position in the list when that is set.
// If an endpoint cannot be determined safely the node logs an error and
// leaves the feature off rather than running half-wired.
//
// When active, branches on this node's identity:
//   - Shard 0's leader opens its __mako_config__ index, wraps it in an
//     OrderedIndexKvStore behind an in-memory mirror, seeds it from the shared shard config (shard
//     count + per-shard replicas, leader, status and replica addresses;
//     no per-table policy), primes and watches its own routing cache, and
//     stands up a dedicated ConfigKvService RPC server so other nodes can
//     read config keys.
//   - Every other node wraps an RPC client to that service in a
//     RemoteKvStore and starts a ConfigWatcher. The client tries shard 0's
//     replicas in turn, so it keeps retrying until shard 0 serves and moves
//     to a promoted replica after a failover.
// On shard 0's leader the store writes through to the Mako index and an
// in-memory mirror; the watcher and the service handler read the mirror,
// so no thread outside the transaction engine touches the index.
//
// Not covered here: the config table is written only on shard 0's leader
// and is neither replicated nor persisted (a promoted replica re-seeds it,
// see PromoteClusterConfigLeader), and there is no runtime write path
// (shardmaster commands).
//
// @unsafe - RPC I/O, storage index open, background thread creation.
void BootstrapClusterConfig(abstract_db* db);

// Take over serving the cluster config when this process becomes shard 0's
// leader after startup (a Paxos learner or p1 taking over, or a Raft
// leadership change). The replica stops reading the old leader, rebuilds
// the config from the shared shard config with itself as shard 0's leader
// and a version above the last one it saw, and serves it at its own
// endpoint; other nodes find it by trying shard 0's replicas. While nothing
// changes the config at runtime this loses nothing. No-op off shard 0,
// when the feature is off, and on a node that already serves.
//
// @unsafe - RPC server bind, background thread creation.
void PromoteClusterConfigLeader();

}  // namespace janus
