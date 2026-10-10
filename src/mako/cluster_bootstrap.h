#pragma once

// Cluster-config runtime bootstrap.
//
// Ties the cluster-config components together at node startup.
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

// Wire the cluster-config path. Call ONCE from init_env(), after
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
//   - Shard 0's leader seeds the config table (__mako_config__, the
//     reserved table id mako::CONFIG_TABLE_ID) from the shared shard
//     config (shard count + per-shard replicas, leader, status and replica
//     addresses; no per-table policy), primes and watches its own routing
//     cache, and stands up a dedicated ConfigKvService RPC server so other
//     nodes can read config keys.
//   - Every other node wraps an RPC client to that service in a
//     RemoteKvStore and starts a ConfigWatcher. The client tries shard 0's
//     replicas in turn, so it keeps retrying until shard 0 serves and moves
//     to a promoted replica after a failover.
// One thread registered with the transaction engine owns the config
// table and runs every read and write of it for the other threads here.
// Each config change (one ConfigManager write, or the whole seed) is one
// transaction that goes into shard 0's replication log at once (partition
// 0, and the leader's log persistence), so shard 0's followers replay the
// table like any other.
//
// The replica that serves the config also takes changes at runtime, from
// an operator (the mako_config tool) or another process, through the
// service's ApplyConfigChange RPC; ApplyConfigChangeTo in
// config_kv_service.h lists them. Each change is one transaction, answered
// once shard 0's replicas hold it, and every node loads it on its next
// poll. Of these, kill_shard changes what nodes do (routing sends the dead
// shard's keys to the shard that takes over); the rest record shard and
// node metadata.
//
// @unsafe - RPC I/O, storage index open, background thread creation.
void BootstrapClusterConfig(abstract_db* db);

// Paxos: take over serving the cluster config when this process becomes
// shard 0's leader after startup (a learner or p1 taking over). The work
// runs on its own thread so the failover callback is not held up. The
// replica stops reading the old leader and serves
// its own copy of the config table, which it received through the
// replication log, after naming itself shard 0's leader in it; if no
// complete copy arrived (promoted before the seed replicated), it rebuilds
// the config from the shared shard config. The version continues above the
// last one it saw and above a floor made of partition 0's replication term
// (the Paxos epoch, or the Raft term) in the high half and the replica's
// position in shard 0's replica list below it, so every watcher reloads and
// no two leaders share a version. Other nodes find it by
// trying shard 0's replicas. No-op under Raft, off shard 0, when the
// feature is off, and on a node that already serves.
//
// @unsafe - storage index, RPC server bind, background thread creation.
void PromoteClusterConfigLeader();

// Raft: call on every leadership change (Raft reports them per
// partition). Under Raft the shard-0 replica that leads partition 0 serves
// the config, since only its writes enter the Raft log: every node starts
// as a reader, the partition-0 leader promotes itself as above (the first
// one seeds the table), and a replica that loses partition 0 stops serving
// and reads from the new leader. Raft can briefly report a node as leader
// after it moved to a newer term, so a promotion serves only if the node
// still leads partition 0 in the term it started in. The check runs on its own thread and
// reads the current leadership, so it does not matter which partition's
// event triggered it. No-op under Paxos, off shard 0, and when the
// feature is off.
//
// @unsafe - storage index, RPC server bind/teardown, background threads.
void ClusterConfigLeadershipChanged();

}  // namespace janus
