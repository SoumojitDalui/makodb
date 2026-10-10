// Unit tests for the ConfigKvService server-side logic.
//
// The RPC handler ReadConfigKey delegates to DoReadConfigKey(key, &value),
// which is just a KvStore read, and ApplyConfigChange runs the injected
// apply function, which on a live node checks leadership and then calls
// ApplyConfigChangeTo. That logic is what we exercise here, over an
// InMemoryKvStore, with no socket / DeferredReply. The wire transport
// (both RPCs over a real connection, and the mako_config client) is
// runtime-verified by examples/test_2shard_config_failover.sh against a
// live cluster.

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "config_kv_service.h"
import cluster;   // config/sharding metadata module (was #include "cluster/...")

namespace janus {
namespace {

TEST(ConfigKvServiceTest, DoReadConfigKeyReturnsValueOnHit) {
    InMemoryKvStore kv;
    kv.put("shard_count", "3");
    kv.put("__version__", "7");

    ConfigKvServiceImpl svc(&kv);

    std::string out;
    ASSERT_TRUE(svc.DoReadConfigKey("shard_count", &out));
    EXPECT_EQ(out, "3");
    ASSERT_TRUE(svc.DoReadConfigKey("__version__", &out));
    EXPECT_EQ(out, "7");
}

TEST(ConfigKvServiceTest, DoReadConfigKeyReturnsFalseOnMiss) {
    InMemoryKvStore kv;
    ConfigKvServiceImpl svc(&kv);

    std::string out = "sentinel";
    EXPECT_FALSE(svc.DoReadConfigKey("absent", &out));
}

TEST(ConfigKvServiceTest, DoReadConfigKeyPreservesRawBytes) {
    // Serialized sharding-policy values contain embedded NULs.
    InMemoryKvStore kv;
    static const char kBytes[] = {'a', '\x00', 'b', '\x00', 'c'};
    const std::string payload(kBytes, sizeof(kBytes));
    kv.put("sharding/policy/WAREHOUSE", payload);

    ConfigKvServiceImpl svc(&kv);
    std::string out;
    ASSERT_TRUE(svc.DoReadConfigKey("sharding/policy/WAREHOUSE", &out));
    EXPECT_EQ(out, payload);
    EXPECT_EQ(out.size(), 5u);
}

TEST(ConfigKvServiceTest, DoReadConfigKeyNullBackingStoreIsSafe) {
    ConfigKvServiceImpl svc(nullptr);
    std::string out;
    EXPECT_FALSE(svc.DoReadConfigKey("k", &out));
}

// A three-shard config as the bootstrap seeds it.
class ConfigChangeTest : public ::testing::Test {
protected:
    void SetUp() override {
        for (uint32_t i = 0; i < 3; ++i) {
            const std::string s = std::to_string(i);
            ASSERT_TRUE(cm_.set_shard_replicas(i, {"s" + s + "a", "s" + s + "b"}));
            ASSERT_TRUE(cm_.set_shard_leader(i, "s" + s + "a"));
            ASSERT_TRUE(cm_.set_shard_status(i, "active"));
        }
        ASSERT_TRUE(cm_.set_shard_count(3));
    }

    ConfigChangeResult Apply(const std::string& op, const std::vector<std::string>& args) {
        return ApplyConfigChangeTo(&cm_, op, args);
    }

    InMemoryKvStore kv_;
    ConfigManager cm_{&kv_};
};

TEST_F(ConfigChangeTest, KillShardRedirectsItsKeys) {
    const uint64_t before = cm_.get_version();
    ConfigChangeResult r = Apply("kill_shard", {"2", "1"});
    ASSERT_EQ(r.status, kConfigChangeApplied) << r.message;
    EXPECT_GT(r.version, before);
    EXPECT_EQ(r.version, cm_.get_version());
    EXPECT_EQ(cm_.get_shard_status(2), "dead");
    EXPECT_EQ(cm_.get_shard_replacement(2), 1u);

    ClusterConfig cc = ClusterConfig::new_();
    ASSERT_TRUE(cc.load_from_config_manager(&cm_));
    EXPECT_EQ(cc.resolve_live_shard(2), 1u);
}

TEST_F(ConfigChangeTest, MetadataChangesApply) {
    EXPECT_EQ(Apply("set_shard_status", {"1", "draining"}).status, kConfigChangeApplied);
    EXPECT_EQ(cm_.get_shard_status(1), "draining");
    EXPECT_EQ(Apply("set_shard_leader", {"1", "s1b"}).status, kConfigChangeApplied);
    EXPECT_EQ(cm_.get_shard_leader(1), "s1b");
    EXPECT_EQ(Apply("set_shard_replicas", {"2", "x", "y", "z"}).status, kConfigChangeApplied);
    EXPECT_EQ(cm_.get_shard_replicas(2), (std::vector<std::string>{"x", "y", "z"}));
    EXPECT_EQ(Apply("set_node_addr", {"s1a", "10.0.0.5:31100"}).status, kConfigChangeApplied);
    EXPECT_EQ(cm_.get_node_addr("s1a"), "10.0.0.5:31100");
    EXPECT_EQ(Apply("set_node_status", {"s1a", "suspect"}).status, kConfigChangeApplied);
    EXPECT_EQ(cm_.get_node_status("s1a"), "suspect");
    const uint64_t epoch = cm_.get_epoch();
    EXPECT_EQ(Apply("advance_epoch", {}).status, kConfigChangeApplied);
    EXPECT_EQ(cm_.get_epoch(), epoch + 1);
}

// Shard 0 holds the config; its entries change only through a takeover.
TEST_F(ConfigChangeTest, ShardZeroEntriesAreRejected) {
    const uint64_t before = cm_.get_version();
    EXPECT_EQ(Apply("kill_shard", {"0", "1"}).status, kConfigChangeRejected);
    EXPECT_EQ(Apply("set_shard_status", {"0", "draining"}).status, kConfigChangeRejected);
    EXPECT_EQ(Apply("set_shard_leader", {"0", "s0b"}).status, kConfigChangeRejected);
    EXPECT_EQ(Apply("set_shard_replicas", {"0", "x"}).status, kConfigChangeRejected);
    EXPECT_EQ(cm_.get_version(), before);
    EXPECT_EQ(cm_.get_shard_leader(0), "s0a");
    // Shard 0 may still take over from another shard.
    EXPECT_EQ(Apply("kill_shard", {"1", "0"}).status, kConfigChangeApplied);
}

TEST_F(ConfigChangeTest, BadChangesAreRejectedWithoutWriting) {
    const uint64_t before = cm_.get_version();
    const std::vector<std::pair<std::string, std::vector<std::string>>> bad = {
        {"add_shard", {"3", "x"}},              // not offered
        {"set_sharding_policy", {"t", "p"}},    // not offered
        {"kill_shard", {"2"}},                  // missing taker
        {"kill_shard", {"2", "2"}},             // takes over from itself
        {"kill_shard", {"2", "7"}},             // no such taker
        {"kill_shard", {"x", "1"}},             // not a shard id
        {"set_shard_status", {"5", "draining"}},  // no such shard
        {"set_shard_status", {"1", "dead"}},    // kill_shard names a taker
        {"set_shard_status", {"1", ""}},
        {"set_shard_leader", {"1", "s2a"}},     // not one of shard 1's replicas
        {"set_shard_replicas", {"1"}},
        {"set_shard_replicas", {"1", "a,b"}},   // the list is comma-joined
        {"set_node_addr", {"s1a"}},
        {"set_node_status", {"", "up"}},
        {"advance_epoch", {"1"}},
    };
    for (const auto& c : bad) {
        ConfigChangeResult r = Apply(c.first, c.second);
        EXPECT_EQ(r.status, kConfigChangeRejected) << c.first;
        EXPECT_FALSE(r.message.empty()) << c.first;
    }
    EXPECT_EQ(cm_.get_version(), before);
}

// A shard killed once can no longer take over: its replicas are cleared.
TEST_F(ConfigChangeTest, KilledShardCannotTakeOver) {
    ASSERT_EQ(Apply("kill_shard", {"2", "1"}).status, kConfigChangeApplied);
    EXPECT_EQ(Apply("kill_shard", {"1", "2"}).status, kConfigChangeRejected);
}

TEST(ConfigKvServiceTest, ApplyWithoutApplyFunctionIsNotLeader) {
    InMemoryKvStore kv;
    ConfigKvServiceImpl svc(&kv);
    ConfigChangeResult r = svc.DoApplyConfigChange("advance_epoch", {});
    EXPECT_EQ(r.status, kConfigChangeNotLeader);
}

TEST(ConfigKvServiceTest, ApplyRunsTheInjectedFunction) {
    InMemoryKvStore kv;
    std::string seen;
    ConfigKvServiceImpl svc(&kv, [&](const std::string& op, const std::vector<std::string>& args) {
        seen = op + ":" + (args.empty() ? "" : args[0]);
        ConfigChangeResult r;
        r.status = kConfigChangeApplied;
        r.version = 42;
        return r;
    });
    ConfigChangeResult r = svc.DoApplyConfigChange("set_node_status", {"n1", "up"});
    EXPECT_EQ(r.status, kConfigChangeApplied);
    EXPECT_EQ(r.version, 42u);
    EXPECT_EQ(seen, "set_node_status:n1");
}

}  // namespace
}  // namespace janus
