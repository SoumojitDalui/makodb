#!/bin/bash

# Failover test for the cluster config (MAKO_CLUSTER_CONFIG=1) on a 2-shard
# replicated run. Once every node reads the config from the shard-0 replica
# that serves it, the test changes the config through mako_config, then
# kills that replica (or, under Raft, pauses it and resumes it), and checks
# from the logs and the tool that:
#   1. the change is applied, confirmed replicated, and loaded by every node,
#   2. another shard-0 replica takes over and serves the config,
#   3. every live node then reads the config from one replica,
#   4. no two takeovers served the same config version,
#   5. the change is still in the config after the failover, and the new
#      leader applies and confirms a change of its own,
#   6. no node exited before the event, and no shard-0 replica after it.
# Killing shard 0's leader also runs Mako's own cross-shard failover
# handling, which can stop another shard's replicas; such exits are
# reported as warnings, and at least one node outside shard 0 must stay up
# and follow the new leader.
# Shard 1's benchmark cannot finish once shard 0's leader is gone, so the test
# stops the run itself and judges only the config.
#
# Usage: test_2shard_config_failover.sh [paxos|raft] [kill|pause]
#   pause needs Raft: a Paxos leader that resumes after a takeover has no path
#   that makes it step down.
# Env: MAKO_CFG_FAILOVER_PAUSE (seconds paused, default 12),
#      MAKO_CFG_FAILOVER_TIMEOUT (seconds to converge after the event, default 60).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/simple_transaction_rep_port_utils.sh"

replication="${1:-paxos}"
event="${2:-kill}"
case "$replication/$event" in
    paxos/kill|raft/kill|raft/pause) ;;
    *)
        echo "Usage: $0 [paxos|raft] [kill|pause]   (pause needs raft)"
        exit 2
        ;;
esac

echo "========================================="
echo "Testing cluster-config failover ($replication, $event the config leader)"
echo "========================================="

rm -f nfs_sync_*
USERNAME=${USER:-unknown}
rm -rf /tmp/${USERNAME}_mako_rocksdb_shard*

trd=${MAKO_CI_TRD:-6}
pause_seconds="${MAKO_CFG_FAILOVER_PAUSE:-12}"
converge_timeout="${MAKO_CFG_FAILOVER_TIMEOUT:-60}"
script_name="$(basename "$0")"
binary_path="./${BUILD_DIR:-build}/dbtest"
tool_path="./${BUILD_DIR:-build}/mako_config"
log_prefix="${script_name}_${replication}"
export MAKO_CLUSTER_CONFIG=1
export LD_LIBRARY_PATH="$(pwd)/${BUILD_DIR:-build}:${LD_LIBRARY_PATH}"

if [ "$replication" = "raft" ]; then
    export MAKO_RAFT_PREFERRED_GRACE_US="${MAKO_RAFT_PREFERRED_GRACE_US:-30000000}"
    export MAKO_RAFT_NONPREFERRED_GRACE_ELECTION_MIN_US="${MAKO_RAFT_NONPREFERRED_GRACE_ELECTION_MIN_US:-5000000}"
    export MAKO_RAFT_NONPREFERRED_GRACE_ELECTION_MAX_US="${MAKO_RAFT_NONPREFERRED_GRACE_ELECTION_MAX_US:-10000000}"
    REPLICAS=(localhost p2 p1)
else
    REPLICAS=(localhost learner p2 p1)
fi
NODES=()
for s in 0 1; do
    for r in "${REPLICAS[@]}"; do
        NODES+=("shard${s}-${r}")
    done
done
# The runtime change: a node status, which nothing acts on, so the
# benchmark runs on undisturbed.
change_site="shard1-p1"
change_status="draining"

WRAPPER_PIDS=()
CLEANUP_DONE=0
event_node=""
event_pid=""

for bin in "$binary_path" "$tool_path"; do
    if [ ! -x "$bin" ]; then
        echo "Error: $(basename "$bin") not found or not executable at '$bin'"
        echo "Build it first (for Docker: ./docker_build.sh build), then retry."
        exit 1
    fi
done

if [ "$replication" = "paxos" ] && ! ensure_paxos_replication_configs "$trd" 2; then
    exit 1
fi

TEMP_CONFIG=$(make_simple_txn_rep_config 2 $trd)
if [ -z "$TEMP_CONFIG" ]; then
    exit 1
fi
export MAKO_CONFIG="$TEMP_CONFIG"

TEMP_PAXOS_DIR=$(make_paxos_replication_configs 2 "$trd" "$replication")
if [ -z "$TEMP_PAXOS_DIR" ]; then
    exit 1
fi
export MAKO_PAXOS_CONFIG_DIR="$TEMP_PAXOS_DIR"

cleanup() {
    if [ "$CLEANUP_DONE" -eq 1 ]; then
        return
    fi
    CLEANUP_DONE=1

    # A stopped process only acts on SIGTERM once it runs again.
    if [ -n "$event_pid" ]; then
        kill -CONT "$event_pid" 2>/dev/null || true
    fi
    for pid in "${WRAPPER_PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    pkill -TERM -f "$TEMP_CONFIG" 2>/dev/null || true
    sleep 3
    pkill -9 -f "$TEMP_CONFIG" 2>/dev/null || true
    for pid in "${WRAPPER_PIDS[@]}"; do
        wait "$pid" 2>/dev/null || true
    done

    rm -f "$TEMP_CONFIG"
    rm -rf "$TEMP_PAXOS_DIR"
    unset MAKO_CONFIG MAKO_PAXOS_CONFIG_DIR
}

handle_interrupt() {
    cleanup
    exit 130
}

trap cleanup EXIT
trap handle_interrupt INT TERM

log_of() {
    echo "${log_prefix}_$1.log"
}

# dbtest pid of a node such as shard0-p1.
pid_of() {
    local shard="${1%%-*}" role="${1#*-}"
    pgrep -f -- "--shard-index ${shard#shard} --shard-config $TEMP_CONFIG -P ${role}( |\$)" | head -1
}

# True when the node's last word on the config is that it serves it.
serves_config() {
    local line
    line=$(grep -a -E "config service listening on|now leads shard 0 and serves|no longer leads shard 0" \
        "$(log_of "$1")" 2>/dev/null | tail -1)
    [ -n "$line" ] && [[ "$line" != *"no longer leads"* ]]
}

serving_port() {
    grep -a -o -E "config service listening on [0-9.]+:[0-9]+" "$(log_of "$1")" 2>/dev/null \
        | tail -1 | sed -E 's/.*:([0-9]+)$/\1/'
}

reading_port() {
    grep -a -o -E "reading shard-0 config from [0-9.]+:[0-9]+" "$(log_of "$1")" 2>/dev/null \
        | tail -1 | sed -E 's/.*:([0-9]+)$/\1/'
}

# Highest config version a node has loaded.
loaded_version() {
    grep -a -o -E "loaded cluster config version [0-9]+" "$(log_of "$1")" 2>/dev/null \
        | sed -E 's/.* //' | sort -n | tail -1
}

# True when the node is the killed one or has exited.
gone() {
    [ "$1" = "$dead_node" ] || [[ " $MISSING " == *" $1 "* ]]
}

# Converged: exactly one live shard-0 replica serves the config, and every
# other live node, at least one of them outside shard 0, reads it from that
# replica. Sets CONFIG_LEADER.
converged() {
    local leader="" port n readers=0
    CONFIG_LEADER=""
    for n in "${NODES[@]}"; do
        [[ "$n" == shard0-* ]] || continue
        gone "$n" && continue
        serves_config "$n" || continue
        [ -n "$leader" ] && return 1
        leader=$n
    done
    [ -n "$leader" ] || return 1
    port=$(serving_port "$leader")
    [ -n "$port" ] || return 1
    for n in "${NODES[@]}"; do
        if [ "$n" = "$leader" ] || gone "$n"; then
            continue
        fi
        [ "$(reading_port "$n")" = "$port" ] || return 1
        [[ "$n" == shard0-* ]] || readers=$((readers + 1))
    done
    [ "$readers" -gt 0 ] || return 1
    CONFIG_LEADER=$leader
}

# Nodes, other than a killed one, whose dbtest process is gone.
missing_nodes() {
    local n out=()
    for n in "${NODES[@]}"; do
        [ "$n" = "$dead_node" ] && continue
        [ -n "$(pid_of "$n")" ] || out+=("$n")
    done
    echo "${out[*]}"
}

# Waits up to $1 seconds for the cluster to converge and stay converged on
# the same leader for $2 seconds (a Raft leader can still change hands
# right after an election). Sets CONFIG_LEADER and CONVERGED_AFTER (seconds
# until it first converged on that leader). Every 5 seconds it looks for
# nodes whose process has exited (MISSING): with $3 = stop it gives up on
# the first one (after 10 seconds of startup); with $3 = skip it leaves them
# out and the caller reports them.
wait_for_convergence() {
    local timeout=$1 stable_for=$2 on_exit=$3 waited=0 stable=0 last="" grace=0
    [ "$on_exit" = stop ] && grace=10
    MISSING=""
    while [ "$waited" -lt "$timeout" ]; do
        if [ "$waited" -ge "$grace" ] && [ $((waited % 5)) -eq 0 ]; then
            MISSING=$(missing_nodes)
            if [ -n "$MISSING" ] && [ "$on_exit" = stop ]; then
                CONFIG_LEADER=""
                return 1
            fi
        fi
        if converged; then
            if [ "$CONFIG_LEADER" = "$last" ]; then
                stable=$((stable + 1))
            else
                last=$CONFIG_LEADER
                stable=0
            fi
        else
            last=""
            stable=0
        fi
        if [ -n "$last" ] && [ "$stable" -ge "$stable_for" ]; then
            CONFIG_LEADER=$last
            CONVERGED_AFTER=$((waited - stable_for))
            return 0
        fi
        sleep 1
        waited=$((waited + 1))
    done
    CONFIG_LEADER=""
    return 1
}

# Why wait_for_convergence gave up ($1: when; $2: its timeout), and what
# each node shows.
report_no_convergence() {
    local n
    if [ -n "$MISSING" ]; then
        echo "  ✗ $MISSING exited $1"
    else
        echo "  ✗ Nodes did not converge on one config leader within $2s $1"
    fi
    for n in "${NODES[@]}"; do
        gone "$n" && continue
        echo "    $n: serves=$(serves_config "$n" && echo yes || echo no) reads_port=$(reading_port "$n")"
    done
    report_exits $MISSING
}

# Last lines of each exited node's log.
report_exits() {
    local n
    for n in "$@"; do
        echo "    $n exited; last lines of its log:"
        tail -5 "$(log_of "$n")" | sed "s/^/      /"
        if grep -a -q "remoteControl throw an error" "$(log_of "$n")"; then
            echo "    ($n panicked in Mako's cross-shard failover control, client_control in"
            echo "     src/mako/sto/sync_util.hh, which is outside the cluster-config code)"
        fi
    done
}

# shard 0's config endpoints as the nodes log them, comma-separated.
config_endpoints() {
    grep -a -o -h -E "watching shard-0 config at [0-9.:, ]+" "$(log_of "${NODES[0]}")" \
        "$(log_of "${NODES[${#NODES[@]}-1]}")" 2>/dev/null | head -1 \
        | sed -E 's/.* at //; s/[ ]+//g; s/,$//'
}

pkill -9 -x dbtest 2>/dev/null || true
sleep 1
for n in "${NODES[@]}"; do
    rm -f "$(log_of "$n")"
done

start_shard() {
    local shard=$1 r
    for r in "${REPLICAS[@]}"; do
        if [ "$r" = "p1" ]; then
            sleep 1
        fi
        if [ "$replication" = "raft" ]; then
            nohup bash bash/shard.sh 2 "$shard" $trd "$r" 0 1 raft > "$(log_of "shard${shard}-${r}")" 2>&1 &
        else
            nohup bash bash/shard.sh 2 "$shard" $trd "$r" 0 1 > "$(log_of "shard${shard}-${r}")" 2>&1 &
        fi
        WRAPPER_PIDS+=($!)
    done
}

echo "Starting shard 0..."
start_shard 0
sleep 5
echo "Starting shard 1..."
start_shard 1

failed=0
dead_node=""

echo "Waiting for every node to read the cluster config..."
if ! wait_for_convergence 120 3 stop; then
    report_no_convergence "before every node read the config" 120
    exit 1
fi
echo "  ✓ All nodes read the config from $CONFIG_LEADER (port $(serving_port "$CONFIG_LEADER"))"

# Apply a config change through mako_config, as an operator would: while
# no replica serves the config (a Raft election is running) or the outcome
# is uncertain, try again; the change sets a value, so repeating it is
# harmless. Sets TOOL_OUT and TOOL_RC, and CHANGE_VERSION once applied and
# confirmed.
apply_change() {
    local deadline=$((SECONDS + 30))
    CHANGE_VERSION=""
    while :; do
        TOOL_OUT=$("$tool_path" --endpoints "$endpoints" "$@" 2>&1)
        TOOL_RC=$?
        if [ "$TOOL_RC" -eq 0 ]; then
            CHANGE_VERSION=$(echo "$TOOL_OUT" | sed -n -E 's/^applied: version ([0-9]+)$/\1/p')
            return 0
        fi
        if [ "$TOOL_RC" -eq 2 ] || [ "$SECONDS" -ge "$deadline" ]; then
            return 1
        fi
        sleep 1
    done
}

# Let the benchmark run past its load phase, then change the config.
sleep 5
endpoints=$(config_endpoints)
echo "Changing the config through mako_config ($endpoints)..."
if ! apply_change set_node_status "$change_site" "$change_status" || [ -z "$CHANGE_VERSION" ]; then
    echo "  ✗ mako_config set_node_status exited $TOOL_RC: $TOOL_OUT"
    exit 1
fi
change_version=$CHANGE_VERSION
echo "  ✓ Applied and confirmed replicated at version $change_version"

waited=0
lagging=("${NODES[@]}")
while [ "${#lagging[@]}" -gt 0 ] && [ "$waited" -lt 15 ]; do
    sleep 1
    waited=$((waited + 1))
    still=()
    for n in "${lagging[@]}"; do
        v=$(loaded_version "$n")
        if [ -z "$v" ] || [ "$v" -lt "$change_version" ]; then
            still+=("$n")
        fi
    done
    lagging=("${still[@]}")
done
if [ "${#lagging[@]}" -eq 0 ]; then
    echo "  ✓ Every node loaded version $change_version within ${waited}s"
else
    echo "  ✗ Not loaded version $change_version after 15s: ${lagging[*]}"
    exit 1
fi

# Act on whichever replica serves the config now.
if ! wait_for_convergence 60 1 stop; then
    report_no_convergence "before the $event" 60
    exit 1
fi
event_node=$CONFIG_LEADER
event_pid=$(pid_of "$event_node")
if [ -z "$event_pid" ]; then
    echo "  ✗ Could not find the dbtest process of $event_node"
    exit 1
fi
LINES_BEFORE=()
for i in "${!NODES[@]}"; do
    LINES_BEFORE[$i]=$(wc -l < "$(log_of "${NODES[$i]}")")
done

if [ "$event" = "kill" ]; then
    echo "Killing $event_node (pid $event_pid)..."
    kill -9 "$event_pid"
    dead_node=$event_node
    event_pid=""
    since="the kill"
else
    echo "Pausing $event_node (pid $event_pid) for ${pause_seconds}s..."
    kill -STOP "$event_pid"
    sleep "$pause_seconds"
    kill -CONT "$event_pid"
    echo "Resumed $event_node"
    since="the resume"
fi

converge_ok=1
if ! wait_for_convergence "$converge_timeout" 5 skip; then
    report_no_convergence "after $since" "$converge_timeout"
    converge_ok=0
    failed=1
fi
final_leader=$CONFIG_LEADER

# Lines a node logged after the event.
new_lines() {
    local i
    for i in "${!NODES[@]}"; do
        if [ "${NODES[$i]}" = "$1" ]; then
            tail -n +"$((LINES_BEFORE[$i] + 1))" "$(log_of "$1")"
            return
        fi
    done
}

echo ""
echo "========================================="
echo "Checking test results..."
echo "========================================="

takeovers=()
for n in "${NODES[@]}"; do
    [[ "$n" == shard0-* ]] || continue
    [ "$n" = "$event_node" ] && continue
    if new_lines "$n" | grep -a -q "now leads shard 0 and serves"; then
        takeovers+=("$n")
    fi
done
if [ "${#takeovers[@]}" -gt 0 ]; then
    echo "  ✓ Took over the config after the $event: ${takeovers[*]}"
    for n in "${takeovers[@]}"; do
        new_lines "$n" | grep -a -o "now leads shard 0 and serves.*" | head -1 | sed "s/^/    $n /"
    done
else
    echo "  ✗ No other shard-0 replica took over the config after the $event"
    failed=1
fi

if [ "$converge_ok" -eq 1 ]; then
    echo "  ✓ All live nodes read the config from $final_leader (port $(serving_port "$final_leader")), converged ${CONVERGED_AFTER}s after $since"
fi

if [ "$event" = "pause" ]; then
    if new_lines "$event_node" | grep -a -q "no longer leads shard 0"; then
        echo "  ✓ $event_node stepped down after it resumed"
    else
        echo "  ✗ $event_node never logged that it stepped down after it resumed"
        failed=1
    fi
fi

# Each takeover starts above a floor made of its term and its place in
# shard 0's replica list; two takeovers serving the same version would mean
# two leaders wrote the same version.
duplicate_versions=$(for n in "${NODES[@]}"; do
        [[ "$n" == shard0-* ]] || continue
        grep -a -o -E "now leads shard 0 and serves the [a-z]+ cluster config \(version [0-9]+" "$(log_of "$n")" \
            | sed -E 's/.*version //'
    done | sort | uniq -d)
if [ -z "$duplicate_versions" ]; then
    echo "  ✓ Every takeover served a distinct config version"
else
    echo "  ✗ Config versions served by more than one takeover: $duplicate_versions"
    failed=1
fi

if [ "$converge_ok" -eq 1 ]; then
    after=$("$tool_path" --endpoints "$endpoints" get "node/${change_site}/status" 2>&1)
    if [ "$after" = "$change_status" ]; then
        echo "  ✓ The change survived the $event: node/${change_site}/status is still $after"
    else
        echo "  ✗ node/${change_site}/status after the $event: '$after', expected '$change_status'"
        failed=1
    fi
    if apply_change set_node_status "$change_site" active; then
        echo "  ✓ The new leader takes changes too: $TOOL_OUT"
    else
        echo "  ✗ mako_config set_node_status after the $event exited $TOOL_RC: $TOOL_OUT"
        failed=1
    fi
fi

died=$(missing_nodes)
shard0_died=()
others_died=()
for n in $died; do
    if [[ "$n" == shard0-* ]]; then
        shard0_died+=("$n")
    else
        others_died+=("$n")
    fi
done
if [ -z "$died" ]; then
    echo "  ✓ Every other node is still running"
fi
if [ "${#shard0_died[@]}" -gt 0 ]; then
    echo "  ✗ Shard-0 replicas exited after the $event: ${shard0_died[*]}"
    report_exits "${shard0_died[@]}"
    failed=1
fi
if [ "${#others_died[@]}" -gt 0 ]; then
    echo "  ⚠ Exited after the $event, outside shard 0: ${others_died[*]}"
    echo "    (killing shard 0's leader also runs Mako's own cross-shard failover handling)"
    report_exits "${others_died[@]}"
fi

echo ""
echo "========================================="
if [ "$failed" -eq 0 ]; then
    echo "All checks passed! (config failover: $replication $event)"
    echo "========================================="
    exit 0
else
    echo "Some checks failed! (config failover: $replication $event)"
    echo "========================================="
    echo "Logs: ${log_prefix}_shard*.log"
    exit 1
fi
