#!/bin/bash

# Restart test for the cluster config (MAKO_CONFIG_TABLE=1) on a 2-shard
# Paxos run. It starts the cluster three times, each time on new ports, and
# crashes every process (SIGKILL) between starts. In the first run it
# changes the config through mako_config: a node's status, shard 1's status
# and a node's address. After the second start it checks that:
#   1. shard 0's leader restored the config from the first run's log,
#   2. both status changes are still there, while the node address is the
#      one in the new shard config, not the changed one (a restart takes the
#      topology from the shard config it starts with),
#   3. every node loads the restored config, and the leader takes a change.
# After the third start it checks that the second run persisted what it
# restored and its own change: all of them are still there.
#
# Usage: test_2shard_config_restart.sh
# Env: MAKO_CFG_RESTART_SETTLE (seconds between the last change and the
#      crash, default 3: shard 0's leader persists its log asynchronously).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/simple_transaction_rep_port_utils.sh"

echo "========================================="
echo "Testing cluster-config restart (paxos, crash and restart twice)"
echo "========================================="

rm -f nfs_sync_*
# The name dbtest puts in its log directories (getpwuid, like whoami).
USERNAME=$(whoami 2>/dev/null || echo "${USER:-unknown}")
rm -rf /tmp/${USERNAME}_mako_rocksdb_shard*

trd=${MAKO_CI_TRD:-6}
settle="${MAKO_CFG_RESTART_SETTLE:-3}"
script_name="$(basename "$0")"
tool_path="./${BUILD_DIR:-build}/mako_config"
export MAKO_CONFIG_TABLE=1
export LD_LIBRARY_PATH="$(pwd)/${BUILD_DIR:-build}:${LD_LIBRARY_PATH}"

REPLICAS=(localhost learner p2 p1)
NODES=()
for s in 0 1; do
    for r in "${REPLICAS[@]}"; do
        NODES+=("shard${s}-${r}")
    done
done
bogus_addr="10.255.255.1:1"

WRAPPER_PIDS=()
TEMP_CONFIG=""
TEMP_PAXOS_DIR=""
run=0
failed=0

for bin in "./${BUILD_DIR:-build}/dbtest" "$tool_path"; do
    if [ ! -x "$bin" ]; then
        echo "Error: $(basename "$bin") not found or not executable at '$bin'"
        echo "Build it first (for Docker: ./docker_build.sh build), then retry."
        exit 1
    fi
done

if ! ensure_paxos_replication_configs "$trd" 2; then
    exit 1
fi

# Stops every process of the current run with SIGKILL, as a crash would.
crash_cluster() {
    local pid
    if [ -n "$TEMP_CONFIG" ]; then
        pkill -9 -f "$TEMP_CONFIG" 2>/dev/null || true
    fi
    for pid in "${WRAPPER_PIDS[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
    WRAPPER_PIDS=()
    [ -n "$TEMP_CONFIG" ] && rm -f "$TEMP_CONFIG"
    [ -n "$TEMP_PAXOS_DIR" ] && rm -rf "$TEMP_PAXOS_DIR"
    TEMP_CONFIG=""
    TEMP_PAXOS_DIR=""
}

cleanup() {
    crash_cluster
    rm -rf /tmp/${USERNAME}_mako_rocksdb_shard*
    unset MAKO_CONFIG MAKO_PAXOS_CONFIG_DIR
}

trap cleanup EXIT
trap 'cleanup; exit 130' INT TERM

log_of() {
    echo "${script_name}_run${run}_$1.log"
}

# Whether two numbers read from the logs are the same. A process's threads
# share its log, and another thread's output can land right after a
# number, before the line ends, so one of them may carry extra digits.
same_number() {
    [ -n "$1" ] && [ -n "$2" ] && { [[ "$1" == "$2"* ]] || [[ "$2" == "$1"* ]]; }
}

# Last config version a node loaded.
loaded_version() {
    grep -a -o -E "loaded cluster config version [0-9]+" "$(log_of "$1")" 2>/dev/null \
        | tail -1 | sed -E 's/.* //'
}

# Starts the cluster on newly picked ports.
start_cluster() {
    local shard r
    run=$((run + 1))
    TEMP_CONFIG=$(make_simple_txn_rep_config 2 "$trd") || return 1
    TEMP_PAXOS_DIR=$(make_paxos_replication_configs 2 "$trd" paxos) || return 1
    export MAKO_CONFIG="$TEMP_CONFIG"
    export MAKO_PAXOS_CONFIG_DIR="$TEMP_PAXOS_DIR"
    rm -f nfs_sync_*
    echo "Starting run $run..."
    for shard in 0 1; do
        for r in "${REPLICAS[@]}"; do
            if [ "$r" = "p1" ]; then
                sleep 1
            fi
            rm -f "$(log_of "shard${shard}-${r}")"
            nohup bash bash/shard.sh 2 "$shard" "$trd" "$r" 0 1 > "$(log_of "shard${shard}-${r}")" 2>&1 &
            WRAPPER_PIDS+=($!)
        done
        if [ "$shard" = 0 ]; then
            sleep 5
        fi
    done
    return 0
}

# Waits up to 120 s until shard 0's leader serves the config and every
# node has loaded the version it serves. Sets SERVED_VERSION and ENDPOINTS.
wait_ready() {
    local waited=0 n v ready
    SERVED_VERSION=""
    while [ "$waited" -lt 120 ]; do
        if grep -a -q "config service listening on" "$(log_of shard0-localhost)" 2>/dev/null; then
            SERVED_VERSION=$(loaded_version shard0-localhost)
            ready=1
            for n in "${NODES[@]}"; do
                v=$(loaded_version "$n")
                if ! same_number "$v" "$SERVED_VERSION"; then
                    ready=0
                    break
                fi
            done
            if [ "$ready" = 1 ]; then
                ENDPOINTS=$(grep -a -o -h -E "watching shard-0 config at [0-9.:, ]+" \
                    "$(log_of shard1-localhost)" | head -1 | sed -E 's/.* at //; s/[ ]+//g; s/,$//')
                [ -n "$ENDPOINTS" ] && return 0
            fi
        fi
        sleep 1
        waited=$((waited + 1))
    done
    echo "  ✗ Run $run: not every node loaded shard 0's config within 120s"
    for n in "${NODES[@]}"; do
        echo "    $n: loaded version $(loaded_version "$n")"
    done
    return 1
}

# Applies one change through mako_config; it must be confirmed. While no
# replica serves the config or the outcome is uncertain, it tries again
# for up to 30 s: the change sets a value, so repeating it is harmless.
change() {
    local out rc deadline=$((SECONDS + 30))
    while :; do
        out=$("$tool_path" --endpoints "$ENDPOINTS" "$@" 2>&1)
        rc=$?
        if [ "$rc" -eq 0 ]; then
            echo "  ✓ Run $run: $* ($out)"
            return 0
        fi
        if [ "$rc" -eq 2 ] || [ "$SECONDS" -ge "$deadline" ]; then
            echo "  ✗ Run $run: mako_config $* exited $rc: $out"
            failed=1
            return 1
        fi
        sleep 1
    done
}

# Checks that a key reads as expected ($3 = "not" for "anything but").
expect() {
    local key=$1 want=$2 how=$3 got
    got=$("$tool_path" --endpoints "$ENDPOINTS" get "$key" 2>&1)
    if [ "$how" = "not" ]; then
        if [ -n "$got" ] && [ "$got" != "$want" ]; then
            echo "  ✓ Run $run: $key is $got"
        else
            echo "  ✗ Run $run: $key is '$got', expected anything but '$want'"
            failed=1
        fi
    elif [ "$got" = "$want" ]; then
        echo "  ✓ Run $run: $key is still $got"
    else
        echo "  ✗ Run $run: $key is '$got', expected '$want'"
        failed=1
    fi
}

# Checks shard 0's leader restored the config from the previous run's log.
expect_restored() {
    local prev_log=$1 line
    line=$(grep -a -o -E "restored [0-9]+ of [0-9]+ config entries from [^ ]+ .*" \
        "$(log_of shard0-localhost)" | head -1)
    if [ -n "$line" ] && [[ "$line" == *"from ${prev_log} "* ]]; then
        echo "  ✓ Run $run: shard 0's leader $line"
    else
        echo "  ✗ Run $run: shard 0's leader did not restore the config from $prev_log (${line:-no restore logged})"
        failed=1
    fi
}

# The log shard 0's leader persists to in the current run.
leader_log() {
    grep -a -o -E "Leader initializing RocksDB at path: [^ ]+" "$(log_of shard0-localhost)" \
        | head -1 | sed -E 's/.*path: //'
}

# ---- run 1: change the config, then crash -------------------------------
start_cluster || exit 1
wait_ready || exit 1
echo "  ✓ Run 1: every node loaded version $SERVED_VERSION from shard 0's leader"
if grep -a -q "restored [0-9]* of" "$(log_of shard0-localhost)"; then
    echo "  ✗ Run 1: shard 0's leader restored a config on a fresh start"
    failed=1
fi
change set_node_status shard1-p1 draining
change set_shard_status 1 draining
change set_node_addr shard1-p2 "$bogus_addr"
run1_log=$(leader_log)
sleep "$settle"
echo "Crashing run 1..."
crash_cluster

# ---- run 2: restored from run 1, change once more, crash ----------------
start_cluster || exit 1
wait_ready || exit 1
echo "  ✓ Run 2: every node loaded version $SERVED_VERSION from shard 0's leader"
expect_restored "$run1_log"
expect node/shard1-p1/status draining
expect shard/1/status draining
expect node/shard1-p2/addr "$bogus_addr" not
change set_node_status shard1-learner draining
run2_log=$(leader_log)
sleep "$settle"
echo "Crashing run 2..."
crash_cluster

# ---- run 3: restored from run 2, which persisted run 1's changes --------
start_cluster || exit 1
wait_ready || exit 1
echo "  ✓ Run 3: every node loaded version $SERVED_VERSION from shard 0's leader"
expect_restored "$run2_log"
expect node/shard1-p1/status draining
expect shard/1/status draining
expect node/shard1-learner/status draining
change set_node_status shard1-p1 active

echo ""
echo "========================================="
if [ "$failed" -eq 0 ]; then
    echo "All checks passed! (config restart)"
    echo "========================================="
    exit 0
else
    echo "Some checks failed! (config restart)"
    echo "========================================="
    echo "Logs: ${script_name}_run*_shard*.log"
    exit 1
fi
