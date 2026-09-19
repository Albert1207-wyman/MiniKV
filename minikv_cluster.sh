#!/usr/bin/env bash

set -u

ROOT="$(cd "$(dirname "$0")" && pwd)"
SERVER="$ROOT/build/kv_server"
CLIENT="$ROOT/build/kv_client"

BASE="$ROOT/.minikv-cluster"
LOG_DIR="$BASE/logs"
PID_DIR="$BASE/pids"
DATA_DIR="$BASE/data"

mkdir -p "$LOG_DIR" "$PID_DIR" "$DATA_DIR"

CLIENT_PORTS=(9400 9401 9402)
RAFT_PORTS=(9300 9301 9302)

PASS=0
FAIL=0

green='\033[32m'
red='\033[31m'
yellow='\033[33m'
blue='\033[34m'
reset='\033[0m'

info() {
    echo -e "${blue}[INFO]${reset} $*"
}

pass() {
    echo -e "${green}[PASS]${reset} $*"
    PASS=$((PASS + 1))
}

fail() {
    echo -e "${red}[FAIL]${reset} $*"
    FAIL=$((FAIL + 1))
}

node_pid_file() {
    echo "$PID_DIR/node$1.pid"
}

node_log_file() {
    echo "$LOG_DIR/node$1.log"
}

node_data_dir() {
    echo "$DATA_DIR/node$1"
}

is_running() {
    local id="$1"
    local file
    file="$(node_pid_file "$id")"

    [[ -f "$file" ]] || return 1

    local pid
    pid="$(cat "$file" 2>/dev/null || true)"

    [[ -n "$pid" ]] || return 1

    kill -0 "$pid" 2>/dev/null
}

start_node() {
    local id="$1"

    if is_running "$id"; then
        info "node$id already running"
        return 0
    fi

    mkdir -p "$(node_data_dir "$id")"

    local log
    local pid_file

    log="$(node_log_file "$id")"
    pid_file="$(node_pid_file "$id")"

    info "starting node$id"

    {
        echo
        echo "========== node$id process start =========="
        echo
    } >> "$log"

    nohup stdbuf -oL -eL "$SERVER" \
        "$(node_data_dir "$id")" \
        "${CLIENT_PORTS[$id]}" \
        "$id" \
        "${RAFT_PORTS[$id]}" \
        "${RAFT_PORTS[0]}" \
        "${RAFT_PORTS[1]}" \
        "${RAFT_PORTS[2]}" \
        >> "$log" 2>&1 < /dev/null &

    echo $! > "$pid_file"

    sleep 0.3

    if is_running "$id"; then
        info "node$id started pid=$(cat "$pid_file")"
        return 0
    fi

    fail "node$id failed to start"
    return 1
}

stop_node() {
    local id="$1"

    if ! is_running "$id"; then
        rm -f "$(node_pid_file "$id")"
        return 0
    fi

    local pid
    pid="$(cat "$(node_pid_file "$id")")"

    info "stopping node$id pid=$pid"

    kill -TERM "$pid" 2>/dev/null || true

    for _ in {1..30}; do
        if ! kill -0 "$pid" 2>/dev/null; then
            break
        fi

        sleep 0.1
    done

    if kill -0 "$pid" 2>/dev/null; then
        kill -KILL "$pid" 2>/dev/null || true
    fi

    rm -f "$(node_pid_file "$id")"
}

start_cluster() {
    [[ -x "$SERVER" ]] || {
        echo "kv_server not found: $SERVER"
        return 1
    }

    [[ -x "$CLIENT" ]] || {
        echo "kv_client not found: $CLIENT"
        return 1
    }

    for id in 0 1 2; do
        start_node "$id" || return 1
    done
}

stop_cluster() {
    for id in 0 1 2; do
        stop_node "$id"
    done

    info "cluster stopped"
}

get_state() {
    local id="$1"
    local log

    log="$(node_log_file "$id")"

    [[ -f "$log" ]] || {
        echo "UNKNOWN"
        return
    }

    local line

    line="$(grep -E -- '-> (LEADER|FOLLOWER|CANDIDATE)' "$log" \
        2>/dev/null | tail -n 1)"

    case "$line" in
        *"-> LEADER"*)
            echo "LEADER"
            ;;
        *"-> CANDIDATE"*)
            echo "CANDIDATE"
            ;;
        *"-> FOLLOWER"*)
            echo "FOLLOWER"
            ;;
        *)
            echo "UNKNOWN"
            ;;
    esac
}

find_leader() {
    for _ in {1..60}; do
        local leader=-1
        local count=0

        for id in 0 1 2; do
            if ! is_running "$id"; then
                continue
            fi

            if [[ "$(get_state "$id")" == "LEADER" ]]; then
                leader="$id"
                count=$((count + 1))
            fi
        done

        if [[ "$count" -eq 1 ]]; then
            echo "$leader"
            return 0
        fi

        sleep 0.2
    done

    return 1
}

client_command() {
    local id="$1"
    shift

    "$CLIENT" \
        127.0.0.1 \
        "${CLIENT_PORTS[$id]}" \
        <<< "$(printf '%s\n' "$@" ; echo exit)"
}

get_value() {
    local id="$1"
    local key="$2"

    client_command "$id" "get $key" 2>/dev/null |
        sed -n 's/^kv> OK: //p' |
        tail -n 1
}

put_value() {
    local id="$1"
    local key="$2"
    local value="$3"

    client_command "$id" "put $key $value" 2>/dev/null |
        grep -E "OK: OK$" |
        tail -n 1 |
        sed "s/.*OK: //"
}

wait_value_on_leader() {
    local id="$1"
    local key="$2"
    local expected="$3"

    for _ in {1..50}; do
        local value

        value="$(get_value "$id" "$key")"

        if [[ "$value" == "$expected" ]]; then
            return 0
        fi

        sleep 0.2
    done

    return 1
}

get_log_line_count() {
    local id="$1"
    local log

    log="$(node_log_file "$id")"

    if [[ ! -f "$log" ]]; then
        echo 0
        return
    fi

    wc -l < "$log"
}

wait_for_apply() {
    local id="$1"
    local index="$2"
    local start_line="$3"

    local log
    log="$(node_log_file "$id")"

    for _ in {1..50}; do
        if [[ -f "$log" ]] &&
            tail -n +"$((start_line + 1))" "$log" 2>/dev/null |
                grep -qE "\\[RAFT ${id}\\] applied index=${index} "; then
            return 0
        fi

        sleep 0.2
    done

    return 1
}

check_state_file() {
    local id="$1"
    local file
    file="$(node_data_dir "$id")/raft_state"

    if [[ ! -f "$file" ]]; then
        fail "node$id raft_state missing"
        return 1
    fi

    pass "node$id raft_state exists"

    if grep -q '^term ' "$file"; then
        pass "node$id term persisted"
    else
        fail "node$id term missing"
    fi

    if grep -q '^voted_for ' "$file"; then
        pass "node$id voted_for persisted"
    else
        fail "node$id voted_for missing"
    fi

    if grep -q '^log_count [1-9]' "$file"; then
        pass "node$id log persisted"
    else
        fail "node$id log missing"
    fi

    if grep -q '^commit_index [1-9]' "$file"; then
        pass "node$id commit_index persisted"
    else
        fail "node$id commit_index missing"
    fi
}

status() {
    echo
    echo "=============================="
    echo " MiniKV Cluster"
    echo "=============================="

    for id in 0 1 2; do
        local state="DOWN"
        local pid="-"

        if is_running "$id"; then
            state="RUNNING"
            pid="$(cat "$(node_pid_file "$id")")"
        fi

        printf "node%d  client=%d  raft=%d  %-7s  role=%s  pid=%s\n" \
            "$id" \
            "${CLIENT_PORTS[$id]}" \
            "${RAFT_PORTS[$id]}" \
            "$state" \
            "$(get_state "$id")" \
            "$pid"
    done

    echo "=============================="
}

logs() {
    for id in 0 1 2; do
        echo
        echo "========== node$id =========="

        if [[ -f "$(node_log_file "$id")" ]]; then
            tail -n 40 "$(node_log_file "$id")"
        else
            echo "log not found"
        fi
    done
}

clean() {
    stop_cluster

    rm -rf "$BASE"

    mkdir -p \
        "$LOG_DIR" \
        "$PID_DIR" \
        "$DATA_DIR"

    info "cluster data cleaned"
}

test_persistence() {
    PASS=0
    FAIL=0

    echo
    echo "========================================"
    echo " MiniKV Raft Persistence Test"
    echo "========================================"
    echo

    clean

    start_cluster || return 1

    sleep 1

    pass "3 nodes started"

    local old_leader

    if ! old_leader="$(find_leader)"; then
        fail "Leader election failed"
        status
        logs
        return 1
    fi

    pass "node$old_leader elected as Leader"

    echo
    info "initial Leader: node$old_leader"

    local before_apply_line_0
    local before_apply_line_1
    local before_apply_line_2

    before_apply_line_0="$(get_log_line_count 0)"
    before_apply_line_1="$(get_log_line_count 1)"
    before_apply_line_2="$(get_log_line_count 2)"

    if [[ "$(put_value "$old_leader" persist_test hello)" == "OK" ]]; then
        pass "Leader PUT persist_test=hello"
    else
        fail "Leader PUT failed"
        logs
        return 1
    fi

    if wait_for_apply 0 1 "$before_apply_line_0"; then
        pass "node0 applied Raft index=1"
    else
        fail "node0 did not apply Raft index=1"
    fi

    if wait_for_apply 1 1 "$before_apply_line_1"; then
        pass "node1 applied Raft index=1"
    else
        fail "node1 did not apply Raft index=1"
    fi

    if wait_for_apply 2 1 "$before_apply_line_2"; then
        pass "node2 applied Raft index=1"
    else
        fail "node2 did not apply Raft index=1"
    fi

    echo
    info "checking persistent Raft state"

    for id in 0 1 2; do
        check_state_file "$id"
    done

    if wait_value_on_leader \
        "$old_leader" \
        persist_test \
        hello; then

        pass "Leader retained persist_test=hello"

    else

        fail "Leader could not read persist_test=hello"

    fi

    echo
    info "stopping old Leader node$old_leader"

    stop_node "$old_leader"

    if ! is_running "$old_leader"; then
        pass "old Leader stopped"
    else
        fail "old Leader still running"
    fi

    local new_leader

    if ! new_leader="$(find_leader)"; then
        fail "new Leader election failed"
        status
        logs
        return 1
    fi

    if [[ "$new_leader" != "$old_leader" ]]; then
        pass "node$new_leader became new Leader"
    else
        fail "old Leader remained Leader"
    fi

    echo
    info "new Leader: node$new_leader"

    if wait_value_on_leader \
        "$new_leader" \
        persist_test \
        hello; then

        pass "node$new_leader retained old committed data"

    else

        fail "node$new_leader lost old committed data"

    fi

    echo
    info "restarting old Leader node$old_leader"

    #
    # The log file is append-only across process restarts.
    # Record the current line count before starting the new process.
    #
    local old_leader_log_before_restart

    old_leader_log_before_restart="$(
        get_log_line_count "$old_leader"
    )"

    start_node "$old_leader"

    sleep 1

    local old_log
    old_log="$(node_log_file "$old_leader")"

    if grep -q "loaded persistent state" "$old_log"; then
        pass "node$old_leader loaded persistent Raft state"
    else
        fail "node$old_leader did not load persistent state"
    fi

    if wait_for_apply \
        "$old_leader" \
        1 \
        "$old_leader_log_before_restart"; then

        pass "node$old_leader replayed persisted index=1"

    else

        fail "node$old_leader failed to replay persisted index=1"

    fi

    if [[ "$(put_value \
        "$new_leader" \
        persist_test2 \
        world)" == "OK" ]]; then

        pass "new Leader PUT persist_test2=world"

    else

        fail "new Leader PUT persist_test2 failed"
        logs

    fi

    local old_leader_log_before_second_put

    old_leader_log_before_second_put="$(
        get_log_line_count "$old_leader"
    )"

    if wait_for_apply \
        "$old_leader" \
        2 \
        "$old_leader_log_before_second_put"; then

        pass "restarted node$old_leader applied new index=2"

    else

        fail "restarted node$old_leader did not apply new index=2"

    fi

    if wait_value_on_leader \
        "$new_leader" \
        persist_test2 \
        world; then

        pass "new Leader retained persist_test2=world"

    else

        fail "new Leader could not read persist_test2=world"

    fi

    echo
    echo "========================================"
    echo " Persistence Test Result"
    echo "========================================"
    echo "PASS: $PASS"
    echo "FAIL: $FAIL"
    echo "Old Leader: node$old_leader"
    echo "New Leader: node$new_leader"
    echo "========================================"

    if [[ "$FAIL" -eq 0 ]]; then
        echo
        echo -e "${green}ALL RAFT PERSISTENCE TESTS PASSED${reset}"
        return 0
    fi

    echo
    echo -e "${red}RAFT PERSISTENCE TEST FAILED${reset}"
    return 1
}

usage() {
    echo "Usage:"
    echo "  $0 start"
    echo "  $0 stop"
    echo "  $0 status"
    echo "  $0 logs"
    echo "  $0 test"
    echo "  $0 clean"
}

case "${1:-}" in
    start)
        start_cluster
        ;;
    stop)
        stop_cluster
        ;;
    status)
        status
        ;;
    logs)
        logs
        ;;
    test)
        test_persistence
        ;;
    clean)
        clean
        ;;
    *)
        usage
        exit 1
        ;;
esac