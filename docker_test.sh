#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLIENT="${ROOT_DIR}/build/kv_client"

NODES=(0 1 2)

INITIAL_KEY="docker_test_${RANDOM}_$(date +%s)"
INITIAL_VALUE="hello_docker"

FAILOVER_KEY="docker_failover_${RANDOM}_$(date +%s)"
FAILOVER_VALUE="hello_after_failover"

WAIT_SECONDS=15

PASS=0
FAIL=0

log() {
    echo "[INFO] $*"
}

pass() {
    echo "[PASS] $*"
    PASS=$((PASS + 1))
}

fail() {
    echo "[FAIL] $*"
    FAIL=$((FAIL + 1))
}

port_of() {
    local node="$1"
    echo $((9400 + node))
}

container_of() {
    local node="$1"
    echo "minikv-node${node}"
}

is_running() {
    local node="$1"

    local running
    running="$(
        docker inspect \
            -f '{{.State.Running}}' \
            "$(container_of "$node")" 2>/dev/null || true
    )"

    [[ "$running" == "true" ]]
}

run_client() {
    local port="$1"
    shift

    printf "%s\n" "$@" |
        timeout 8 "${CLIENT}" 127.0.0.1 "$port" 2>&1 || true
}

wait_for_running() {
    local node="$1"

    local i

    for ((i = 1; i <= WAIT_SECONDS; ++i)); do
        if is_running "$node"; then
            return 0
        fi

        sleep 1
    done

    return 1
}

find_leader() {
    local test_key="$1"
    local test_value="$2"

    local node
    local port
    local output

    for node in "${NODES[@]}"; do

        if ! is_running "$node"; then
            continue
        fi

        port="$(port_of "$node")"

        output="$(
            run_client "$port" \
                "put ${test_key} ${test_value}" \
                "quit"
        )"

        if grep -q "OK: OK" <<<"$output"; then
            echo "$node"
            return 0
        fi
    done

    return 1
}

check_get() {
    local node="$1"
    local key="$2"
    local expected="$3"

    local port
    local output

    port="$(port_of "$node")"

    output="$(
        run_client "$port" \
            "get ${key}" \
            "quit"
    )"

    if grep -q "OK: ${expected}" <<<"$output"; then
        return 0
    fi

    echo "$output" >&2
    return 1
}

wait_for_leader() {
    local key="$1"
    local value="$2"

    local i
    local leader

    for ((i = 1; i <= WAIT_SECONDS; ++i)); do

        leader="$(
            find_leader \
                "$key" \
                "$value" ||
                true
        )"

        if [[ -n "$leader" ]]; then
            echo "$leader"
            return 0
        fi

        sleep 1
    done

    return 1
}

echo "========================================"
echo " MiniKV Docker Integration Test"
echo "========================================"
echo

# ------------------------------------------------------------
# 1. 检查 kv_client
# ------------------------------------------------------------

echo "==> [1/7] Check test client"

if [[ ! -x "$CLIENT" ]]; then
    fail "kv_client not found: $CLIENT"

    echo
    echo "Build it first:"
    echo "  cmake --build build -j4"

    exit 1
fi

pass "kv_client found"

# ------------------------------------------------------------
# 2. 确保 Docker 集群运行
# ------------------------------------------------------------

echo
echo "==> [2/7] Ensure Docker cluster is running"

RUNNING_COUNT=0

for node in "${NODES[@]}"; do
    if is_running "$node"; then
        ((RUNNING_COUNT += 1))
        log "$(container_of "$node") is already running"
    fi
done

if [[ "$RUNNING_COUNT" -ne 3 ]]; then

    log "Docker cluster is not fully running, starting it..."

    docker compose up -d --build

    for node in "${NODES[@]}"; do
        if wait_for_running "$node"; then
            pass "$(container_of "$node") is running"
        else
            fail "$(container_of "$node") failed to start"

            docker compose ps
            docker compose logs --tail=100

            exit 1
        fi
    done

else

    pass "all 3 Docker nodes are already running"

fi

# ------------------------------------------------------------
# 3. 检查 Docker 网络
# ------------------------------------------------------------

echo
echo "==> [3/7] Check Docker node DNS"

for node in "${NODES[@]}"; do

    host="minikv-node${node}"

    ip="$(
        docker exec \
            "$(container_of 0)" \
            getent hosts "$host" 2>/dev/null |
        awk 'NR == 1 { print $1; exit }'
    )"

    if [[ -n "$ip" ]]; then
        pass "${host} resolves to ${ip}"
    else
        fail "${host} DNS resolution failed"
        exit 1
    fi

done

# ------------------------------------------------------------
# 4. 找 Leader + 初始 PUT/GET
# ------------------------------------------------------------

echo
echo "==> [4/7] Find initial Leader"

INITIAL_LEADER="$(
    wait_for_leader \
        "$INITIAL_KEY" \
        "$INITIAL_VALUE" ||
        true
)"

if [[ -z "$INITIAL_LEADER" ]]; then

    fail "cannot find initial Leader"

    docker compose logs --tail=200

    exit 1

fi

INITIAL_PORT="$(port_of "$INITIAL_LEADER")"

pass "initial Leader = node${INITIAL_LEADER} (${INITIAL_PORT})"

echo
echo "==> Verify initial data"

if check_get \
    "$INITIAL_LEADER" \
    "$INITIAL_KEY" \
    "$INITIAL_VALUE"; then

    pass "initial PUT/GET succeeded"

else

    fail "initial GET failed"
    exit 1

fi

# ------------------------------------------------------------
# 5. 杀 Leader
# ------------------------------------------------------------

echo
echo "==> [5/7] Kill initial Leader"

INITIAL_CONTAINER="$(container_of "$INITIAL_LEADER")"

log "stopping ${INITIAL_CONTAINER}"

docker kill "$INITIAL_CONTAINER" >/dev/null

sleep 3

if ! is_running "$INITIAL_LEADER"; then
    pass "old Leader node${INITIAL_LEADER} stopped"
else
    fail "old Leader node${INITIAL_LEADER} is still running"
    exit 1
fi

# ------------------------------------------------------------
# 6. 找新 Leader + 验证旧数据
# ------------------------------------------------------------

echo
echo "==> [6/7] Find new Leader after failover"

NEW_LEADER="$(
    wait_for_leader \
        "$FAILOVER_KEY" \
        "$FAILOVER_VALUE" ||
        true
)"

if [[ -z "$NEW_LEADER" ]]; then

    fail "new Leader was not elected"

    docker compose ps
    docker compose logs --tail=200

    exit 1

fi

NEW_PORT="$(port_of "$NEW_LEADER")"

if [[ "$NEW_LEADER" == "$INITIAL_LEADER" ]]; then

    fail "new Leader is the stopped node"

    exit 1

fi

pass "new Leader = node${NEW_LEADER} (${NEW_PORT})"

if check_get \
    "$NEW_LEADER" \
    "$INITIAL_KEY" \
    "$INITIAL_VALUE"; then

    pass "old data survived Leader failure"

else

    fail "old data is missing after failover"
    exit 1

fi

if check_get \
    "$NEW_LEADER" \
    "$FAILOVER_KEY" \
    "$FAILOVER_VALUE"; then

    pass "new Leader can serve new data"

else

    fail "new Leader cannot read new data"
    exit 1

fi

# ------------------------------------------------------------
# 7. 恢复旧 Leader
# ------------------------------------------------------------

echo
echo "==> [7/7] Restart old Leader"

docker compose start "$INITIAL_CONTAINER" >/dev/null

if wait_for_running "$INITIAL_LEADER"; then

    pass "old Leader node${INITIAL_LEADER} restarted"

else

    fail "old Leader did not restart"

    docker compose ps
    docker compose logs --tail=100 "$INITIAL_CONTAINER"

    exit 1

fi

sleep 3

echo
echo "==> Final cluster status"

docker compose ps

echo
echo "==> Recent logs"

docker compose logs --tail=80

echo
echo "========================================"
echo " Docker Integration Test Result"
echo "========================================"

echo "PASS: ${PASS}"
echo "FAIL: ${FAIL}"

echo "Initial Leader: node${INITIAL_LEADER}"
echo "New Leader:     node${NEW_LEADER}"

echo "========================================"

if [[ "$FAIL" -eq 0 ]]; then

    echo
    echo "ALL DOCKER INTEGRATION TESTS PASSED"

    exit 0

else

    echo
    echo "DOCKER INTEGRATION TEST FAILED"

    exit 1

fi