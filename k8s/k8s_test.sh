#!/usr/bin/env bash

set -u

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLIENT="${ROOT_DIR}/build/kv_client"

NODES=(0 1 2)
CLIENT_PORTS=(30400 30401 30402)

PASS_COUNT=0
FAIL_COUNT=0

log() {
    printf '\n[%s] %s\n' "$(date '+%H:%M:%S')" "$1"
}

pass() {
    echo "PASS: $1"
    PASS_COUNT=$((PASS_COUNT + 1))
}

fail() {
    echo "FAIL: $1"
    FAIL_COUNT=$((FAIL_COUNT + 1))
}

run_client() {
    local port="$1"
    shift

    "$CLIENT" "$NODE_IP" "$port" <<< "$*" 2>&1 || true
}

get_node_ip() {
    docker inspect minikv-control-plane \
        --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}'
}

node_port() {
    local node="$1"
    echo "${CLIENT_PORTS[$node]}"
}

pod_exists() {
    local node="$1"

    kubectl get pod "minikv-${node}" \
        >/dev/null 2>&1
}

pod_ready() {
    local node="$1"

    kubectl get pod "minikv-${node}" \
        -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' \
        2>/dev/null | grep -q '^True$'
}

state_value() {
    local node="$1"
    local field="$2"

    kubectl exec "minikv-${node}" -- \
        sh -c "awk '\$1==\"${field}\" {print \$2}' /data/raft_state" \
        2>/dev/null
}

print_state() {
    local node="$1"

    local term
    local voted_for
    local commit_index
    local log_count

    term="$(state_value "$node" term)"
    voted_for="$(state_value "$node" voted_for)"
    commit_index="$(state_value "$node" commit_index)"
    log_count="$(state_value "$node" log_count)"

    printf 'node%d: term=%s voted_for=%s commit_index=%s log_count=%s\n' \
        "$node" \
        "$term" \
        "$voted_for" \
        "$commit_index" \
        "$log_count"
}

find_leader() {
    local test_key="$1"
    local test_value="$2"

    local node
    local port
    local output

    for node in "${NODES[@]}"; do
        port="$(node_port "$node")"

        output="$(
            run_client "$port" \
                "put ${test_key} ${test_value}
exit"
        )"

        if echo "$output" | grep -q 'OK: OK'; then
            echo "$node"
            return 0
        fi
    done

    return 1
}

wait_for_pods_ready() {
    local timeout="$1"
    local elapsed=0

    while (( elapsed < timeout )); do
        if pod_ready 0 &&
           pod_ready 1 &&
           pod_ready 2; then
            return 0
        fi

        sleep 1
        elapsed=$((elapsed + 1))
    done

    return 1
}

wait_for_replication() {
    local expected_commit="$1"
    local expected_log_count="$2"
    local timeout="$3"

    local elapsed=0
    local node
    local commit_index
    local log_count

    while (( elapsed < timeout )); do
        local all_ok=1

        for node in "${NODES[@]}"; do
            commit_index="$(state_value "$node" commit_index)"
            log_count="$(state_value "$node" log_count)"

            if [[ -z "$commit_index" ||
                  -z "$log_count" ]]; then
                all_ok=0
                break
            fi

            if (( commit_index < expected_commit ||
                  log_count < expected_log_count )); then
                all_ok=0
                break
            fi
        done

        if (( all_ok == 1 )); then
            return 0
        fi

        sleep 1
        elapsed=$((elapsed + 1))
    done

    return 1
}

wait_for_get() {
    local node="$1"
    local key="$2"
    local expected="$3"
    local timeout="$4"

    local port
    local output
    local elapsed=0

    port="$(node_port "$node")"

    while (( elapsed < timeout )); do
        output="$(
            run_client "$port" \
                "get ${key}
exit"
        )"

        if echo "$output" | grep -q "OK: ${expected}"; then
            return 0
        fi

        sleep 1
        elapsed=$((elapsed + 1))
    done

    return 1
}

echo
echo "========================================"
echo " MiniKV Kubernetes Integration Test"
echo "========================================"

if [[ ! -x "$CLIENT" ]]; then
    fail "kv_client not found: $CLIENT"
    exit 1
fi

log "Checking Kubernetes cluster"

if ! kubectl get nodes >/dev/null 2>&1; then
    fail "kubectl cluster is not available"
    exit 1
fi

NODE_IP="$(get_node_ip)"

if [[ -z "$NODE_IP" ]]; then
    fail "failed to get Kind control-plane IP"
    exit 1
fi

echo "Kind Node IP: ${NODE_IP}"

log "Checking StatefulSet"

if kubectl get statefulset minikv >/dev/null 2>&1; then
    pass "StatefulSet minikv exists"
else
    fail "StatefulSet minikv not found"
    exit 1
fi

log "Waiting for all MiniKV Pods"

if wait_for_pods_ready 90; then
    pass "minikv-0/1/2 are Ready"
else
    fail "MiniKV Pods did not become Ready"
    kubectl get pods -o wide
    exit 1
fi

kubectl get pods -o wide

log "Checking PVCs"

PVC_COUNT="$(kubectl get pvc \
    -o name 2>/dev/null |
    grep -c '^persistentvolumeclaim/data-minikv-' || true)"

if [[ "$PVC_COUNT" == "3" ]]; then
    pass "3 MiniKV PVCs exist"
else
    fail "expected 3 MiniKV PVCs, found ${PVC_COUNT}"
fi

log "Checking NodePort services"

SERVICE_COUNT="$(kubectl get svc \
    -o name 2>/dev/null |
    grep -c '^service/minikv-[012]-client$' || true)"

if [[ "$SERVICE_COUNT" == "3" ]]; then
    pass "3 MiniKV client Services exist"
else
    fail "expected 3 client Services, found ${SERVICE_COUNT}"
fi

log "Checking basic client connectivity"

BASIC_OK=1

for node in "${NODES[@]}"; do
    port="$(node_port "$node")"

    output="$(
        run_client "$port" \
            "ping
exit"
    )"

    if echo "$output" | grep -q 'OK: PONG'; then
        echo "node${node}: PONG"
    else
        echo "node${node}: ping failed"
        BASIC_OK=0
    fi
done

if (( BASIC_OK == 1 )); then
    pass "all 3 NodePorts accept client connections"
else
    fail "NodePort client connectivity check failed"
fi

log "Current Raft state"

for node in "${NODES[@]}"; do
    print_state "$node"
done

TEST_ID="$(date +%s)"
KEY1="k8s_integration_${TEST_ID}"
VALUE1="hello_k8s"

log "Discovering current Leader"

LEADER=""

for attempt in $(seq 1 10); do
    LEADER="$(find_leader "$KEY1" "$VALUE1" || true)"

    if [[ -n "$LEADER" ]]; then
        break
    fi

    sleep 1
done

if [[ -z "$LEADER" ]]; then
    fail "could not discover a Leader through NodePort"
    echo
    echo "Current Raft state:"
    for node in "${NODES[@]}"; do
        print_state "$node"
    done
    exit 1
fi

LEADER_PORT="$(node_port "$LEADER")"

pass "Leader discovered: minikv-${LEADER} (NodePort ${LEADER_PORT})"

log "Checking Leader can read the committed value"

if wait_for_get "$LEADER" "$KEY1" "$VALUE1" 10; then
    pass "Leader can GET ${KEY1}"
else
    fail "Leader cannot GET ${KEY1}"
fi

BASE_LOG_COUNT="$(state_value "$LEADER" log_count)"
BASE_COMMIT_INDEX="$(state_value "$LEADER" commit_index)"

if [[ -z "$BASE_LOG_COUNT" ||
      -z "$BASE_COMMIT_INDEX" ]]; then
    fail "failed to read Leader raft_state"
    exit 1
fi

EXPECTED_LOG_COUNT=$((BASE_LOG_COUNT))
EXPECTED_COMMIT_INDEX=$((BASE_COMMIT_INDEX))

log "Waiting for replication of first write"

if wait_for_replication \
    "$EXPECTED_COMMIT_INDEX" \
    "$EXPECTED_LOG_COUNT" \
    15; then
    pass "all 3 nodes reached the current committed state"
else
    fail "replication did not converge"
fi

echo
echo "Raft state after first write:"
for node in "${NODES[@]}"; do
    print_state "$node"
done

KEY2="k8s_failover_${TEST_ID}"
VALUE2="hello_failover"

log "Deleting current Leader Pod: minikv-${LEADER}"

if kubectl delete pod "minikv-${LEADER}" \
    --wait=false >/dev/null 2>&1; then
    pass "Leader Pod deletion requested"
else
    fail "failed to delete Leader Pod"
    exit 1
fi

log "Waiting for StatefulSet to recreate minikv-${LEADER}"

sleep 2

if kubectl wait \
    --for=condition=ready \
    "pod/minikv-${LEADER}" \
    --timeout=120s >/dev/null 2>&1; then
    pass "minikv-${LEADER} was recreated and became Ready"
else
    fail "minikv-${LEADER} did not become Ready after failure"
    kubectl get pods -o wide
    exit 1
fi

log "Discovering new Leader after failure"

NEW_LEADER=""

for attempt in $(seq 1 20); do
    NEW_LEADER="$(find_leader "$KEY2" "$VALUE2" || true)"

    if [[ -n "$NEW_LEADER" ]]; then
        break
    fi

    sleep 1
done

if [[ -z "$NEW_LEADER" ]]; then
    fail "no new Leader found after failure"
    echo
    echo "Current Raft state:"
    for node in "${NODES[@]}"; do
        print_state "$node"
    done
    exit 1
fi

NEW_LEADER_PORT="$(node_port "$NEW_LEADER")"

if [[ "$NEW_LEADER" == "$LEADER" ]]; then
    echo "Leader identity did not change after Pod recreation:"
    echo "old Leader: minikv-${LEADER}"
    echo "current:    minikv-${NEW_LEADER}"
    fail "Leader did not fail over to another node"
else
    pass "new Leader elected: minikv-${NEW_LEADER} (NodePort ${NEW_LEADER_PORT})"
fi

log "Checking old data on the new Leader"

if wait_for_get "$NEW_LEADER" "$KEY1" "$VALUE1" 20; then
    pass "new Leader recovered ${KEY1}=${VALUE1}"
else
    fail "new Leader could not recover the pre-failure value"
fi

log "Checking new Leader can serve new writes"

if wait_for_get "$NEW_LEADER" "$KEY2" "$VALUE2" 10; then
    pass "new Leader can read ${KEY2}"
else
    fail "new Leader could not read its failover test value"
fi

NEW_COMMIT_INDEX="$(state_value "$NEW_LEADER" commit_index)"
NEW_LOG_COUNT="$(state_value "$NEW_LEADER" log_count)"

if [[ -n "$NEW_COMMIT_INDEX" &&
      -n "$NEW_LOG_COUNT" ]]; then
    EXPECTED_NEW_COMMIT=$((NEW_COMMIT_INDEX))
    EXPECTED_NEW_LOG=$((NEW_LOG_COUNT))

    log "Waiting for all nodes to catch up after failover"

    if wait_for_replication \
        "$EXPECTED_NEW_COMMIT" \
        "$EXPECTED_NEW_LOG" \
        20; then
        pass "all 3 nodes converged after failover"
    else
        fail "cluster did not converge after failover"
    fi
else
    fail "failed to read new Leader raft_state"
fi

log "Checking recovered Pod still has persistent state"

RECOVERED_TERM="$(state_value "$LEADER" term)"
RECOVERED_COMMIT="$(state_value "$LEADER" commit_index)"
RECOVERED_LOG="$(state_value "$LEADER" log_count)"

if [[ -n "$RECOVERED_TERM" &&
      -n "$RECOVERED_COMMIT" &&
      -n "$RECOVERED_LOG" ]]; then
    pass "recreated minikv-${LEADER} has persistent Raft state"
else
    fail "recreated minikv-${LEADER} has incomplete raft_state"
fi

echo
echo "========================================"
echo " Final Cluster State"
echo "========================================"

for node in "${NODES[@]}"; do
    print_state "$node"
done

echo
echo "========================================"
echo " Kubernetes Integration Test Result"
echo "========================================"
echo "PASS: ${PASS_COUNT}"
echo "FAIL: ${FAIL_COUNT}"
echo "========================================"

if (( FAIL_COUNT == 0 )); then
    echo "ALL KUBERNETES MINIKV TESTS PASSED"
    exit 0
fi

echo "KUBERNETES MINIKV TEST FAILED"
exit 1
