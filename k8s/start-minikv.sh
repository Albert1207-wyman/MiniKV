#!/usr/bin/env bash

set -euo pipefail

SERVER="${SERVER:-/app/kv_server}"
DATA_DIR="${DATA_DIR:-/data}"
POD_NAME="${POD_NAME:-}"

if [[ -z "${POD_NAME}" ]]; then
    echo "[ERROR] POD_NAME is required" >&2
    exit 1
fi

NODE_ID=""
CLIENT_PORT=""
RAFT_PORT=""
PEER_MODE=""

PEER0_HOST=""
PEER1_HOST=""
PEER2_HOST=""

PEER0_PORT="9300"
PEER1_PORT="9301"
PEER2_PORT="9302"

# Kubernetes StatefulSet
#   minikv-0
#   minikv-1
#   minikv-2
if [[ "${POD_NAME}" =~ ^minikv-[0-2]$ ]]; then
    NODE_ID="${POD_NAME##*-}"

    CLIENT_PORT=$((9400 + NODE_ID))
    RAFT_PORT=$((9300 + NODE_ID))

    HEADLESS_SERVICE="${HEADLESS_SERVICE:-minikv-headless}"
    NAMESPACE="${POD_NAMESPACE:-default}"

    DOMAIN="${HEADLESS_SERVICE}.${NAMESPACE}.svc.cluster.local"

    PEER0_HOST="minikv-0.${DOMAIN}"
    PEER1_HOST="minikv-1.${DOMAIN}"
    PEER2_HOST="minikv-2.${DOMAIN}"

    PEER_MODE="kubernetes dns"

# Docker Compose
#   minikv-node0
#   minikv-node1
#   minikv-node2
elif [[ "${POD_NAME}" =~ ^minikv-node[0-2]$ ]]; then
    NODE_ID="${POD_NAME##*node}"

    CLIENT_PORT=$((9400 + NODE_ID))
    RAFT_PORT=$((9300 + NODE_ID))

    PEER0_HOST="minikv-node0"
    PEER1_HOST="minikv-node1"
    PEER2_HOST="minikv-node2"

    PEER_MODE="docker dns"

else
    echo "[ERROR] unsupported POD_NAME: ${POD_NAME}" >&2
    echo "[ERROR] expected minikv-0..2 or minikv-node0..2" >&2
    exit 1
fi

mkdir -p "${DATA_DIR}"

echo "========================================"
echo " MiniKV container startup"
echo "========================================"
echo " POD      : ${POD_NAME}"
echo " Node ID  : ${NODE_ID}"
echo " Client   : ${CLIENT_PORT}"
echo " Raft UDP : ${RAFT_PORT}"
echo " Peer Mode: ${PEER_MODE}"
echo
echo " Peer0    : ${PEER0_HOST}:${PEER0_PORT}"
echo " Peer1    : ${PEER1_HOST}:${PEER1_PORT}"
echo " Peer2    : ${PEER2_HOST}:${PEER2_PORT}"
echo "========================================"

echo "[INFO] starting kv_server with custom peer host/port arguments"

exec "${SERVER}" \
    "${DATA_DIR}" \
    "${CLIENT_PORT}" \
    "${NODE_ID}" \
    "${RAFT_PORT}" \
    "${PEER0_HOST}" \
    "${PEER0_PORT}" \
    "${PEER1_HOST}" \
    "${PEER1_PORT}" \
    "${PEER2_HOST}" \
    "${PEER2_PORT}"