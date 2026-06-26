#!/usr/bin/env bash
# Initialise the cluster brought up by docker-compose.yml.
# Idempotent: re-running after a successful init prints harmless errors but
# leaves the cluster in the expected state.
#
# Cross-platform note: --network=host is Linux-only on Docker Desktop
# (macOS/Windows ignore it and the inner mongo client can't reach the
# compose services). Prefer `docker compose exec` so the mongo client
# runs inside the compose network and addresses services by their
# compose service names.
set -euo pipefail

COMPOSE=${COMPOSE:-docker compose}

mongo_run() {
    # Args: <service> <port> <eval-string>
    local svc=$1
    local port=$2
    shift 2
    # Use the mongo client baked into the service's own image; the client
    # is reachable via the compose network's service DNS, so this works
    # identically on Linux / macOS / Windows.
    $COMPOSE exec -T "$svc" mongo --quiet --host "127.0.0.1:${port}" --eval "$*"
}

echo "[1/5] init configsvr replset cfg"
mongo_run configsvr 27019 'rs.initiate({_id: "cfg", configsvr: true, members: [{_id: 0, host: "configsvr:27019"}]})' || true

echo "[2/5] init shard rs1"
mongo_run shard1 27018 'rs.initiate({_id: "rs1", members: [{_id: 0, host: "shard1:27018"}]})' || true

echo "[3/5] init shard rs2"
mongo_run shard2 27028 'rs.initiate({_id: "rs2", members: [{_id: 0, host: "shard2:27028"}]})' || true

echo "[4/5] init dest replset dst"
mongo_run dest 27117 'rs.initiate({_id: "dst", members: [{_id: 0, host: "dest:27117"}]})' || true

# Wait for mongos to see the configsvr.
echo "[5/5] add shards via mongos"
sleep 5
mongo_run mongos 27017 'sh.addShard("rs1/shard1:27018"); sh.addShard("rs2/shard2:27028")'

# Disable balancer so we can craft orphan rows deterministically.
mongo_run mongos 27017 'sh.stopBalancer()'

echo "cluster ready."
