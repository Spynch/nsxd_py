#!/usr/bin/env bash
set -euo pipefail

function wait_for_health() {
  local url=$1
  for _ in {1..20}; do
    if curl -s "$url" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}

docker compose up -d --build

wait_for_health "http://localhost:8001/health"
wait_for_health "http://localhost:8002/health"
wait_for_health "http://localhost:8003/health"

leader=""
for port in 8001 8002 8003; do
  role=$(curl -s "http://localhost:${port}/health" | python -c "import json,sys; print(json.load(sys.stdin)['role'])")
  if [[ "$role" == "leader" ]]; then
    leader=$port
  fi
  done

if [[ -z "$leader" ]]; then
  echo "Leader not found" >&2
  exit 1
fi

echo "Leader is on port ${leader}"

curl -s -X POST "http://localhost:${leader}/kv/put" \
  -H 'Content-Type: application/json' \
  -d '{"key":"demo","value":"value"}'

echo "Stopping leader container"
case "$leader" in
  8001) service="node1" ;;
  8002) service="node2" ;;
  8003) service="node3" ;;
  *) echo "Unknown leader port" >&2; exit 1 ;;
esac
docker compose stop "$service"

sleep 5

new_leader=""
for port in 8001 8002 8003; do
  if curl -s "http://localhost:${port}/health" >/dev/null; then
    role=$(curl -s "http://localhost:${port}/health" | python -c "import json,sys; print(json.load(sys.stdin)['role'])")
    if [[ "$role" == "leader" ]]; then
      new_leader=$port
    fi
  fi
  done

if [[ -z "$new_leader" ]]; then
  echo "New leader not found" >&2
  exit 1
fi

echo "New leader is on port ${new_leader}"

curl -s "http://localhost:${new_leader}/kv/get?key=demo"

docker compose down
