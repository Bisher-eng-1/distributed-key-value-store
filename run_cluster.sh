#!/usr/bin/env bash
set -euo pipefail

mkdir -p data
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  wait 2>/dev/null || true
}

trap cleanup EXIT INT TERM

python3 node.py --node-id node-1 --role primary --port 5001 \
  --peers http://127.0.0.1:5002,http://127.0.0.1:5003 \
  --quorum-size 2 --wal-path data/primary.wal &
PIDS+=("$!")

sleep 1

python3 node.py --node-id node-2 --role replica --port 5002 \
  --primary-url http://127.0.0.1:5001 --wal-path data/replica-a.wal &
PIDS+=("$!")

python3 node.py --node-id node-3 --role replica --port 5003 \
  --primary-url http://127.0.0.1:5001 --wal-path data/replica-b.wal &
PIDS+=("$!")

echo "Cluster running: primary=:5001, replica-a=:5002, replica-b=:5003 (Ctrl+C to stop)"
wait
