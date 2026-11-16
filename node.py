#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import signal
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.parse import quote

from flask import Flask, jsonify, request
from werkzeug.serving import make_server

from kvstore.state import StoreState
from kvstore.transport import http_json
from kvstore.wal import WriteAheadLog

LOGGER = logging.getLogger("kv-node")


class KeyValueNode:
    def __init__(
        self,
        node_id: str,
        role: str,
        peers: list[str] | None = None,
        primary_url: str | None = None,
        timeout: float = 0.5,
        quorum_size: int = 2,
        wal_path: str | Path | None = None,
    ) -> None:
        if role not in ("primary", "replica"):
            raise ValueError("role must be 'primary' or 'replica'")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if quorum_size < 1:
            raise ValueError("quorum_size must be at least 1")

        self.node_id = node_id
        self.role = role
        self.peers = [peer.rstrip("/") for peer in (peers or []) if peer.strip()]
        self.primary_url = primary_url.rstrip("/") if primary_url else None
        self.timeout = timeout
        self.quorum_size = quorum_size
        self.wal_path = Path(wal_path) if wal_path else None

        # Only serialize primary write rounds; state reads stay lock-free of peer I/O.
        self._write_lock = threading.Lock()
        self.state = StoreState(wal=WriteAheadLog(self.wal_path) if self.wal_path else None)
        self.pending_operation: dict[str, Any] | None = None

        self._executor = ThreadPoolExecutor(
            max_workers=max(1, len(self.peers)),
            thread_name_prefix=f"rep-{node_id}",
        )

        self.app = Flask(f"kv-{node_id}")
        self._register_routes()

    @property
    def store(self) -> dict[str, dict[str, Any]]:
        return self.state.store

    @property
    def last_index(self) -> int:
        return self.state.last_index

    @property
    def applied_operations(self) -> dict[int, dict[str, Any]]:
        return self.state.applied_operations

    @property
    def stats(self) -> dict[str, int]:
        return self.state.stats

    def close(self) -> None:
        self._executor.shutdown(wait=True, cancel_futures=False)

    def _register_routes(self) -> None:
        self.app.add_url_rule("/health", "health", self.health, methods=["GET"])
        self.app.add_url_rule("/kv", "write", self.write, methods=["POST"])
        self.app.add_url_rule("/kv/<path:key>", "read", self.read, methods=["GET"])
        self.app.add_url_rule("/kv/<path:key>", "delete", self.delete, methods=["DELETE"])
        self.app.add_url_rule("/internal/replicate", "replicate", self.replicate, methods=["POST"])
        self.app.add_url_rule("/internal/log", "internal_log", self.internal_log, methods=["GET"])
        self.app.add_url_rule("/internal/state", "internal_state", self.internal_state, methods=["GET"])

    def sync_from_primary(self) -> int:
        if self.role != "replica" or not self.primary_url:
            return 0
        from_index = self.state.next_index()
        try:
            status, body = http_json(
                "GET",
                f"{self.primary_url}/internal/log?from_index={from_index}",
                timeout=self.timeout,
            )
        except ConnectionError:
            return 0
        if status != 200:
            return 0
        operations = body.get("operations", [])
        if not isinstance(operations, list):
            return 0
        return self.state.apply_catchup_batch(operations)

    def health(self):
        snap = self.state.snapshot()
        return jsonify(
            node_id=self.node_id,
            role=self.role,
            last_index=snap["last_index"],
            key_count=snap["key_count"],
            quorum_size=self.quorum_size,
            has_pending_operation=self.pending_operation is not None,
            wal_enabled=self.wal_path is not None,
            stats=snap["stats"],
        )

    def _catch_up_peer(
        self,
        peer: str,
        start_index: int,
        target_operation: dict[str, Any],
    ) -> dict[str, Any]:
        for idx in range(start_index, target_operation["index"]):
            missing_op = self.state.get_op(idx)
            if missing_op is None:
                return {
                    "peer": peer,
                    "acknowledged": False,
                    "status": 409,
                    "detail": f"missing primary log entry {idx}",
                }
            status, body = http_json(
                "POST",
                f"{peer}/internal/replicate",
                missing_op,
                timeout=self.timeout,
            )
            if status != 200 or body.get("acknowledged") is not True:
                return {
                    "peer": peer,
                    "acknowledged": False,
                    "status": status,
                    "detail": body.get("error", body.get("message", "catch-up failed")),
                }

        status, body = http_json(
            "POST",
            f"{peer}/internal/replicate",
            target_operation,
            timeout=self.timeout,
        )
        return {
            "peer": peer,
            "acknowledged": status == 200 and body.get("acknowledged") is True,
            "status": status,
            "caught_up_from": start_index,
            "detail": body.get("message", body.get("error", "")),
        }

    def _replicate_to_peer(self, peer: str, operation: dict[str, Any]) -> dict[str, Any]:
        try:
            status, body = http_json(
                "POST",
                f"{peer}/internal/replicate",
                operation,
                timeout=self.timeout,
            )
            if (
                status == 409
                and isinstance(body.get("expected_index"), int)
                and 1 <= body["expected_index"] < operation["index"]
            ):
                return self._catch_up_peer(peer, body["expected_index"], operation)
            return {
                "peer": peer,
                "acknowledged": status == 200 and body.get("acknowledged") is True,
                "status": status,
                "detail": body.get("message", body.get("error", "")),
            }
        except ConnectionError as exc:
            return {
                "peer": peer,
                "acknowledged": False,
                "status": None,
                "detail": str(exc),
            }

    def _broadcast_operation(self, operation: dict[str, Any]) -> list[dict[str, Any]]:
        if not self.peers:
            return []
        futures = [
            self._executor.submit(self._replicate_to_peer, peer, operation)
            for peer in self.peers
        ]
        results = [future.result() for future in as_completed(futures)]
        caught_up = sum(1 for item in results if "caught_up_from" in item and item["acknowledged"])
        if caught_up:
            self.state.inc_stat("replica_catchups", caught_up)
        return results

    def _commit_with_quorum_locked(self, op_type: str, key: str, value: Any = None):
        required = self.quorum_size - 1

        if self.pending_operation is not None:
            pending = self.pending_operation
            retry_results = self._broadcast_operation(pending)
            retry_acks = sum(1 for r in retry_results if r["acknowledged"])
            if retry_acks < required:
                self.state.inc_stat("writes_rejected_quorum")
                return jsonify(
                    error="write blocked: previous operation is still awaiting quorum",
                    pending_index=pending["index"],
                    acknowledgements=retry_acks,
                    required_replica_acks=required,
                    peer_results=retry_results,
                ), 503

            self.state.commit_primary(pending)
            self.pending_operation = None

        next_index = self.state.next_index()
        operation: dict[str, Any] = {
            "index": next_index,
            "op": op_type,
            "key": key,
        }
        if op_type == "put":
            operation["value"] = value

        results = self._broadcast_operation(operation)
        acknowledgements = sum(1 for r in results if r["acknowledged"])

        if acknowledgements < required:
            self.pending_operation = operation
            self.state.inc_stat("writes_rejected_quorum")
            return jsonify(
                error="write rejected: replication quorum not reached",
                index=next_index,
                op=op_type,
                acknowledgements=acknowledgements,
                required_replica_acks=required,
                pending_operation=True,
                peer_results=results,
            ), 503

        self.state.commit_primary(operation)

        response_body: dict[str, Any] = {
            "committed": True,
            "op": op_type,
            "key": key,
            "version": next_index,
            "acknowledgements": acknowledgements,
            "required_replica_acks": required,
            "peer_results": results,
            "consistency": "primary_write_quorum",
        }
        if op_type == "put":
            response_body["value"] = value
        return jsonify(response_body), 200

    def write(self):
        if self.role != "primary":
            return jsonify(
                error="writes must be sent to the configured primary",
                primary=self.primary_url,
            ), 409

        payload = request.get_json(silent=True)
        if not isinstance(payload, dict) or "key" not in payload or "value" not in payload:
            return jsonify(error="JSON body must contain both 'key' and 'value'"), 400

        key = payload["key"]
        if not isinstance(key, str) or not key:
            return jsonify(error="'key' must be a non-empty string"), 400

        try:
            json.dumps(payload["value"])
        except (TypeError, ValueError):
            return jsonify(error="'value' must be JSON serialisable"), 400

        with self._write_lock:
            return self._commit_with_quorum_locked("put", key, payload["value"])

    def delete(self, key: str):
        if self.role != "primary":
            return jsonify(
                error="deletes must be sent to the configured primary",
                primary=self.primary_url,
            ), 409
        if not key:
            return jsonify(error="'key' must be a non-empty string"), 400

        with self._write_lock:
            return self._commit_with_quorum_locked("delete", key)

    def replicate(self):
        if self.role != "replica":
            return jsonify(error="replication endpoint is only valid on replicas"), 409

        operation = request.get_json(silent=True)
        if not isinstance(operation, dict):
            return jsonify(error="expected a JSON operation"), 400

        index = operation.get("index")
        key = operation.get("key")
        op_type = operation.get("op", "put")
        if (
            not isinstance(index, int)
            or isinstance(index, bool)
            or index < 1
            or not isinstance(key, str)
            or not key
            or op_type not in ("put", "delete")
            or (op_type == "put" and "value" not in operation)
        ):
            return jsonify(error="invalid replication operation"), 400

        outcome, detail_index = self.state.apply_replicated(operation)
        if outcome == "duplicate":
            return jsonify(acknowledged=True, message="duplicate already applied"), 200
        if outcome == "conflict":
            return jsonify(error="conflicting or obsolete operation"), 409
        if outcome == "gap":
            return jsonify(
                error="out-of-order operation",
                expected_index=detail_index,
                received_index=index,
            ), 409
        return jsonify(acknowledged=True, message="operation applied"), 200

    def read(self, key: str):
        if self.role == "primary":
            record, index = self.state.read_local(key, "reads_primary")
            if record is None:
                return jsonify(error="key not found", last_index=index), 404
            return jsonify(
                key=key,
                value=record["value"],
                version=record["version"],
                last_index=index,
                consistency="primary",
            ), 200

        mode = request.args.get("consistency", "primary")
        if mode != "local" and self.primary_url:
            encoded_key = quote(key, safe="")
            try:
                status, body = http_json(
                    "GET",
                    f"{self.primary_url}/kv/{encoded_key}",
                    timeout=self.timeout,
                )
                if status == 200:
                    body["consistency"] = "forwarded_to_primary"
                self.state.inc_stat("reads_forwarded")
                return jsonify(body), status
            except ConnectionError:
                pass

        record, index = self.state.read_local(key, "reads_stale_fallback")
        label = "local_explicit" if mode == "local" else "local_fallback_stale_possible"
        if record is None:
            return jsonify(
                error="key not found on local replica",
                last_index=index,
                consistency=label,
            ), 404
        return jsonify(
            key=key,
            value=record["value"],
            version=record["version"],
            last_index=index,
            consistency=label,
        ), 200

    def internal_log(self):
        from_index_raw = request.args.get("from_index", "1")
        try:
            from_index = max(1, int(from_index_raw))
        except ValueError:
            return jsonify(error="from_index must be an integer"), 400

        operations, last_index = self.state.log_slice(from_index)
        return jsonify(
            node_id=self.node_id,
            from_index=from_index,
            last_index=last_index,
            operations=operations,
        ), 200

    def internal_state(self):
        snap = self.state.snapshot()
        return jsonify(
            node_id=self.node_id,
            role=self.role,
            last_index=snap["last_index"],
            store=snap["store"],
            has_pending_operation=self.pending_operation is not None,
            wal_enabled=self.wal_path is not None,
            stats=snap["stats"],
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one key-value store node.")
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--role", choices=("primary", "replica"), required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--peers", default="")
    parser.add_argument("--primary-url", default="")
    parser.add_argument("--timeout", type=float, default=0.5)
    parser.add_argument("--quorum-size", type=int, default=2)
    parser.add_argument("--wal-path", default="")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not 1 <= args.port <= 65535:
        raise SystemExit("--port must be between 1 and 65535.")
    if args.timeout <= 0:
        raise SystemExit("--timeout must be positive.")

    peers = [item.strip() for item in args.peers.split(",") if item.strip()]
    if args.role == "primary":
        if not peers:
            raise SystemExit("A primary requires at least one replica in --peers.")
        if not 1 <= args.quorum_size <= len(peers) + 1:
            raise SystemExit("--quorum-size must be between 1 and cluster size.")
    elif not args.primary_url:
        raise SystemExit("A replica requires --primary-url.")

    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s %(levelname)s [{args.node_id}] %(message)s",
    )

    node = KeyValueNode(
        node_id=args.node_id,
        role=args.role,
        peers=peers,
        primary_url=args.primary_url or None,
        timeout=args.timeout,
        quorum_size=args.quorum_size,
        wal_path=args.wal_path or None,
    )

    if args.role == "replica":
        synced = node.sync_from_primary()
        if synced:
            LOGGER.info("Caught up %d entries from primary", synced)

    server = make_server(args.host, args.port, node.app, threaded=True)

    def _shutdown_handler(signum, _frame):
        LOGGER.info("Shutting down on signal %s", signum)
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, _shutdown_handler)
    signal.signal(signal.SIGINT, _shutdown_handler)

    LOGGER.info("Listening (%s) on %s:%s", args.role, args.host, args.port)
    try:
        server.serve_forever()
    finally:
        node.close()


if __name__ == "__main__":
    main()
