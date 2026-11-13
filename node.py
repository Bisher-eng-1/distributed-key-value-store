#!/usr/bin/env python3
"""Primary-replica key-value store with quorum replication and log catch-up.

State is held in memory with a fixed primary role (no leader election or
consensus protocol). Writes commit only after reaching the configured write
quorum across the primary and its replicas, and lagging replicas automatically
replay missed log entries when reconnecting.
"""

from __future__ import annotations

import argparse
import copy
import http.client
import json
import logging
import signal
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from flask import Flask, jsonify, request
from werkzeug.serving import make_server

LOGGER = logging.getLogger("kv-node")


def http_json(
    method: str,
    url: str,
    payload: dict[str, Any] | None = None,
    timeout: float = 0.5,
) -> tuple[int, dict[str, Any]]:
    """Send a JSON request and return (HTTP status, decoded JSON body)."""
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    req = Request(
        url,
        data=body,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(req, timeout=timeout) as response:
            raw = response.read()
            if not raw:
                return response.status, {}
            try:
                return response.status, json.loads(raw)
            except json.JSONDecodeError:
                return response.status, {"raw": raw.decode("utf-8", errors="replace")}
    except HTTPError as exc:
        raw = exc.read()
        try:
            decoded = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            decoded = {"error": raw.decode("utf-8", errors="replace")}
        return exc.code, decoded
    except (URLError, TimeoutError, OSError, http.client.HTTPException) as exc:
        raise ConnectionError(str(exc)) from exc


class KeyValueNode:
    def __init__(
        self,
        node_id: str,
        role: str,
        peers: list[str] | None = None,
        primary_url: str | None = None,
        timeout: float = 0.5,
        quorum_size: int = 2,
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

        # One lock serializes both state mutations and monotonically increasing
        # log indices. Holding it through replication preserves linear log order.
        self.lock = threading.Lock()
        self.store: dict[str, dict[str, Any]] = {}
        self.last_index = 0
        self.applied_operations: dict[int, dict[str, Any]] = {}
        self.pending_operation: dict[str, Any] | None = None
        self.stats: dict[str, int] = {
            "writes_committed": 0,
            "deletes_committed": 0,
            "writes_rejected_quorum": 0,
            "writes_rejected_conflict": 0,
            "reads_primary": 0,
            "reads_forwarded": 0,
            "reads_stale_fallback": 0,
            "replica_catchups": 0,
        }

        self._executor = ThreadPoolExecutor(
            max_workers=max(1, len(self.peers)),
            thread_name_prefix=f"rep-{node_id}",
        )

        self.app = Flask(f"kv-{node_id}")
        self._register_routes()

    def close(self) -> None:
        self._executor.shutdown(wait=True, cancel_futures=False)

    def _register_routes(self) -> None:
        self.app.add_url_rule("/health", "health", self.health, methods=["GET"])
        self.app.add_url_rule("/kv", "write", self.write, methods=["POST"])
        self.app.add_url_rule("/kv/<path:key>", "read", self.read, methods=["GET"])
        self.app.add_url_rule("/kv/<path:key>", "delete", self.delete, methods=["DELETE"])
        self.app.add_url_rule(
            "/internal/replicate",
            "replicate",
            self.replicate,
            methods=["POST"],
        )
        self.app.add_url_rule(
            "/internal/log",
            "internal_log",
            self.internal_log,
            methods=["GET"],
        )
        self.app.add_url_rule(
            "/internal/state",
            "internal_state",
            self.internal_state,
            methods=["GET"],
        )

    @staticmethod
    def _normalize_operation(operation: dict[str, Any]) -> dict[str, Any]:
        op_type = operation.get("op", "put")
        normalized: dict[str, Any] = {
            "index": operation["index"],
            "op": op_type,
            "key": operation["key"],
        }
        if op_type == "put":
            normalized["value"] = operation.get("value")
        return normalized

    def _apply_operation_locked(self, operation: dict[str, Any]) -> None:
        normalized = self._normalize_operation(operation)
        index = normalized["index"]
        key = normalized["key"]

        if normalized["op"] == "delete":
            self.store.pop(key, None)
        else:
            self.store[key] = {
                "value": copy.deepcopy(normalized["value"]),
                "version": index,
            }

        self.last_index = index
        self.applied_operations[index] = normalized

    def sync_from_primary(self) -> int:
        """Replica startup helper: fetch any existing primary log entries."""
        if self.role != "replica" or not self.primary_url:
            return 0

        with self.lock:
            from_index = self.last_index + 1

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

        applied = 0
        with self.lock:
            for item in operations:
                if not isinstance(item, dict):
                    continue
                index = item.get("index")
                if index == self.last_index + 1:
                    self._apply_operation_locked(item)
                    applied += 1
            if applied > 0:
                self.stats["replica_catchups"] += 1
        return applied

    def health(self):
        with self.lock:
            return jsonify(
                node_id=self.node_id,
                role=self.role,
                last_index=self.last_index,
                key_count=len(self.store),
                quorum_size=self.quorum_size,
                has_pending_operation=self.pending_operation is not None,
                stats=dict(self.stats),
            )

    def _catch_up_peer(
        self,
        peer: str,
        start_index: int,
        target_operation: dict[str, Any],
    ) -> dict[str, Any]:
        """Replay missing ordered log entries to bring a lagging replica up to date."""
        for idx in range(start_index, target_operation["index"]):
            missing_op = self.applied_operations.get(idx)
            if missing_op is None:
                return {
                    "peer": peer,
                    "acknowledged": False,
                    "status": 409,
                    "detail": f"missing primary log entry {idx} during catch-up",
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

    def _replicate_to_peer(
        self, peer: str, operation: dict[str, Any]
    ) -> dict[str, Any]:
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

    def _broadcast_operation(
        self, operation: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if not self.peers:
            return []
        futures = [
            self._executor.submit(self._replicate_to_peer, peer, operation)
            for peer in self.peers
        ]
        results = [future.result() for future in as_completed(futures)]
        caught_up = sum(1 for item in results if "caught_up_from" in item and item["acknowledged"])
        if caught_up:
            self.stats["replica_catchups"] += caught_up
        return results

    def _commit_with_quorum_locked(
        self, op_type: str, key: str, value: Any = None
    ):
        required = self.quorum_size - 1

        # Resolve any blocked prior operation before assigning a new log index.
        if self.pending_operation is not None:
            pending = self.pending_operation
            retry_results = self._broadcast_operation(pending)
            retry_acks = sum(1 for r in retry_results if r["acknowledged"])
            if retry_acks < required:
                self.stats["writes_rejected_quorum"] += 1
                return jsonify(
                    error="write blocked: previous operation is still awaiting quorum",
                    pending_index=pending["index"],
                    acknowledgements=retry_acks,
                    required_replica_acks=required,
                    peer_results=retry_results,
                ), 503

            self._apply_operation_locked(pending)
            if pending["op"] == "delete":
                self.stats["deletes_committed"] += 1
            else:
                self.stats["writes_committed"] += 1
            self.pending_operation = None

        next_index = self.last_index + 1
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
            # Keep pending_operation so the primary never reuses next_index.
            self.pending_operation = operation
            self.stats["writes_rejected_quorum"] += 1
            return jsonify(
                error="write rejected: replication quorum not reached",
                index=next_index,
                op=op_type,
                acknowledgements=acknowledgements,
                required_replica_acks=required,
                pending_operation=True,
                peer_results=results,
            ), 503

        self._apply_operation_locked(operation)
        if op_type == "delete":
            self.stats["deletes_committed"] += 1
        else:
            self.stats["writes_committed"] += 1

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

        with self.lock:
            return self._commit_with_quorum_locked("put", key, payload["value"])

    def delete(self, key: str):
        if self.role != "primary":
            return jsonify(
                error="deletes must be sent to the configured primary",
                primary=self.primary_url,
            ), 409

        if not isinstance(key, str) or not key:
            return jsonify(error="'key' must be a non-empty string"), 400

        with self.lock:
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

        normalized = self._normalize_operation(operation)

        with self.lock:
            existing = self.applied_operations.get(index)

            if index <= self.last_index:
                if existing is not None and self._normalize_operation(existing) == normalized:
                    return jsonify(acknowledged=True, message="duplicate already applied"), 200
                self.stats["writes_rejected_conflict"] += 1
                return jsonify(error="conflicting or obsolete operation"), 409

            if index != self.last_index + 1:
                self.stats["writes_rejected_conflict"] += 1
                return jsonify(
                    error="out-of-order operation",
                    expected_index=self.last_index + 1,
                    received_index=index,
                ), 409

            self._apply_operation_locked(normalized)

        return jsonify(acknowledged=True, message="operation applied"), 200

    def read(self, key: str):
        if self.role == "primary":
            with self.lock:
                self.stats["reads_primary"] += 1
                record = self.store.get(key)
                index = self.last_index
            if record is None:
                return jsonify(error="key not found", last_index=index), 404
            return jsonify(
                key=key,
                value=record["value"],
                version=record["version"],
                last_index=index,
                consistency="primary",
            ), 200

        if self.primary_url:
            encoded_key = quote(key, safe="")
            try:
                status, body = http_json(
                    "GET",
                    f"{self.primary_url}/kv/{encoded_key}",
                    timeout=self.timeout,
                )
                if status == 200:
                    body["consistency"] = "forwarded_to_primary"
                with self.lock:
                    self.stats["reads_forwarded"] += 1
                return jsonify(body), status
            except ConnectionError:
                pass

        with self.lock:
            self.stats["reads_stale_fallback"] += 1
            record = self.store.get(key)
            index = self.last_index
        if record is None:
            return jsonify(
                error="key not found on local replica",
                last_index=index,
                consistency="local_fallback_stale_possible",
            ), 404
        return jsonify(
            key=key,
            value=record["value"],
            version=record["version"],
            last_index=index,
            consistency="local_fallback_stale_possible",
        ), 200

    def internal_log(self):
        from_index_raw = request.args.get("from_index", "1")
        try:
            from_index = max(1, int(from_index_raw))
        except ValueError:
            return jsonify(error="from_index must be an integer"), 400

        with self.lock:
            operations = [
                copy.deepcopy(self.applied_operations[idx])
                for idx in range(from_index, self.last_index + 1)
                if idx in self.applied_operations
            ]
            last_index = self.last_index

        return jsonify(
            node_id=self.node_id,
            from_index=from_index,
            last_index=last_index,
            operations=operations,
        ), 200

    def internal_state(self):
        with self.lock:
            return jsonify(
                node_id=self.node_id,
                role=self.role,
                last_index=self.last_index,
                store=copy.deepcopy(self.store),
                has_pending_operation=self.pending_operation is not None,
                stats=dict(self.stats),
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one key-value store node.")
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--role", choices=("primary", "replica"), required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument(
        "--peers",
        default="",
        help="Comma-separated replica base URLs; used by the primary.",
    )
    parser.add_argument(
        "--primary-url",
        default="",
        help="Primary base URL; used by replicas for read forwarding and sync.",
    )
    parser.add_argument("--timeout", type=float, default=0.5)
    parser.add_argument("--quorum-size", type=int, default=2)
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
    )

    if args.role == "replica":
        synced = node.sync_from_primary()
        if synced:
            LOGGER.info("Caught up %d log entries from primary before serving.", synced)

    server = make_server(args.host, args.port, node.app, threaded=True)

    def _shutdown_handler(signum, _frame):
        LOGGER.info("Received signal %s; shutting down cleanly.", signum)
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, _shutdown_handler)
    signal.signal(signal.SIGINT, _shutdown_handler)

    LOGGER.info("Starting %s node on %s:%s", args.role, args.host, args.port)
    try:
        server.serve_forever()
    finally:
        node.close()


if __name__ == "__main__":
    main()
