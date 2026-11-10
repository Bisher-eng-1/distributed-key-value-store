#!/usr/bin/env python3
"""A small primary-replica key-value store for local fault-tolerance experiments.

This is an educational prototype, not a production database. State is held in
memory and the primary is fixed; there is no consensus protocol or leader election.
"""

from __future__ import annotations

import argparse
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from flask import Flask, jsonify, request


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
            return response.status, json.loads(raw) if raw else {}
    except HTTPError as exc:
        raw = exc.read()
        try:
            decoded = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            decoded = {"error": raw.decode("utf-8", errors="replace")}
        return exc.code, decoded
    except (URLError, TimeoutError, OSError) as exc:
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
        self.node_id = node_id
        self.role = role
        self.peers = [peer.rstrip("/") for peer in (peers or [])]
        self.primary_url = primary_url.rstrip("/") if primary_url else None
        self.timeout = timeout
        self.quorum_size = quorum_size

        # One lock protects both the key/value map and the monotonically
        # increasing log index. A primary write holds it through replication,
        # so two concurrent writes cannot receive the same index or overtake.
        self.lock = threading.Lock()
        self.store: dict[str, dict[str, Any]] = {}
        self.last_index = 0
        self.applied_operations: dict[int, dict[str, Any]] = {}
        self.pending_operation: dict[str, Any] | None = None

        self.app = Flask(f"kv-{node_id}")
        self._register_routes()

    def _register_routes(self) -> None:
        self.app.add_url_rule("/health", "health", self.health, methods=["GET"])
        self.app.add_url_rule("/kv", "write", self.write, methods=["POST"])
        self.app.add_url_rule("/kv/<path:key>", "read", self.read, methods=["GET"])
        self.app.add_url_rule(
            "/internal/replicate",
            "replicate",
            self.replicate,
            methods=["POST"],
        )
        # Test/inspection endpoint for this localhost-only prototype.
        self.app.add_url_rule(
            "/internal/state",
            "internal_state",
            self.internal_state,
            methods=["GET"],
        )

    def health(self):
        with self.lock:
            return jsonify(
                node_id=self.node_id,
                role=self.role,
                last_index=self.last_index,
                key_count=len(self.store),
            )

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

            # Retry an earlier operation before accepting a new one.
            if self.pending_operation is not None:
                pending = self.pending_operation
                retry_results = []

                with ThreadPoolExecutor(max_workers=len(self.peers)) as executor:
                    futures = [
                        executor.submit(self._replicate_to_peer, peer, pending)
                        for peer in self.peers
                    ]
                    for future in as_completed(futures):
                        retry_results.append(future.result())

                retry_acks = sum(
                    1 for result in retry_results if result["acknowledged"]
                )
                required = self.quorum_size - 1

                if retry_acks < required:
                    return jsonify(
                        error="write blocked: previous operation is still awaiting quorum",
                        pending_index=pending["index"],
                        acknowledgements=retry_acks,
                        required_replica_acks=required,
                        peer_results=retry_results,
                    ), 503

                self.store[pending["key"]] = {
                    "value": pending["value"],
                    "version": pending["index"],
                }
                self.last_index = pending["index"]
                self.applied_operations[pending["index"]] = pending
                self.pending_operation = None

            next_index = self.last_index + 1
            operation = {
                "index": next_index,
                "key": key,
                "value": payload["value"],
            }

            results = []

            with ThreadPoolExecutor(max_workers=len(self.peers)) as executor:
                futures = [
                    executor.submit(self._replicate_to_peer, peer, operation)
                    for peer in self.peers
                ]
                for future in as_completed(futures):
                    results.append(future.result())

            acknowledgements = sum(
                1 for result in results if result["acknowledged"]
            )
            required = self.quorum_size - 1

            if acknowledgements < required:
                # Keep the operation so the primary never reuses its index.
                self.pending_operation = operation

                return jsonify(
                    error="write rejected: replication quorum not reached",
                    index=next_index,
                    acknowledgements=acknowledgements,
                    required_replica_acks=required,
                    pending_operation=True,
                    peer_results=results,
                ), 503

            self.store[key] = {
                "value": payload["value"],
                "version": next_index,
            }
            self.last_index = next_index
            self.applied_operations[next_index] = operation

            return jsonify(
                committed=True,
                key=key,
                value=payload["value"],
                version=next_index,
                acknowledgements=acknowledgements,
                required_replica_acks=required,
                peer_results=results,
                consistency="primary_write_quorum",
            ), 200

    def replicate(self):
        if self.role != "replica":
            return jsonify(error="replication endpoint is only valid on replicas"), 409

        operation = request.get_json(silent=True)
        if not isinstance(operation, dict):
            return jsonify(error="expected a JSON operation"), 400

        index = operation.get("index")
        key = operation.get("key")
        if (
            not isinstance(index, int)
            or isinstance(index, bool)
            or index < 1
            or not isinstance(key, str)
            or not key
            or "value" not in operation
        ):
            return jsonify(error="invalid replication operation"), 400

        with self.lock:
            existing = self.applied_operations.get(index)

            # Retries are safe: an identical already-applied operation is
            # acknowledged, while a conflicting operation is rejected.
            if index <= self.last_index:
                if existing == operation:
                    return jsonify(acknowledged=True, message="duplicate already applied")
                return jsonify(error="conflicting or obsolete operation"), 409

            # Enforce ordered application. A replica never silently skips a
            # missing log index, because that would hide divergence.
            if index != self.last_index + 1:
                return jsonify(
                    error="out-of-order operation",
                    expected_index=self.last_index + 1,
                    received_index=index,
                ), 409

            self.store[key] = {"value": operation["value"], "version": index}
            self.last_index = index
            self.applied_operations[index] = operation

        return jsonify(acknowledged=True, message="operation applied"), 200

    def read(self, key: str):
        if self.role == "primary":
            with self.lock:
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

        # Replicas prefer a primary read, giving clients a current answer
        # while the primary is healthy. If it cannot be reached, the replica
        # serves its local copy and explicitly marks it as potentially stale.
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
                return jsonify(body), status
            except ConnectionError:
                pass

        with self.lock:
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

    def internal_state(self):
        with self.lock:
            return jsonify(
                node_id=self.node_id,
                role=self.role,
                last_index=self.last_index,
                store=self.store,
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one key-value store node.")
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--role", choices=("primary", "replica"), required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument(
        "--peers",
        default="",
        help="Comma-separated replica base URLs; used by the primary.",
    )
    parser.add_argument(
        "--primary-url",
        default="",
        help="Primary base URL; used by replicas for read forwarding.",
    )
    parser.add_argument("--timeout", type=float, default=0.5)
    parser.add_argument("--quorum-size", type=int, default=2)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    peers = [item.strip() for item in args.peers.split(",") if item.strip()]
    if args.role == "primary":
        if not peers:
            raise SystemExit("A primary requires at least one replica in --peers.")
        if not 1 <= args.quorum_size <= len(peers) + 1:
            raise SystemExit("--quorum-size must be between 1 and cluster size.")
    elif not args.primary_url:
        raise SystemExit("A replica requires --primary-url.")

    node = KeyValueNode(
        node_id=args.node_id,
        role=args.role,
        peers=peers,
        primary_url=args.primary_url or None,
        timeout=args.timeout,
        quorum_size=args.quorum_size,
    )

    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s %(levelname)s [{args.node_id}] %(message)s",
    )
    logging.info("Starting %s node on 127.0.0.1:%s", args.role, args.port)
    node.app.run(host="127.0.0.1", port=args.port, threaded=True, use_reloader=False)


if __name__ == "__main__":
    main()
