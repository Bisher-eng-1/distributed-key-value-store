#!/usr/bin/env python3
"""Start a local 3-node cluster and exercise concurrent writes and replica loss."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parent
NODE_SCRIPT = ROOT / "node.py"
HOST = "127.0.0.1"
PRIMARY_PORT = 5001
REPLICA_A_PORT = 5002
REPLICA_B_PORT = 5003
PRIMARY_URL = f"http://{HOST}:{PRIMARY_PORT}"
REPLICA_A_URL = f"http://{HOST}:{REPLICA_A_PORT}"
REPLICA_B_URL = f"http://{HOST}:{REPLICA_B_PORT}"
REQUEST_TIMEOUT = 2.0


def request_json(
    method: str,
    url: str,
    payload: dict[str, Any] | None = None,
    timeout: float = REQUEST_TIMEOUT,
) -> tuple[int, dict[str, Any]]:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = Request(
        url,
        data=body,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            raw = response.read()
            return response.status, json.loads(raw) if raw else {}
    except HTTPError as exc:
        raw = exc.read()
        try:
            decoded = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            decoded = {"error": raw.decode("utf-8", errors="replace")}
        return exc.code, decoded


def wait_for_node(url: str, process: subprocess.Popen, timeout: float = 12.0) -> None:
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"Node process exited early (code {process.returncode}).")
        try:
            status, _ = request_json("GET", f"{url}/health", timeout=0.4)
            if status == 200:
                return
        except (URLError, TimeoutError, OSError) as exc:
            last_error = exc
        time.sleep(0.1)
    raise TimeoutError(f"Node at {url} did not become ready. Last error: {last_error}")


def start_node(
    node_id: str,
    role: str,
    port: int,
    peers: str = "",
    primary_url: str = "",
) -> subprocess.Popen:
    command = [
        sys.executable,
        str(NODE_SCRIPT),
        "--node-id", node_id,
        "--role", role,
        "--port", str(port),
        "--timeout", "0.35",
        "--quorum-size", "2",
    ]
    if peers:
        command.extend(["--peers", peers])
    if primary_url:
        command.extend(["--primary-url", primary_url])

    # Keep node output visible so a failed experiment is diagnosable.
    return subprocess.Popen(command, cwd=ROOT)


def write_one(index: int, phase: str) -> dict[str, Any]:
    key = f"{phase}-key-{index:03d}"
    value = {"sequence": index, "phase": phase, "payload": f"value-{index:03d}"}
    status, body = request_json(
        "POST",
        f"{PRIMARY_URL}/kv",
        {"key": key, "value": value},
    )
    if status != 200 or body.get("committed") is not True:
        raise AssertionError(f"Write failed for {key}: HTTP {status}, {body}")
    return {"key": key, "value": value, "response": body}


def run_concurrent_writes(count: int, phase: str, workers: int = 12):
    results = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(write_one, index, phase) for index in range(count)]
        for future in as_completed(futures):
            results.append(future.result())
    return results


def assert_read(url: str, key: str, expected_value: Any) -> None:
    status, body = request_json("GET", f"{url}/kv/{quote(key, safe='')}")
    if status != 200 or body.get("value") != expected_value:
        raise AssertionError(f"Read mismatch for {key} from {url}: HTTP {status}, {body}")


def main() -> int:
    processes: list[subprocess.Popen] = []
    try:
        print("[1/6] Starting the primary and two replicas...")
        primary = start_node(
            "node-1", "primary", PRIMARY_PORT,
            peers=f"{REPLICA_A_URL},{REPLICA_B_URL}",
        )
        processes.append(primary)

        replica_a = start_node(
            "node-2", "replica", REPLICA_A_PORT, primary_url=PRIMARY_URL
        )
        processes.append(replica_a)

        replica_b = start_node(
            "node-3", "replica", REPLICA_B_PORT, primary_url=PRIMARY_URL
        )
        processes.append(replica_b)

        wait_for_node(PRIMARY_URL, primary)
        wait_for_node(REPLICA_A_URL, replica_a)
        wait_for_node(REPLICA_B_URL, replica_b)

        print("[2/6] Sending concurrent writes while all three nodes are healthy...")
        before_failure = run_concurrent_writes(count=30, phase="before")
        print(f"      Committed {len(before_failure)} concurrent writes.")

        print("[3/6] Terminating replica node-2 (port 5002)...")
        replica_a.terminate()
        try:
            replica_a.wait(timeout=4)
        except subprocess.TimeoutExpired:
            replica_a.kill()
            replica_a.wait(timeout=2)

        print("[4/6] Continuing concurrent writes with one replica unavailable...")
        after_failure = run_concurrent_writes(count=20, phase="after")
        print(f"      Committed {len(after_failure)} writes after replica loss.")

        # Each successful post-failure write must have reached the remaining
        # replica, satisfying a 2-of-3 quorum (primary + one replica).
        for item in after_failure:
            response = item["response"]
            if response["acknowledgements"] < 1:
                raise AssertionError("A post-failure write did not reach a replica.")
            successful_peers = [
                result for result in response["peer_results"]
                if result["acknowledged"]
            ]
            if len(successful_peers) < 1:
                raise AssertionError("No surviving replica acknowledged a write.")

        print("[5/6] Verifying reads through the primary and surviving replica...")
        all_writes = before_failure + after_failure
        for item in all_writes:
            assert_read(PRIMARY_URL, item["key"], item["value"])
            # A replica read is forwarded to the primary while it is healthy.
            assert_read(REPLICA_B_URL, item["key"], item["value"])

        print("[6/6] Comparing the primary and surviving replica state...")
        _, primary_state = request_json("GET", f"{PRIMARY_URL}/internal/state")
        _, replica_state = request_json("GET", f"{REPLICA_B_URL}/internal/state")

        if primary_state["last_index"] != replica_state["last_index"]:
            raise AssertionError(
                f"Log index mismatch: primary={primary_state['last_index']}, "
                f"replica={replica_state['last_index']}"
            )
        if primary_state["store"] != replica_state["store"]:
            raise AssertionError("Primary and surviving replica stores diverged.")

        print("\nPASS: the experiment completed successfully.")
        print(json.dumps({
            "cluster": {
                "primary": PRIMARY_URL,
                "terminated_replica": REPLICA_A_URL,
                "surviving_replica": REPLICA_B_URL,
            },
            "writes_before_failure": len(before_failure),
            "writes_after_failure": len(after_failure),
            "total_committed_writes": len(all_writes),
            "primary_last_index": primary_state["last_index"],
            "surviving_replica_last_index": replica_state["last_index"],
            "primary_and_survivor_state_equal": True,
            "read_checks": len(all_writes) * 2,
            "quorum": "2 of 3 nodes (primary + at least one replica)",
        }, indent=2))
        return 0

    except Exception as exc:
        print(f"\nFAIL: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    finally:
        print("\nCleaning up node processes...")
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=2)


if __name__ == "__main__":
    raise SystemExit(main())
