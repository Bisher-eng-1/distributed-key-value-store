#!/usr/bin/env python3
"""Start a local 3-node cluster and exercise fault tolerance, recovery, and quorum."""

from __future__ import annotations

import json
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
REQUEST_TIMEOUT = 15.0


def request_json(
    method: str,
    url: str,
    payload: dict[str, Any] | None = None,
    timeout: float = REQUEST_TIMEOUT,
) -> tuple[int, dict[str, Any]]:
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


def stop_node(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=4)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=2)


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
        "--node-id",
        node_id,
        "--role",
        role,
        "--port",
        str(port),
        "--timeout",
        "0.35",
        "--quorum-size",
        "2",
    ]
    if peers:
        command.extend(["--peers", peers])
    if primary_url:
        command.extend(["--primary-url", primary_url])

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
        print("[1/7] Starting primary and two replicas...")
        primary = start_node(
            "node-1",
            "primary",
            PRIMARY_PORT,
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

        print("[2/7] Sending 30 concurrent writes while all three nodes are healthy...")
        before_failure = run_concurrent_writes(count=30, phase="before")
        print(f"      Committed {len(before_failure)} concurrent writes.")

        print("[3/7] Terminating replica node-2 (port 5002) and sending 20 writes...")
        stop_node(replica_a)
        after_failure = run_concurrent_writes(count=20, phase="after")
        for item in after_failure:
            response = item["response"]
            if response["acknowledgements"] < 1:
                raise AssertionError("Post-failure write did not satisfy quorum.")
        print(f"      Committed {len(after_failure)} writes with 1 replica down.")

        print("[4/7] Restarting replica node-2 and verifying automatic log catch-up...")
        replica_a_restarted = start_node(
            "node-2", "replica", REPLICA_A_PORT, primary_url=PRIMARY_URL
        )
        processes.append(replica_a_restarted)
        wait_for_node(REPLICA_A_URL, replica_a_restarted)

        # Verify catch-up plus replicated DELETE operation.
        temp_key = "ephemeral-check"
        status, _ = request_json("POST", f"{PRIMARY_URL}/kv", {"key": temp_key, "value": 1})
        if status != 200:
            raise AssertionError("Post-recovery write failed.")
        del_status, _ = request_json("DELETE", f"{PRIMARY_URL}/kv/{temp_key}")
        if del_status != 200:
            raise AssertionError("Replicated DELETE failed.")

        _, p_state = request_json("GET", f"{PRIMARY_URL}/internal/state")
        _, ra_state = request_json("GET", f"{REPLICA_A_URL}/internal/state")
        _, rb_state = request_json("GET", f"{REPLICA_B_URL}/internal/state")
        if not (
            p_state["last_index"] == ra_state["last_index"] == rb_state["last_index"]
            and p_state["store"] == ra_state["store"] == rb_state["store"]
        ):
            raise AssertionError("Recovered replica failed to reach full state parity.")
        print(f"      All 3 nodes synchronized at log index {p_state['last_index']}.")

        print("[5/7] Stopping both replicas to verify quorum-loss write rejection (503)...")
        stop_node(replica_a_restarted)
        stop_node(replica_b)
        q_status, q_body = request_json(
            "POST", f"{PRIMARY_URL}/kv", {"key": "quorum-lost", "value": "must-wait"}
        )
        if q_status != 503:
            raise AssertionError(f"Expected 503 on quorum loss, got {q_status}: {q_body}")

        # Restore one replica and verify the pending operation commits cleanly.
        replica_b_restarted = start_node(
            "node-3", "replica", REPLICA_B_PORT, primary_url=PRIMARY_URL
        )
        processes.append(replica_b_restarted)
        wait_for_node(REPLICA_B_URL, replica_b_restarted)
        rec_status, _ = request_json(
            "POST", f"{PRIMARY_URL}/kv", {"key": "post-quorum", "value": "ok"}
        )
        if rec_status != 200:
            raise AssertionError("Failed to recover pending operation after quorum restoration.")
        assert_read(PRIMARY_URL, "quorum-lost", "must-wait")
        assert_read(PRIMARY_URL, "post-quorum", "ok")
        print("      Quorum-loss rejection and pending-operation recovery verified.")

        print("[6/7] Verifying forwarded reads and stale-possible local fallback reads...")
        all_writes = before_failure + after_failure
        for item in all_writes:
            assert_read(PRIMARY_URL, item["key"], item["value"])
            assert_read(REPLICA_B_URL, item["key"], item["value"])

        stop_node(primary)
        fb_status, fb_body = request_json("GET", f"{REPLICA_B_URL}/kv/post-quorum")
        if (
            fb_status != 200
            or fb_body.get("value") != "ok"
            or fb_body.get("consistency") != "local_fallback_stale_possible"
        ):
            raise AssertionError(f"Unexpected fallback read response: {fb_status}, {fb_body}")

        print("[7/7] PASS: all fault-tolerance, catch-up, quorum, and fallback checks succeeded.")
        print(
            json.dumps(
                {
                    "writes_before_failure": len(before_failure),
                    "writes_after_failure": len(after_failure),
                    "delete_and_catchup_verified": True,
                    "quorum_loss_503_verified": True,
                    "pending_recovery_verified": True,
                    "stale_fallback_read_verified": True,
                    "total_read_checks": len(all_writes) * 2 + 3,
                },
                indent=2,
            )
        )
        return 0

    except Exception as exc:
        print(f"\nFAIL: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    finally:
        print("\nCleaning up node processes...")
        for process in reversed(processes):
            stop_node(process)


if __name__ == "__main__":
    raise SystemExit(main())
