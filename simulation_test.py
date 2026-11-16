#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
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
            raise RuntimeError(f"Node exited early (code {process.returncode}).")
        try:
            status, _ = request_json("GET", f"{url}/health", timeout=0.4)
            if status == 200:
                return
        except (URLError, TimeoutError, OSError) as exc:
            last_error = exc
        time.sleep(0.1)
    raise TimeoutError(f"Timed out waiting for {url}. Last error: {last_error}")


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
    wal_path: str = "",
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
    if wal_path:
        command.extend(["--wal-path", wal_path])
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
    with tempfile.TemporaryDirectory() as tmpdir:
        primary_wal = str(Path(tmpdir) / "primary.wal")
        try:
            print("[1/8] Starting primary (WAL enabled) and two replicas...")
            primary = start_node(
                "node-1",
                "primary",
                PRIMARY_PORT,
                peers=f"{REPLICA_A_URL},{REPLICA_B_URL}",
                wal_path=primary_wal,
            )
            processes.append(primary)

            replica_a = start_node("node-2", "replica", REPLICA_A_PORT, primary_url=PRIMARY_URL)
            processes.append(replica_a)

            replica_b = start_node("node-3", "replica", REPLICA_B_PORT, primary_url=PRIMARY_URL)
            processes.append(replica_b)

            wait_for_node(PRIMARY_URL, primary)
            wait_for_node(REPLICA_A_URL, replica_a)
            wait_for_node(REPLICA_B_URL, replica_b)

            print("[2/8] Running 30 concurrent writes...")
            before_failure = run_concurrent_writes(count=30, phase="before")

            print("[3/8] Killing replica-a and running 20 writes on 2-of-3 quorum...")
            stop_node(replica_a)
            after_failure = run_concurrent_writes(count=20, phase="after")
            for item in after_failure:
                if item["response"]["acknowledgements"] < 1:
                    raise AssertionError("Write committed without quorum.")

            print("[4/8] Restarting replica-a and checking log catch-up + DELETE...")
            replica_a_restarted = start_node(
                "node-2", "replica", REPLICA_A_PORT, primary_url=PRIMARY_URL
            )
            processes.append(replica_a_restarted)
            wait_for_node(REPLICA_A_URL, replica_a_restarted)

            temp_key = "ephemeral-check"
            if request_json("POST", f"{PRIMARY_URL}/kv", {"key": temp_key, "value": 1})[0] != 200:
                raise AssertionError("Post-recovery write failed.")
            if request_json("DELETE", f"{PRIMARY_URL}/kv/{temp_key}")[0] != 200:
                raise AssertionError("Replicated DELETE failed.")

            _, p_state = request_json("GET", f"{PRIMARY_URL}/internal/state")
            _, ra_state = request_json("GET", f"{REPLICA_A_URL}/internal/state")
            _, rb_state = request_json("GET", f"{REPLICA_B_URL}/internal/state")
            if not (
                p_state["last_index"] == ra_state["last_index"] == rb_state["last_index"]
                and p_state["store"] == ra_state["store"] == rb_state["store"]
            ):
                raise AssertionError("Replicas out of sync with primary.")

            print("[5/8] Restarting primary from WAL and checking index continuity...")
            stop_node(primary)
            primary_restarted = start_node(
                "node-1",
                "primary",
                PRIMARY_PORT,
                peers=f"{REPLICA_A_URL},{REPLICA_B_URL}",
                wal_path=primary_wal,
            )
            processes.append(primary_restarted)
            wait_for_node(PRIMARY_URL, primary_restarted)

            wal_check_status, _ = request_json(
                "POST", f"{PRIMARY_URL}/kv", {"key": "wal-restart", "value": "survived"}
            )
            if wal_check_status != 200:
                raise AssertionError("Primary write after WAL restart failed.")
            assert_read(PRIMARY_URL, "wal-restart", "survived")

            print("[6/8] Dropping below quorum and verifying 503 + pending recovery...")
            stop_node(replica_a_restarted)
            stop_node(replica_b)
            q_status, _ = request_json(
                "POST", f"{PRIMARY_URL}/kv", {"key": "quorum-lost", "value": "must-wait"}
            )
            if q_status != 503:
                raise AssertionError(f"Expected 503 on quorum loss, got {q_status}")

            replica_b_restarted = start_node(
                "node-3", "replica", REPLICA_B_PORT, primary_url=PRIMARY_URL
            )
            processes.append(replica_b_restarted)
            wait_for_node(REPLICA_B_URL, replica_b_restarted)
            if request_json("POST", f"{PRIMARY_URL}/kv", {"key": "post-quorum", "value": "ok"})[0] != 200:
                raise AssertionError("Failed to recover pending operation.")
            assert_read(PRIMARY_URL, "quorum-lost", "must-wait")
            assert_read(PRIMARY_URL, "post-quorum", "ok")

            print("[7/8] Checking forwarded reads and primary-down local fallback...")
            all_writes = before_failure + after_failure
            for item in all_writes:
                assert_read(PRIMARY_URL, item["key"], item["value"])
                assert_read(REPLICA_B_URL, item["key"], item["value"])

            stop_node(primary_restarted)
            fb_status, fb_body = request_json("GET", f"{REPLICA_B_URL}/kv/post-quorum")
            if fb_status != 200 or fb_body.get("consistency") != "local_fallback_stale_possible":
                raise AssertionError(f"Unexpected fallback read: {fb_status}, {fb_body}")

            print(
                "[8/8] PASS:",
                json.dumps(
                    {
                        "writes_before_failure": len(before_failure),
                        "writes_after_failure": len(after_failure),
                        "wal_restart_verified": True,
                        "quorum_loss_503_verified": True,
                        "stale_fallback_read_verified": True,
                    }
                ),
            )
            return 0
        except Exception as exc:
            print(f"FAIL: {type(exc).__name__}: {exc}", file=sys.stderr)
            return 1
        finally:
            for process in reversed(processes):
                stop_node(process)


if __name__ == "__main__":
    raise SystemExit(main())
