#!/usr/bin/env python3
from __future__ import annotations

import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from kvstore.state import StoreState
from kvstore.wal import WriteAheadLog
from node import KeyValueNode


class WalAndStateTests(unittest.TestCase):
    def test_wal_recovers_committed_entries_and_truncates_torn_tail(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_file = Path(tmpdir) / "node.wal"
            wal = WriteAheadLog(wal_file)
            state = StoreState(wal=wal)
            state.commit_primary({"index": 1, "op": "put", "key": "a", "value": 10})
            state.commit_primary({"index": 2, "op": "put", "key": "b", "value": 20})
            state.commit_primary({"index": 3, "op": "delete", "key": "a"})

            # Simulate an unclean crash halfway through writing frame 4.
            with wal_file.open("ab") as fh:
                fh.write(b"deadbeef|{\"index\":4,\"op\":\"pu")
            size_with_tail = wal_file.stat().st_size

            recovered = StoreState(wal=WriteAheadLog(wal_file))
            self.assertEqual(recovered.last_index, 3)
            self.assertNotIn("a", recovered.store)
            self.assertEqual(recovered.store["b"]["value"], 20)
            self.assertLess(wal_file.stat().st_size, size_with_tail)


class KeyValueNodeTests(unittest.TestCase):
    def _make_cluster(self, quorum_size: int = 2, primary_wal: str | None = None):
        replica_a = KeyValueNode(
            node_id="replica-a",
            role="replica",
            primary_url="http://primary.local",
            quorum_size=quorum_size,
        )
        replica_b = KeyValueNode(
            node_id="replica-b",
            role="replica",
            primary_url="http://primary.local",
            quorum_size=quorum_size,
        )
        primary = KeyValueNode(
            node_id="primary",
            role="primary",
            peers=["http://replica-a.local", "http://replica-b.local"],
            quorum_size=quorum_size,
            wal_path=primary_wal,
        )
        return primary, replica_a, replica_b

    @staticmethod
    def _route_to_nodes(nodes: dict[str, KeyValueNode], offline: set[str] | None = None):
        if offline is None:
            offline = set()

        def fake_http_json(method: str, url: str, payload=None, timeout: float = 0.5):
            for base_url, target_node in nodes.items():
                if url.startswith(base_url):
                    if base_url in offline:
                        raise ConnectionError(f"Simulated connection failure to {base_url}")
                    path = url[len(base_url) :]
                    client = target_node.app.test_client()
                    resp = client.open(path, method=method, json=payload)
                    return resp.status_code, resp.get_json() or {}
            raise ConnectionError(f"Unknown node URL: {url}")

        return fake_http_json

    def test_validation_rejects_bad_payloads(self):
        primary, _, _ = self._make_cluster()
        try:
            client = primary.app.test_client()
            for bad in [{}, {"key": ""}, {"key": "k"}, {"key": 123, "value": 1}]:
                self.assertEqual(client.post("/kv", json=bad).status_code, 400)
        finally:
            primary.close()

    def test_replica_rejects_direct_writes_and_deletes(self):
        _, replica_a, _ = self._make_cluster()
        try:
            client = replica_a.app.test_client()
            w_resp = client.post("/kv", json={"key": "k", "value": "v"})
            self.assertEqual(w_resp.status_code, 409)
            self.assertEqual(w_resp.get_json()["primary"], "http://primary.local")
            self.assertEqual(client.delete("/kv/k").status_code, 409)
        finally:
            replica_a.close()

    def test_quorum_write_and_delete_replication(self):
        primary, replica_a, replica_b = self._make_cluster()
        nodes = {
            "http://primary.local": primary,
            "http://replica-a.local": replica_a,
            "http://replica-b.local": replica_b,
        }
        try:
            with patch("node.http_json", side_effect=self._route_to_nodes(nodes)):
                p_client = primary.app.test_client()
                resp = p_client.post("/kv", json={"key": "alpha", "value": {"n": 1}})
                self.assertEqual(resp.status_code, 200)
                body = resp.get_json()
                self.assertTrue(body["committed"])
                self.assertEqual(body["version"], 1)
                self.assertEqual(body["acknowledgements"], 2)

                r_resp = p_client.get("/kv/alpha")
                self.assertEqual(r_resp.status_code, 200)
                self.assertEqual(r_resp.get_json()["consistency"], "primary")

                del_resp = p_client.delete("/kv/alpha")
                self.assertEqual(del_resp.status_code, 200)
                self.assertEqual(del_resp.get_json()["version"], 2)
                self.assertEqual(p_client.get("/kv/alpha").status_code, 404)
                self.assertNotIn("alpha", replica_a.store)
                self.assertNotIn("alpha", replica_b.store)
        finally:
            primary.close()
            replica_a.close()
            replica_b.close()

    def test_quorum_loss_blocks_commit_and_recovers_pending_operation(self):
        primary, replica_a, replica_b = self._make_cluster(quorum_size=2)
        offline = {"http://replica-a.local", "http://replica-b.local"}
        nodes = {
            "http://primary.local": primary,
            "http://replica-a.local": replica_a,
            "http://replica-b.local": replica_b,
        }
        try:
            with patch("node.http_json", side_effect=self._route_to_nodes(nodes, offline)):
                p_client = primary.app.test_client()
                fail_resp = p_client.post("/kv", json={"key": "k1", "value": "v1"})
                self.assertEqual(fail_resp.status_code, 503)
                self.assertTrue(fail_resp.get_json()["pending_operation"])
                self.assertEqual(primary.last_index, 0)

                blocked_resp = p_client.post("/kv", json={"key": "k2", "value": "v2"})
                self.assertEqual(blocked_resp.status_code, 503)
                self.assertEqual(blocked_resp.get_json()["pending_index"], 1)

                offline.remove("http://replica-b.local")
                recover_resp = p_client.post("/kv", json={"key": "k2", "value": "v2"})
                self.assertEqual(recover_resp.status_code, 200)
                self.assertEqual(primary.last_index, 2)
                self.assertEqual(replica_b.last_index, 2)
                self.assertEqual(primary.store["k1"]["value"], "v1")
                self.assertEqual(primary.store["k2"]["value"], "v2")
        finally:
            primary.close()
            replica_a.close()
            replica_b.close()

    def test_primary_reads_do_not_stall_during_slow_replication(self):
        primary, replica_a, replica_b = self._make_cluster(quorum_size=2)
        nodes = {
            "http://primary.local": primary,
            "http://replica-a.local": replica_a,
            "http://replica-b.local": replica_b,
        }
        primary.state.commit_primary({"index": 1, "op": "put", "key": "seed", "value": "ok"})

        entered_network = threading.Event()
        release_network = threading.Event()
        real_router = self._route_to_nodes(nodes)

        def slow_router(method: str, url: str, payload=None, timeout: float = 0.5):
            if "/internal/replicate" in url:
                entered_network.set()
                release_network.wait(timeout=2.0)
            return real_router(method, url, payload, timeout)

        try:
            with patch("node.http_json", side_effect=slow_router):
                writer = threading.Thread(
                    target=lambda: primary.app.test_client().post(
                        "/kv", json={"key": "k2", "value": "v2"}
                    )
                )
                writer.start()
                self.assertTrue(entered_network.wait(timeout=1.0))

                read_resp = primary.app.test_client().get("/kv/seed")
                self.assertEqual(read_resp.status_code, 200)
                self.assertEqual(read_resp.get_json()["value"], "ok")

                release_network.set()
                writer.join(timeout=2.0)
        finally:
            release_network.set()
            primary.close()
            replica_a.close()
            replica_b.close()

    def test_lagging_replica_automatic_log_catch_up(self):
        primary, replica_a, replica_b = self._make_cluster(quorum_size=2)
        offline = {"http://replica-a.local"}
        nodes = {
            "http://primary.local": primary,
            "http://replica-a.local": replica_a,
            "http://replica-b.local": replica_b,
        }
        try:
            with patch("node.http_json", side_effect=self._route_to_nodes(nodes, offline)):
                p_client = primary.app.test_client()
                for i in range(1, 5):
                    resp = p_client.post("/kv", json={"key": f"k{i}", "value": i})
                    self.assertEqual(resp.status_code, 200)

                self.assertEqual(replica_a.last_index, 0)
                self.assertEqual(replica_b.last_index, 4)

                offline.clear()
                resp = p_client.post("/kv", json={"key": "k5", "value": 5})
                self.assertEqual(resp.status_code, 200)
                self.assertEqual(replica_a.last_index, 5)
                self.assertEqual(replica_a.store, primary.store)
                self.assertGreaterEqual(primary.stats["replica_catchups"], 1)
        finally:
            primary.close()
            replica_a.close()
            replica_b.close()

    def test_duplicate_and_conflicting_replication(self):
        _, replica_a, _ = self._make_cluster()
        try:
            client = replica_a.app.test_client()
            op1 = {"index": 1, "key": "x", "value": 42}
            self.assertEqual(client.post("/internal/replicate", json=op1).status_code, 200)
            self.assertEqual(client.post("/internal/replicate", json=op1).status_code, 200)

            conflict = client.post(
                "/internal/replicate", json={"index": 1, "key": "x", "value": 99}
            )
            self.assertEqual(conflict.status_code, 409)

            gap = client.post(
                "/internal/replicate", json={"index": 3, "key": "z", "value": 1}
            )
            self.assertEqual(gap.status_code, 409)
            self.assertEqual(gap.get_json()["expected_index"], 2)
        finally:
            replica_a.close()

    def test_replica_read_modes_and_stale_fallback(self):
        primary, replica_a, replica_b = self._make_cluster()
        offline: set[str] = set()
        nodes = {
            "http://primary.local": primary,
            "http://replica-a.local": replica_a,
            "http://replica-b.local": replica_b,
        }
        try:
            with patch("node.http_json", side_effect=self._route_to_nodes(nodes, offline)):
                primary.app.test_client().post("/kv", json={"key": "greeting", "value": "hello"})
                ra_client = replica_a.app.test_client()

                forwarded = ra_client.get("/kv/greeting")
                self.assertEqual(forwarded.status_code, 200)
                self.assertEqual(forwarded.get_json()["consistency"], "forwarded_to_primary")

                local = ra_client.get("/kv/greeting?consistency=local")
                self.assertEqual(local.status_code, 200)
                self.assertEqual(local.get_json()["consistency"], "local_explicit")

                offline.add("http://primary.local")
                fallback = ra_client.get("/kv/greeting")
                self.assertEqual(fallback.status_code, 200)
                self.assertEqual(
                    fallback.get_json()["consistency"],
                    "local_fallback_stale_possible",
                )
        finally:
            primary.close()
            replica_a.close()
            replica_b.close()

    def test_node_wal_survives_restart(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_file = os.path.join(tmpdir, "primary.wal")
            primary, replica_a, replica_b = self._make_cluster(primary_wal=wal_file)
            nodes = {
                "http://primary.local": primary,
                "http://replica-a.local": replica_a,
                "http://replica-b.local": replica_b,
            }
            try:
                with patch("node.http_json", side_effect=self._route_to_nodes(nodes)):
                    client = primary.app.test_client()
                    self.assertEqual(client.post("/kv", json={"key": "a", "value": 1}).status_code, 200)
                    self.assertEqual(client.post("/kv", json={"key": "b", "value": 2}).status_code, 200)
                    self.assertEqual(client.delete("/kv/a").status_code, 200)
            finally:
                primary.close()
                replica_a.close()
                replica_b.close()

            restarted = KeyValueNode(
                node_id="primary",
                role="primary",
                peers=["http://replica-a.local", "http://replica-b.local"],
                wal_path=wal_file,
            )
            try:
                self.assertEqual(restarted.last_index, 3)
                self.assertNotIn("a", restarted.store)
                self.assertEqual(restarted.store["b"]["value"], 2)
            finally:
                restarted.close()


if __name__ == "__main__":
    unittest.main()
