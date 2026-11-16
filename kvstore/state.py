from __future__ import annotations

import copy
import threading
from typing import Any

from kvstore.wal import WriteAheadLog


def normalize_operation(operation: dict[str, Any]) -> dict[str, Any]:
    op_type = operation.get("op", "put")
    normalized: dict[str, Any] = {
        "index": operation["index"],
        "op": op_type,
        "key": operation["key"],
    }
    if op_type == "put":
        normalized["value"] = operation.get("value")
    return normalized


class StoreState:
    def __init__(self, wal: WriteAheadLog | None = None) -> None:
        self._lock = threading.Lock()
        self.wal = wal
        self.store: dict[str, dict[str, Any]] = {}
        self.last_index = 0
        self.applied_operations: dict[int, dict[str, Any]] = {}
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

        if self.wal is not None:
            for entry in self.wal.replay():
                self._apply_unlocked(entry, persist=False)

    def _apply_unlocked(self, operation: dict[str, Any], persist: bool = True) -> None:
        normalized = normalize_operation(operation)
        if persist and self.wal is not None:
            self.wal.append(normalized)

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

    def commit_primary(self, operation: dict[str, Any]) -> None:
        with self._lock:
            self._apply_unlocked(operation, persist=True)
            if operation.get("op") == "delete":
                self.stats["deletes_committed"] += 1
            else:
                self.stats["writes_committed"] += 1

    def apply_replicated(self, operation: dict[str, Any]) -> tuple[str, int]:
        normalized = normalize_operation(operation)
        index = normalized["index"]
        with self._lock:
            existing = self.applied_operations.get(index)
            if index <= self.last_index:
                if existing is not None and normalize_operation(existing) == normalized:
                    return "duplicate", self.last_index
                self.stats["writes_rejected_conflict"] += 1
                return "conflict", self.last_index

            if index != self.last_index + 1:
                self.stats["writes_rejected_conflict"] += 1
                return "gap", self.last_index + 1

            self._apply_unlocked(normalized, persist=True)
            return "applied", self.last_index

    def apply_catchup_batch(self, operations: list[dict[str, Any]]) -> int:
        applied = 0
        with self._lock:
            for item in operations:
                if not isinstance(item, dict):
                    continue
                if item.get("index") == self.last_index + 1:
                    self._apply_unlocked(item, persist=True)
                    applied += 1
            if applied > 0:
                self.stats["replica_catchups"] += 1
        return applied

    def next_index(self) -> int:
        with self._lock:
            return self.last_index + 1

    def get_op(self, index: int) -> dict[str, Any] | None:
        with self._lock:
            op = self.applied_operations.get(index)
            return copy.deepcopy(op) if op is not None else None

    def log_slice(self, from_index: int) -> tuple[list[dict[str, Any]], int]:
        with self._lock:
            ops = [
                copy.deepcopy(self.applied_operations[idx])
                for idx in range(from_index, self.last_index + 1)
                if idx in self.applied_operations
            ]
            return ops, self.last_index

    def read_local(self, key: str, stat_key: str) -> tuple[dict[str, Any] | None, int]:
        with self._lock:
            self.stats[stat_key] += 1
            rec = self.store.get(key)
            return (copy.deepcopy(rec) if rec is not None else None), self.last_index

    def inc_stat(self, key: str, amount: int = 1) -> None:
        with self._lock:
            self.stats[key] += amount

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "last_index": self.last_index,
                "key_count": len(self.store),
                "store": copy.deepcopy(self.store),
                "stats": dict(self.stats),
            }
