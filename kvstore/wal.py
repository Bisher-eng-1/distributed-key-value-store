from __future__ import annotations

import json
import logging
import os
import zlib
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger("kv-wal")


class WriteAheadLog:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def replay(self) -> list[dict[str, Any]]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            return []

        entries: list[dict[str, Any]] = []
        valid_offset = 0
        total_size = self.path.stat().st_size

        with self.path.open("rb") as fh:
            line_no = 0
            while True:
                line_start = fh.tell()
                raw = fh.readline()
                if not raw:
                    break
                line_no += 1
                stripped = raw.rstrip(b"\r\n")
                if not stripped:
                    valid_offset = fh.tell()
                    continue

                is_last_chunk = fh.tell() == total_size
                has_newline = raw.endswith(b"\n")
                parsed = self._decode_frame(stripped)

                if parsed is None:
                    # Torn tail from an unclean shutdown: rewind to last complete frame.
                    if is_last_chunk or not has_newline:
                        LOGGER.warning(
                            "Truncating incomplete WAL tail at byte %d in %s",
                            line_start,
                            self.path,
                        )
                        break
                    raise ValueError(f"Corrupt WAL frame at line {line_no} in {self.path}")

                entries.append(parsed)
                valid_offset = fh.tell()

        if valid_offset < total_size:
            with self.path.open("r+b") as fh:
                fh.truncate(valid_offset)
                fh.flush()
                os.fsync(fh.fileno())

        return entries

    @staticmethod
    def _decode_frame(raw: bytes) -> dict[str, Any] | None:
        sep = raw.find(b"|")
        if sep != 8:
            return None
        try:
            expected_crc = int(raw[:8].decode("ascii"), 16)
        except ValueError:
            return None

        payload = raw[9:]
        if (zlib.crc32(payload) & 0xFFFFFFFF) != expected_crc:
            return None

        try:
            obj = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
        return obj if isinstance(obj, dict) else None

    def append(self, entry: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(entry, separators=(",", ":"), sort_keys=True).encode("utf-8")
        crc = zlib.crc32(payload) & 0xFFFFFFFF
        frame = f"{crc:08x}|".encode("ascii") + payload + b"\n"
        with self.path.open("ab") as fh:
            fh.write(frame)
            fh.flush()
            os.fsync(fh.fileno())
