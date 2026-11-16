from __future__ import annotations

import http.client
import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def http_json(
    method: str,
    url: str,
    payload: dict[str, Any] | None = None,
    timeout: float = 0.5,
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
