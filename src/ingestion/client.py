import threading
import time
from typing import Iterator

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)


class OpenFDAClient:
    BASE_URL = "https://api.fda.gov"

    def __init__(self, api_key: str = "", requests_per_minute: int = 200):
        self.api_key = api_key
        self._min_interval = 60.0 / max(requests_per_minute, 1)
        self._last = 0.0
        self._lock = threading.Lock()
        self._local = threading.local()

    def _http(self) -> httpx.Client:
        if not hasattr(self._local, "client"):
            self._local.client = httpx.Client(timeout=60.0)
        return self._local.client

    def _throttle(self) -> None:
        with self._lock:
            elapsed = time.monotonic() - self._last
            wait = self._min_interval - elapsed
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    )
    def _get(self, endpoint: str, params: dict) -> dict:
        self._throttle()
        url = f"{self.BASE_URL}{endpoint}"
        if self.api_key:
            params = {**params, "api_key": self.api_key}
        response = self._http().get(url, params=params)
        if response.status_code == 404:
            return {"results": [], "meta": {"results": {"total": 0}}}
        if response.status_code == 429:
            raise httpx.HTTPError(f"Rate limited: {response.text}")
        if response.status_code == 500:
            raise ValueError(f"openFDA 500 for search: {params.get('search', '')}")
        response.raise_for_status()
        return response.json()

    def fetch_page(
        self, endpoint: str, search: str = "", limit: int = 1000, skip: int = 0
    ) -> tuple[list[dict], int]:
        params: dict = {"limit": limit, "skip": skip}
        if search:
            params["search"] = search
        data = self._get(endpoint, params)
        total = data.get("meta", {}).get("results", {}).get("total", 0)
        return data.get("results", []), total

    def paginate(
        self, endpoint: str, search: str = "", limit: int = 1000
    ) -> Iterator[list[dict]]:
        """Paginate up to the openFDA hard cap of 25,000 records per query."""
        skip = 0
        cap: int | None = None
        while True:
            records, total = self.fetch_page(endpoint, search, limit, skip)
            if cap is None:
                cap = min(total, 25_000)
            if not records:
                break
            yield records
            skip += len(records)
            if skip >= cap:
                break

    def close(self) -> None:
        if hasattr(self._local, "client"):
            self._local.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
