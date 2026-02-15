from __future__ import annotations

import asyncio
import logging
import os
import random
import time
from typing import Any

import httpx


logger = logging.getLogger("ontario_data.ckan")

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class CKANError(Exception):
    """Error returned by the CKAN API."""
    pass


class CKANClient:
    """Async client for the CKAN 2.8 Action API with retry and rate limiting."""

    def __init__(
        self,
        base_url: str = "https://data.ontario.ca",
        timeout: float | None = None,
        http_client: httpx.AsyncClient | None = None,
        max_retries: int = 3,
        base_delay: float = 1.0,
        rate_limit: float | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_url = f"{self.base_url}/api/3/action"
        if timeout is None:
            timeout = float(os.environ.get("ONTARIO_DATA_TIMEOUT", "30"))
        self.timeout = timeout
        self._http_client = http_client
        self._owns_client = http_client is None
        self.max_retries = max_retries
        self.base_delay = base_delay
        if rate_limit is None:
            rate_limit = float(os.environ.get("ONTARIO_DATA_RATE_LIMIT", "10"))
        self._min_interval = 1.0 / rate_limit if rate_limit > 0 else 0
        self._last_request_time: float = 0

    async def _get_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=self.timeout)
            self._owns_client = True
        return self._http_client

    async def _rate_limit(self):
        """Enforce per-session rate limiting."""
        if self._min_interval <= 0:
            return
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < self._min_interval:
            await asyncio.sleep(self._min_interval - elapsed)
        self._last_request_time = time.monotonic()

    async def _request(self, action: str, params: dict[str, Any] | None = None) -> Any:
        """Make a GET request with retry and rate limiting."""
        client = await self._get_client()
        url = f"{self.api_url}/{action}"

        for attempt in range(self.max_retries + 1):
            await self._rate_limit()
            try:
                response = await client.get(url, params=params)

                if response.status_code in RETRYABLE_STATUS_CODES and attempt < self.max_retries:
                    delay = self.base_delay * (2 ** attempt) + random.uniform(0, 0.5)
                    logger.warning(
                        "Retryable HTTP %d from %s (attempt %d/%d), waiting %.1fs",
                        response.status_code, action, attempt + 1, self.max_retries, delay,
                    )
                    await asyncio.sleep(delay)
                    continue

                response.raise_for_status()
                data = response.json()
                if not data.get("success"):
                    error = data.get("error", {})
                    msg = error.get("message", str(error))
                    raise CKANError(msg)
                return data["result"]

            except (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout) as e:
                if attempt < self.max_retries:
                    delay = self.base_delay * (2 ** attempt) + random.uniform(0, 0.5)
                    logger.warning(
                        "Connection error for %s (attempt %d/%d): %s, waiting %.1fs",
                        action, attempt + 1, self.max_retries, e, delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                raise

    async def close(self):
        """Close the HTTP client if we own it."""
        if self._owns_client and self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    async def package_search(
        self,
        query: str = "*:*",
        filters: dict[str, str] | None = None,
        sort: str | None = None,
        rows: int = 10,
        start: int = 0,
        facet_fields: list[str] | None = None,
    ) -> dict[str, Any]:
        """Search for datasets."""
        params: dict[str, Any] = {"q": query, "rows": rows, "start": start}
        if filters:
            fq_parts = [f"{k}:{v}" for k, v in filters.items()]
            params["fq"] = " ".join(fq_parts)
        if sort:
            params["sort"] = sort
        if facet_fields:
            params["facet.field"] = str(facet_fields)
            params["facet"] = "true"
        return await self._request("package_search", params)

    async def package_search_all(
        self,
        query: str = "*:*",
        filters: dict[str, str] | None = None,
        sort: str | None = None,
        page_size: int = 100,
    ) -> list[dict[str, Any]]:
        """Paginate through all search results."""
        all_results = []
        start = 0
        while True:
            result = await self.package_search(
                query=query, filters=filters, sort=sort, rows=page_size, start=start,
            )
            all_results.extend(result["results"])
            if len(all_results) >= result["count"]:
                break
            start += page_size
        return all_results

    async def package_show(self, id: str) -> dict[str, Any]:
        """Get full metadata for a dataset."""
        return await self._request("package_show", {"id": id})

    async def resource_show(self, id: str) -> dict[str, Any]:
        """Get metadata for a single resource."""
        return await self._request("resource_show", {"id": id})

    async def resource_search(
        self,
        query: str | list[str],
        order_by: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> dict[str, Any]:
        """Search resources by field values."""
        params: dict[str, Any] = {"query": query}
        if order_by:
            params["order_by"] = order_by
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        return await self._request("resource_search", params)

    async def datastore_search(
        self,
        resource_id: str,
        filters: dict[str, Any] | None = None,
        fields: list[str] | None = None,
        sort: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        """Query data from the CKAN Datastore."""
        import json
        params: dict[str, Any] = {
            "resource_id": resource_id,
            "limit": limit,
            "offset": offset,
        }
        if filters:
            params["filters"] = json.dumps(filters)
        if fields:
            params["fields"] = ",".join(fields)
        if sort:
            params["sort"] = sort
        return await self._request("datastore_search", params)

    async def datastore_search_all(
        self,
        resource_id: str,
        filters: dict[str, Any] | None = None,
        fields: list[str] | None = None,
        sort: str | None = None,
        page_size: int = 1000,
    ) -> dict[str, Any]:
        """Paginate through all datastore records for a resource."""
        all_records = []
        result_fields = None
        offset = 0
        total = None
        while True:
            result = await self.datastore_search(
                resource_id=resource_id,
                filters=filters,
                fields=fields,
                sort=sort,
                limit=page_size,
                offset=offset,
            )
            if result_fields is None:
                result_fields = result["fields"]
            if total is None:
                total = result["total"]
            all_records.extend(result["records"])
            if len(all_records) >= total:
                break
            offset += page_size
        return {"records": all_records, "fields": result_fields, "total": total}

    async def datastore_sql(self, sql: str) -> dict[str, Any]:
        """Execute a SQL query against the CKAN Datastore."""
        return await self._request("datastore_search_sql", {"sql": sql})

    async def tag_list(self, query: str | None = None, all_fields: bool = False) -> list:
        """List tags."""
        params: dict[str, Any] = {"all_fields": all_fields}
        if query:
            params["query"] = query
        return await self._request("tag_list", params)

    async def organization_list(
        self,
        sort: str = "package_count desc",
        all_fields: bool = False,
        include_dataset_count: bool = True,
    ) -> list:
        """List organizations."""
        return await self._request("organization_list", {
            "sort": sort,
            "all_fields": all_fields,
            "include_dataset_count": include_dataset_count,
        })

    async def group_list(
        self,
        sort: str = "package_count desc",
        all_fields: bool = False,
        include_dataset_count: bool = True,
    ) -> list:
        """List groups."""
        return await self._request("group_list", {
            "sort": sort,
            "all_fields": all_fields,
            "include_dataset_count": include_dataset_count,
        })

    async def package_list(self, limit: int | None = None, offset: int | None = None) -> list[str]:
        """List all dataset names."""
        params: dict[str, Any] = {}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        return await self._request("package_list", params)
