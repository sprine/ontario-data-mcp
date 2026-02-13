from __future__ import annotations

import httpx
from typing import Any


class CKANError(Exception):
    """Error returned by the CKAN API."""
    pass


class CKANClient:
    """Async client for the CKAN 2.8 Action API."""

    def __init__(
        self,
        base_url: str = "https://data.ontario.ca",
        timeout: float = 30.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_url = f"{self.base_url}/api/3/action"
        self.timeout = timeout

    async def _request(self, action: str, params: dict[str, Any] | None = None) -> Any:
        """Make a GET request to a CKAN action endpoint."""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(f"{self.api_url}/{action}", params=params)
            response.raise_for_status()
            data = response.json()
            if not data.get("success"):
                error = data.get("error", {})
                msg = error.get("message", str(error))
                raise CKANError(msg)
            return data["result"]

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
        params: dict[str, Any] = {
            "resource_id": resource_id,
            "limit": limit,
            "offset": offset,
        }
        if filters:
            import json
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
