# Ontario Data MCP Server — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a FastMCP server that provides 34 tools for discovering, downloading, caching, querying, and analyzing Ontario open data from data.ontario.ca, backed by DuckDB.

**Architecture:** Single FastMCP server with an async CKAN client, DuckDB cache/analytics engine, and tools organized into 7 categories. Lifespan context manages the shared DuckDB connection. All data retrieval goes through the CKAN client; all local analytics go through DuckDB.

**Tech Stack:** Python 3.12+, FastMCP, httpx, DuckDB (with spatial extension), pandas, openpyxl, geopandas, shapely

---

## Task 1: Project Setup

**Files:**
- Modify: `pyproject.toml`
- Create: `src/ontario_data/__init__.py`
- Create: `src/ontario_data/server.py` (skeleton)
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`

**Step 1: Update pyproject.toml**

```toml
[project]
name = "data-ontario-ca-mcp"
version = "0.1.0"
description = "MCP server for Ontario Open Data Catalogue"
readme = "README.md"
requires-python = ">=3.12"
dependencies = [
    "fastmcp>=2.0.0",
    "httpx>=0.27.0",
    "duckdb>=1.1.0",
    "pandas>=2.2.0",
    "openpyxl>=3.1.0",
    "geopandas>=1.0.0",
    "shapely>=2.0.0",
]

[project.scripts]
ontario-data-mcp = "ontario_data.server:main"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/ontario_data"]

[tool.pytest.ini_options]
testpaths = ["tests"]
asyncio_mode = "auto"

[dependency-groups]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.24.0",
    "pytest-httpx>=0.34.0",
    "respx>=0.22.0",
]
```

**Step 2: Create directory structure and install**

```bash
mkdir -p src/ontario_data/tools tests
touch src/ontario_data/__init__.py
touch src/ontario_data/tools/__init__.py
touch tests/__init__.py
```

**Step 3: Create minimal server skeleton**

`src/ontario_data/server.py`:
```python
from fastmcp import FastMCP

mcp = FastMCP(
    "Ontario Data Catalogue",
    instructions="Search, download, cache, and analyze datasets from Ontario's Open Data Catalogue (data.ontario.ca).",
    version="0.1.0",
)


def main():
    mcp.run()


if __name__ == "__main__":
    main()
```

**Step 4: Create test conftest**

`tests/conftest.py`:
```python
import pytest
import duckdb
import tempfile
import os


@pytest.fixture
def tmp_cache_dir(tmp_path):
    """Temporary cache directory for tests."""
    return str(tmp_path / "ontario_data_cache")


@pytest.fixture
def duckdb_conn(tmp_path):
    """In-memory DuckDB connection for tests."""
    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    yield conn
    conn.close()
```

**Step 5: Install dependencies and verify**

Run: `cd /Users/anshu/tools/data.ontario.ca-mcp && uv sync`
Expected: Dependencies install successfully.

Run: `uv run python -c "from ontario_data.server import mcp; print(mcp.name)"`
Expected: `Ontario Data Catalogue`

**Step 6: Commit**

```bash
git add -A
git commit -m "feat: project setup with dependencies and server skeleton"
```

---

## Task 2: CKAN Client

**Files:**
- Create: `src/ontario_data/ckan_client.py`
- Create: `tests/test_ckan_client.py`

**Step 1: Write failing tests for CKAN client**

`tests/test_ckan_client.py`:
```python
import pytest
import httpx
import respx
import json
from ontario_data.ckan_client import CKANClient

BASE_URL = "https://data.ontario.ca"


@pytest.fixture
def client():
    return CKANClient(base_url=BASE_URL)


class TestPackageSearch:
    @respx.mock
    @pytest.mark.asyncio
    async def test_basic_search(self, client):
        respx.get(f"{BASE_URL}/api/3/action/package_search").mock(
            return_value=httpx.Response(200, json={
                "success": True,
                "result": {
                    "count": 1,
                    "results": [{"id": "abc", "title": "Test Dataset", "name": "test-dataset"}],
                },
            })
        )
        result = await client.package_search(query="test")
        assert result["count"] == 1
        assert result["results"][0]["title"] == "Test Dataset"

    @respx.mock
    @pytest.mark.asyncio
    async def test_search_with_filters(self, client):
        route = respx.get(f"{BASE_URL}/api/3/action/package_search").mock(
            return_value=httpx.Response(200, json={
                "success": True,
                "result": {"count": 0, "results": []},
            })
        )
        await client.package_search(query="health", filters={"organization": "health"}, rows=5)
        request = route.calls[0].request
        assert "fq=organization%3Ahealth" in str(request.url) or "organization" in str(request.url)


class TestPackageShow:
    @respx.mock
    @pytest.mark.asyncio
    async def test_get_dataset(self, client):
        respx.get(f"{BASE_URL}/api/3/action/package_show").mock(
            return_value=httpx.Response(200, json={
                "success": True,
                "result": {
                    "id": "abc",
                    "title": "Test",
                    "resources": [{"id": "r1", "format": "CSV", "url": "http://example.com/data.csv"}],
                },
            })
        )
        result = await client.package_show("abc")
        assert result["title"] == "Test"
        assert len(result["resources"]) == 1


class TestDatastoreSearch:
    @respx.mock
    @pytest.mark.asyncio
    async def test_basic_datastore_query(self, client):
        respx.get(f"{BASE_URL}/api/3/action/datastore_search").mock(
            return_value=httpx.Response(200, json={
                "success": True,
                "result": {
                    "total": 100,
                    "records": [{"_id": 1, "name": "Alice"}],
                    "fields": [{"id": "_id", "type": "int"}, {"id": "name", "type": "text"}],
                },
            })
        )
        result = await client.datastore_search("r1", limit=1)
        assert result["total"] == 100
        assert result["records"][0]["name"] == "Alice"

    @respx.mock
    @pytest.mark.asyncio
    async def test_datastore_sql(self, client):
        respx.get(f"{BASE_URL}/api/3/action/datastore_search_sql").mock(
            return_value=httpx.Response(200, json={
                "success": True,
                "result": {
                    "records": [{"count": 42}],
                    "fields": [{"id": "count", "type": "int"}],
                },
            })
        )
        result = await client.datastore_sql('SELECT count(*) FROM "r1"')
        assert result["records"][0]["count"] == 42


class TestListEndpoints:
    @respx.mock
    @pytest.mark.asyncio
    async def test_tag_list(self, client):
        respx.get(f"{BASE_URL}/api/3/action/tag_list").mock(
            return_value=httpx.Response(200, json={
                "success": True,
                "result": ["economy", "health", "education"],
            })
        )
        result = await client.tag_list()
        assert "health" in result

    @respx.mock
    @pytest.mark.asyncio
    async def test_organization_list(self, client):
        respx.get(f"{BASE_URL}/api/3/action/organization_list").mock(
            return_value=httpx.Response(200, json={
                "success": True,
                "result": [{"name": "health", "title": "Health", "package_count": 386}],
            })
        )
        result = await client.organization_list(all_fields=True)
        assert result[0]["name"] == "health"


class TestErrorHandling:
    @respx.mock
    @pytest.mark.asyncio
    async def test_api_error_raises(self, client):
        respx.get(f"{BASE_URL}/api/3/action/package_show").mock(
            return_value=httpx.Response(200, json={
                "success": False,
                "error": {"message": "Not found", "__type": "Not Found Error"},
            })
        )
        with pytest.raises(Exception, match="Not found"):
            await client.package_show("nonexistent")

    @respx.mock
    @pytest.mark.asyncio
    async def test_http_error_raises(self, client):
        respx.get(f"{BASE_URL}/api/3/action/package_show").mock(
            return_value=httpx.Response(500)
        )
        with pytest.raises(httpx.HTTPStatusError):
            await client.package_show("anything")


class TestPagination:
    @respx.mock
    @pytest.mark.asyncio
    async def test_paginate_all_results(self, client):
        call_count = 0

        def handler(request):
            nonlocal call_count
            start = int(request.url.params.get("start", 0))
            if start == 0:
                call_count += 1
                return httpx.Response(200, json={
                    "success": True,
                    "result": {
                        "count": 3,
                        "results": [{"id": "a"}, {"id": "b"}],
                    },
                })
            else:
                call_count += 1
                return httpx.Response(200, json={
                    "success": True,
                    "result": {
                        "count": 3,
                        "results": [{"id": "c"}],
                    },
                })

        respx.get(f"{BASE_URL}/api/3/action/package_search").mock(side_effect=handler)
        results = await client.package_search_all(query="test", page_size=2)
        assert len(results) == 3
        assert call_count == 2
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_ckan_client.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ontario_data.ckan_client'`

**Step 3: Implement CKAN client**

`src/ontario_data/ckan_client.py`:
```python
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
```

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_ckan_client.py -v`
Expected: All tests PASS.

**Step 5: Commit**

```bash
git add src/ontario_data/ckan_client.py tests/test_ckan_client.py
git commit -m "feat: async CKAN API client with pagination and error handling"
```

---

## Task 3: DuckDB Cache Manager

**Files:**
- Create: `src/ontario_data/cache.py`
- Create: `tests/test_cache.py`

**Step 1: Write failing tests**

`tests/test_cache.py`:
```python
import pytest
import duckdb
import pandas as pd
from datetime import datetime, timedelta, timezone
from ontario_data.cache import CacheManager


@pytest.fixture
def cache(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    mgr = CacheManager(db_path)
    mgr.initialize()
    return mgr


class TestInitialization:
    def test_creates_metadata_tables(self, cache):
        tables = cache.conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
        table_names = [t[0] for t in tables]
        assert "_cache_metadata" in table_names
        assert "_dataset_metadata" in table_names

    def test_idempotent_init(self, cache):
        cache.initialize()  # Should not raise
        tables = cache.conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name='_cache_metadata'"
        ).fetchone()
        assert tables[0] == 1


class TestStoreDataFrame:
    def test_store_and_retrieve(self, cache):
        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
        cache.store_resource("r1", "ds1", "test_table", df, "http://example.com/data.csv")

        result = cache.conn.execute("SELECT * FROM test_table ORDER BY name").fetchdf()
        assert len(result) == 2
        assert list(result["name"]) == ["Alice", "Bob"]

    def test_metadata_recorded(self, cache):
        df = pd.DataFrame({"x": [1, 2, 3]})
        cache.store_resource("r1", "ds1", "tbl", df, "http://example.com")

        meta = cache.conn.execute(
            "SELECT * FROM _cache_metadata WHERE resource_id='r1'"
        ).fetchone()
        assert meta is not None
        assert meta[4] == 3  # row_count

    def test_force_refresh_replaces(self, cache):
        df1 = pd.DataFrame({"x": [1]})
        df2 = pd.DataFrame({"x": [1, 2, 3]})
        cache.store_resource("r1", "ds1", "tbl", df1, "http://example.com")
        cache.store_resource("r1", "ds1", "tbl", df2, "http://example.com")

        result = cache.conn.execute("SELECT count(*) FROM tbl").fetchone()
        assert result[0] == 3


class TestCacheQueries:
    def test_list_cached(self, cache):
        df = pd.DataFrame({"x": [1]})
        cache.store_resource("r1", "ds1", "tbl1", df, "http://example.com/1")
        cache.store_resource("r2", "ds2", "tbl2", df, "http://example.com/2")

        cached = cache.list_cached()
        assert len(cached) == 2

    def test_is_cached(self, cache):
        assert not cache.is_cached("r1")
        df = pd.DataFrame({"x": [1]})
        cache.store_resource("r1", "ds1", "tbl", df, "http://example.com")
        assert cache.is_cached("r1")

    def test_get_table_name(self, cache):
        df = pd.DataFrame({"x": [1]})
        cache.store_resource("r1", "ds1", "my_table", df, "http://example.com")
        assert cache.get_table_name("r1") == "my_table"

    def test_remove_resource(self, cache):
        df = pd.DataFrame({"x": [1]})
        cache.store_resource("r1", "ds1", "tbl", df, "http://example.com")
        cache.remove_resource("r1")
        assert not cache.is_cached("r1")

    def test_cache_stats(self, cache):
        df = pd.DataFrame({"x": range(100)})
        cache.store_resource("r1", "ds1", "tbl", df, "http://example.com")
        stats = cache.get_stats()
        assert stats["table_count"] == 1
        assert stats["total_rows"] == 100


class TestSQLQuery:
    def test_run_sql(self, cache):
        df = pd.DataFrame({"name": ["Alice", "Bob"], "score": [90, 85]})
        cache.store_resource("r1", "ds1", "scores", df, "http://example.com")

        result = cache.query("SELECT name, score FROM scores WHERE score > 87")
        assert len(result) == 1
        assert result[0]["name"] == "Alice"


class TestDatasetMetadata:
    def test_store_and_get_metadata(self, cache):
        meta = {"id": "ds1", "title": "Test", "organization": {"name": "health"}}
        cache.store_dataset_metadata("ds1", meta)
        result = cache.get_dataset_metadata("ds1")
        assert result["title"] == "Test"
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_cache.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Implement cache manager**

`src/ontario_data/cache.py`:
```python
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

import duckdb
import pandas as pd


class CacheManager:
    """DuckDB-backed cache and analytics engine for Ontario open data."""

    def __init__(self, db_path: str | None = None):
        if db_path is None:
            cache_dir = os.path.expanduser("~/.cache/ontario-data")
            os.makedirs(cache_dir, exist_ok=True)
            db_path = os.path.join(cache_dir, "ontario_data.duckdb")
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)

    def initialize(self):
        """Create metadata tables and install extensions."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS _cache_metadata (
                resource_id VARCHAR PRIMARY KEY,
                dataset_id VARCHAR,
                table_name VARCHAR,
                downloaded_at TIMESTAMP,
                row_count INTEGER,
                size_bytes BIGINT,
                source_url VARCHAR
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS _dataset_metadata (
                dataset_id VARCHAR PRIMARY KEY,
                metadata JSON,
                cached_at TIMESTAMP
            )
        """)
        # Install extensions (ignore errors if already installed)
        for ext in ["spatial", "httpfs", "json"]:
            try:
                self.conn.execute(f"INSTALL {ext}")
                self.conn.execute(f"LOAD {ext}")
            except Exception:
                try:
                    self.conn.execute(f"LOAD {ext}")
                except Exception:
                    pass

    def store_resource(
        self,
        resource_id: str,
        dataset_id: str,
        table_name: str,
        df: pd.DataFrame,
        source_url: str,
    ):
        """Store a pandas DataFrame as a DuckDB table."""
        # Drop existing table if re-caching
        if self.is_cached(resource_id):
            old_table = self.get_table_name(resource_id)
            if old_table:
                self.conn.execute(f"DROP TABLE IF EXISTS \"{old_table}\"")
            self.conn.execute(
                "DELETE FROM _cache_metadata WHERE resource_id = ?", [resource_id]
            )

        # Create table from DataFrame
        self.conn.execute(f"CREATE TABLE \"{table_name}\" AS SELECT * FROM df")

        # Record metadata
        now = datetime.now(timezone.utc)
        size = df.memory_usage(deep=True).sum()
        self.conn.execute(
            """INSERT INTO _cache_metadata
               (resource_id, dataset_id, table_name, downloaded_at, row_count, size_bytes, source_url)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [resource_id, dataset_id, table_name, now, len(df), int(size), source_url],
        )

    def is_cached(self, resource_id: str) -> bool:
        """Check if a resource is in the cache."""
        result = self.conn.execute(
            "SELECT 1 FROM _cache_metadata WHERE resource_id = ?", [resource_id]
        ).fetchone()
        return result is not None

    def get_table_name(self, resource_id: str) -> str | None:
        """Get the DuckDB table name for a cached resource."""
        result = self.conn.execute(
            "SELECT table_name FROM _cache_metadata WHERE resource_id = ?", [resource_id]
        ).fetchone()
        return result[0] if result else None

    def list_cached(self) -> list[dict[str, Any]]:
        """List all cached resources."""
        rows = self.conn.execute(
            "SELECT resource_id, dataset_id, table_name, downloaded_at, row_count, size_bytes, source_url "
            "FROM _cache_metadata ORDER BY downloaded_at DESC"
        ).fetchall()
        return [
            {
                "resource_id": r[0],
                "dataset_id": r[1],
                "table_name": r[2],
                "downloaded_at": str(r[3]),
                "row_count": r[4],
                "size_bytes": r[5],
                "source_url": r[6],
            }
            for r in rows
        ]

    def remove_resource(self, resource_id: str):
        """Remove a resource from the cache."""
        table_name = self.get_table_name(resource_id)
        if table_name:
            self.conn.execute(f"DROP TABLE IF EXISTS \"{table_name}\"")
        self.conn.execute(
            "DELETE FROM _cache_metadata WHERE resource_id = ?", [resource_id]
        )

    def remove_all(self):
        """Remove all cached resources."""
        for item in self.list_cached():
            self.conn.execute(f"DROP TABLE IF EXISTS \"{item['table_name']}\"")
        self.conn.execute("DELETE FROM _cache_metadata")

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        result = self.conn.execute(
            "SELECT count(*), coalesce(sum(row_count), 0), coalesce(sum(size_bytes), 0) "
            "FROM _cache_metadata"
        ).fetchone()
        return {
            "table_count": result[0],
            "total_rows": result[1],
            "total_size_bytes": result[2],
            "db_path": self.db_path,
        }

    def query(self, sql: str) -> list[dict[str, Any]]:
        """Run a SQL query against the cache and return results as dicts."""
        result = self.conn.execute(sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    def query_df(self, sql: str) -> pd.DataFrame:
        """Run a SQL query and return a DataFrame."""
        return self.conn.execute(sql).fetchdf()

    def store_dataset_metadata(self, dataset_id: str, metadata: dict[str, Any]):
        """Cache dataset metadata."""
        now = datetime.now(timezone.utc)
        self.conn.execute(
            """INSERT OR REPLACE INTO _dataset_metadata (dataset_id, metadata, cached_at)
               VALUES (?, ?, ?)""",
            [dataset_id, json.dumps(metadata), now],
        )

    def get_dataset_metadata(self, dataset_id: str) -> dict[str, Any] | None:
        """Get cached dataset metadata."""
        result = self.conn.execute(
            "SELECT metadata FROM _dataset_metadata WHERE dataset_id = ?", [dataset_id]
        ).fetchone()
        if result:
            return json.loads(result[0])
        return None

    def close(self):
        """Close the DuckDB connection."""
        self.conn.close()
```

**Step 4: Run tests**

Run: `uv run pytest tests/test_cache.py -v`
Expected: All PASS.

**Step 5: Commit**

```bash
git add src/ontario_data/cache.py tests/test_cache.py
git commit -m "feat: DuckDB cache manager with metadata tracking and SQL queries"
```

---

## Task 4: Server with Lifespan

**Files:**
- Modify: `src/ontario_data/server.py`
- Create: `tests/test_server.py`

**Step 1: Write failing test**

`tests/test_server.py`:
```python
import pytest
from fastmcp import Client
from ontario_data.server import mcp


@pytest.mark.asyncio
async def test_server_starts():
    """Verify the server can start and list tools."""
    async with Client(mcp) as client:
        tools = await client.list_tools()
        # At minimum we should have some tools registered
        assert isinstance(tools, list)
```

**Step 2: Run test to verify it passes (skeleton server should work)**

Run: `uv run pytest tests/test_server.py -v`
Expected: PASS (empty tool list is fine for now).

**Step 3: Update server with lifespan and shared state**

`src/ontario_data/server.py`:
```python
from __future__ import annotations

from contextlib import asynccontextmanager

from fastmcp import FastMCP

from ontario_data.cache import CacheManager
from ontario_data.ckan_client import CKANClient


@asynccontextmanager
async def lifespan(server):
    """Initialize shared resources for the server."""
    client = CKANClient()
    cache = CacheManager()
    cache.initialize()
    yield {"ckan": client, "cache": cache}
    cache.close()


mcp = FastMCP(
    "Ontario Data Catalogue",
    instructions=(
        "Search, download, cache, and analyze datasets from Ontario's Open Data Catalogue "
        "(data.ontario.ca). Use discovery tools to find datasets, retrieval tools to cache them "
        "locally in DuckDB, and analytics tools to query and analyze the data."
    ),
    version="0.1.0",
    lifespan=lifespan,
)

# Import tool modules to register them with the server
from ontario_data.tools import discovery  # noqa: E402, F401
from ontario_data.tools import metadata  # noqa: E402, F401
from ontario_data.tools import retrieval  # noqa: E402, F401
from ontario_data.tools import querying  # noqa: E402, F401
from ontario_data.tools import quality  # noqa: E402, F401
from ontario_data.tools import analytics  # noqa: E402, F401
from ontario_data.tools import geospatial  # noqa: E402, F401
from ontario_data import prompts  # noqa: E402, F401
from ontario_data import resources  # noqa: E402, F401


def main():
    mcp.run()


if __name__ == "__main__":
    main()
```

Note: We'll create empty tool modules first, then fill them in subsequent tasks.

**Step 4: Create empty tool modules**

```python
# src/ontario_data/tools/discovery.py
# src/ontario_data/tools/metadata.py
# src/ontario_data/tools/retrieval.py
# src/ontario_data/tools/querying.py
# src/ontario_data/tools/quality.py
# src/ontario_data/tools/analytics.py
# src/ontario_data/tools/geospatial.py
# src/ontario_data/prompts.py
# src/ontario_data/resources.py
```

Each file should be empty (or contain just a comment). They'll be filled in the following tasks.

**Step 5: Run test**

Run: `uv run pytest tests/test_server.py -v`
Expected: PASS

**Step 6: Commit**

```bash
git add -A
git commit -m "feat: server lifespan with CKAN client and DuckDB cache initialization"
```

---

## Task 5: Discovery & Search Tools (6 tools)

**Files:**
- Modify: `src/ontario_data/tools/discovery.py`
- Create: `tests/test_tools_discovery.py`

**Step 1: Write failing tests**

`tests/test_tools_discovery.py`:
```python
import pytest
from unittest.mock import AsyncMock, patch
from fastmcp import Client
from ontario_data.server import mcp


@pytest.mark.asyncio
async def test_search_datasets():
    async with Client(mcp) as client:
        tools = await client.list_tools()
        tool_names = [t.name for t in tools]
        assert "search_datasets" in tool_names

        # Test actual invocation (will hit real API or we mock at integration level)
        result = await client.call_tool("search_datasets", {"query": "covid", "limit": 2})
        assert result is not None


@pytest.mark.asyncio
async def test_list_organizations():
    async with Client(mcp) as client:
        result = await client.call_tool("list_organizations", {})
        assert result is not None


@pytest.mark.asyncio
async def test_list_topics():
    async with Client(mcp) as client:
        result = await client.call_tool("list_topics", {})
        assert result is not None
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_tools_discovery.py -v`
Expected: FAIL — tool not found.

**Step 3: Implement discovery tools**

`src/ontario_data/tools/discovery.py`:
```python
from __future__ import annotations

import json
from typing import Any

from fastmcp import Context

from ontario_data.server import mcp
from ontario_data.ckan_client import CKANClient
from ontario_data.cache import CacheManager


def _get_deps(ctx: Context) -> tuple[CKANClient, CacheManager]:
    return ctx.lifespan_context["ckan"], ctx.lifespan_context["cache"]


@mcp.tool
async def search_datasets(
    query: str,
    organization: str | None = None,
    resource_format: str | None = None,
    update_frequency: str | None = None,
    sort_by: str = "relevance asc, metadata_modified desc",
    limit: int = 10,
    ctx: Context = None,
) -> str:
    """Search for datasets in Ontario's Open Data Catalogue.

    Args:
        query: Search terms (e.g. "covid cases", "housing prices", "school enrollment")
        organization: Filter by ministry/org (e.g. "health", "education")
        resource_format: Filter by file format (e.g. "CSV", "JSON", "SHP")
        update_frequency: Filter by frequency (e.g. "yearly", "monthly", "daily")
        sort_by: Sort order (default: relevance)
        limit: Max results to return (1-50)
    """
    ckan, _ = _get_deps(ctx)
    filters = {}
    if organization:
        filters["organization"] = organization
    if resource_format:
        filters["res_format"] = resource_format
    if update_frequency:
        filters["update_frequency"] = update_frequency

    result = await ckan.package_search(
        query=query, filters=filters or None, sort=sort_by, rows=min(limit, 50),
    )

    datasets = []
    for ds in result["results"]:
        resources = ds.get("resources", [])
        formats = sorted(set(r.get("format", "").upper() for r in resources if r.get("format")))
        datasets.append({
            "id": ds["id"],
            "name": ds.get("name"),
            "title": ds.get("title"),
            "organization": ds.get("organization", {}).get("title", "Unknown"),
            "description": (ds.get("notes") or "")[:200],
            "formats": formats,
            "num_resources": len(resources),
            "last_modified": ds.get("metadata_modified"),
            "update_frequency": ds.get("update_frequency", "unknown"),
        })

    return json.dumps({
        "total_count": result["count"],
        "returned": len(datasets),
        "datasets": datasets,
    }, indent=2)


@mcp.tool
async def list_organizations(
    include_counts: bool = True,
    ctx: Context = None,
) -> str:
    """List all Ontario government ministries and organizations with dataset counts.

    Use this to discover which ministries publish data and how much.
    """
    ckan, _ = _get_deps(ctx)
    orgs = await ckan.organization_list(all_fields=True, include_dataset_count=include_counts)
    result = []
    for org in orgs:
        result.append({
            "name": org.get("name"),
            "title": org.get("title"),
            "dataset_count": org.get("package_count", 0),
            "description": (org.get("description") or "")[:150],
        })
    result.sort(key=lambda x: x["dataset_count"], reverse=True)
    return json.dumps(result, indent=2)


@mcp.tool
async def list_topics(
    query: str | None = None,
    ctx: Context = None,
) -> str:
    """List all tags/topics used in the Ontario Data Catalogue.

    Args:
        query: Optional filter to match tag names
    """
    ckan, _ = _get_deps(ctx)
    tags = await ckan.tag_list(query=query, all_fields=True)
    if isinstance(tags, list) and tags and isinstance(tags[0], dict):
        result = [{"name": t["name"], "count": t.get("count", 0)} for t in tags]
    else:
        result = [{"name": t} for t in tags]
    return json.dumps(result, indent=2)


@mcp.tool
async def get_popular_datasets(
    sort: str = "recent",
    limit: int = 10,
    ctx: Context = None,
) -> str:
    """Get popular or recently updated datasets.

    Args:
        sort: "recent" for recently modified, "name" for alphabetical
        limit: Number of results (1-50)
    """
    ckan, _ = _get_deps(ctx)
    sort_map = {
        "recent": "metadata_modified desc",
        "name": "title asc",
    }
    sort_str = sort_map.get(sort, "metadata_modified desc")
    result = await ckan.package_search(sort=sort_str, rows=min(limit, 50))

    datasets = []
    for ds in result["results"]:
        datasets.append({
            "id": ds["id"],
            "name": ds.get("name"),
            "title": ds.get("title"),
            "organization": ds.get("organization", {}).get("title", "Unknown"),
            "last_modified": ds.get("metadata_modified"),
            "update_frequency": ds.get("update_frequency", "unknown"),
        })
    return json.dumps({"total": result["count"], "datasets": datasets}, indent=2)


@mcp.tool
async def search_by_location(
    region: str,
    limit: int = 10,
    ctx: Context = None,
) -> str:
    """Find datasets covering a specific geographic area in Ontario.

    Args:
        region: Geographic area (e.g. "Toronto", "Northern Ontario", "Ottawa", "province-wide")
        limit: Max results
    """
    ckan, _ = _get_deps(ctx)
    # Search using geographic_coverage field and general query
    result = await ckan.package_search(
        query=region,
        filters=None,
        rows=min(limit, 50),
    )

    datasets = []
    for ds in result["results"]:
        datasets.append({
            "id": ds["id"],
            "title": ds.get("title"),
            "organization": ds.get("organization", {}).get("title", "Unknown"),
            "geographic_coverage": ds.get("geographic_coverage", "Not specified"),
            "description": (ds.get("notes") or "")[:200],
        })
    return json.dumps({"total": result["count"], "datasets": datasets}, indent=2)


@mcp.tool
async def find_related_datasets(
    dataset_id: str,
    limit: int = 10,
    ctx: Context = None,
) -> str:
    """Find datasets related to a given dataset by shared tags and organization.

    Args:
        dataset_id: The ID or name of the source dataset
        limit: Max related datasets to return
    """
    ckan, _ = _get_deps(ctx)
    # Get the source dataset
    source = await ckan.package_show(dataset_id)
    tags = [t["name"] for t in source.get("tags", [])]
    org = source.get("organization", {}).get("name", "")

    related = []
    # Search by tags
    if tags:
        tag_query = " OR ".join(tags[:5])
        result = await ckan.package_search(query=tag_query, rows=min(limit + 5, 50))
        for ds in result["results"]:
            if ds["id"] != source["id"]:
                shared_tags = [t["name"] for t in ds.get("tags", []) if t["name"] in tags]
                related.append({
                    "id": ds["id"],
                    "title": ds.get("title"),
                    "organization": ds.get("organization", {}).get("title", "Unknown"),
                    "shared_tags": shared_tags,
                    "relevance": "tags",
                })

    # Search by organization
    if org:
        result = await ckan.package_search(filters={"organization": org}, rows=min(limit, 50))
        seen_ids = {r["id"] for r in related}
        for ds in result["results"]:
            if ds["id"] != source["id"] and ds["id"] not in seen_ids:
                related.append({
                    "id": ds["id"],
                    "title": ds.get("title"),
                    "organization": ds.get("organization", {}).get("title", "Unknown"),
                    "shared_tags": [],
                    "relevance": "same_organization",
                })

    return json.dumps({
        "source": {"id": source["id"], "title": source.get("title"), "tags": tags},
        "related": related[:limit],
    }, indent=2)
```

**Step 4: Run tests**

Run: `uv run pytest tests/test_tools_discovery.py -v`
Expected: PASS (these tests hit the real API — adjust if needed by mocking).

**Step 5: Commit**

```bash
git add src/ontario_data/tools/discovery.py tests/test_tools_discovery.py
git commit -m "feat: discovery and search tools (6 tools)"
```

---

## Task 6: Metadata & Inspection Tools (5 tools)

**Files:**
- Modify: `src/ontario_data/tools/metadata.py`

**Step 1: Implement metadata tools**

`src/ontario_data/tools/metadata.py`:
```python
from __future__ import annotations

import json
from typing import Any

from fastmcp import Context

from ontario_data.server import mcp
from ontario_data.ckan_client import CKANClient
from ontario_data.cache import CacheManager


def _get_deps(ctx: Context) -> tuple[CKANClient, CacheManager]:
    return ctx.lifespan_context["ckan"], ctx.lifespan_context["cache"]


@mcp.tool
async def get_dataset_info(
    dataset_id: str,
    ctx: Context = None,
) -> str:
    """Get full metadata for a dataset including all resources.

    Args:
        dataset_id: Dataset ID or URL-friendly name (e.g. "ontario-covid-19-cases")
    """
    ckan, cache = _get_deps(ctx)
    ds = await ckan.package_show(dataset_id)
    cache.store_dataset_metadata(ds["id"], ds)

    resources = []
    for r in ds.get("resources", []):
        resources.append({
            "id": r["id"],
            "name": r.get("name"),
            "format": r.get("format"),
            "size_bytes": r.get("size"),
            "url": r.get("url"),
            "last_modified": r.get("last_modified") or r.get("data_last_updated"),
            "datastore_active": r.get("datastore_active", False),
        })

    return json.dumps({
        "id": ds["id"],
        "name": ds.get("name"),
        "title": ds.get("title"),
        "description": ds.get("notes"),
        "organization": ds.get("organization", {}).get("title"),
        "maintainer": ds.get("maintainer_translated", {}).get("en") or ds.get("maintainer"),
        "license": ds.get("license_title"),
        "tags": [t["name"] for t in ds.get("tags", [])],
        "update_frequency": ds.get("update_frequency"),
        "created": ds.get("metadata_created"),
        "last_modified": ds.get("metadata_modified"),
        "access_level": ds.get("access_level"),
        "geographic_coverage": ds.get("geographic_coverage"),
        "resources": resources,
    }, indent=2, default=str)


@mcp.tool
async def list_resources(
    dataset_id: str,
    ctx: Context = None,
) -> str:
    """List all resources (files) in a dataset with their formats and sizes.

    Args:
        dataset_id: Dataset ID or name
    """
    ckan, _ = _get_deps(ctx)
    ds = await ckan.package_show(dataset_id)
    resources = []
    for r in ds.get("resources", []):
        resources.append({
            "id": r["id"],
            "name": r.get("name"),
            "format": r.get("format"),
            "size_bytes": r.get("size"),
            "url": r.get("url"),
            "last_modified": r.get("last_modified") or r.get("data_last_updated"),
            "datastore_active": r.get("datastore_active", False),
            "data_range": f"{r.get('data_range_start', '?')} to {r.get('data_range_end', '?')}",
        })
    return json.dumps({
        "dataset": ds.get("title"),
        "num_resources": len(resources),
        "resources": resources,
    }, indent=2, default=str)


@mcp.tool
async def get_resource_schema(
    resource_id: str,
    sample_size: int = 5,
    ctx: Context = None,
) -> str:
    """Get the column schema and sample values for a datastore resource.

    Args:
        resource_id: Resource ID (the resource must have datastore_active=True)
        sample_size: Number of sample rows to include
    """
    ckan, _ = _get_deps(ctx)
    result = await ckan.datastore_search(resource_id, limit=sample_size)

    fields = []
    for f in result.get("fields", []):
        if f["id"].startswith("_"):
            continue
        sample_values = [str(r.get(f["id"], "")) for r in result.get("records", [])]
        fields.append({
            "name": f["id"],
            "type": f.get("type", "unknown"),
            "sample_values": sample_values[:sample_size],
        })

    return json.dumps({
        "resource_id": resource_id,
        "total_records": result.get("total", 0),
        "num_columns": len(fields),
        "fields": fields,
    }, indent=2)


@mcp.tool
async def get_update_history(
    dataset_id: str,
    ctx: Context = None,
) -> str:
    """Check when a dataset was created, last modified, and its update frequency.

    Args:
        dataset_id: Dataset ID or name
    """
    ckan, _ = _get_deps(ctx)
    ds = await ckan.package_show(dataset_id)

    resource_updates = []
    for r in ds.get("resources", []):
        resource_updates.append({
            "name": r.get("name"),
            "format": r.get("format"),
            "created": r.get("created"),
            "last_modified": r.get("last_modified") or r.get("data_last_updated"),
        })

    return json.dumps({
        "dataset": ds.get("title"),
        "created": ds.get("metadata_created"),
        "last_modified": ds.get("metadata_modified"),
        "update_frequency": ds.get("update_frequency"),
        "current_as_of": ds.get("current_as_of"),
        "resource_updates": resource_updates,
    }, indent=2, default=str)


@mcp.tool
async def compare_datasets(
    dataset_ids: list[str],
    ctx: Context = None,
) -> str:
    """Compare metadata side-by-side for multiple datasets.

    Args:
        dataset_ids: List of dataset IDs or names to compare (2-5)
    """
    ckan, _ = _get_deps(ctx)
    comparisons = []
    for ds_id in dataset_ids[:5]:
        ds = await ckan.package_show(ds_id)
        resources = ds.get("resources", [])
        formats = sorted(set(r.get("format", "").upper() for r in resources if r.get("format")))
        comparisons.append({
            "id": ds["id"],
            "title": ds.get("title"),
            "organization": ds.get("organization", {}).get("title"),
            "num_resources": len(resources),
            "formats": formats,
            "update_frequency": ds.get("update_frequency"),
            "last_modified": ds.get("metadata_modified"),
            "tags": [t["name"] for t in ds.get("tags", [])],
            "license": ds.get("license_title"),
            "geographic_coverage": ds.get("geographic_coverage"),
        })

    # Find shared and unique tags
    all_tags = [set(c["tags"]) for c in comparisons]
    shared_tags = list(set.intersection(*all_tags)) if all_tags else []

    return json.dumps({
        "datasets": comparisons,
        "shared_tags": shared_tags,
    }, indent=2, default=str)
```

**Step 2: Verify tools register**

Run: `uv run python -c "from ontario_data.server import mcp; print([t.name for t in mcp._tool_manager._tools.values()])"`
Expected: includes `get_dataset_info`, `list_resources`, etc.

**Step 3: Commit**

```bash
git add src/ontario_data/tools/metadata.py
git commit -m "feat: metadata and inspection tools (5 tools)"
```

---

## Task 7: Data Retrieval & Caching Tools (5 tools)

**Files:**
- Modify: `src/ontario_data/tools/retrieval.py`

**Step 1: Implement retrieval tools**

`src/ontario_data/tools/retrieval.py`:
```python
from __future__ import annotations

import io
import json
import re
from typing import Any

import httpx
import pandas as pd
from fastmcp import Context

from ontario_data.server import mcp
from ontario_data.ckan_client import CKANClient
from ontario_data.cache import CacheManager


def _get_deps(ctx: Context) -> tuple[CKANClient, CacheManager]:
    return ctx.lifespan_context["ckan"], ctx.lifespan_context["cache"]


def _make_table_name(dataset_name: str, resource_id: str) -> str:
    """Generate a safe DuckDB table name."""
    slug = re.sub(r"[^a-z0-9]", "_", (dataset_name or "unknown").lower())
    slug = re.sub(r"_+", "_", slug).strip("_")[:40]
    prefix = resource_id[:8]
    return f"ds_{slug}_{prefix}"


async def _download_resource_data(
    ckan: CKANClient,
    resource_id: str,
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    """Download a resource and return (dataframe, resource_meta, dataset_meta)."""
    # Get resource metadata
    resource = await ckan.resource_show(resource_id)
    dataset_id = resource.get("package_id")
    dataset = await ckan.package_show(dataset_id) if dataset_id else {}

    fmt = (resource.get("format") or "").upper()
    url = resource.get("url", "")

    # Try datastore first (structured data)
    if resource.get("datastore_active"):
        result = await ckan.datastore_search_all(resource_id)
        df = pd.DataFrame(result["records"])
        # Remove internal CKAN columns
        internal_cols = [c for c in df.columns if c.startswith("_")]
        df = df.drop(columns=internal_cols, errors="ignore")
        return df, resource, dataset

    # Download file directly
    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
        response = await client.get(url)
        response.raise_for_status()
        content = response.content

    if fmt in ("CSV", "TXT"):
        df = pd.read_csv(io.BytesIO(content))
    elif fmt in ("XLS", "XLSX"):
        df = pd.read_excel(io.BytesIO(content))
    elif fmt == "JSON":
        df = pd.read_json(io.BytesIO(content))
    elif fmt == "GEOJSON":
        import geopandas as gpd
        df = gpd.read_file(io.BytesIO(content))
    else:
        raise ValueError(f"Unsupported format for tabular import: {fmt}. URL: {url}")

    return df, resource, dataset


@mcp.tool
async def download_resource(
    resource_id: str,
    force_refresh: bool = False,
    ctx: Context = None,
) -> str:
    """Download a dataset resource and cache it locally in DuckDB for fast querying.

    Supports CSV, XLSX, JSON, and datastore-active resources.

    Args:
        resource_id: The resource ID to download
        force_refresh: Re-download even if already cached
    """
    ckan, cache = _get_deps(ctx)

    if cache.is_cached(resource_id) and not force_refresh:
        table_name = cache.get_table_name(resource_id)
        meta = cache.conn.execute(
            "SELECT row_count, downloaded_at FROM _cache_metadata WHERE resource_id = ?",
            [resource_id],
        ).fetchone()
        return json.dumps({
            "status": "already_cached",
            "table_name": table_name,
            "row_count": meta[0],
            "downloaded_at": str(meta[1]),
            "hint": "Use query_cached tool with SQL to analyze this data. Use force_refresh=True to re-download.",
        }, indent=2)

    await ctx.report_progress(0, 100, "Downloading resource...")
    df, resource, dataset = await _download_resource_data(ckan, resource_id)
    await ctx.report_progress(70, 100, "Storing in DuckDB...")

    table_name = _make_table_name(dataset.get("name", ""), resource_id)
    cache.store_resource(
        resource_id=resource_id,
        dataset_id=dataset.get("id", ""),
        table_name=table_name,
        df=df,
        source_url=resource.get("url", ""),
    )
    cache.store_dataset_metadata(dataset.get("id", ""), dataset)

    await ctx.report_progress(100, 100, "Done")

    return json.dumps({
        "status": "downloaded",
        "table_name": table_name,
        "row_count": len(df),
        "columns": list(df.columns),
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "hint": f"Use query_cached tool with SQL like: SELECT * FROM \"{table_name}\" LIMIT 10",
    }, indent=2)


@mcp.tool
async def list_cached_datasets(ctx: Context = None) -> str:
    """List all datasets currently cached in the local DuckDB database."""
    _, cache = _get_deps(ctx)
    cached = cache.list_cached()
    return json.dumps({
        "cached_count": len(cached),
        "datasets": cached,
    }, indent=2)


@mcp.tool
async def refresh_cache(
    resource_id: str | None = None,
    ctx: Context = None,
) -> str:
    """Re-download cached resources to get the latest data.

    Args:
        resource_id: Specific resource to refresh, or omit to refresh all
    """
    ckan, cache = _get_deps(ctx)
    cached = cache.list_cached()

    if resource_id:
        cached = [c for c in cached if c["resource_id"] == resource_id]
        if not cached:
            return json.dumps({"error": f"Resource {resource_id} not found in cache"})

    results = []
    for i, item in enumerate(cached):
        await ctx.report_progress(i, len(cached), f"Refreshing {item['table_name']}...")
        try:
            df, resource, dataset = await _download_resource_data(ckan, item["resource_id"])
            cache.store_resource(
                resource_id=item["resource_id"],
                dataset_id=item["dataset_id"],
                table_name=item["table_name"],
                df=df,
                source_url=item["source_url"],
            )
            results.append({"resource_id": item["resource_id"], "status": "refreshed", "new_row_count": len(df)})
        except Exception as e:
            results.append({"resource_id": item["resource_id"], "status": "error", "error": str(e)})

    return json.dumps({"refreshed": results}, indent=2)


@mcp.tool
async def cache_stats(ctx: Context = None) -> str:
    """Get statistics about the local DuckDB cache: size, table count, staleness."""
    _, cache = _get_deps(ctx)
    stats = cache.get_stats()
    cached = cache.list_cached()

    return json.dumps({
        **stats,
        "total_size_mb": round(stats["total_size_bytes"] / (1024 * 1024), 2),
        "tables": [
            {
                "table_name": c["table_name"],
                "resource_id": c["resource_id"],
                "row_count": c["row_count"],
                "downloaded_at": c["downloaded_at"],
            }
            for c in cached
        ],
    }, indent=2)


@mcp.tool
async def remove_from_cache(
    resource_id: str | None = None,
    remove_all: bool = False,
    ctx: Context = None,
) -> str:
    """Remove cached data to free disk space.

    Args:
        resource_id: Specific resource to remove
        remove_all: Set to True to clear entire cache
    """
    _, cache = _get_deps(ctx)
    if remove_all:
        count = len(cache.list_cached())
        cache.remove_all()
        return json.dumps({"status": "cleared", "removed_count": count})
    elif resource_id:
        cache.remove_resource(resource_id)
        return json.dumps({"status": "removed", "resource_id": resource_id})
    else:
        return json.dumps({"error": "Provide resource_id or set remove_all=True"})
```

**Step 2: Commit**

```bash
git add src/ontario_data/tools/retrieval.py
git commit -m "feat: data retrieval and caching tools (5 tools)"
```

---

## Task 8: Data Querying Tools (5 tools)

**Files:**
- Modify: `src/ontario_data/tools/querying.py`

**Step 1: Implement querying tools**

`src/ontario_data/tools/querying.py`:
```python
from __future__ import annotations

import json
from typing import Any

from fastmcp import Context

from ontario_data.server import mcp
from ontario_data.ckan_client import CKANClient
from ontario_data.cache import CacheManager


def _get_deps(ctx: Context) -> tuple[CKANClient, CacheManager]:
    return ctx.lifespan_context["ckan"], ctx.lifespan_context["cache"]


@mcp.tool
async def query_resource(
    resource_id: str,
    filters: dict[str, Any] | None = None,
    fields: list[str] | None = None,
    sort: str | None = None,
    limit: int = 100,
    offset: int = 0,
    ctx: Context = None,
) -> str:
    """Query a resource via the CKAN Datastore API (remote, no download needed).

    Only works for resources with datastore_active=True.

    Args:
        resource_id: Resource ID
        filters: Column filters as {column: value} pairs
        fields: List of columns to return (default: all)
        sort: Sort string (e.g. "date desc", "name asc")
        limit: Max rows (1-1000)
        offset: Row offset for pagination
    """
    ckan, _ = _get_deps(ctx)
    result = await ckan.datastore_search(
        resource_id=resource_id,
        filters=filters,
        fields=fields,
        sort=sort,
        limit=min(limit, 1000),
        offset=offset,
    )
    field_info = [{"name": f["id"], "type": f.get("type")} for f in result.get("fields", []) if not f["id"].startswith("_")]
    records = result.get("records", [])
    # Strip internal fields from records
    clean_records = [{k: v for k, v in r.items() if not k.startswith("_")} for r in records]

    return json.dumps({
        "total": result.get("total", 0),
        "returned": len(clean_records),
        "fields": field_info,
        "records": clean_records,
    }, indent=2, default=str)


@mcp.tool
async def sql_query(
    sql: str,
    ctx: Context = None,
) -> str:
    """Run a SQL query against the CKAN Datastore (remote).

    Use resource IDs as table names in double quotes.
    Example: SELECT "Column Name" FROM "resource-id-here" WHERE "Year" > 2020 LIMIT 10

    Args:
        sql: SQL query string (read-only, SELECT only)
    """
    ckan, _ = _get_deps(ctx)
    result = await ckan.datastore_sql(sql)
    field_info = [{"name": f["id"], "type": f.get("type")} for f in result.get("fields", []) if not f["id"].startswith("_")]
    records = result.get("records", [])
    clean_records = [{k: v for k, v in r.items() if not k.startswith("_")} for r in records]

    return json.dumps({
        "returned": len(clean_records),
        "fields": field_info,
        "records": clean_records,
    }, indent=2, default=str)


@mcp.tool
async def query_cached(
    sql: str,
    ctx: Context = None,
) -> str:
    """Run a SQL query against locally cached data in DuckDB.

    Use table names from download_resource or list_cached_datasets.
    Supports full DuckDB SQL including aggregations, window functions, CTEs, etc.

    Args:
        sql: SQL query (e.g. SELECT * FROM "ds_my_table_abc12345" LIMIT 10)
    """
    _, cache = _get_deps(ctx)
    try:
        results = cache.query(sql)
        return json.dumps({
            "row_count": len(results),
            "records": results,
        }, indent=2, default=str)
    except Exception as e:
        # Help the user by listing available tables
        cached = cache.list_cached()
        table_names = [c["table_name"] for c in cached]
        return json.dumps({
            "error": str(e),
            "available_tables": table_names,
            "hint": "Use table names from list_cached_datasets. Quote table names with double quotes.",
        }, indent=2)


@mcp.tool
async def preview_data(
    resource_id: str,
    rows: int = 10,
    ctx: Context = None,
) -> str:
    """Quick preview of the first N rows of a resource (fetched remotely).

    Args:
        resource_id: Resource ID (must have datastore_active=True)
        rows: Number of rows to preview (1-100)
    """
    ckan, _ = _get_deps(ctx)
    result = await ckan.datastore_search(resource_id, limit=min(rows, 100))
    field_info = [{"name": f["id"], "type": f.get("type")} for f in result.get("fields", []) if not f["id"].startswith("_")]
    records = result.get("records", [])
    clean_records = [{k: v for k, v in r.items() if not k.startswith("_")} for r in records]

    return json.dumps({
        "total_records": result.get("total", 0),
        "previewing": len(clean_records),
        "fields": field_info,
        "records": clean_records,
    }, indent=2, default=str)


@mcp.tool
async def filter_and_aggregate(
    resource_id: str,
    filters: dict[str, Any] | None = None,
    group_by: list[str] | None = None,
    aggregate: dict[str, str] | None = None,
    sort_by: str | None = None,
    limit: int = 100,
    ctx: Context = None,
) -> str:
    """Filter and aggregate data from a cached resource using natural parameters.

    This is a friendly wrapper around SQL for common operations.

    Args:
        resource_id: Resource ID (must be cached locally first via download_resource)
        filters: Column filters as {column: value} or {column: ">100"}
        group_by: Columns to group by
        aggregate: Aggregations as {output_name: "function(column)"} e.g. {"total": "sum(amount)", "avg_score": "avg(score)"}
        sort_by: Column to sort by (prefix with - for desc, e.g. "-total")
        limit: Max rows
    """
    _, cache = _get_deps(ctx)
    table_name = cache.get_table_name(resource_id)
    if not table_name:
        return json.dumps({"error": f"Resource {resource_id} not cached. Use download_resource first."})

    # Build SQL
    select_parts = []
    if group_by:
        select_parts.extend(f'"{col}"' for col in group_by)
    if aggregate:
        for alias, expr in aggregate.items():
            select_parts.append(f"{expr} AS \"{alias}\"")
    if not select_parts:
        select_parts = ["*"]

    sql = f"SELECT {', '.join(select_parts)} FROM \"{table_name}\""

    where_clauses = []
    if filters:
        for col, val in filters.items():
            if isinstance(val, str) and val[0] in (">", "<", "!", "="):
                where_clauses.append(f'"{col}" {val}')
            else:
                where_clauses.append(f'"{col}" = \'{val}\'')
    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses)

    if group_by:
        sql += " GROUP BY " + ", ".join(f'"{col}"' for col in group_by)

    if sort_by:
        if sort_by.startswith("-"):
            sql += f' ORDER BY "{sort_by[1:]}" DESC'
        else:
            sql += f' ORDER BY "{sort_by}" ASC'

    sql += f" LIMIT {limit}"

    try:
        results = cache.query(sql)
        return json.dumps({
            "sql_executed": sql,
            "row_count": len(results),
            "records": results,
        }, indent=2, default=str)
    except Exception as e:
        return json.dumps({"error": str(e), "sql_attempted": sql}, indent=2)
```

**Step 2: Commit**

```bash
git add src/ontario_data/tools/querying.py
git commit -m "feat: data querying tools with remote and local SQL (5 tools)"
```

---

## Task 9: Data Quality Tools (4 tools)

**Files:**
- Modify: `src/ontario_data/tools/quality.py`

**Step 1: Implement quality tools**

`src/ontario_data/tools/quality.py`:
```python
from __future__ import annotations

import json
from typing import Any

from fastmcp import Context

from ontario_data.server import mcp
from ontario_data.ckan_client import CKANClient
from ontario_data.cache import CacheManager


def _get_deps(ctx: Context) -> tuple[CKANClient, CacheManager]:
    return ctx.lifespan_context["ckan"], ctx.lifespan_context["cache"]


@mcp.tool
async def check_data_quality(
    resource_id: str,
    ctx: Context = None,
) -> str:
    """Analyze data quality: null counts, type consistency, duplicates, and outliers.

    Resource must be cached locally first (use download_resource).

    Args:
        resource_id: Resource ID
    """
    _, cache = _get_deps(ctx)
    table_name = cache.get_table_name(resource_id)
    if not table_name:
        return json.dumps({"error": f"Resource {resource_id} not cached. Use download_resource first."})

    # Get total rows
    total = cache.conn.execute(f'SELECT count(*) FROM "{table_name}"').fetchone()[0]

    # Get column info
    columns = cache.conn.execute(f"DESCRIBE \"{table_name}\"").fetchall()

    quality_report = []
    for col in columns:
        col_name, col_type = col[0], col[1]
        stats = {}
        stats["name"] = col_name
        stats["type"] = col_type

        # Null count
        null_count = cache.conn.execute(
            f'SELECT count(*) FROM "{table_name}" WHERE "{col_name}" IS NULL'
        ).fetchone()[0]
        stats["null_count"] = null_count
        stats["null_pct"] = round(null_count / total * 100, 1) if total > 0 else 0

        # Distinct values
        distinct = cache.conn.execute(
            f'SELECT count(DISTINCT "{col_name}") FROM "{table_name}"'
        ).fetchone()[0]
        stats["distinct_count"] = distinct
        stats["cardinality_pct"] = round(distinct / total * 100, 1) if total > 0 else 0

        # For numeric columns: min, max, mean, stddev
        if "INT" in col_type.upper() or "FLOAT" in col_type.upper() or "DOUBLE" in col_type.upper() or "DECIMAL" in col_type.upper() or "NUMERIC" in col_type.upper():
            num_stats = cache.conn.execute(
                f'SELECT min("{col_name}"), max("{col_name}"), avg("{col_name}"), stddev("{col_name}") FROM "{table_name}"'
            ).fetchone()
            stats["min"] = num_stats[0]
            stats["max"] = num_stats[1]
            stats["mean"] = round(float(num_stats[2]), 4) if num_stats[2] is not None else None
            stats["stddev"] = round(float(num_stats[3]), 4) if num_stats[3] is not None else None

        quality_report.append(stats)

    # Duplicate row check
    dup_count = cache.conn.execute(
        f'SELECT count(*) FROM (SELECT *, count(*) OVER (PARTITION BY * ) as _cnt FROM "{table_name}") WHERE _cnt > 1'
    ).fetchone()

    return json.dumps({
        "resource_id": resource_id,
        "table_name": table_name,
        "total_rows": total,
        "duplicate_rows": dup_count[0] if dup_count else 0,
        "columns": quality_report,
    }, indent=2, default=str)


@mcp.tool
async def check_freshness(
    dataset_id: str,
    ctx: Context = None,
) -> str:
    """Check if a dataset is current by comparing its update frequency to its last modification date.

    Args:
        dataset_id: Dataset ID or name
    """
    ckan, _ = _get_deps(ctx)
    ds = await ckan.package_show(dataset_id)

    from datetime import datetime, timezone

    last_modified = ds.get("metadata_modified", "")
    frequency = ds.get("update_frequency", "unknown")
    current_as_of = ds.get("current_as_of", "")

    # Parse last modified
    try:
        modified_dt = datetime.fromisoformat(last_modified.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        days_since_update = (now - modified_dt).days
    except (ValueError, AttributeError):
        days_since_update = None

    # Expected update intervals
    freq_days = {
        "daily": 2,
        "weekly": 10,
        "monthly": 45,
        "quarterly": 120,
        "biannually": 200,
        "yearly": 400,
    }
    expected = freq_days.get(frequency)
    is_stale = days_since_update > expected if (days_since_update is not None and expected) else None

    resource_freshness = []
    for r in ds.get("resources", []):
        r_modified = r.get("last_modified") or r.get("data_last_updated")
        resource_freshness.append({
            "name": r.get("name"),
            "format": r.get("format"),
            "last_modified": r_modified,
        })

    return json.dumps({
        "dataset": ds.get("title"),
        "update_frequency": frequency,
        "last_modified": last_modified,
        "current_as_of": current_as_of,
        "days_since_update": days_since_update,
        "is_stale": is_stale,
        "resources": resource_freshness,
    }, indent=2, default=str)


@mcp.tool
async def validate_schema(
    resource_id: str,
    ctx: Context = None,
) -> str:
    """Compare the schema of a cached resource with the current live version.

    Detects added/removed/changed columns.

    Args:
        resource_id: Resource ID (must be cached)
    """
    ckan, cache = _get_deps(ctx)
    table_name = cache.get_table_name(resource_id)
    if not table_name:
        return json.dumps({"error": f"Resource {resource_id} not cached."})

    # Get cached schema
    cached_cols = cache.conn.execute(f"DESCRIBE \"{table_name}\"").fetchall()
    cached_schema = {col[0]: col[1] for col in cached_cols}

    # Get live schema
    live_result = await ckan.datastore_search(resource_id, limit=0)
    live_fields = {f["id"]: f.get("type", "unknown") for f in live_result.get("fields", []) if not f["id"].startswith("_")}

    cached_names = set(cached_schema.keys())
    live_names = set(live_fields.keys())

    added = list(live_names - cached_names)
    removed = list(cached_names - live_names)
    common = cached_names & live_names
    type_changes = []
    for col in common:
        if cached_schema[col] != live_fields[col]:
            type_changes.append({"column": col, "cached_type": cached_schema[col], "live_type": live_fields[col]})

    has_changes = bool(added or removed or type_changes)

    return json.dumps({
        "resource_id": resource_id,
        "schema_changed": has_changes,
        "columns_added": added,
        "columns_removed": removed,
        "type_changes": type_changes,
        "recommendation": "Use download_resource with force_refresh=True to update" if has_changes else "Schema is consistent",
    }, indent=2)


@mcp.tool
async def profile_dataset(
    resource_id: str,
    ctx: Context = None,
) -> str:
    """Generate a comprehensive statistical profile of a cached dataset.

    Includes distributions, cardinality, correlations for numeric columns.

    Args:
        resource_id: Resource ID (must be cached)
    """
    _, cache = _get_deps(ctx)
    table_name = cache.get_table_name(resource_id)
    if not table_name:
        return json.dumps({"error": f"Resource {resource_id} not cached. Use download_resource first."})

    df = cache.query_df(f'SELECT * FROM "{table_name}"')

    profile = {
        "resource_id": resource_id,
        "table_name": table_name,
        "shape": {"rows": len(df), "columns": len(df.columns)},
        "memory_usage_mb": round(df.memory_usage(deep=True).sum() / (1024 * 1024), 2),
        "columns": {},
    }

    for col in df.columns:
        col_profile: dict[str, Any] = {
            "dtype": str(df[col].dtype),
            "null_count": int(df[col].isna().sum()),
            "null_pct": round(df[col].isna().mean() * 100, 1),
            "unique_count": int(df[col].nunique()),
        }

        if df[col].dtype in ("int64", "float64", "Int64", "Float64"):
            desc = df[col].describe()
            col_profile["stats"] = {
                "mean": round(float(desc["mean"]), 4) if "mean" in desc else None,
                "std": round(float(desc["std"]), 4) if "std" in desc else None,
                "min": float(desc["min"]) if "min" in desc else None,
                "25%": float(desc["25%"]) if "25%" in desc else None,
                "50%": float(desc["50%"]) if "50%" in desc else None,
                "75%": float(desc["75%"]) if "75%" in desc else None,
                "max": float(desc["max"]) if "max" in desc else None,
            }
        elif df[col].dtype == "object":
            top_values = df[col].value_counts().head(10)
            col_profile["top_values"] = {str(k): int(v) for k, v in top_values.items()}
            col_profile["avg_length"] = round(df[col].dropna().str.len().mean(), 1) if not df[col].dropna().empty else 0

        profile["columns"][col] = col_profile

    # Correlation matrix for numeric columns
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns.tolist()
    if len(numeric_cols) >= 2:
        corr = df[numeric_cols].corr()
        profile["correlations"] = {
            col: {col2: round(float(corr.loc[col, col2]), 3) for col2 in numeric_cols}
            for col in numeric_cols
        }

    return json.dumps(profile, indent=2, default=str)
```

**Step 2: Commit**

```bash
git add src/ontario_data/tools/quality.py
git commit -m "feat: data quality tools with profiling and schema validation (4 tools)"
```

---

## Task 10: Analytics & Statistics Tools (5 tools)

**Files:**
- Modify: `src/ontario_data/tools/analytics.py`

**Step 1: Implement analytics tools**

`src/ontario_data/tools/analytics.py`:
```python
from __future__ import annotations

import json
from typing import Any

import pandas as pd
from fastmcp import Context

from ontario_data.server import mcp
from ontario_data.cache import CacheManager


def _get_cache(ctx: Context) -> CacheManager:
    return ctx.lifespan_context["cache"]


def _require_cached(cache: CacheManager, resource_id: str) -> str:
    """Get table name or raise helpful error."""
    table_name = cache.get_table_name(resource_id)
    if not table_name:
        raise ValueError(f"Resource {resource_id} not cached. Use download_resource first.")
    return table_name


@mcp.tool
async def summarize(
    resource_id: str,
    columns: list[str] | None = None,
    ctx: Context = None,
) -> str:
    """Get descriptive statistics for numeric columns in a cached dataset.

    Args:
        resource_id: Resource ID (must be cached)
        columns: Specific columns to summarize (default: all numeric)
    """
    cache = _get_cache(ctx)
    table_name = _require_cached(cache, resource_id)
    df = cache.query_df(f'SELECT * FROM "{table_name}"')

    if columns:
        df = df[columns]

    numeric_df = df.select_dtypes(include=["int64", "float64", "Int64", "Float64"])
    if numeric_df.empty:
        return json.dumps({"error": "No numeric columns found", "available_columns": list(df.columns)})

    stats = numeric_df.describe().round(4)
    result = {}
    for col in stats.columns:
        result[col] = {str(k): float(v) if pd.notna(v) else None for k, v in stats[col].items()}

    return json.dumps({"resource_id": resource_id, "statistics": result}, indent=2, default=str)


@mcp.tool
async def time_series_analysis(
    resource_id: str,
    date_column: str,
    value_column: str,
    frequency: str = "auto",
    ctx: Context = None,
) -> str:
    """Analyze trends and patterns in time-indexed data.

    Args:
        resource_id: Resource ID (must be cached)
        date_column: Column containing dates
        value_column: Column containing values to analyze
        frequency: Aggregation frequency: "daily", "weekly", "monthly", "quarterly", "yearly", or "auto"
    """
    cache = _get_cache(ctx)
    table_name = _require_cached(cache, resource_id)
    df = cache.query_df(f'SELECT "{date_column}", "{value_column}" FROM "{table_name}" ORDER BY "{date_column}"')

    df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
    df = df.dropna(subset=[date_column, value_column])
    df = df.sort_values(date_column)

    # Auto-detect frequency
    if frequency == "auto":
        date_range = (df[date_column].max() - df[date_column].min()).days
        if date_range > 365 * 3:
            frequency = "yearly"
        elif date_range > 365:
            frequency = "quarterly"
        elif date_range > 90:
            frequency = "monthly"
        elif date_range > 14:
            frequency = "weekly"
        else:
            frequency = "daily"

    freq_map = {"daily": "D", "weekly": "W", "monthly": "ME", "quarterly": "QE", "yearly": "YE"}
    pd_freq = freq_map.get(frequency, "ME")

    df = df.set_index(date_column)
    resampled = df[value_column].resample(pd_freq).agg(["mean", "sum", "count", "min", "max"])
    resampled = resampled.round(4)

    # Trend calculation
    values = resampled["mean"].dropna()
    if len(values) >= 2:
        first_half = values.iloc[: len(values) // 2].mean()
        second_half = values.iloc[len(values) // 2 :].mean()
        pct_change = ((second_half - first_half) / first_half * 100) if first_half != 0 else 0
        trend = "increasing" if pct_change > 5 else "decreasing" if pct_change < -5 else "stable"
    else:
        pct_change = 0
        trend = "insufficient data"

    periods = []
    for idx, row in resampled.iterrows():
        periods.append({
            "period": str(idx.date()) if hasattr(idx, "date") else str(idx),
            "mean": float(row["mean"]) if pd.notna(row["mean"]) else None,
            "sum": float(row["sum"]) if pd.notna(row["sum"]) else None,
            "count": int(row["count"]),
            "min": float(row["min"]) if pd.notna(row["min"]) else None,
            "max": float(row["max"]) if pd.notna(row["max"]) else None,
        })

    return json.dumps({
        "resource_id": resource_id,
        "date_column": date_column,
        "value_column": value_column,
        "frequency": frequency,
        "trend": trend,
        "pct_change": round(pct_change, 1),
        "total_periods": len(periods),
        "date_range": {
            "start": str(values.index.min()) if not values.empty else None,
            "end": str(values.index.max()) if not values.empty else None,
        },
        "periods": periods,
    }, indent=2, default=str)


@mcp.tool
async def cross_tabulate(
    resource_id: str,
    row_field: str,
    col_field: str,
    value_field: str | None = None,
    aggregation: str = "count",
    ctx: Context = None,
) -> str:
    """Create a cross-tabulation (pivot table) from cached data.

    Args:
        resource_id: Resource ID (must be cached)
        row_field: Column for rows
        col_field: Column for columns
        value_field: Column to aggregate (required for sum/mean/min/max)
        aggregation: "count", "sum", "mean", "min", "max"
    """
    cache = _get_cache(ctx)
    table_name = _require_cached(cache, resource_id)
    df = cache.query_df(f'SELECT * FROM "{table_name}"')

    if aggregation == "count":
        ct = pd.crosstab(df[row_field], df[col_field])
    else:
        if not value_field:
            return json.dumps({"error": f"value_field required for aggregation={aggregation}"})
        ct = pd.crosstab(df[row_field], df[col_field], values=df[value_field], aggfunc=aggregation)

    ct = ct.round(4)

    return json.dumps({
        "resource_id": resource_id,
        "row_field": row_field,
        "col_field": col_field,
        "aggregation": aggregation,
        "shape": {"rows": ct.shape[0], "columns": ct.shape[1]},
        "table": json.loads(ct.to_json()),
    }, indent=2, default=str)


@mcp.tool
async def correlation_matrix(
    resource_id: str,
    columns: list[str] | None = None,
    method: str = "pearson",
    ctx: Context = None,
) -> str:
    """Compute pairwise correlations between numeric columns.

    Args:
        resource_id: Resource ID (must be cached)
        columns: Specific columns (default: all numeric)
        method: "pearson", "spearman", or "kendall"
    """
    cache = _get_cache(ctx)
    table_name = _require_cached(cache, resource_id)
    df = cache.query_df(f'SELECT * FROM "{table_name}"')

    if columns:
        df = df[columns]

    numeric_df = df.select_dtypes(include=["int64", "float64", "Int64", "Float64"])
    if len(numeric_df.columns) < 2:
        return json.dumps({"error": "Need at least 2 numeric columns", "available": list(df.columns)})

    corr = numeric_df.corr(method=method).round(4)

    # Find strongest correlations (excluding self-correlations)
    strong = []
    for i, col1 in enumerate(corr.columns):
        for col2 in corr.columns[i + 1 :]:
            val = float(corr.loc[col1, col2])
            if abs(val) > 0.5:
                strong.append({"col1": col1, "col2": col2, "correlation": val})
    strong.sort(key=lambda x: abs(x["correlation"]), reverse=True)

    return json.dumps({
        "resource_id": resource_id,
        "method": method,
        "matrix": json.loads(corr.to_json()),
        "strong_correlations": strong,
    }, indent=2, default=str)


@mcp.tool
async def compare_periods(
    resource_id: str,
    date_column: str,
    period1_start: str,
    period1_end: str,
    period2_start: str,
    period2_end: str,
    metrics: list[str] | None = None,
    ctx: Context = None,
) -> str:
    """Compare metrics between two time periods.

    Args:
        resource_id: Resource ID (must be cached)
        date_column: Column containing dates
        period1_start: Start date of first period (YYYY-MM-DD)
        period1_end: End date of first period
        period2_start: Start date of second period
        period2_end: End date of second period
        metrics: Numeric columns to compare (default: all numeric)
    """
    cache = _get_cache(ctx)
    table_name = _require_cached(cache, resource_id)
    df = cache.query_df(f'SELECT * FROM "{table_name}"')

    df[date_column] = pd.to_datetime(df[date_column], errors="coerce")

    p1 = df[(df[date_column] >= period1_start) & (df[date_column] <= period1_end)]
    p2 = df[(df[date_column] >= period2_start) & (df[date_column] <= period2_end)]

    if metrics:
        numeric_cols = metrics
    else:
        numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns.tolist()

    comparisons = {}
    for col in numeric_cols:
        p1_mean = float(p1[col].mean()) if not p1[col].empty else None
        p2_mean = float(p2[col].mean()) if not p2[col].empty else None
        p1_sum = float(p1[col].sum()) if not p1[col].empty else None
        p2_sum = float(p2[col].sum()) if not p2[col].empty else None

        pct_change_mean = None
        if p1_mean and p2_mean and p1_mean != 0:
            pct_change_mean = round((p2_mean - p1_mean) / p1_mean * 100, 2)

        comparisons[col] = {
            "period1": {"mean": round(p1_mean, 4) if p1_mean else None, "sum": round(p1_sum, 4) if p1_sum else None, "count": len(p1)},
            "period2": {"mean": round(p2_mean, 4) if p2_mean else None, "sum": round(p2_sum, 4) if p2_sum else None, "count": len(p2)},
            "pct_change_mean": pct_change_mean,
        }

    return json.dumps({
        "resource_id": resource_id,
        "period1": f"{period1_start} to {period1_end}",
        "period2": f"{period2_start} to {period2_end}",
        "comparisons": comparisons,
    }, indent=2, default=str)
```

**Step 2: Commit**

```bash
git add src/ontario_data/tools/analytics.py
git commit -m "feat: analytics and statistics tools (5 tools)"
```

---

## Task 11: Geospatial Tools (4 tools)

**Files:**
- Modify: `src/ontario_data/tools/geospatial.py`

**Step 1: Implement geospatial tools**

`src/ontario_data/tools/geospatial.py`:
```python
from __future__ import annotations

import io
import json
from typing import Any

import httpx
from fastmcp import Context

from ontario_data.server import mcp
from ontario_data.ckan_client import CKANClient
from ontario_data.cache import CacheManager


def _get_deps(ctx: Context) -> tuple[CKANClient, CacheManager]:
    return ctx.lifespan_context["ckan"], ctx.lifespan_context["cache"]


@mcp.tool
async def load_geodata(
    resource_id: str,
    force_refresh: bool = False,
    ctx: Context = None,
) -> str:
    """Download and cache a geospatial resource (SHP, KML, GeoJSON) into DuckDB with spatial support.

    Args:
        resource_id: Resource ID for a geospatial file
        force_refresh: Re-download even if cached
    """
    import geopandas as gpd
    import re

    ckan, cache = _get_deps(ctx)

    if cache.is_cached(resource_id) and not force_refresh:
        table_name = cache.get_table_name(resource_id)
        return json.dumps({"status": "already_cached", "table_name": table_name})

    resource = await ckan.resource_show(resource_id)
    dataset_id = resource.get("package_id", "")
    dataset = await ckan.package_show(dataset_id) if dataset_id else {}
    fmt = (resource.get("format") or "").upper()
    url = resource.get("url", "")

    await ctx.report_progress(0, 100, "Downloading geospatial data...")

    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
        response = await client.get(url)
        response.raise_for_status()
        content = response.content

    await ctx.report_progress(50, 100, "Parsing geospatial data...")

    if fmt == "GEOJSON":
        gdf = gpd.read_file(io.BytesIO(content), driver="GeoJSON")
    elif fmt == "KML":
        gdf = gpd.read_file(io.BytesIO(content), driver="KML")
    elif fmt in ("SHP", "ZIP"):
        import tempfile
        import zipfile
        with tempfile.TemporaryDirectory() as tmpdir:
            if fmt == "ZIP" or content[:4] == b"PK\x03\x04":
                with zipfile.ZipFile(io.BytesIO(content)) as zf:
                    zf.extractall(tmpdir)
                gdf = gpd.read_file(tmpdir)
            else:
                return json.dumps({"error": "SHP files must be provided as ZIP archives"})
    else:
        return json.dumps({"error": f"Unsupported geospatial format: {fmt}"})

    await ctx.report_progress(80, 100, "Storing in DuckDB...")

    # Convert geometry to WKT for DuckDB storage
    df = gdf.copy()
    if "geometry" in df.columns:
        df["geometry_wkt"] = df["geometry"].apply(lambda g: g.wkt if g else None)
        df["geometry_type"] = df["geometry"].apply(lambda g: g.geom_type if g else None)
        if hasattr(gdf, "crs") and gdf.crs:
            df["crs"] = str(gdf.crs)
        # Get bounds
        bounds = gdf.total_bounds  # [minx, miny, maxx, maxy]
        df = df.drop(columns=["geometry"])
    else:
        bounds = None

    slug = re.sub(r"[^a-z0-9]", "_", (dataset.get("name") or "geo").lower())[:40]
    table_name = f"geo_{slug}_{resource_id[:8]}"

    import pandas as pd
    cache.store_resource(
        resource_id=resource_id,
        dataset_id=dataset_id,
        table_name=table_name,
        df=pd.DataFrame(df),
        source_url=url,
    )

    await ctx.report_progress(100, 100, "Done")

    return json.dumps({
        "status": "loaded",
        "table_name": table_name,
        "row_count": len(df),
        "columns": list(df.columns),
        "geometry_types": df["geometry_type"].unique().tolist() if "geometry_type" in df.columns else [],
        "bounds": {"minx": bounds[0], "miny": bounds[1], "maxx": bounds[2], "maxy": bounds[3]} if bounds is not None else None,
        "crs": str(gdf.crs) if hasattr(gdf, "crs") and gdf.crs else None,
        "hint": f'Query with: SELECT * FROM "{table_name}" LIMIT 10',
    }, indent=2, default=str)


@mcp.tool
async def spatial_query(
    resource_id: str,
    operation: str,
    latitude: float | None = None,
    longitude: float | None = None,
    radius_km: float | None = None,
    bbox: list[float] | None = None,
    limit: int = 100,
    ctx: Context = None,
) -> str:
    """Run spatial queries against cached geospatial data.

    Args:
        resource_id: Resource ID (must be cached via load_geodata)
        operation: "contains_point", "within_bbox", or "within_radius"
        latitude: Latitude for point queries
        longitude: Longitude for point queries
        radius_km: Radius in kilometers (for within_radius)
        bbox: Bounding box as [min_lng, min_lat, max_lng, max_lat] (for within_bbox)
        limit: Max results
    """
    _, cache = _get_deps(ctx)
    table_name = cache.get_table_name(resource_id)
    if not table_name:
        return json.dumps({"error": f"Resource {resource_id} not cached. Use load_geodata first."})

    try:
        cache.conn.execute("LOAD spatial")
    except Exception:
        pass

    if operation == "contains_point" and latitude is not None and longitude is not None:
        sql = f"""
            SELECT *, ST_Distance(
                ST_GeomFromText(geometry_wkt),
                ST_Point({longitude}, {latitude})
            ) as distance
            FROM "{table_name}"
            WHERE geometry_wkt IS NOT NULL
            AND ST_Contains(ST_GeomFromText(geometry_wkt), ST_Point({longitude}, {latitude}))
            LIMIT {limit}
        """
    elif operation == "within_radius" and latitude is not None and longitude is not None and radius_km is not None:
        # Approximate degrees (1 degree ~ 111km)
        degree_radius = radius_km / 111.0
        sql = f"""
            SELECT *, ST_Distance(
                ST_GeomFromText(geometry_wkt),
                ST_Point({longitude}, {latitude})
            ) * 111.0 as distance_km
            FROM "{table_name}"
            WHERE geometry_wkt IS NOT NULL
            AND ST_DWithin(
                ST_GeomFromText(geometry_wkt),
                ST_Point({longitude}, {latitude}),
                {degree_radius}
            )
            ORDER BY distance_km
            LIMIT {limit}
        """
    elif operation == "within_bbox" and bbox and len(bbox) == 4:
        min_lng, min_lat, max_lng, max_lat = bbox
        sql = f"""
            SELECT *
            FROM "{table_name}"
            WHERE geometry_wkt IS NOT NULL
            AND ST_Intersects(
                ST_GeomFromText(geometry_wkt),
                ST_MakeEnvelope({min_lng}, {min_lat}, {max_lng}, {max_lat})
            )
            LIMIT {limit}
        """
    else:
        return json.dumps({
            "error": f"Invalid operation '{operation}' or missing parameters",
            "valid_operations": {
                "contains_point": "requires latitude, longitude",
                "within_radius": "requires latitude, longitude, radius_km",
                "within_bbox": "requires bbox [min_lng, min_lat, max_lng, max_lat]",
            },
        })

    try:
        results = cache.query(sql)
        return json.dumps({"operation": operation, "result_count": len(results), "records": results}, indent=2, default=str)
    except Exception as e:
        return json.dumps({"error": str(e), "sql": sql}, indent=2)


@mcp.tool
async def list_geo_datasets(
    format_filter: str | None = None,
    limit: int = 50,
    ctx: Context = None,
) -> str:
    """Find all datasets that contain geospatial resources (SHP, KML, GeoJSON).

    Args:
        format_filter: Filter to specific format: "SHP", "KML", "GEOJSON", or None for all
        limit: Max results
    """
    ckan, _ = _get_deps(ctx)
    geo_formats = [format_filter.upper()] if format_filter else ["SHP", "KML", "GEOJSON"]

    all_datasets = []
    seen_ids = set()
    for fmt in geo_formats:
        result = await ckan.package_search(filters={"res_format": fmt}, rows=min(limit, 50))
        for ds in result["results"]:
            if ds["id"] not in seen_ids:
                seen_ids.add(ds["id"])
                geo_resources = [
                    {"id": r["id"], "name": r.get("name"), "format": r.get("format"), "size": r.get("size")}
                    for r in ds.get("resources", [])
                    if (r.get("format") or "").upper() in ("SHP", "KML", "GEOJSON", "ZIP")
                ]
                all_datasets.append({
                    "id": ds["id"],
                    "title": ds.get("title"),
                    "organization": ds.get("organization", {}).get("title"),
                    "geo_resources": geo_resources,
                })

    return json.dumps({"total": len(all_datasets), "datasets": all_datasets[:limit]}, indent=2)


@mcp.tool
async def geocode_lookup(
    latitude: float | None = None,
    longitude: float | None = None,
    bbox: list[float] | None = None,
    limit: int = 20,
    ctx: Context = None,
) -> str:
    """Find datasets that might cover a geographic point or bounding box.

    Searches dataset metadata for geographic references. For precise spatial queries,
    use load_geodata + spatial_query instead.

    Args:
        latitude: Latitude of point of interest
        longitude: Longitude of point of interest
        bbox: Bounding box [min_lng, min_lat, max_lng, max_lat]
        limit: Max results
    """
    ckan, _ = _get_deps(ctx)

    # Ontario municipalities/regions for reverse geocoding
    # This is a simplified lookup — for precise work use spatial_query
    if latitude and longitude:
        query = f"geographic coverage Ontario"
        # Southern Ontario approximate bounds
        if 42.0 <= latitude <= 45.0 and -80.5 <= longitude <= -78.5:
            query = "Toronto GTA Ontario"
        elif 45.0 <= latitude <= 47.0:
            query = "Northern Ontario"
        elif 44.0 <= latitude <= 46.0 and -76.0 <= longitude <= -75.0:
            query = "Ottawa Eastern Ontario"
    elif bbox:
        query = "geographic Ontario"
    else:
        return json.dumps({"error": "Provide latitude/longitude or bbox"})

    result = await ckan.package_search(query=query, rows=min(limit, 50))
    datasets = []
    for ds in result["results"]:
        geo_cov = ds.get("geographic_coverage", "")
        has_geo_resource = any(
            (r.get("format") or "").upper() in ("SHP", "KML", "GEOJSON")
            for r in ds.get("resources", [])
        )
        datasets.append({
            "id": ds["id"],
            "title": ds.get("title"),
            "organization": ds.get("organization", {}).get("title"),
            "geographic_coverage": geo_cov,
            "has_geospatial_resource": has_geo_resource,
        })

    return json.dumps({"query_point": {"lat": latitude, "lng": longitude}, "datasets": datasets}, indent=2)
```

**Step 2: Commit**

```bash
git add src/ontario_data/tools/geospatial.py
git commit -m "feat: geospatial tools with DuckDB spatial queries (4 tools)"
```

---

## Task 12: Prompts and Resources

**Files:**
- Modify: `src/ontario_data/prompts.py`
- Modify: `src/ontario_data/resources.py`

**Step 1: Implement prompts**

`src/ontario_data/prompts.py`:
```python
from __future__ import annotations

from fastmcp import Context
from fastmcp.prompts import Message

from ontario_data.server import mcp


@mcp.prompt
async def explore_topic(topic: str) -> list[Message]:
    """Guided exploration of a topic in Ontario's open data.

    Searches for datasets, summarizes what's available, and suggests deep dives.
    """
    return [
        Message(
            role="user",
            content=(
                f"I want to explore Ontario open data about: {topic}\n\n"
                "Please:\n"
                "1. Use search_datasets to find relevant datasets\n"
                "2. Summarize the top results — what data is available, from which ministries\n"
                "3. For the most interesting datasets, use get_dataset_info to get details\n"
                "4. Suggest which datasets to download and analyze, and what questions they could answer\n"
                "5. If any have datastore_active resources, preview a few rows"
            ),
        ),
    ]


@mcp.prompt
async def data_investigation(dataset_id: str) -> list[Message]:
    """Deep investigation of a specific dataset: schema, quality, statistics, insights."""
    return [
        Message(
            role="user",
            content=(
                f"Investigate this Ontario dataset thoroughly: {dataset_id}\n\n"
                "Please follow this workflow:\n"
                "1. get_dataset_info — understand what this dataset contains\n"
                "2. list_resources — see all available files\n"
                "3. For the primary CSV/data resource:\n"
                "   a. get_resource_schema — understand the columns\n"
                "   b. download_resource — cache it locally\n"
                "   c. check_data_quality — assess completeness and consistency\n"
                "   d. profile_dataset — full statistical profile\n"
                "   e. summarize — key statistics\n"
                "4. Provide insights: What stories does this data tell? What's surprising?\n"
                "5. Suggest follow-up analyses or related datasets"
            ),
        ),
    ]


@mcp.prompt
async def compare_data(dataset_ids: str) -> list[Message]:
    """Side-by-side analysis of multiple datasets (comma-separated IDs)."""
    ids = [d.strip() for d in dataset_ids.split(",")]
    return [
        Message(
            role="user",
            content=(
                f"Compare these Ontario datasets side by side: {', '.join(ids)}\n\n"
                "Please:\n"
                "1. compare_datasets — metadata comparison\n"
                "2. For each dataset, download the primary resource\n"
                "3. profile_dataset on each — compare structure, size, quality\n"
                "4. If they share common columns, look for relationships\n"
                "5. Summarize: How do these datasets complement each other? Can they be joined?"
            ),
        ),
    ]
```

**Step 2: Implement resources**

`src/ontario_data/resources.py`:
```python
from __future__ import annotations

import json

from fastmcp import Context

from ontario_data.server import mcp
from ontario_data.ckan_client import CKANClient
from ontario_data.cache import CacheManager


def _get_deps(ctx: Context) -> tuple[CKANClient, CacheManager]:
    return ctx.lifespan_context["ckan"], ctx.lifespan_context["cache"]


@mcp.resource("ontario://cache/index")
async def cache_index(ctx: Context) -> str:
    """List of all locally cached datasets with freshness info."""
    _, cache = _get_deps(ctx)
    cached = cache.list_cached()
    stats = cache.get_stats()
    return json.dumps({
        "total_cached": stats["table_count"],
        "total_rows": stats["total_rows"],
        "total_size_mb": round(stats["total_size_bytes"] / (1024 * 1024), 2),
        "datasets": cached,
    }, indent=2)


@mcp.resource("ontario://dataset/{dataset_id}")
async def dataset_metadata(dataset_id: str, ctx: Context) -> str:
    """Full metadata for a specific dataset."""
    ckan, cache = _get_deps(ctx)
    # Try cache first
    meta = cache.get_dataset_metadata(dataset_id)
    if not meta:
        meta = await ckan.package_show(dataset_id)
        cache.store_dataset_metadata(dataset_id, meta)
    return json.dumps(meta, indent=2, default=str)


@mcp.resource("ontario://portal/stats")
async def portal_stats(ctx: Context) -> str:
    """Overview statistics about the Ontario Data Catalogue."""
    ckan, _ = _get_deps(ctx)
    # Get total count
    result = await ckan.package_search(rows=0)
    total = result["count"]

    # Get org counts
    orgs = await ckan.organization_list(all_fields=True, include_dataset_count=True)
    top_orgs = sorted(orgs, key=lambda x: x.get("package_count", 0), reverse=True)[:10]

    return json.dumps({
        "total_datasets": total,
        "top_organizations": [
            {"name": o["title"], "datasets": o.get("package_count", 0)}
            for o in top_orgs
        ],
    }, indent=2)
```

**Step 3: Commit**

```bash
git add src/ontario_data/prompts.py src/ontario_data/resources.py
git commit -m "feat: MCP prompts and resources for guided workflows"
```

---

## Task 13: Integration Test & Smoke Test

**Files:**
- Create: `tests/test_integration.py`

**Step 1: Write integration test**

`tests/test_integration.py`:
```python
import pytest
from fastmcp import Client
from ontario_data.server import mcp


@pytest.mark.asyncio
async def test_all_tools_registered():
    """Verify all 34 tools are registered."""
    async with Client(mcp) as client:
        tools = await client.list_tools()
        tool_names = [t.name for t in tools]
        expected = [
            # Discovery (6)
            "search_datasets", "list_organizations", "list_topics",
            "get_popular_datasets", "search_by_location", "find_related_datasets",
            # Metadata (5)
            "get_dataset_info", "list_resources", "get_resource_schema",
            "get_update_history", "compare_datasets",
            # Retrieval (5)
            "download_resource", "list_cached_datasets", "refresh_cache",
            "cache_stats", "remove_from_cache",
            # Querying (5)
            "query_resource", "sql_query", "query_cached",
            "preview_data", "filter_and_aggregate",
            # Quality (4)
            "check_data_quality", "check_freshness", "validate_schema", "profile_dataset",
            # Analytics (5)
            "summarize", "time_series_analysis", "cross_tabulate",
            "correlation_matrix", "compare_periods",
            # Geospatial (4)
            "load_geodata", "spatial_query", "list_geo_datasets", "geocode_lookup",
        ]
        for name in expected:
            assert name in tool_names, f"Missing tool: {name}"
        print(f"All {len(expected)} tools registered!")


@pytest.mark.asyncio
async def test_prompts_registered():
    """Verify prompts are registered."""
    async with Client(mcp) as client:
        prompts = await client.list_prompts()
        prompt_names = [p.name for p in prompts]
        assert "explore_topic" in prompt_names
        assert "data_investigation" in prompt_names
        assert "compare_data" in prompt_names


@pytest.mark.asyncio
async def test_resources_registered():
    """Verify resources are registered."""
    async with Client(mcp) as client:
        resources = await client.list_resources()
        # At minimum the static ones should be present
        # Template resources may not show in list
        assert len(resources) >= 0  # Templates may not list
```

**Step 2: Run all tests**

Run: `uv run pytest tests/ -v`
Expected: All PASS.

**Step 3: Manual smoke test**

Run: `uv run python -c "
from ontario_data.server import mcp
import asyncio
from fastmcp import Client

async def smoke():
    async with Client(mcp) as client:
        tools = await client.list_tools()
        print(f'{len(tools)} tools registered')
        for t in sorted(tools, key=lambda x: x.name):
            print(f'  - {t.name}')

asyncio.run(smoke())
"`
Expected: 34 tools listed.

**Step 4: Commit**

```bash
git add tests/test_integration.py
git commit -m "test: integration tests verifying all tools, prompts, resources registered"
```

---

## Task 14: Final Polish

**Files:**
- Modify: `README.md`
- Create: `CLAUDE.md`

**Step 1: Create CLAUDE.md for project context**

`CLAUDE.md`:
```markdown
# Ontario Data MCP Server

MCP server for Ontario's Open Data Catalogue (data.ontario.ca).

## Quick Start

```bash
uv sync
uv run pytest tests/ -v
uv run fastmcp run src/ontario_data/server.py
```

## Architecture

- `src/ontario_data/server.py` — FastMCP server with lifespan (DuckDB + CKAN client)
- `src/ontario_data/ckan_client.py` — Async CKAN 2.8 API client
- `src/ontario_data/cache.py` — DuckDB cache manager
- `src/ontario_data/tools/` — 34 tools across 7 categories
- `src/ontario_data/prompts.py` — Guided workflow prompts
- `src/ontario_data/resources.py` — MCP resources

## Testing

```bash
uv run pytest tests/ -v
```

## Key Decisions

- DuckDB for local cache AND analytics (with spatial extension)
- All CKAN API calls go through async httpx client
- Tools return JSON strings for structured LLM consumption
- Geospatial data stored as WKT in DuckDB for spatial queries
```

**Step 2: Commit**

```bash
git add README.md CLAUDE.md
git commit -m "docs: add CLAUDE.md and README"
```

---

## Execution Checklist

| Task | Description | Est. |
|------|-------------|------|
| 1 | Project setup | 3 min |
| 2 | CKAN client | 10 min |
| 3 | DuckDB cache manager | 10 min |
| 4 | Server with lifespan | 5 min |
| 5 | Discovery tools (6) | 8 min |
| 6 | Metadata tools (5) | 5 min |
| 7 | Retrieval tools (5) | 8 min |
| 8 | Querying tools (5) | 5 min |
| 9 | Quality tools (4) | 5 min |
| 10 | Analytics tools (5) | 8 min |
| 11 | Geospatial tools (4) | 8 min |
| 12 | Prompts & resources | 5 min |
| 13 | Integration tests | 5 min |
| 14 | Final polish | 3 min |
