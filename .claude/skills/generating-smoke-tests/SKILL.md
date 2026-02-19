---
name: generating-smoke-tests
description: Generate and run a live smoke test that exercises the MCP server's tools against the real CKAN API. Use during releases, after refactoring tools, or when verifying end-to-end connectivity.
---

# Generating Smoke Tests

Generate a temporary Python smoke-test script that exercises multiple MCP tools against the live Ontario CKAN API, run it, verify all assertions pass, then clean up.

## When to Use

- During the release process (after unit tests pass, before version bump)
- After refactoring tool code, server wiring, or lifespan/context changes
- When verifying end-to-end connectivity to data.ontario.ca

## Instructions

### 1. Generate the smoke test script

Write a file called `smoke_test.py` in the project root. The script must:

1. **Discover the latest dataset dynamically** — fetch `https://data.ontario.ca/feeds/dataset.atom`, parse the first `<entry>` to extract the dataset UUID from its `<id>` tag (format: `https://data.ontario.ca/dataset/{uuid}`)
2. **Set up real dependencies** — `httpx.AsyncClient`, `CKANClient`, `CacheManager` with a temp DuckDB path
3. **Mock only the FastMCP context** — use the pattern from `tests/test_tools_unit.py`:
   ```python
   from unittest.mock import AsyncMock, MagicMock
   from ontario_data.portals import PORTALS
   ctx = MagicMock()
   ctx.report_progress = AsyncMock()
   ctx.fastmcp._lifespan_result = {
       "http_client": http_client,
       "portal_configs": PORTALS,
       "portal_clients": {"ontario": ckan},
       "cache": cache,
       "active_portal": "ontario",
   }
   ```
4. **Access tools via the tool manager**: `tools = mcp._tool_manager._tools`
5. **Exercise this tool chain** (each step asserts success before continuing):
   - `search_datasets(query="ontario")` — assert `total_count > 0`
   - `get_dataset_info(dataset_id=<uuid from feed>)` — assert returns id or name
   - `download_resource(resource_id=...)` — pick first datastore-active resource; if none found, try the next feed entry (up to 5) until one with a datastore-active resource is found
   - `query_cached(sql=...)` — `SELECT COUNT(*) as cnt` from the cached table
   - `cache_info()` — assert `table_count > 0`
6. **Clean up** — close the `httpx.AsyncClient`; `CacheManager` uses short-lived connections and needs no close
7. **Print progress** — each step prints a summary line prefixed with two spaces
8. **Print "All smoke tests passed!"** on success

### 2. Run the script

```bash
uv run python smoke_test.py
```

Timeout: 120 seconds. The CKAN API can be slow.

### 3. Evaluate results

- If all assertions pass and "All smoke tests passed!" appears, the test **succeeded**.
- If any assertion fails, **stop the release** and report the failure.
- If the script errors on cleanup/teardown (not in an assertion), the test still **succeeded** — note the cleanup issue but don't block.

### 4. Clean up

Delete `smoke_test.py` after the test completes (pass or fail).

## Key Facts

- `CacheManager` has no `.close()` method — it uses short-lived DuckDB connections
- `download_resource` calls `await ctx.report_progress(...)` — the mock context MUST have `ctx.report_progress = AsyncMock()`
- Use `tempfile.mkdtemp` for the DuckDB path to avoid conflicts with any running server
- The Atom feed at `https://data.ontario.ca/feeds/dataset.atom` returns the 20 most recently updated datasets; the `<id>` tag contains `https://data.ontario.ca/dataset/{uuid}`
- Not all datasets have datastore-active resources — iterate feed entries until one is found (up to 5 attempts)
- Tool functions are accessed via `mcp._tool_manager._tools["tool_name"].fn(...)`

## Template

```python
import asyncio
import json
import os
import tempfile
import xml.etree.ElementTree as ET

import httpx

from unittest.mock import AsyncMock, MagicMock
from ontario_data.ckan_client import CKANClient
from ontario_data.cache import CacheManager
from ontario_data.portals import PORTALS
from ontario_data.server import mcp

ATOM_FEED_URL = "https://data.ontario.ca/feeds/dataset.atom"
ATOM_NS = {"atom": "http://www.w3.org/2005/Atom"}


def get_latest_dataset_ids(xml_text: str, max_entries: int = 5) -> list[str]:
    """Extract dataset UUIDs from the Atom feed."""
    root = ET.fromstring(xml_text)
    ids = []
    for entry in root.findall("atom:entry", ATOM_NS)[:max_entries]:
        id_el = entry.find("atom:id", ATOM_NS)
        if id_el is not None and id_el.text:
            # id format: https://data.ontario.ca/dataset/{uuid}
            uuid = id_el.text.strip().rsplit("/", 1)[-1]
            ids.append(uuid)
    return ids


async def smoke_test():
    http_client = httpx.AsyncClient(timeout=30)
    ckan = CKANClient(http_client=http_client)

    tmp_dir = tempfile.mkdtemp(prefix="ontario_smoke_")
    db_path = os.path.join(tmp_dir, "smoke.duckdb")
    cache = CacheManager(db_path=db_path)
    cache.initialize()

    ctx = MagicMock()
    ctx.report_progress = AsyncMock()
    ctx.fastmcp._lifespan_result = {
        "http_client": http_client,
        "portal_configs": PORTALS,
        "portal_clients": {"ontario": ckan},
        "cache": cache,
        "active_portal": "ontario",
    }
    tools = mcp._tool_manager._tools

    # 0. Discover latest datasets from Atom feed
    resp = await http_client.get(ATOM_FEED_URL)
    resp.raise_for_status()
    dataset_ids = get_latest_dataset_ids(resp.text)
    assert dataset_ids, "Atom feed returned no entries"
    print(f"  atom feed: {len(dataset_ids)} recent datasets discovered")

    # 1. search_datasets
    result = await tools["search_datasets"].fn(query="ontario", ctx=ctx)
    data = json.loads(result)
    assert data["total_count"] > 0, "search_datasets returned no results"
    print(f"  search_datasets: {data['total_count']} datasets found")

    # 2. get_dataset_info + find a datastore-active resource
    ds_resource = None
    dataset_title = None
    for dataset_id in dataset_ids:
        result = await tools["get_dataset_info"].fn(dataset_id=dataset_id, ctx=ctx)
        data = json.loads(result)
        assert data.get("id") or data.get("name"), "get_dataset_info returned no dataset"
        dataset_title = data.get("title", data.get("name"))
        resources = data.get("resources", [])
        ds_resource = next((r for r in resources if r.get("datastore_active")), None)
        if ds_resource:
            print(f"  get_dataset_info: {dataset_title} (has datastore resources)")
            break
        print(f"  get_dataset_info: {dataset_title} (no datastore resources, trying next)")

    # 3. download_resource + query_cached + cache_info
    if ds_resource:
        rid = ds_resource["id"]
        result = await tools["download_resource"].fn(resource_id=rid, ctx=ctx)
        dl_data = json.loads(result)
        table_name = dl_data.get("table_name")
        assert table_name, "download_resource returned no table_name"
        print(f"  download_resource: cached as {table_name}")

        result = await tools["query_cached"].fn(
            sql=f'SELECT COUNT(*) as cnt FROM "{table_name}"', ctx=ctx
        )
        q_data = json.loads(result)
        rows = q_data.get("results", [{}])
        print(f"  query_cached: {rows[0].get('cnt', '?')} rows")

        result = await tools["cache_info"].fn(ctx=ctx)
        c_data = json.loads(result)
        assert c_data.get("table_count", 0) > 0, "cache_info shows no tables"
        print(f"  cache_info: {c_data['table_count']} cached table(s)")
    else:
        print("  skipped download/query/cache (no datastore resource in recent datasets)")

    await http_client.aclose()
    print("  All smoke tests passed!")


asyncio.run(smoke_test())
```

## Adapting for New Tools

If new tools are added to the server, extend the smoke test chain:

- **Read-only tools** (discovery, metadata, quality): Add after `get_dataset_info`, assert the response parses as JSON with expected keys
- **Cache-dependent tools** (querying, retrieval): Add after `download_resource`, use the cached `table_name`
- **Geospatial tools**: Only test if a geo-capable dataset is available; skip gracefully otherwise
