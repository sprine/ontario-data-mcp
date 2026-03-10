"""Live smoke tests — full MCP tool pipeline against real government APIs.

Run: uv run pytest tests/test_live_smoke.py -v --live

Covers: search → metadata → download → query → cache
"""
from __future__ import annotations

import random
import re

import pytest
from fastmcp import Client
from fastmcp.exceptions import ToolError

from ontario_data.server import mcp

pytestmark = pytest.mark.live

SEARCH_TERMS = ["election", "parking", "parks", "health care"]


@pytest.fixture
def query():
    return random.choice(SEARCH_TERMS)


@pytest.fixture
async def client():
    async with Client(mcp) as c:
        yield c


def _text(result) -> str:
    """Extract text content from a CallToolResult."""
    return "\n".join(block.text for block in result.content if hasattr(block, "text"))


class TestLiveSmokePipeline:
    """End-to-end pipeline: search → metadata → download → query → cache."""

    async def test_search_returns_results(self, client, query):
        result = await client.call_tool(
            "search_datasets", {"query": query, "limit": 3, "portal": "ontario"}
        )
        text = _text(result)
        assert text, f"Search for '{query}' returned empty output"

    async def test_full_pipeline(self, client, query):
        # 1. Search for CSV datasets
        search_result = await client.call_tool(
            "search_datasets",
            {"query": query, "limit": 5, "portal": "ontario", "resource_format": "CSV"},
        )
        search_text = _text(search_result)
        assert search_text, f"Empty search result for '{query}'"

        # Extract dataset IDs (portal-prefixed, e.g. "ontario:abc-123")
        dataset_ids = re.findall(r"(ontario:[a-z0-9_-]+)", search_text, re.IGNORECASE)
        assert dataset_ids, f"No dataset ID found in search results for '{query}'"

        # Try datasets until we find one with a downloadable resource
        table_name = None
        for dataset_id in dataset_ids:
            # 2. Get dataset info
            info_result = await client.call_tool(
                "get_dataset_info", {"dataset_id": dataset_id}
            )
            info_text = _text(info_result)
            assert info_text, f"Empty metadata for {dataset_id}"

            # 3. List resources — extract bare UUIDs near CSV format
            res_result = await client.call_tool(
                "list_resources", {"dataset_id": dataset_id}
            )
            res_text = _text(res_result)
            resource_ids = _extract_csv_resource_ids(res_text)
            if not resource_ids:
                continue

            # 4. Try downloading each resource until one succeeds
            for bare_id in resource_ids:
                prefixed_id = f"ontario:{bare_id}"
                try:
                    dl_result = await client.call_tool(
                        "download_resource", {"resource_id": prefixed_id}
                    )
                except ToolError:
                    continue  # resource may have been removed from portal

                dl_text = _text(dl_result)
                table_name = _extract_table_name(dl_text)
                if table_name:
                    break

            if table_name:
                break

        if not table_name:
            pytest.skip(f"No downloadable CSV resource found for '{query}'")

        # 5. Query cached data
        query_result = await client.call_tool(
            "query_cached", {"sql": f'SELECT COUNT(*) AS n FROM "{table_name}"'}
        )
        query_text = _text(query_result)
        assert query_text, "Empty query result"

        # 6. Cache info shows the downloaded table
        cache_result = await client.call_tool("cache_info", {})
        cache_text = _text(cache_result)
        assert table_name in cache_text, (
            f"Downloaded table '{table_name}' not found in cache_info"
        )


class TestLiveSmokeToronto:
    """Smoke test against Toronto portal."""

    async def test_search_toronto(self, client, query):
        result = await client.call_tool(
            "search_datasets", {"query": query, "limit": 3, "portal": "toronto"}
        )
        text = _text(result)
        assert text, f"Empty Toronto search for '{query}'"


# --- Helpers ---

# UUID pattern: 8-4-4-4-12 hex
_UUID_RE = re.compile(r"[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}", re.IGNORECASE)


def _extract_csv_resource_ids(text: str) -> list[str]:
    """Extract resource UUIDs from list_resources output, preferring CSV rows."""
    # The table has rows with columns: id | name | format | ...
    # Look for UUIDs on lines that mention CSV
    csv_ids = []
    other_ids = []
    for line in text.splitlines():
        uuids = _UUID_RE.findall(line)
        if not uuids:
            continue
        if "csv" in line.lower():
            csv_ids.extend(uuids)
        else:
            other_ids.extend(uuids)
    return csv_ids or other_ids


def _extract_table_name(text: str) -> str | None:
    """Extract a DuckDB table name from download_resource output."""
    # Matches: "- **table_name:** ds_foo_abc123" or "table_name: ds_foo"
    match = re.search(r"table_name[*:\s]+`?([a-z0-9_]+)`?", text, re.IGNORECASE)
    return match.group(1) if match else None
