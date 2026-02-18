from __future__ import annotations

import os
from contextlib import asynccontextmanager
from importlib.metadata import version

import httpx
from fastmcp import FastMCP
from mcp.types import ToolAnnotations

from ontario_data.cache import CacheManager
from ontario_data.ckan_client import CKANClient
from ontario_data.logging_config import setup_logging


@asynccontextmanager
async def lifespan(server):
    """Initialize shared resources for the server."""
    logger = setup_logging()
    logger.info("Ontario Data MCP server starting")
    http_client = httpx.AsyncClient(
        timeout=float(os.environ.get("ONTARIO_DATA_TIMEOUT", "30"))
    )
    client = CKANClient(http_client=http_client)
    cache = CacheManager()
    cache.initialize()
    yield {"ckan": client, "cache": cache}
    await client.close()
    logger.info("Ontario Data MCP server stopped")


READONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False)
DESTRUCTIVE = ToolAnnotations(readOnlyHint=False, destructiveHint=True)

mcp = FastMCP(
    "Ontario Data Catalogue",
    instructions=(
        "Search, download, cache, and analyze datasets from Ontario's Open Data Catalogue "
        "(data.ontario.ca). Use discovery tools to find datasets, retrieval tools to cache them "
        "locally in DuckDB, and querying tools to analyze the data.\n\n"
        "Key guidelines:\n"
        "- Prefer download_resource + query_cached over sql_query (avoids remote API rate limits)\n"
        "- Many numeric columns are stored as text — use TRY_CAST(col AS DOUBLE) in DuckDB queries\n"
        "- Use SUM(quantity_col) not COUNT(*) when rows have a count/quantity column\n"
        "- Check unit columns before comparing datasets (e.g. mg/L vs µg/L)\n"
        "- Values may contain semicolons — use LIKE patterns instead of exact string matches\n"
        "- Column names may vary across resources in the same dataset — always check with get_resource_schema\n"
        "- Some resources are XLSX-only: downloadable via download_resource but not queryable via remote datastore API"
    ),
    version=version("ontario-data-mcp"),
    lifespan=lifespan,
)

# Import tool modules to register them with the server
from ontario_data.tools import discovery  # noqa: E402, F401
from ontario_data.tools import metadata  # noqa: E402, F401
from ontario_data.tools import retrieval  # noqa: E402, F401
from ontario_data.tools import querying  # noqa: E402, F401
from ontario_data.tools import quality  # noqa: E402, F401
from ontario_data.tools import geospatial  # noqa: E402, F401
from ontario_data import prompts  # noqa: E402, F401
from ontario_data import resources  # noqa: E402, F401


def main():
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "cache":
        from ontario_data.cli import run
        run(sys.argv[2:])
    else:
        mcp.run()


if __name__ == "__main__":
    main()
