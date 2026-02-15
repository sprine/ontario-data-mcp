from __future__ import annotations

import os
from contextlib import asynccontextmanager

import httpx
from fastmcp import FastMCP

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
    cache.close()
    await client.close()
    logger.info("Ontario Data MCP server stopped")


mcp = FastMCP(
    "Ontario Data Catalogue",
    instructions=(
        "Search, download, cache, and analyze datasets from Ontario's Open Data Catalogue "
        "(data.ontario.ca). Use discovery tools to find datasets, retrieval tools to cache them "
        "locally in DuckDB, and querying tools to analyze the data."
    ),
    version="0.1.1",
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
