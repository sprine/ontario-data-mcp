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
