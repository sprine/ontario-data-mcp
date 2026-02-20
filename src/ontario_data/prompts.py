from __future__ import annotations

import logging

from fastmcp import Context
from fastmcp.prompts import Message

from ontario_data.server import mcp
from ontario_data.utils import get_cache, get_deps

logger = logging.getLogger("ontario_data.prompts")


def _format_cached_context(cache) -> str:
    cached = cache.list_cached()
    if not cached:
        return ""
    lines = [f"\nYou have {len(cached)} cached dataset(s):"]
    for c in cached[:10]:
        lines.append(f"  - {c['table_name']} ({c['row_count']} rows, downloaded {c['downloaded_at']})")
    return "\n".join(lines)


async def _get_topic_context(ctx, topic: str) -> str:
    """Pre-fetch a few search results from each CKAN portal so the prompt
    has concrete dataset names to reference."""
    lines = []
    from ontario_data.utils import _lifespan_state
    configs = _lifespan_state(ctx)["portal_configs"]
    for portal_key, config in configs.items():
        try:
            ckan, _ = get_deps(ctx, portal_key)
            result = await ckan.package_search(query=topic, rows=5)
            count = result["count"]
            titles = [ds.get("title", "?") for ds in result["results"][:5]]
            lines.append(
                f"\n{config.name} has {count} dataset(s) matching '{topic}'."
                f"\nTop results: {', '.join(titles)}"
            )
        except Exception as e:
            logger.warning("Failed to fetch topic context from %s: %s", portal_key, e)
    return "".join(lines)


@mcp.prompt
async def explore_topic(topic: str, ctx: Context = None) -> list[Message]:
    """Guided exploration of a topic across all open data portals.

    Searches for datasets, summarizes what's available, and suggests deep dives.
    """
    cache = get_cache(ctx)
    topic_ctx = await _get_topic_context(ctx, topic)
    cache_ctx = _format_cached_context(cache)

    return [
        Message(
            role="user",
            content=(
                f"I want to explore open data about: {topic}\n"
                f"{topic_ctx}{cache_ctx}\n\n"
                "Please:\n"
                "1. Use search_datasets to find relevant datasets across all portals\n"
                "2. Summarize the top results — what data is available, from which organizations\n"
                "3. For the most interesting datasets, use get_dataset_info to get details\n"
                "4. Suggest which datasets to download and analyze, and what questions they could answer\n"
                "5. Flag any datasets that are XLSX-only or have 0 datastore-active resources\n"
                "6. If any have datastore_active resources, preview a few rows"
            ),
        ),
    ]


@mcp.prompt
async def data_investigation(dataset_id: str, ctx: Context = None) -> list[Message]:
    """Deep investigation of a specific dataset: schema, quality, statistics, insights."""
    from ontario_data.utils import resolve_dataset

    cache = get_cache(ctx)

    ds_title = dataset_id
    try:
        _, _, ds = await resolve_dataset(ctx, dataset_id)
        ds_title = ds.get("title", dataset_id)
    except Exception as e:
        logger.warning("Failed to fetch dataset context: %s", e)

    cache_ctx = _format_cached_context(cache)

    return [
        Message(
            role="user",
            content=(
                f"Investigate this dataset thoroughly: {ds_title} ({dataset_id})\n"
                f"{cache_ctx}\n\n"
                "Please follow this workflow:\n"
                "1. get_dataset_info — understand what this dataset contains\n"
                "2. list_resources — check formats and datastore_active status; "
                "XLSX-only resources must be downloaded before querying\n"
                "3. For the primary CSV/data resource:\n"
                "   a. get_resource_schema — understand the columns\n"
                "   b. download_resource — cache it locally\n"
                "   c. check_data_quality — assess completeness and consistency\n"
                "   d. profile_data — statistical profile using DuckDB SUMMARIZE\n"
                "4. Provide insights: What stories does this data tell? What's surprising?\n"
                "5. Suggest follow-up analyses or related datasets\n"
                "6. For time series or correlations, write DuckDB SQL directly with query_cached"
            ),
        ),
    ]


@mcp.prompt
async def compare_data(dataset_ids: str, ctx: Context = None) -> list[Message]:
    """Side-by-side analysis of multiple datasets (comma-separated IDs, can be cross-portal)."""
    cache = get_cache(ctx)
    ids = [d.strip() for d in dataset_ids.split(",")]
    cache_ctx = _format_cached_context(cache)

    return [
        Message(
            role="user",
            content=(
                f"Compare these datasets side by side: {', '.join(ids)}\n"
                f"{cache_ctx}\n\n"
                "Please:\n"
                "1. compare_datasets — metadata comparison\n"
                "2. For each dataset, download the primary resource\n"
                "3. profile_data on each — compare structure, size, quality\n"
                "4. If they share common columns, use query_cached with DuckDB SQL to find relationships\n"
                "   IMPORTANT: Check unit columns before comparing values across datasets (e.g. mg/L vs µg/L)\n"
                "5. Summarize: How do these datasets complement each other? Can they be joined?"
            ),
        ),
    ]
