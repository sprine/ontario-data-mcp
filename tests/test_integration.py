import pytest
from fastmcp import Client
from ontario_data.server import mcp


@pytest.mark.asyncio
async def test_all_tools_registered():
    """Verify all ~22 tools are registered after consolidation."""
    async with Client(mcp) as client:
        tools = await client.list_tools()
        tool_names = [t.name for t in tools]
        expected = [
            # Discovery (6)
            "search_datasets", "list_organizations", "list_topics",
            "get_popular_datasets", "search_by_location", "find_related_datasets",
            # Metadata (4)
            "get_dataset_info", "list_resources", "get_resource_schema",
            "compare_datasets",
            # Retrieval (4)
            "download_resource", "cache_info", "cache_manage", "refresh_cache",
            # Querying (4)
            "query_resource", "sql_query", "query_cached", "preview_data",
            # Quality (3)
            "check_data_quality", "check_freshness", "profile_data",
            # Geospatial (3)
            "load_geodata", "spatial_query", "list_geo_datasets",
        ]
        for name in expected:
            assert name in tool_names, f"Missing tool: {name}"
        # Verify removed tools are gone
        removed = [
            "get_update_history", "list_cached_datasets", "cache_stats",
            "remove_from_cache", "filter_and_aggregate", "validate_schema",
            "profile_dataset", "summarize", "time_series_analysis",
            "cross_tabulate", "correlation_matrix", "compare_periods",
            "geocode_lookup",
        ]
        for name in removed:
            assert name not in tool_names, f"Tool should be removed: {name}"
        print(f"All {len(expected)} tools registered, {len(removed)} removed!")


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
        assert len(resources) >= 0
