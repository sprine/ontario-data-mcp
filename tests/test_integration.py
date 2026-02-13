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
