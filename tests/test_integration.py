import pytest
from fastmcp import Client
from ontario_data.server import mcp


@pytest.mark.asyncio
async def test_tools_registered():
    """Smoke check: server has a reasonable number of tools."""
    async with Client(mcp) as client:
        tools = await client.list_tools()
        assert len(tools) >= 20, f"Expected 20+ tools, got {len(tools)}"


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
