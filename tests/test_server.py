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


@pytest.mark.asyncio
async def test_all_tools_have_annotations():
    """Every registered tool must have annotations set."""
    async with Client(mcp) as client:
        tools = await client.list_tools()
        unannotated = [t.name for t in tools if t.annotations is None]
        assert unannotated == [], f"Tools missing annotations: {unannotated}"


@pytest.mark.asyncio
async def test_annotation_values_are_correct():
    """All tools are READONLY except cache_manage which is DESTRUCTIVE."""
    async with Client(mcp) as client:
        tools = await client.list_tools()
        for tool in tools:
            assert tool.annotations is not None, f"{tool.name} has no annotations"
            if tool.name == "cache_manage":
                assert tool.annotations.readOnlyHint is False, (
                    "cache_manage should not be readOnlyHint"
                )
                assert tool.annotations.destructiveHint is True, (
                    "cache_manage should be destructiveHint"
                )
            else:
                assert tool.annotations.readOnlyHint is True, (
                    f"{tool.name} should be readOnlyHint=True"
                )
                assert tool.annotations.destructiveHint is False, (
                    f"{tool.name} should be destructiveHint=False"
                )
