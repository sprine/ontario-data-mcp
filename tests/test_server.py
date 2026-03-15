import pytest
from fastmcp import Client
from ontario_data.server import mcp


@pytest.mark.asyncio
async def test_server_starts():
    """Verify the server can start and list tools."""
    async with Client(mcp) as client:
        tools = await client.list_tools()
        assert len(tools) > 0, "Server should register at least one tool"


@pytest.mark.asyncio
async def test_all_tools_have_annotations():
    """Every registered tool must have annotations set."""
    async with Client(mcp) as client:
        tools = await client.list_tools()
        unannotated = [t.name for t in tools if t.annotations is None]
        assert unannotated == [], f"Tools missing annotations: {unannotated}"


@pytest.mark.asyncio
async def test_query_cached_invalid_sql_returns_is_error():
    """SEP-1303: invalid SQL via MCP wire format returns isError=True."""
    async with Client(mcp) as client:
        result = await client.call_tool_mcp(
            name="query_cached",
            arguments={"sql": "DROP TABLE foo"},
        )
        assert result.isError is True
        assert len(result.content) > 0
        assert "read-only" in result.content[0].text.lower()


@pytest.mark.asyncio
async def test_annotation_values_are_correct():
    """All tools are READONLY except cache_manage and refresh_cache which are DESTRUCTIVE."""
    destructive_tools = {"cache_manage", "refresh_cache"}
    async with Client(mcp) as client:
        tools = await client.list_tools()
        for tool in tools:
            assert tool.annotations is not None, f"{tool.name} has no annotations"
            if tool.name in destructive_tools:
                assert tool.annotations.readOnlyHint is False, (
                    f"{tool.name} should not be readOnlyHint"
                )
                assert tool.annotations.destructiveHint is True, (
                    f"{tool.name} should be destructiveHint"
                )
            else:
                assert tool.annotations.readOnlyHint is True, (
                    f"{tool.name} should be readOnlyHint=True"
                )
                assert tool.annotations.destructiveHint is False, (
                    f"{tool.name} should be destructiveHint=False"
                )
