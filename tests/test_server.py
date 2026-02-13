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
