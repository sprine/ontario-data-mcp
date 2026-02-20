#!/bin/bash
# Quick diagnostic commands for ontario-data-mcp tool visibility issues
# Run from project root: bash bugs/quick-diagnostics.sh

set -e
cd "$(dirname "$0")/.."

echo "=== Ontario Data MCP Diagnostics ==="
echo

echo "1. Checking Python environment..."
uv run python --version
uv run python -c "import fastmcp; print(f'FastMCP: {fastmcp.__version__}')"
uv run python -c "import mcp; print(f'MCP SDK: {mcp.__version__}')"
echo

echo "2. Checking tool registration..."
uv run python -c "
import asyncio
from ontario_data.server import mcp

async def check():
    async with mcp._lifespan(mcp):
        tools = await mcp.list_tools()
        print(f'Tools registered: {len(tools)}')
        for t in tools:
            print(f'  - {t.name}')

asyncio.run(check())
"
echo

echo "3. Testing MCP protocol (should return JSON with tools)..."
echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"diag","version":"1.0"}}}
{"jsonrpc":"2.0","method":"notifications/initialized"}  
{"jsonrpc":"2.0","id":2,"method":"tools/list"}' | timeout 5 uv run fastmcp run src/ontario_data/server.py 2>&1 | grep -E '^\{' | head -2
echo

echo "4. Checking .mcp.json..."
if [ -f .mcp.json ]; then
    cat .mcp.json
else
    echo "No .mcp.json found in project root"
fi
echo

echo "5. Checking for Claude Desktop config..."
CLAUDE_CONFIG="$HOME/Library/Application Support/Claude/claude_desktop_config.json"
if [ -f "$CLAUDE_CONFIG" ]; then
    echo "Claude Desktop config found:"
    cat "$CLAUDE_CONFIG" | python -m json.tool 2>/dev/null || cat "$CLAUDE_CONFIG"
else
    echo "No Claude Desktop config found at: $CLAUDE_CONFIG"
fi
echo

echo "=== Diagnostics Complete ==="
echo "If tools show here but not in client, the issue is client-side."
