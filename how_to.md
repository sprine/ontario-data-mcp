# How to Connect the Ontario Data MCP Server

## Claude Code

**Important:** Use the absolute path to `uv`. MCP subprocesses don't inherit your shell's PATH, so bare `uv` will fail to connect. Find yours with `which uv`.

Add the server:

```bash
claude mcp add ontario-data -- /Users/anshu/.local/bin/uv run --directory /Users/anshu/tools/data.ontario.ca-mcp fastmcp run src/ontario_data/server.py
```

Restart Claude Code (or start a new session). Verify it's connected:

```bash
claude mcp list
```

You should see `ontario-data` listed with a green checkmark. Then in any conversation you can ask things like "search Ontario open data for housing prices" and Claude will use the tools.

If the server shows as `failed`, double-check the absolute path to `uv` is correct. To re-add:

```bash
claude mcp remove ontario-data
claude mcp add ontario-data -- /Users/anshu/.local/bin/uv run --directory /Users/anshu/tools/data.ontario.ca-mcp fastmcp run src/ontario_data/server.py
```

## Claude Desktop

Add this to your MCP config file (`~/Library/Application Support/Claude/claude_desktop_config.json`).
Use the absolute path to `uv` here as well:

```json
{
  "mcpServers": {
    "ontario-data": {
      "command": "/Users/anshu/.local/bin/uv",
      "args": ["run", "--directory", "/Users/anshu/tools/data.ontario.ca-mcp", "fastmcp", "run", "src/ontario_data/server.py"]
    }
  }
}
```

Restart Claude Desktop after saving.

## Running Standalone

To run the server directly (e.g. for testing):

```bash
cd /Users/anshu/tools/data.ontario.ca-mcp
uv run fastmcp run src/ontario_data/server.py
```

## Running Tests

```bash
cd /Users/anshu/tools/data.ontario.ca-mcp
uv run pytest tests/ -v
```
