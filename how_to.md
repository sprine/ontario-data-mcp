# How to Connect the Ontario Data MCP Server

## Prerequisites

- [uv](https://docs.astral.sh/uv/) installed
- Clone this repository and note its absolute path

**Important:** MCP subprocesses don't inherit your shell's PATH, so you must use the absolute path to `uv`. Find yours with `which uv`.

In the examples below, replace:
- `/absolute/path/to/uv` with the output of `which uv`
- `/path/to/ontario-data-mcp` with the absolute path to this repository

## Claude Code

Add the server:

```bash
claude mcp add ontario-data -- /absolute/path/to/uv run --directory /path/to/ontario-data-mcp fastmcp run src/ontario_data/server.py
```

Restart Claude Code (or start a new session). Verify it's connected:

```bash
claude mcp list
```

You should see `ontario-data` listed with a green checkmark. Then in any conversation you can ask things like "search Ontario open data for housing prices" and Claude will use the tools.

If the server shows as `failed`, double-check the absolute path to `uv` is correct. To re-add:

```bash
claude mcp remove ontario-data
claude mcp add ontario-data -- /absolute/path/to/uv run --directory /path/to/ontario-data-mcp fastmcp run src/ontario_data/server.py
```

## Claude Desktop

Add this to your MCP config file (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "ontario-data": {
      "command": "/absolute/path/to/uv",
      "args": ["run", "--directory", "/path/to/ontario-data-mcp", "fastmcp", "run", "src/ontario_data/server.py"]
    }
  }
}
```

Restart Claude Desktop after saving.

## Running Standalone

To run the server directly (e.g. for testing):

```bash
cd /path/to/ontario-data-mcp
uv run fastmcp run src/ontario_data/server.py
```

## Running Tests

```bash
cd /path/to/ontario-data-mcp
uv run pytest tests/ -v
```
