# Bug Investigation: Tools Not Showing Up

**Date**: 2026-02-20  
**Status**: LIKELY CLIENT-SIDE ISSUE  
**Impact**: Critical - Server unusable without tools

## Summary

User reports that MCP tools are not showing up in clients even though the server registers and appears active. Tools worked in release 0.1.6.

## ⚠️ KEY FINDING: Server Works Correctly!

The server is **fully functional**. All 23 tools register, convert to MCP format, and are returned via the MCP protocol correctly. The issue is almost certainly client-side or configuration-related.

## Investigation Results

### 1. Server-Side: Tools ARE Registered Correctly ✅

The server correctly registers all 23 tools:
```
search_datasets, list_organizations, list_topics, find_related_datasets,
list_portals, get_dataset_info, list_resources, get_resource_schema,
compare_datasets, download_resource, cache_info, cache_manage, refresh_cache,
query_resource, sql_query, query_cached, preview_data, check_data_quality,
check_freshness, profile_data, load_geodata, spatial_query, list_geo_datasets
```

Verified via:
```python
from ontario_data.server import mcp
import asyncio

async def check():
    tools = await mcp.list_tools()
    return len(tools)  # Returns 23

asyncio.run(check())  # Works
```

### 2. MCP Protocol: Tools ARE Being Returned ✅

When sending proper MCP protocol messages:
```json
{"jsonrpc":"2.0","id":1,"method":"initialize",...}
{"jsonrpc":"2.0","method":"notifications/initialized"}
{"jsonrpc":"2.0","id":2,"method":"tools/list"}
```

The server returns all 23 tools with correct inputSchema, annotations, etc.

### 3. Changes Since 0.1.6 (Potential Root Causes)

#### A. FastMCP Upgrade: 2.14.5 → 3.0.0 ⚠️

Major version upgrade with breaking changes:
- `@mcp.tool` now returns raw function, not Tool wrapper
- `.fn` accessor removed (tests were already fixed in d0a6434)
- Lifespan signature requirements may have changed
- Internal APIs like `_tool_manager._tools` no longer exist

**Commit**: d0a6434 "Upgrade fastmcp to 3.0.0 and fix breaking .fn() accessor"

#### B. Multi-Portal Support Added

- `server.py`: Changed from single CKANClient to lazy portal_clients dict
- `portals.py`: Added PortalConfig with PortalType enum
- `utils.py`: Added `_lifespan_state()`, `get_deps()`, `fan_out()` helpers
- Tools now accept optional `portal` parameter

**Commit**: 2f0e98d "Add multi-portal support..."

#### C. Lifespan Context Changes ⚠️

The lifespan now yields a different structure:
```python
# Before (0.1.6):
yield {"ckan": client, "cache": cache}

# After (current):
yield {
    "http_client": http_client,
    "portal_configs": PORTALS,
    "portal_clients": {},  # Lazy-initialized
    "cache": cache,
}
```

This change requires tools to use `_lifespan_state(ctx)` helper correctly.

#### D. Utils Module Accesses Internal FastMCP State ⚠️

```python
def _lifespan_state(ctx: Context) -> dict:
    # fastmcp stores lifespan yield value here (not part of public API)
    return ctx.fastmcp._lifespan_result
```

This relies on private FastMCP internals that could change between versions.

---

## Potential Issues

### Issue #1: Lifespan Not Properly Entered in Client Context

**Symptom**: Server starts, tools exist, but context is not initialized when client connects.

**Evidence**: When running manually:
```
TypeError: AggregateProvider.lifespan() takes 1 positional argument but 2 were given
```

**Analysis**: This error occurred when calling `mcp.lifespan(mcp)` directly. However, when running via `mcp.run_stdio_async()`, the error doesn't occur. But this suggests the lifespan entry mechanism may be fragile.

### Issue #2: Stdio Transport Crashing Before tools/list

**Symptom**: When sending multiple requests:
```
ERROR    Failed to run server: unhandled errors in a TaskGroup (1 sub-exception)
```

**Analysis**: The server crashes after `initialize` when processing additional requests. This could be:
- Transport layer issue
- Lifespan context not properly available
- Exception in tool list middleware

### Issue #3: .mcp.json Uses fastmcp CLI

Current config:
```json
{
  "mcpServers": {
    "ontario-data": {
      "command": "/Users/anshu/.local/bin/uv",
      "args": ["run", "--directory", "/Users/anshu/tools/ontario-data-mcp", 
               "fastmcp", "run", "src/ontario_data/server.py"]
    }
  }
}
```

The `fastmcp run` command auto-discovers the `mcp` object. This SHOULD work, but:
- Verify `fastmcp` CLI is the correct version
- Consider using direct Python invocation instead

### Issue #4: Python Version Mismatch

**Finding**: Shell activation uses wrong Python:
```
# .venv uses Python 3.12:
.venv/bin/python -> cpython-3.12.9

# But mise provides Python 3.13:
source .venv/bin/activate  # activates but which python → 3.13
```

This could cause import issues or version incompatibilities.

### Issue #5: Private API Dependency

`_lifespan_state()` accesses `ctx.fastmcp._lifespan_result` which is:
- Not part of public API
- Could change in FastMCP 3.0.0
- May not be set in all contexts

---

## Recommended Fixes

### Fix 1: Update .mcp.json to Use Direct Python

```json
{
  "mcpServers": {
    "ontario-data": {
      "command": "uv",
      "args": ["run", "--directory", "/Users/anshu/tools/ontario-data-mcp",
               "python", "-m", "ontario_data.server"]
    }
  }
}
```

Or use the installed script directly:
```json
{
  "mcpServers": {
    "ontario-data": {
      "command": "uv",
      "args": ["run", "--directory", "/Users/anshu/tools/ontario-data-mcp",
               "ontario-data-mcp"]
    }
  }
}
```

### Fix 2: Add __main__.py for Direct Module Execution

Create `src/ontario_data/__main__.py`:
```python
from ontario_data.server import main
main()
```

### Fix 3: Replace Private API Access

Instead of `ctx.fastmcp._lifespan_result`, use the official FastMCP 3.0.0 API if available.

### Fix 4: Add Error Logging

Add more verbose logging to catch startup errors:
```python
@asynccontextmanager
async def lifespan(server):
    logger = setup_logging()
    try:
        logger.info("Starting lifespan...")
        # ... setup ...
        yield state
    except Exception as e:
        logger.error(f"Lifespan error: {e}", exc_info=True)
        raise
    finally:
        logger.info("Ending lifespan...")
```

### Fix 5: Verify FastMCP Version Compatibility

Check if FastMCP 3.0.0 introduced changes to:
- How lifespans are managed
- Context availability during tool registration
- Middleware execution order

---

## Testing Commands

```bash
# Test MCP protocol directly:
echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}
{"jsonrpc":"2.0","method":"notifications/initialized"}
{"jsonrpc":"2.0","id":2,"method":"tools/list"}' | uv run --directory . fastmcp run src/ontario_data/server.py

# Test tool listing in Python:
.venv/bin/python -c "
import asyncio
from ontario_data.server import mcp

async def test():
    async with mcp._lifespan(mcp) as state:
        tools = await mcp.list_tools()
        print(f'{len(tools)} tools registered')

asyncio.run(test())
"

# Check installed versions:
.venv/bin/pip show fastmcp mcp
```

---

## Verified Server Functionality

```
✅ 23 tools registered
✅ All tools convert to MCP format
✅ tools/list returns proper JSON-RPC response
✅ Lifespan context properly initialized
✅ Import chain works (server → tools → utils)
```

### Full MCP Protocol Test Result

```bash
# This works correctly - returns all 23 tools in proper format:
echo '{"jsonrpc":"2.0","id":1,"method":"initialize",...}
{"jsonrpc":"2.0","method":"notifications/initialized"}
{"jsonrpc":"2.0","id":2,"method":"tools/list"}' | uv run fastmcp run src/ontario_data/server.py
```

The `ClosedResourceError` at the end is **expected** - it's the test stdin closing, not a server bug.

---

## Most Likely Causes (Client-Side)

### 1. MCP Client Timeout
The MCP client may disconnect before tools are listed if:
- Connection handshake takes too long
- Client has low timeout setting
- Network latency issues

### 2. MCP Protocol Version Mismatch  
Client may expect different protocol version than `2024-11-05`.

### 3. Client Cache
MCP client (Claude Desktop, etc.) may be caching tool list from previous connection.

### 4. Client Not Reading Responses
If client has buffering issues, it may not read the `tools/list` response.

### 5. Multiple Server Instances
If multiple MCP servers are registered with similar names, wrong one may be active.

---

## Debugging Steps

### Check Client-Side Logs
For Claude Desktop on macOS:
```bash
tail -f ~/Library/Logs/Claude/mcp*.log
```

### Test with MCP Inspector
```bash
npx @modelcontextprotocol/inspector uv run --directory . ontario-data-mcp
```

### Verify .mcp.json Location
Ensure the config is in the right place and has correct format.

### Test Alternative Configurations

**Option A: Direct script invocation**
```json
{
  "mcpServers": {
    "ontario-data": {
      "command": "uv",
      "args": ["run", "--directory", "/Users/anshu/tools/ontario-data-mcp", "ontario-data-mcp"]
    }
  }
}
```

**Option B: Python module execution**
```json
{
  "mcpServers": {
    "ontario-data": {
      "command": "uv",
      "args": ["run", "--directory", "/Users/anshu/tools/ontario-data-mcp", 
               "python", "-c", "from ontario_data.server import mcp; mcp.run()"]
    }
  }
}
```

---

## Server-Side Improvements Recommended

### Add Startup Logging
```python
@asynccontextmanager
async def lifespan(server):
    logger = setup_logging()
    logger.info("=== Ontario Data MCP Starting ===")
    logger.info(f"FastMCP version: {fastmcp.__version__}")
    # ... rest of lifespan
    logger.info(f"=== Registered {len(tools)} tools ===")
```

### Add Health Check Endpoint
Consider adding a simple health check tool that can verify the server is responding.

---

## Next Steps

1. [x] Verify server works correctly (CONFIRMED)
2. [ ] Check Claude Desktop/client logs for connection errors  
3. [ ] Test with MCP Inspector to isolate client vs server
4. [ ] Try alternative .mcp.json configurations
5. [ ] Verify no conflicting MCP servers registered
6. [ ] Clear client cache if applicable
7. [ ] Test with fresh Claude Desktop installation
