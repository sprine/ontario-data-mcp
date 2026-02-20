# FastMCP 3.0.0 Migration Notes

## Upgrade Details

- **From**: fastmcp 2.14.5
- **To**: fastmcp 3.0.0
- **Commit**: d0a6434

## Breaking Changes Addressed

### 1. Tool Decorator Return Value

**Before (2.x)**: `@mcp.tool` returned a `Tool` wrapper with `.fn` accessor
**After (3.x)**: `@mcp.tool` returns the raw function

**Fix Applied**: Removed all `.fn()` calls in tests (commit d0a6434)

### 2. Private API Access

The codebase uses `ctx.fastmcp._lifespan_result` to access lifespan state:

```python
def _lifespan_state(ctx: Context) -> dict:
    return ctx.fastmcp._lifespan_result  # Private API!
```

**Risk**: This may change between versions. Consider filing issue with FastMCP for public API.

## Verified Working

- [x] Tool registration with `@mcp.tool`
- [x] Tool annotations (READONLY, DESTRUCTIVE)
- [x] Lifespan context management
- [x] `mcp.list_tools()` returns all tools
- [x] `to_mcp_tool()` conversion works
- [x] MCP protocol `tools/list` returns correct JSON-RPC

## Potential Issues (Not Yet Verified)

### Context Availability in Tools

Tools access context via parameter:
```python
async def some_tool(param: str, ctx: Context = None):
```

Need to verify `ctx.fastmcp._lifespan_result` is accessible during tool execution (not just registration).

### Middleware Execution Order

FastMCP 3.0 may have changed middleware execution order. If tools have middleware dependencies, this could affect behavior.

### Task/Docket Integration

FastMCP 3.0 added task scheduling (Docket). This shouldn't affect basic tool operation but adds complexity.

## Rollback Option

If issues persist, temporarily pin to older version:

```toml
# pyproject.toml
dependencies = [
    "fastmcp==2.14.5",  # Pin to last known working
    ...
]
```

Then run:
```bash
uv sync
```

## Testing Procedure

```bash
# Full tool execution test (not just registration)
uv run python -c "
import asyncio
from ontario_data.server import mcp

async def test():
    # This tests the full lifecycle
    async with mcp._lifespan(mcp) as state:
        # Test a real tool call
        from fastmcp import Context
        ctx = Context()  # Need to mock properly
        # ...
        
asyncio.run(test())
"
```
