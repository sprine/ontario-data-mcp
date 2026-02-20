# BUG-001: Tools not visible to MCP hosts (0 tools returned)

**Severity:** Critical — server appears active but is completely non-functional
**Status:** Root cause confirmed, fix identified
**Affects:** Any host using `fastmcp run src/ontario_data/server.py` to launch
**Works in:** v0.1.6 (used `uvx ontario-data-mcp` entry point)
**Broke in:** Post-v0.1.6 when `.mcp.json` was added

---

## Root Cause

**Dual `mcp` instance caused by `fastmcp run`'s module loading.**

`.mcp.json` launches the server with:
```json
{
  "command": "uv",
  "args": ["run", "--directory", "...", "fastmcp", "run", "src/ontario_data/server.py"]
}
```

FastMCP 3.0's `run` command uses `importlib.util.spec_from_file_location("server_module", path)` to load `server.py`. This creates a module named `"server_module"` — **not** `"ontario_data.server"`.

When the bottom of `server.py` executes:
```python
from ontario_data.tools import discovery  # noqa: E402, F401
```

Python's import system resolves `ontario_data.server` as a **separate** module (since the already-loaded copy is registered as `"server_module"`, not `"ontario_data.server"`). This creates a **second** `FastMCP` instance. All 23 tools register against this second instance.

FastMCP's `_find_server_object` returns the first instance (the one it loaded directly) — which has **zero tools**.

### Proof

```python
# Simulated fastmcp run behavior:
spec = importlib.util.spec_from_file_location("server_module", "src/ontario_data/server.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

run_mcp = module.mcp                        # id: 4344625872
from ontario_data.server import mcp         # id: 4533647552  (DIFFERENT!)

await run_mcp.list_tools()                  # → 0 tools
await mcp.list_tools()                      # → 23 tools
```

### Why it worked in v0.1.6

v0.1.6 had no `.mcp.json`. The README instructed users to use:
```
uvx ontario-data-mcp        # Uses the [project.scripts] entry point
```

The entry point calls `ontario_data.server:main` through the normal import system, so only one `mcp` instance ever exists. All tools register against it.

## Fix

**Option A (simplest):** Change `.mcp.json` to use the entry point:
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

**Option B (defensive):** Also register `server.py` as `ontario_data.server` in `sys.modules` before the tool imports run, so `fastmcp run` and the normal import system resolve to the same object. Add to `server.py` before the tool imports:

```python
import sys
# Ensure fastmcp run's module is also registered as ontario_data.server
# so tool modules' `from ontario_data.server import mcp` gets the same instance.
if __name__ != "ontario_data.server":
    sys.modules.setdefault("ontario_data.server", sys.modules[__name__])
```

**Recommended:** Apply both fixes. Option A fixes the immediate problem. Option B makes the server robust regardless of how it's launched.
