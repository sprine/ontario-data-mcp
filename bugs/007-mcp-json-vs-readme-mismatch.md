# BUG-007: `.mcp.json` uses different launch method than README

**Severity:** Medium — `.mcp.json` is broken, README is correct
**Status:** Identified (consequence of BUG-001)

---

## Description

The README documents the correct launch method:
```bash
claude mcp add ontario-data -- uvx ontario-data-mcp
```

But `.mcp.json` (used by Claude Code when working in this repo) uses:
```json
{
  "command": "uv",
  "args": ["run", "--directory", "...", "fastmcp", "run", "src/ontario_data/server.py"]
}
```

This `fastmcp run` approach causes BUG-001 (zero tools). The `.mcp.json` should use the entry point, matching the README.

The `.mcp.json` was added after v0.1.6 (it's gitignored, not in git history) and was likely created to run the local dev version rather than the published package. The intent was correct but the launch command was wrong.

## Fix

Change `.mcp.json` to:
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
