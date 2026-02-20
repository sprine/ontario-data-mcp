# BUG-005: FastMCP 3.0 prints ASCII banner to stderr on every startup

**Severity:** Low — cosmetic, but can confuse log parsers
**Status:** Identified

---

## Description

FastMCP 3.0 prints a decorative ASCII art banner to stderr every time the server starts:

```
╭──────────────────────────────────────────────────────────────────╮
│                         ▄▀▀ ▄▀█ █▀▀ ▀█▀ █▀▄▀█ █▀▀ █▀█          │
│                         █▀  █▀█ ▄▄█  █  █ ▀ █ █▄▄ █▀▀          │
│                                                                  │
│                            FastMCP 3.0.0                         │
│                        https://gofastmcp.com                     │
│                                                                  │
│            🖥  Server:      Ontario Data Catalogue, 0.1.6        │
│            🚀 Deploy free: https://fastmcp.cloud                 │
│                                                                  │
╰──────────────────────────────────────────────────────────────────╯
```

This wasn't present in FastMCP 2.x. While harmless for stdio transport (stderr is separate from stdout), it can confuse log aggregation tools and adds visual noise.

FastMCP 2.x was silent on startup except for the log line `Starting MCP server...`.

## Recommendation

Check if FastMCP 3.0 has a flag to suppress the banner, or accept it as cosmetic.
