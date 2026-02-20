# BUG-008: FastMCP 3.0 advertises `tasks` capability — potential host incompatibility

**Severity:** Low — no confirmed breakage, theoretical risk
**Status:** Monitoring

---

## Description

FastMCP 3.0's `initialize` response includes capabilities that weren't in 2.x:

```json
{
  "capabilities": {
    "tasks": {
      "list": {},
      "cancel": {},
      "requests": { "tools": { "call": {} }, "prompts": { "get": {} }, "resources": { "read": {} } }
    },
    "extensions": {
      "io.modelcontextprotocol/ui": {}
    }
  }
}
```

The `tasks` capability is part of the official MCP SDK (`mcp.types.ServerTasksCapability`), and the spec uses `extra="allow"` on `ServerCapabilities`, so well-behaved hosts should ignore unknown capabilities.

However, older MCP host implementations that don't use `extra="allow"` or do strict schema validation could reject the initialize response entirely, making the server appear dead.

## Impact

- No confirmed breakage with Claude Desktop or Claude Code
- Could affect third-party MCP hosts with strict schema validation
- The MCP SDK (v1.26.0) includes `tasks` in the schema, so any host using this SDK version should be fine

## Recommendation

Monitor. If users report connection failures with specific hosts, this is worth investigating.
