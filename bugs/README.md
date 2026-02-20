# Bug Tracker

## Fixed

| ID | Title | Fix |
|----|-------|-----|
| [001](001-tools-not-visible.md) | Tools not visible to MCP hosts (0 tools returned) | `sys.modules` guard in server.py + .mcp.json entry point |
| [002](002-prompts-hardcoded-ontario-portal.md) | Prompts and resources hardcoded to Ontario portal | Multi-portal support in prompts.py and resources.py |
| [003](003-lifespan-state-private-api.md) | `_lifespan_result` private API usage | Migrated to `ctx.lifespan_context` |
| [006](006-no-main-module.md) | No `__main__.py` for `python -m` usage | Added `__main__.py` |
| [007](007-mcp-json-vs-readme-mismatch.md) | `.mcp.json` uses broken launch method | Changed to entry point |

## Open (Low)

| ID | Title | Status |
|----|-------|--------|
| [004](004-version-not-bumped.md) | Version still reads 0.1.6 despite 20+ new commits | Bump on next release |
| [005](005-fastmcp-banner-on-stderr.md) | FastMCP 3.0 ASCII banner on stderr | Cosmetic |
| [008](008-tasks-capability-compat.md) | `tasks` capability may confuse older hosts | Monitoring |
