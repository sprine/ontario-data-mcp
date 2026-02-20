# BUG-003: `_lifespan_result` is a private API — fragile across FastMCP versions

**Severity:** Low (works today, time bomb)
**Status:** Identified

---

## Description

`utils.py:35` accesses FastMCP internals:

```python
def _lifespan_state(ctx: Context) -> dict:
    # fastmcp stores lifespan yield value here (not part of public API)
    return ctx.fastmcp._lifespan_result
```

The comment acknowledges it's not public API. FastMCP 3.0 still has `_lifespan_result`, but it also added `ctx.lifespan_context` as a property on `Context`. If this is the public replacement, we should migrate to it.

If a future FastMCP 3.x release removes `_lifespan_result`, every single tool will break with an `AttributeError` — silently returning errors rather than working.

## Recommendation

Check if `ctx.lifespan_context` returns the same data as `ctx.fastmcp._lifespan_result`. If so, migrate to it. If not, open an issue on FastMCP requesting a stable API for lifespan state access.
