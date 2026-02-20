# BUG-002: Prompts and resources hardcoded to `portal="ontario"`

**Severity:** Medium — prompts/resources fail for Toronto/Ottawa datasets
**Status:** Identified
**Introduced in:** multi-portal refactor (commit 2f0e98d)

---

## Description

`prompts.py` and `resources.py` both hardcode `get_deps(ctx, portal="ontario")`:

- `prompts.py:46` — `explore_topic` always uses Ontario portal
- `prompts.py:71` — `data_investigation` always uses Ontario portal
- `prompts.py:109` — `compare_data` always uses Ontario portal
- `resources.py:14` — `cache_index` uses Ontario portal
- `resources.py:28` — `dataset_metadata` uses Ontario portal
- `resources.py:39` — `portal_stats` uses Ontario portal

After the multi-portal refactor, these should either:
1. Accept a `portal` parameter, or
2. Use `get_cache(ctx)` directly (for cache-only operations), or
3. Fan out across portals where appropriate

The `cache_index` resource is especially wrong — it gets the cache through `get_deps(ctx, portal="ontario")` but the cache is shared across all portals. Using `get_cache(ctx)` would be correct and simpler.

## Impact

- Prompts like `data_investigation` will fail if given a Toronto or Ottawa dataset ID
- `portal_stats` resource only shows Ontario stats, ignoring Toronto/Ottawa
- Not a regression (prompts were Ontario-only before multi-portal), but they should have been updated
