# README & Site Audit

Audit of `README.md` and `site/index.html` against the actual codebase (2025-03-09).

See also: [release-review.md](release-review.md) for release command issues.

---

## Completed

The following issues were fixed in commit `9b854be` and are now enforced by `tests/test_docs_accuracy.py`:

- **Shared root causes** — ghost `check_data_quality` removed, missing `ontario://schema/{table_name}` added
- **R1, R2, R4** — tool names, counts, and version disclaimer fixed in README
- **S1–S7** — tool/resource counts, ghost tools, and missing resources fixed in site
- **Scriptable pre-release checks** — implemented as `tests/test_docs_accuracy.py` covering tool/resource inventory, category counts, licence attribution (name + URL), and version consistency
- **R3** — `cache_manage` description fixed: removed false "refresh" action, now accurately says "remove" and "clear"
- **R5** — Added MCP Resources section documenting all 5 `@mcp.resource` URIs
- **R6** — MCP link updated from personal Gist to `https://modelcontextprotocol.io`
- **R7** — From-source install command updated to use `ontario-data-mcp` entrypoint
- **R12** — Fixed annotation messaging: clarified that download tools are read-only (populate cache) while only `cache_manage`/`refresh_cache` are destructive

---

## README.md Issues (remaining)

### Medium (incomplete or misleading)

| # | Section | Issue | Scriptable? |
|---|---------|-------|-------------|
| R8 | `profile_data` description | Says "Statistical profile using DuckDB SUMMARIZE" but tool also checks duplicates and reports VARCHAR-as-number type warnings | Partially — could diff docstring vs README |
| R9 | `compare_datasets` description | Doesn't mention 2–5 dataset limit enforced in code | Yes — parse parameter constraints from source |
| R10 | `search_datasets` row | Only mentions `portal=` filter. Actual function has 7 params: `query`, `organization`, `resource_format`, `update_frequency`, `sort_by`, `limit`, `portal` | Yes — compare function signature params to README |
| R11 | `spatial_query` row | Says "Run spatial queries against cached geospatial data" but doesn't mention the 3 operations: `contains_point`, `within_bbox`, `within_radius` | Yes — parse operation enum from source |
| R13 | WIP `validate` feature (line 24) | Listed as WIP but zero code exists — purely aspirational | No — requires human decision on whether to keep or remove |

---

## site/index.html Issues (remaining)

### Low (minor / aging)

| # | Location | Issue | Scriptable? |
|---|----------|-------|-------------|
| S8 | Multiple lines | "6,900+ datasets" — approximately correct today (~6,898 from portals.py) but will drift | Partially — could sum portal descriptions, but numbers are themselves approximate |
| S9 | Footer (line 674) | FastMCP link `https://github.com/jlowin/fastmcp` may be stale if repo moved | No — requires web check |

---

## Scriptable Checks Not Yet Implemented

The following checks from the original audit were proposed but not yet added to `test_docs_accuracy.py`:

### Parameter / enum accuracy

```
For tools with Literal[] or enum parameters:
  - Extract allowed values from source
  - Assert README description doesn't list non-existent values
```

This would catch R3 (and similar future drift) automatically.

---

## Summary

| Category | README | Site | Total |
|----------|--------|------|-------|
| Critical (factually wrong) | 0 | 0 | 0 |
| High (missing info) | 0 | 0 | 0 |
| Medium (incomplete) | 5 | 0 | 5 |
| Low (minor/aging) | 0 | 2 | 2 |
| **Total** | **5** | **2** | **7** |
