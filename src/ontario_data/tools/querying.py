from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from fastmcp import Context

from ontario_data.cache import InvalidQueryError
from ontario_data.formatting import format_records
from ontario_data.server import READONLY, mcp
from ontario_data.utils import (
    _lifespan_state,
    arcgis_guard,
    fan_out,
    get_cache,
    get_deps,
    is_arcgis_portal,
    parse_portal_id,
    strip_internal_fields,
    unwrap_first_match,
)

MAX_QUERY_ROWS = 2000

_COUNT_STAR_RE = re.compile(r"\bCOUNT\s*\(\s*\*\s*\)", re.IGNORECASE)
_GROUP_BY_RE = re.compile(r"\bGROUP\s+BY\b", re.IGNORECASE)
_TABLE_RE = re.compile(r'FROM\s+"([^"]+)"', re.IGNORECASE)
_QUANTITY_PATTERNS = re.compile(
    r"(count|quantity|number|no_of|total|amount|exceedances|num_)",
    re.IGNORECASE,
)


def _get_type_warnings_for_tables(cache, table_names: list[str]) -> list[str]:
    """Read numeric VARCHAR warnings from cache metadata for the given tables."""
    warnings = []
    for tname in table_names:
        # Look up resource metadata by table name
        try:
            rows = cache.execute_sql(
                "SELECT type_warnings FROM _cache_metadata WHERE table_name = ?",
                [tname],
            )
            if rows and rows[0][0]:
                import json
                cols = json.loads(rows[0][0])
                warnings.extend(cols)
        except Exception:
            pass
    return warnings


def _generate_query_warnings(
    sql: str,
    results: list[dict[str, Any]],
    fields: list[dict[str, str]],
    cache,
) -> list[str]:
    """Post-query heuristic warnings to catch common accuracy traps."""
    warnings: list[str] = []

    # Extract table name(s) from SQL
    table_matches = _TABLE_RE.findall(sql)

    # 1. COUNT(*) when a quantity column exists
    if _COUNT_STAR_RE.search(sql) and table_matches:
        table = table_matches[0]
        try:
            columns = cache.execute_sql(f'DESCRIBE "{table}"')
            col_names = [c[0] for c in columns]
            quantity_cols = [c for c in col_names if _QUANTITY_PATTERNS.search(c)]
            if quantity_cols:
                warnings.append(
                    f"This table has columns that may contain per-row counts: "
                    f"{quantity_cols}. Consider SUM(\"{quantity_cols[0]}\") instead of COUNT(*)."
                )
        except Exception:
            pass

    # 2. 0 rows from non-empty table
    if not results and table_matches:
        table = table_matches[0]
        try:
            total = cache.execute_sql(f'SELECT COUNT(*) FROM "{table}"')[0][0]
            if total > 0:
                warnings.append(
                    f"0 rows returned but table has {total:,} rows. "
                    f"Check your WHERE/JOIN conditions."
                )
        except Exception:
            pass

    # 3. Very few rows from GROUP BY on large table
    if 1 <= len(results) <= 3 and _GROUP_BY_RE.search(sql) and table_matches:
        table = table_matches[0]
        try:
            total = cache.execute_sql(f'SELECT COUNT(*) FROM "{table}"')[0][0]
            if total > 1000:
                warnings.append(
                    f"Only {len(results)} groups from {total:,} rows. "
                    f"Verify your GROUP BY columns are correct."
                )
        except Exception:
            pass

    return warnings


@mcp.tool(annotations=READONLY)
async def query_resource(
    resource_id: str,
    filters: dict[str, Any] | None = None,
    fields: list[str] | None = None,
    sort: str | None = None,
    limit: int = 100,
    offset: int = 0,
    ctx: Context = None,
) -> str:
    """Query a resource via the CKAN Datastore API (remote, no download needed).

    Only works for resources with datastore_active=True.

    Args:
        resource_id: Prefixed resource ID (e.g. "toronto:abc123") or bare ID
        filters: Column filters as {column: value} pairs
        fields: List of columns to return (default: all)
        sort: Sort string (e.g. "date desc", "name asc")
        limit: Max rows (1-1000)
        offset: Row offset for pagination
    """
    configs = _lifespan_state(ctx)["portal_configs"]
    portal, bare_id = parse_portal_id(resource_id, set(configs.keys()))

    if portal and is_arcgis_portal(ctx, portal):
        return arcgis_guard(resource_id)

    async def _query(pk: str):
        ckan, _ = get_deps(ctx, pk)
        return await ckan.datastore_search(
            resource_id=bare_id,
            filters=filters,
            fields=fields,
            sort=sort,
            limit=min(limit, 1000),
            offset=offset,
        )

    if portal:
        result = await _query(portal)
    else:
        results = await fan_out(ctx, None, _query, first_match=True)
        _, result = unwrap_first_match(results, bare_id, "Resource")

    field_info = [{"name": f["id"], "type": f.get("type")} for f in result.get("fields", []) if not f["id"].startswith("_")]
    clean_records = strip_internal_fields(result.get("records", []))

    return format_records(clean_records, row_count=len(clean_records), total=result.get("total", 0), fields=field_info)


@mcp.tool(annotations=READONLY)
async def sql_query(
    sql: str,
    portal: str = "ontario",
    ctx: Context = None,
) -> str:
    """Run a SQL query against the CKAN Datastore (remote).

    NOTE: Prefer download_resource + query_cached for repeated queries —
    the remote API has rate limits (429 errors). Use this tool only for
    quick one-off queries on datastore-active resources.

    Use resource IDs as table names in double quotes.
    Example: SELECT "Column Name" FROM "resource-id-here" WHERE "Year" > 2020 LIMIT 10

    Args:
        sql: SQL query string (read-only, SELECT only)
        portal: Portal to query (default: "ontario"). Required because SQL embeds resource IDs directly.
    """
    ckan, _ = get_deps(ctx, portal)
    if is_arcgis_portal(ctx, portal):
        return arcgis_guard("", alternative="download_resource + query_cached")

    result = await ckan.datastore_sql(sql)
    field_info = [{"name": f["id"], "type": f.get("type")} for f in result.get("fields", []) if not f["id"].startswith("_")]
    clean_records = strip_internal_fields(result.get("records", []))

    return format_records(clean_records, row_count=len(clean_records), fields=field_info)


@mcp.tool(annotations=READONLY)
async def query_cached(
    sql: str,
    ctx: Context = None,
) -> str:
    """Run a SQL query against locally cached data in DuckDB.

    Use table names from download_resource or cache_info.
    Supports full DuckDB SQL: aggregations, window functions, CTEs, JOINs across tables.

    IMPORTANT — before querying, check column types with get_resource_schema or
    DESCRIBE "{table}". Many numeric columns are stored as VARCHAR. Use
    TRY_CAST(col AS DOUBLE) for numeric comparisons — bare operators like
    WHERE year > 2020 do string comparison and return wrong results silently.

    Use SUM(quantity_col) not COUNT(*) when rows contain per-row counts (e.g.
    a "count" or "number_of" column). COUNT(*) counts rows, not quantities.
    Column names vary across resources in the same dataset — always DESCRIBE first.
    Values containing semicolons should be matched with LIKE patterns, not = equality.

    After downloading, the table name is returned by download_resource and shown
    by cache_info. Quote table names with double quotes in SQL.

    Args:
        sql: SQL query (e.g. SELECT * FROM "ds_my_table_abc12345" LIMIT 10)
    """
    cache = get_cache(ctx)
    try:
        results, fields = cache.query_with_meta(sql)

        # --- Truncation (Item 2) ---
        truncated_total = None
        if len(results) >= MAX_QUERY_ROWS:
            try:
                count_row = cache.execute_sql(f"SELECT COUNT(*) FROM ({sql})")
                truncated_total = count_row[0][0]
            except Exception:
                truncated_total = len(results)
            results = results[:MAX_QUERY_ROWS]

        # --- Detect numeric VARCHARs from cache metadata (Item 1) ---
        table_names_for_warnings = _TABLE_RE.findall(sql)
        numeric_varchars = _get_type_warnings_for_tables(cache, table_names_for_warnings)

        # --- Post-query heuristic warnings (Item 5) ---
        warnings = _generate_query_warnings(sql, results, fields, cache)

        if numeric_varchars:
            warnings.insert(
                0,
                f"Columns {numeric_varchars} appear numeric but are stored as VARCHAR. "
                f"Use TRY_CAST(col AS DOUBLE) for comparisons.",
            )

        # --- Build response ---
        parts: list[str] = []

        # Echo SQL (Item 3)
        parts.append(f"**Query:** `{sql}`")

        # Format the records table with column types
        table_output = format_records(
            results,
            row_count=len(results),
            total=truncated_total,
            fields=fields,
        )
        parts.append(table_output)

        # Truncation warning
        if truncated_total is not None and truncated_total > MAX_QUERY_ROWS:
            parts.append(
                f"\n**Warning:** Results truncated to {MAX_QUERY_ROWS:,} of "
                f"{truncated_total:,} rows. Add a LIMIT, WHERE, or aggregation "
                f"to your query for accurate analysis."
            )

        # Append warnings
        if warnings:
            parts.append("")
            for w in warnings:
                parts.append(f"⚠ {w}")

        # --- Data provenance (Item 10) ---
        table_names_in_sql = _TABLE_RE.findall(sql)
        if table_names_in_sql:
            try:
                table_metas = cache.get_tables_metadata(table_names_in_sql)
                for tm in table_metas:
                    downloaded = str(tm["downloaded_at"]).split(".")[0] if tm["downloaded_at"] else "unknown"
                    expires = tm.get("expires_at")
                    if expires:
                        now = datetime.now(timezone.utc)
                        is_stale = now > expires if hasattr(expires, '__gt__') else False
                        status = "**stale**" if is_stale else "fresh"
                    else:
                        status = "unknown"
                    parts.append(
                        f"\n**Source:** `{tm['table_name']}` | "
                        f"Resource: `{tm['resource_id']}` | "
                        f"Downloaded: {downloaded} | "
                        f"Status: {status}"
                    )
            except Exception:
                pass

        return "\n".join(parts)
    except Exception as e:
        cached = cache.list_cached()
        table_names = [c["table_name"] for c in cached]
        msg = str(e).lower()
        hints = ["Quote table names with double quotes."]
        if any(kw in msg for kw in ("conversion", "cast", "type mismatch", "could not convert")):
            hints.append("Numeric columns may be stored as text. Use TRY_CAST(column AS DOUBLE).")
        augmented = f"{e}\n\nAvailable tables: {table_names}\nHints: {' '.join(hints)}"
        raise InvalidQueryError(augmented) from e


@mcp.tool(annotations=READONLY)
async def preview_data(
    resource_id: str,
    rows: int = 10,
    ctx: Context = None,
) -> str:
    """Quick preview of the first N rows of a resource (fetched remotely).

    Args:
        resource_id: Prefixed resource ID (e.g. "toronto:abc123") or bare ID
        rows: Number of rows to preview (1-100)
    """
    configs = _lifespan_state(ctx)["portal_configs"]
    portal, bare_id = parse_portal_id(resource_id, set(configs.keys()))

    if portal and is_arcgis_portal(ctx, portal):
        return arcgis_guard(resource_id)

    async def _preview(pk: str):
        ckan, _ = get_deps(ctx, pk)
        return await ckan.datastore_search(bare_id, limit=min(rows, 100))

    if portal:
        result = await _preview(portal)
    else:
        results = await fan_out(ctx, None, _preview, first_match=True)
        _, result = unwrap_first_match(results, bare_id, "Resource")

    field_info = [{"name": f["id"], "type": f.get("type")} for f in result.get("fields", []) if not f["id"].startswith("_")]
    clean_records = strip_internal_fields(result.get("records", []))

    return format_records(clean_records, row_count=len(clean_records), total=result.get("total", 0), preview=True, fields=field_info)
