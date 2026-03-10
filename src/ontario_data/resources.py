from __future__ import annotations

import json

from fastmcp import Context

from ontario_data.portals import PORTALS
from ontario_data.server import mcp
from ontario_data.utils import get_lifespan_state, get_cache, get_deps, parse_portal_id, resolve_dataset


@mcp.resource("ontario://cache/index")
async def cache_index(ctx: Context) -> str:
    """List of all locally cached datasets with freshness info."""
    cache = get_cache(ctx)
    cached = cache.list_cached()
    stats = cache.get_stats()
    return json.dumps({
        "total_cached": stats["table_count"],
        "total_rows": stats["total_rows"],
        "total_size_mb": round(stats["total_size_bytes"] / (1024 * 1024), 2),
        "datasets": cached,
    }, indent=2)


@mcp.resource("ontario://dataset/{dataset_id}")
async def dataset_metadata(dataset_id: str, ctx: Context) -> str:
    """Full metadata for a specific dataset (supports prefixed IDs like toronto:abc)."""
    cache = get_cache(ctx)
    _, bare_id = parse_portal_id(dataset_id, set(PORTALS.keys()))
    meta = cache.get_dataset_metadata(bare_id)
    if not meta:
        _, _, meta = await resolve_dataset(ctx, dataset_id)
        canonical_id = meta.get("id", bare_id)
        cache.store_dataset_metadata(canonical_id, meta)
        # Also store under the bare_id if it differs (e.g. slug vs UUID)
        # so future lookups by slug hit the cache
        if bare_id != canonical_id:
            cache.store_dataset_metadata(bare_id, meta)
    return json.dumps(meta, indent=2, default=str)


@mcp.resource("ontario://portal/stats")
async def portal_stats(ctx: Context) -> str:
    """Overview statistics across all data portals."""
    configs = get_lifespan_state(ctx)["portal_configs"]
    portals = []
    for portal_key, config in configs.items():
        try:
            ckan, _ = get_deps(ctx, portal_key)
            result = await ckan.package_search(rows=0)
            total = result["count"]
            orgs = await ckan.organization_list(all_fields=True, include_dataset_count=True)
            top_orgs = sorted(orgs, key=lambda x: x.get("package_count", 0), reverse=True)[:5]
            portals.append({
                "portal": portal_key,
                "name": config.name,
                "total_datasets": total,
                "top_organizations": [
                    {"name": o["title"], "datasets": o.get("package_count", 0)}
                    for o in top_orgs
                ],
            })
        except Exception:
            portals.append({
                "portal": portal_key,
                "name": config.name,
                "error": "Could not fetch stats",
            })
    return json.dumps({"portals": portals}, indent=2)


@mcp.resource("ontario://schema/{table_name}")
async def schema_resource(table_name: str, ctx: Context) -> str:
    """Column schema, types, sample values, and type warnings for a cached table."""
    cache = get_cache(ctx)

    # Get column info
    try:
        columns = cache.execute_sql_dict(f'DESCRIBE "{table_name}"')
    except Exception:
        return json.dumps({"error": f"Table '{table_name}' not found in cache."})

    # Get sample rows
    try:
        samples = cache.execute_sql_dict(f'SELECT * FROM "{table_name}" LIMIT 3')
    except Exception:
        samples = []

    # Read type_warnings from cache metadata (detected at download time)
    type_warnings = []
    try:
        meta_rows = cache.execute_sql(
            "SELECT type_warnings FROM _cache_metadata WHERE table_name = ?",
            [table_name],
        )
        if meta_rows and meta_rows[0][0]:
            type_warnings = json.loads(meta_rows[0][0])
    except Exception:
        pass

    type_warnings_set = set(type_warnings)
    fields = []
    for col in columns:
        col_name = col.get("column_name", col.get("Field", ""))
        col_type = col.get("column_type", col.get("Type", ""))
        sample_vals = [str(row.get(col_name, "")) for row in samples if row.get(col_name) is not None]
        field = {
            "name": col_name,
            "type": col_type,
            "sample_values": sample_vals,
        }
        if col_name in type_warnings_set:
            field["type_warning"] = "Values appear numeric — use TRY_CAST(col AS DOUBLE) for queries"
        fields.append(field)

    result = {
        "table_name": table_name,
        "columns": fields,
    }
    if type_warnings:
        result["type_warnings"] = type_warnings
        result["hint"] = f"Columns {type_warnings} are VARCHAR but contain numbers. Use TRY_CAST() for comparisons."

    return json.dumps(result, indent=2, default=str)


@mcp.resource("ontario://guides/duckdb-sql")
async def duckdb_sql_guide() -> str:
    """DuckDB SQL reference for Ontario open data analysis."""
    return json.dumps({
        "title": "DuckDB SQL Guide for Ontario Open Data",
        "reference": "https://duckdb.org/docs/sql/functions/overview",
        "tips": [
            "Use DATE_TRUNC and DATE_PART for time series analysis",
            "Use CORR, REGR_SLOPE, REGR_R2 for correlation analysis",
            "Use PIVOT/UNPIVOT for cross-tabulation",
            "Use LAG/LEAD window functions for period comparisons",
            "Common Ontario columns: _id, date, year, region, municipality",
            "SUMMARIZE <table> gives quick statistics for all columns",
            "Many Ontario data columns are VARCHAR even when values are numeric — use TRY_CAST(col AS DOUBLE)",
            "Values containing semicolons break query_cached — use LIKE patterns instead of exact matches",
            "Use SUM(quantity_column) not COUNT(*) when rows represent aggregated counts",
            "Column names may vary across resources in the same dataset (e.g. TotalEV vs Total EV)",
        ],
    }, indent=2)
