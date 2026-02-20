from __future__ import annotations

import json

from fastmcp import Context

from ontario_data.server import mcp
from ontario_data.utils import get_deps


@mcp.resource("ontario://cache/index")
async def cache_index(ctx: Context) -> str:
    """List of all locally cached datasets with freshness info."""
    _, cache = get_deps(ctx, portal="ontario")
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
    """Full metadata for a specific dataset."""
    ckan, cache = get_deps(ctx, portal="ontario")
    meta = cache.get_dataset_metadata(dataset_id)
    if not meta:
        meta = await ckan.package_show(dataset_id)
        cache.store_dataset_metadata(dataset_id, meta)
    return json.dumps(meta, indent=2, default=str)


@mcp.resource("ontario://portal/stats")
async def portal_stats(ctx: Context) -> str:
    """Overview statistics about the Ontario Data Catalogue."""
    ckan, _ = get_deps(ctx, portal="ontario")
    result = await ckan.package_search(rows=0)
    total = result["count"]
    orgs = await ckan.organization_list(all_fields=True, include_dataset_count=True)
    top_orgs = sorted(orgs, key=lambda x: x.get("package_count", 0), reverse=True)[:10]
    return json.dumps({
        "total_datasets": total,
        "top_organizations": [
            {"name": o["title"], "datasets": o.get("package_count", 0)}
            for o in top_orgs
        ],
    }, indent=2)


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
