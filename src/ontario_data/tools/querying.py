from __future__ import annotations

import json
from typing import Any

from fastmcp import Context

from ontario_data.server import mcp
from ontario_data.ckan_client import CKANClient
from ontario_data.cache import CacheManager


def _get_deps(ctx: Context) -> tuple[CKANClient, CacheManager]:
    return ctx.lifespan_context["ckan"], ctx.lifespan_context["cache"]


@mcp.tool
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
        resource_id: Resource ID
        filters: Column filters as {column: value} pairs
        fields: List of columns to return (default: all)
        sort: Sort string (e.g. "date desc", "name asc")
        limit: Max rows (1-1000)
        offset: Row offset for pagination
    """
    ckan, _ = _get_deps(ctx)
    result = await ckan.datastore_search(
        resource_id=resource_id,
        filters=filters,
        fields=fields,
        sort=sort,
        limit=min(limit, 1000),
        offset=offset,
    )
    field_info = [{"name": f["id"], "type": f.get("type")} for f in result.get("fields", []) if not f["id"].startswith("_")]
    records = result.get("records", [])
    # Strip internal fields from records
    clean_records = [{k: v for k, v in r.items() if not k.startswith("_")} for r in records]

    return json.dumps({
        "total": result.get("total", 0),
        "returned": len(clean_records),
        "fields": field_info,
        "records": clean_records,
    }, indent=2, default=str)


@mcp.tool
async def sql_query(
    sql: str,
    ctx: Context = None,
) -> str:
    """Run a SQL query against the CKAN Datastore (remote).

    Use resource IDs as table names in double quotes.
    Example: SELECT "Column Name" FROM "resource-id-here" WHERE "Year" > 2020 LIMIT 10

    Args:
        sql: SQL query string (read-only, SELECT only)
    """
    ckan, _ = _get_deps(ctx)
    result = await ckan.datastore_sql(sql)
    field_info = [{"name": f["id"], "type": f.get("type")} for f in result.get("fields", []) if not f["id"].startswith("_")]
    records = result.get("records", [])
    clean_records = [{k: v for k, v in r.items() if not k.startswith("_")} for r in records]

    return json.dumps({
        "returned": len(clean_records),
        "fields": field_info,
        "records": clean_records,
    }, indent=2, default=str)


@mcp.tool
async def query_cached(
    sql: str,
    ctx: Context = None,
) -> str:
    """Run a SQL query against locally cached data in DuckDB.

    Use table names from download_resource or list_cached_datasets.
    Supports full DuckDB SQL including aggregations, window functions, CTEs, etc.

    Args:
        sql: SQL query (e.g. SELECT * FROM "ds_my_table_abc12345" LIMIT 10)
    """
    _, cache = _get_deps(ctx)
    try:
        results = cache.query(sql)
        return json.dumps({
            "row_count": len(results),
            "records": results,
        }, indent=2, default=str)
    except Exception as e:
        # Help the user by listing available tables
        cached = cache.list_cached()
        table_names = [c["table_name"] for c in cached]
        return json.dumps({
            "error": str(e),
            "available_tables": table_names,
            "hint": "Use table names from list_cached_datasets. Quote table names with double quotes.",
        }, indent=2)


@mcp.tool
async def preview_data(
    resource_id: str,
    rows: int = 10,
    ctx: Context = None,
) -> str:
    """Quick preview of the first N rows of a resource (fetched remotely).

    Args:
        resource_id: Resource ID (must have datastore_active=True)
        rows: Number of rows to preview (1-100)
    """
    ckan, _ = _get_deps(ctx)
    result = await ckan.datastore_search(resource_id, limit=min(rows, 100))
    field_info = [{"name": f["id"], "type": f.get("type")} for f in result.get("fields", []) if not f["id"].startswith("_")]
    records = result.get("records", [])
    clean_records = [{k: v for k, v in r.items() if not k.startswith("_")} for r in records]

    return json.dumps({
        "total_records": result.get("total", 0),
        "previewing": len(clean_records),
        "fields": field_info,
        "records": clean_records,
    }, indent=2, default=str)


@mcp.tool
async def filter_and_aggregate(
    resource_id: str,
    filters: dict[str, Any] | None = None,
    group_by: list[str] | None = None,
    aggregate: dict[str, str] | None = None,
    sort_by: str | None = None,
    limit: int = 100,
    ctx: Context = None,
) -> str:
    """Filter and aggregate data from a cached resource using natural parameters.

    This is a friendly wrapper around SQL for common operations.

    Args:
        resource_id: Resource ID (must be cached locally first via download_resource)
        filters: Column filters as {column: value} or {column: ">100"}
        group_by: Columns to group by
        aggregate: Aggregations as {output_name: "function(column)"} e.g. {"total": "sum(amount)", "avg_score": "avg(score)"}
        sort_by: Column to sort by (prefix with - for desc, e.g. "-total")
        limit: Max rows
    """
    _, cache = _get_deps(ctx)
    table_name = cache.get_table_name(resource_id)
    if not table_name:
        return json.dumps({"error": f"Resource {resource_id} not cached. Use download_resource first."})

    # Build SQL
    select_parts = []
    if group_by:
        select_parts.extend(f'"{col}"' for col in group_by)
    if aggregate:
        for alias, expr in aggregate.items():
            select_parts.append(f"{expr} AS \"{alias}\"")
    if not select_parts:
        select_parts = ["*"]

    sql = f"SELECT {', '.join(select_parts)} FROM \"{table_name}\""

    where_clauses = []
    if filters:
        for col, val in filters.items():
            if isinstance(val, str) and val[0] in (">", "<", "!", "="):
                where_clauses.append(f'"{col}" {val}')
            else:
                where_clauses.append(f'"{col}" = \'{val}\'')
    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses)

    if group_by:
        sql += " GROUP BY " + ", ".join(f'"{col}"' for col in group_by)

    if sort_by:
        if sort_by.startswith("-"):
            sql += f' ORDER BY "{sort_by[1:]}" DESC'
        else:
            sql += f' ORDER BY "{sort_by}" ASC'

    sql += f" LIMIT {limit}"

    try:
        results = cache.query(sql)
        return json.dumps({
            "sql_executed": sql,
            "row_count": len(results),
            "records": results,
        }, indent=2, default=str)
    except Exception as e:
        return json.dumps({"error": str(e), "sql_attempted": sql}, indent=2)
