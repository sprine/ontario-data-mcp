from __future__ import annotations

from typing import Any

from fastmcp import Context

from ontario_data.server import mcp
from ontario_data.utils import get_cache, get_deps, json_response, strip_internal_fields


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
    ckan, _ = get_deps(ctx)
    result = await ckan.datastore_search(
        resource_id=resource_id,
        filters=filters,
        fields=fields,
        sort=sort,
        limit=min(limit, 1000),
        offset=offset,
    )
    field_info = [{"name": f["id"], "type": f.get("type")} for f in result.get("fields", []) if not f["id"].startswith("_")]
    clean_records = strip_internal_fields(result.get("records", []))

    return json_response(
        total=result.get("total", 0),
        returned=len(clean_records),
        fields=field_info,
        records=clean_records,
    )


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
    ckan, _ = get_deps(ctx)
    result = await ckan.datastore_sql(sql)
    field_info = [{"name": f["id"], "type": f.get("type")} for f in result.get("fields", []) if not f["id"].startswith("_")]
    clean_records = strip_internal_fields(result.get("records", []))

    return json_response(
        returned=len(clean_records),
        fields=field_info,
        records=clean_records,
    )


@mcp.tool
async def query_cached(
    sql: str,
    ctx: Context = None,
) -> str:
    """Run a SQL query against locally cached data in DuckDB.

    Use table names from download_resource or cache_info.
    Supports full DuckDB SQL including aggregations, window functions, CTEs, etc.

    Args:
        sql: SQL query (e.g. SELECT * FROM "ds_my_table_abc12345" LIMIT 10)
    """
    cache = get_cache(ctx)
    try:
        results = cache.query(sql)
        return json_response(
            row_count=len(results),
            records=results,
        )
    except Exception as e:
        cached = cache.list_cached()
        table_names = [c["table_name"] for c in cached]
        raise type(e)(
            f"{e}\n\nAvailable tables: {table_names}\n"
            f"Hint: Quote table names with double quotes."
        ) from e


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
    ckan, _ = get_deps(ctx)
    result = await ckan.datastore_search(resource_id, limit=min(rows, 100))
    field_info = [{"name": f["id"], "type": f.get("type")} for f in result.get("fields", []) if not f["id"].startswith("_")]
    clean_records = strip_internal_fields(result.get("records", []))

    return json_response(
        total_records=result.get("total", 0),
        previewing=len(clean_records),
        fields=field_info,
        records=clean_records,
    )
