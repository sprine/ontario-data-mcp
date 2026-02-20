from __future__ import annotations

from typing import Any

from fastmcp import Context

from ontario_data.portals import PortalType
from ontario_data.server import READONLY, mcp
from ontario_data.utils import (
    _lifespan_state,
    fan_out,
    get_cache,
    get_deps,
    json_response,
    parse_portal_id,
    strip_internal_fields,
)


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

    if portal and configs[portal].portal_type == PortalType.ARCGIS_HUB:
        return json_response(
            status="not_available",
            reason="ArcGIS Hub has no remote datastore API.",
            suggestion=f"Use download_resource(resource_id='{resource_id}') + query_cached(sql='...') instead.",
        )

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
        if not results or results[0][2] is not None:
            errors = "; ".join(f"{pk}: {err}" for pk, _, err in results) if results else "no portals available"
            raise ValueError(
                f"Resource '{bare_id}' not found. Tried: {errors}. "
                f"Use search_datasets to find the correct prefixed ID."
            )
        result = results[0][1]

    field_info = [{"name": f["id"], "type": f.get("type")} for f in result.get("fields", []) if not f["id"].startswith("_")]
    clean_records = strip_internal_fields(result.get("records", []))

    return json_response(
        total=result.get("total", 0),
        returned=len(clean_records),
        fields=field_info,
        records=clean_records,
    )


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
    configs = _lifespan_state(ctx)["portal_configs"]
    if configs[portal].portal_type == PortalType.ARCGIS_HUB:
        return json_response(
            status="not_available",
            reason="ArcGIS Hub has no remote SQL API.",
            suggestion="Use download_resource(resource_id='...') + query_cached(sql='...') instead.",
        )

    ckan, _ = get_deps(ctx, portal)
    result = await ckan.datastore_sql(sql)
    field_info = [{"name": f["id"], "type": f.get("type")} for f in result.get("fields", []) if not f["id"].startswith("_")]
    clean_records = strip_internal_fields(result.get("records", []))

    return json_response(
        returned=len(clean_records),
        fields=field_info,
        records=clean_records,
    )


@mcp.tool(annotations=READONLY)
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
        msg = str(e).lower()
        hints = ["Quote table names with double quotes."]
        if any(kw in msg for kw in ("conversion", "cast", "type mismatch", "could not convert")):
            hints.append("Numeric columns may be stored as text. Use TRY_CAST(column AS DOUBLE).")
        raise type(e)(
            f"{e}\n\nAvailable tables: {table_names}\nHints: {' '.join(hints)}"
        ) from e


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

    if portal and configs[portal].portal_type == PortalType.ARCGIS_HUB:
        return json_response(
            status="not_available",
            reason="ArcGIS Hub has no remote datastore API.",
            suggestion=f"Use download_resource(resource_id='{resource_id}') + query_cached(sql='...') instead.",
        )

    async def _preview(pk: str):
        ckan, _ = get_deps(ctx, pk)
        return await ckan.datastore_search(bare_id, limit=min(rows, 100))

    if portal:
        result = await _preview(portal)
    else:
        results = await fan_out(ctx, None, _preview, first_match=True)
        if not results or results[0][2] is not None:
            errors = "; ".join(f"{pk}: {err}" for pk, _, err in results) if results else "no portals available"
            raise ValueError(
                f"Resource '{bare_id}' not found. Tried: {errors}. "
                f"Use search_datasets to find the correct prefixed ID."
            )
        result = results[0][1]

    field_info = [{"name": f["id"], "type": f.get("type")} for f in result.get("fields", []) if not f["id"].startswith("_")]
    clean_records = strip_internal_fields(result.get("records", []))

    return json_response(
        total_records=result.get("total", 0),
        previewing=len(clean_records),
        fields=field_info,
        records=clean_records,
    )
