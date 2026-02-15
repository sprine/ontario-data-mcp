from __future__ import annotations

import io
import logging
from typing import Any

import httpx
import pandas as pd
from fastmcp import Context

from ontario_data.cache import CacheManager
from ontario_data.ckan_client import CKANClient
from ontario_data.server import mcp
from ontario_data.staleness import compute_expires_at, get_staleness_info
from ontario_data.utils import get_cache, get_deps, json_response, make_table_name

logger = logging.getLogger("ontario_data.retrieval")


async def _download_resource_data(
    ckan: CKANClient,
    resource_id: str,
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    """Download a resource and return (dataframe, resource_meta, dataset_meta)."""
    resource = await ckan.resource_show(resource_id)
    dataset_id = resource.get("package_id")
    dataset = await ckan.package_show(dataset_id) if dataset_id else {}

    fmt = (resource.get("format") or "").upper()
    url = resource.get("url", "")

    # Try datastore first (structured data)
    if resource.get("datastore_active"):
        result = await ckan.datastore_search_all(resource_id)
        df = pd.DataFrame(result["records"])
        internal_cols = [c for c in df.columns if c.startswith("_")]
        df = df.drop(columns=internal_cols, errors="ignore")
        return df, resource, dataset

    # Download file directly
    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
        response = await client.get(url)
        response.raise_for_status()
        content = response.content

    if fmt in ("CSV", "TXT"):
        df = pd.read_csv(io.BytesIO(content))
    elif fmt in ("XLS", "XLSX"):
        df = pd.read_excel(io.BytesIO(content))
    elif fmt == "JSON":
        df = pd.read_json(io.BytesIO(content))
    elif fmt == "GEOJSON":
        import geopandas as gpd
        df = gpd.read_file(io.BytesIO(content))
    else:
        raise ValueError(f"Unsupported format for tabular import: {fmt}. URL: {url}")

    return df, resource, dataset


@mcp.tool
async def download_resource(
    resource_id: str,
    ctx: Context = None,
) -> str:
    """Download a dataset resource and cache it locally in DuckDB for fast querying.

    Supports CSV, XLSX, JSON, and datastore-active resources.
    If already cached, returns staleness info so you can decide whether to refresh.

    Args:
        resource_id: The resource ID to download
    """
    ckan, cache = get_deps(ctx)

    if cache.is_cached(resource_id):
        table_name = cache.get_table_name(resource_id)
        meta = cache.conn.execute(
            "SELECT row_count, downloaded_at FROM _cache_metadata WHERE resource_id = ?",
            [resource_id],
        ).fetchone()
        staleness = get_staleness_info(cache, resource_id)
        return json_response(
            status="already_cached",
            table_name=table_name,
            row_count=meta[0],
            downloaded_at=str(meta[1]),
            staleness=staleness,
            hint="Use query_cached tool with SQL to analyze this data. Use cache_manage(action='refresh', resource_id=...) to re-download.",
        )

    await ctx.report_progress(0, 100, "Downloading resource...")
    df, resource, dataset = await _download_resource_data(ckan, resource_id)
    await ctx.report_progress(70, 100, "Storing in DuckDB...")

    table_name = make_table_name(dataset.get("name", ""), resource_id)
    cache.store_resource(
        resource_id=resource_id,
        dataset_id=dataset.get("id", ""),
        table_name=table_name,
        df=df,
        source_url=resource.get("url", ""),
    )
    cache.store_dataset_metadata(dataset.get("id", ""), dataset)

    # Set staleness expiry based on update frequency
    update_freq = dataset.get("update_frequency")
    from datetime import datetime, timezone
    expires_at = compute_expires_at(datetime.now(timezone.utc), update_freq)
    cache.update_expires_at(resource_id, expires_at)

    await ctx.report_progress(100, 100, "Done")

    return json_response(
        status="downloaded",
        table_name=table_name,
        row_count=len(df),
        columns=list(df.columns),
        dtypes={col: str(dtype) for col, dtype in df.dtypes.items()},
        hint=f'Use query_cached tool with SQL like: SELECT * FROM "{table_name}" LIMIT 10',
    )


@mcp.tool
async def cache_info(ctx: Context = None) -> str:
    """Get cache statistics and list all cached datasets.

    Returns size, table count, and details for every cached resource.
    """
    cache = get_cache(ctx)
    stats = cache.get_stats()
    cached = cache.list_cached()

    # Add staleness info for each cached resource
    items = []
    for c in cached:
        staleness = get_staleness_info(cache, c["resource_id"])
        items.append({
            "table_name": c["table_name"],
            "resource_id": c["resource_id"],
            "dataset_id": c["dataset_id"],
            "row_count": c["row_count"],
            "downloaded_at": c["downloaded_at"],
            "is_stale": staleness["is_stale"] if staleness else None,
        })

    return json_response(
        **stats,
        total_size_mb=round(stats["total_size_bytes"] / (1024 * 1024), 2),
        datasets=items,
    )


@mcp.tool
async def cache_manage(
    action: str,
    resource_id: str | None = None,
    ctx: Context = None,
) -> str:
    """Manage the local DuckDB cache: remove, clear, or refresh cached data.

    Args:
        action: One of "remove" (single resource), "clear" (all), or "refresh" (re-download)
        resource_id: Required for "remove" and "refresh" actions
    """
    ckan, cache = get_deps(ctx)

    if action == "remove":
        if not resource_id:
            raise ValueError("resource_id is required for 'remove' action")
        cache.remove_resource(resource_id)
        return json_response(status="removed", resource_id=resource_id)

    elif action == "clear":
        count = len(cache.list_cached())
        cache.remove_all()
        return json_response(status="cleared", removed_count=count)

    elif action == "refresh":
        if not resource_id:
            raise ValueError("resource_id is required for 'refresh' action")
        cached = cache.list_cached()
        item = next((c for c in cached if c["resource_id"] == resource_id), None)
        if not item:
            raise ValueError(f"Resource {resource_id} not found in cache")

        df, resource, dataset = await _download_resource_data(ckan, resource_id)
        cache.store_resource(
            resource_id=resource_id,
            dataset_id=item["dataset_id"],
            table_name=item["table_name"],
            df=df,
            source_url=item["source_url"],
        )
        update_freq = dataset.get("update_frequency")
        from datetime import datetime, timezone
        expires_at = compute_expires_at(datetime.now(timezone.utc), update_freq)
        cache.update_expires_at(resource_id, expires_at)

        return json_response(
            status="refreshed",
            resource_id=resource_id,
            new_row_count=len(df),
        )

    else:
        raise ValueError(f"Invalid action '{action}'. Use 'remove', 'clear', or 'refresh'.")


@mcp.tool
async def refresh_cache(
    resource_id: str | None = None,
    ctx: Context = None,
) -> str:
    """Re-download cached resources to get the latest data.

    Args:
        resource_id: Specific resource to refresh, or omit to refresh all
    """
    ckan, cache = get_deps(ctx)
    cached = cache.list_cached()

    if resource_id:
        cached = [c for c in cached if c["resource_id"] == resource_id]
        if not cached:
            raise ValueError(f"Resource {resource_id} not found in cache")

    results = []
    for i, item in enumerate(cached):
        await ctx.report_progress(i, len(cached), f"Refreshing {item['table_name']}...")
        try:
            df, resource, dataset = await _download_resource_data(ckan, item["resource_id"])
            cache.store_resource(
                resource_id=item["resource_id"],
                dataset_id=item["dataset_id"],
                table_name=item["table_name"],
                df=df,
                source_url=item["source_url"],
            )
            results.append({"resource_id": item["resource_id"], "status": "refreshed", "new_row_count": len(df)})
        except Exception as e:
            results.append({"resource_id": item["resource_id"], "status": "error", "error": str(e)})

    return json_response(refreshed=results)
