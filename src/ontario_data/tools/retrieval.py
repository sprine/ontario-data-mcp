from __future__ import annotations

import io
import logging
from typing import Any

import httpx
import pandas as pd
from fastmcp import Context

from ontario_data.ckan_client import CKANClient
from ontario_data.portals import PortalType
from ontario_data.server import DESTRUCTIVE, READONLY, mcp
from ontario_data.staleness import compute_expires_at, get_staleness_info
from ontario_data.utils import (
    _lifespan_state,
    fan_out,
    get_cache,
    get_deps,
    json_response,
    make_table_name,
    parse_portal_id,
)

logger = logging.getLogger("ontario_data.retrieval")


async def _download_resource_data(
    ckan: CKANClient,
    resource_id: str,
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    """Fetch resource data, preferring the CKAN datastore (structured API)
    and falling back to direct file download for CSV/XLSX/JSON/GeoJSON."""
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


async def _download_arcgis_resource_data(
    client,
    resource_id: str,
    http_client: httpx.AsyncClient,
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    """Fetch ArcGIS Hub resource data via Downloads API (bulk CSV)."""
    dataset = await client.package_show(resource_id)

    csv_url = await client.get_download_url(resource_id, fmt="csv")
    if csv_url:
        resp = await http_client.get(csv_url, follow_redirects=True)
        resp.raise_for_status()
        df = pd.read_csv(io.BytesIO(resp.content))
        resource_meta = {
            "id": resource_id,
            "package_id": resource_id,
            "format": "CSV",
            "url": csv_url,
            "datastore_active": False,
        }
        return df, resource_meta, dataset

    raise ValueError(
        f"No CSV download available for dataset '{resource_id}'. "
        f"Try a different dataset or check the portal directly at {client.base_url}."
    )


@mcp.tool(annotations=READONLY)
async def download_resource(
    resource_id: str,
    ctx: Context = None,
) -> str:
    """Download a dataset resource and cache it locally in DuckDB for fast querying.

    Supports CSV, XLSX, JSON, and datastore-active resources.
    If already cached, returns staleness info so you can decide whether to refresh.

    Args:
        resource_id: Prefixed resource ID (e.g. "toronto:abc123") or bare ID
    """
    configs = _lifespan_state(ctx)["portal_configs"]
    portal, bare_id = parse_portal_id(resource_id, set(configs.keys()))

    async def _try_download(pk: str):
        ckan, _ = get_deps(ctx, pk)
        # Verify the resource exists on this portal
        await ckan.resource_show(bare_id)
        return pk

    cache = get_cache(ctx)

    if cache.is_cached(bare_id):
        table_name = cache.get_table_name(bare_id)
        meta = cache.get_resource_meta(bare_id)
        staleness = get_staleness_info(cache, bare_id)
        return json_response(
            status="already_cached",
            table_name=table_name,
            row_count=meta["row_count"],
            downloaded_at=str(meta["downloaded_at"]),
            staleness=staleness,
            hint="Use query_cached tool with SQL to analyze this data. Use refresh_cache(resource_id=...) to re-download.",
        )

    if not portal:
        results = await fan_out(ctx, None, _try_download, first_match=True)
        if not results or results[0][2] is not None:
            errors = "; ".join(f"{pk}: {err}" for pk, _, err in results) if results else "no portals available"
            raise ValueError(
                f"Resource '{bare_id}' not found. Tried: {errors}. "
                f"Use search_datasets to find the correct prefixed ID."
            )
        portal = results[0][1]

    ckan, _ = get_deps(ctx, portal)

    await ctx.report_progress(0, 100, "Downloading resource...")

    config = configs[portal]
    if config.portal_type == PortalType.ARCGIS_HUB:
        http_client = _lifespan_state(ctx)["http_client"]
        df, resource, dataset = await _download_arcgis_resource_data(ckan, bare_id, http_client)
    else:
        df, resource, dataset = await _download_resource_data(ckan, bare_id)

    await ctx.report_progress(70, 100, "Storing in DuckDB...")

    table_name = make_table_name(dataset.get("name", ""), bare_id, portal=portal)
    cache.store_resource(
        resource_id=bare_id,
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
    cache.update_expires_at(bare_id, expires_at)

    await ctx.report_progress(100, 100, "Done")

    return json_response(
        status="downloaded",
        table_name=table_name,
        row_count=len(df),
        columns=list(df.columns),
        dtypes={col: str(dtype) for col, dtype in df.dtypes.items()},
        hint=f'Use query_cached tool with SQL like: SELECT * FROM "{table_name}" LIMIT 10',
    )


@mcp.tool(annotations=READONLY)
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


@mcp.tool(annotations=DESTRUCTIVE)
async def cache_manage(
    action: str,
    resource_id: str | None = None,
    ctx: Context = None,
) -> str:
    """Manage the local DuckDB cache: remove or clear cached data.

    Args:
        action: One of "remove" (single resource) or "clear" (all)
        resource_id: Required for "remove" action. Prefixed or bare ID accepted.
    """
    cache = get_cache(ctx)

    if action == "remove":
        if not resource_id:
            raise ValueError("resource_id is required for 'remove' action")
        configs = _lifespan_state(ctx)["portal_configs"]
        _, bare_id = parse_portal_id(resource_id, set(configs.keys()))
        cache.remove_resource(bare_id)
        return json_response(status="removed", resource_id=bare_id)

    elif action == "clear":
        count = len(cache.list_cached())
        cache.remove_all()
        return json_response(status="cleared", removed_count=count)

    else:
        raise ValueError(f"Invalid action '{action}'. Use 'remove' or 'clear'.")


@mcp.tool(annotations=DESTRUCTIVE)
async def refresh_cache(
    resource_id: str | None = None,
    ctx: Context = None,
) -> str:
    """Re-download cached resources to get the latest data.

    Args:
        resource_id: Specific resource to refresh (prefixed or bare ID), or omit to refresh all
    """
    cache = get_cache(ctx)
    cached = cache.list_cached()

    bare_id = None
    if resource_id:
        configs = _lifespan_state(ctx)["portal_configs"]
        _, bare_id = parse_portal_id(resource_id, set(configs.keys()))
        cached = [c for c in cached if c["resource_id"] == bare_id]
        if not cached:
            raise ValueError(f"Resource {bare_id} not found in cache")

    results = []
    for i, item in enumerate(cached):
        await ctx.report_progress(i, len(cached), f"Refreshing {item['table_name']}...")
        try:
            # Infer portal from table name prefix (ds_<portal>_...)
            table_name = item["table_name"]
            parts = table_name.split("_", 2)
            portal = parts[1] if len(parts) >= 3 else "ontario"

            ckan, _ = get_deps(ctx, portal)
            configs = _lifespan_state(ctx)["portal_configs"]
            config = configs.get(portal)
            if config and config.portal_type == PortalType.ARCGIS_HUB:
                http_client = _lifespan_state(ctx)["http_client"]
                df, resource, dataset = await _download_arcgis_resource_data(ckan, item["resource_id"], http_client)
            else:
                df, resource, dataset = await _download_resource_data(ckan, item["resource_id"])
            cache.store_resource(
                resource_id=item["resource_id"],
                dataset_id=item["dataset_id"],
                table_name=item["table_name"],
                df=df,
                source_url=item["source_url"],
            )
            update_freq = dataset.get("update_frequency")
            from datetime import datetime, timezone
            expires_at = compute_expires_at(datetime.now(timezone.utc), update_freq)
            cache.update_expires_at(item["resource_id"], expires_at)
            results.append({"resource_id": item["resource_id"], "status": "refreshed", "new_row_count": len(df)})
        except Exception as e:
            results.append({"resource_id": item["resource_id"], "status": "error", "error": str(e)})

    return json_response(refreshed=results)
