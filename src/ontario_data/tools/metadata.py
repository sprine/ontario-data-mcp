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
async def get_dataset_info(
    dataset_id: str,
    ctx: Context = None,
) -> str:
    """Get full metadata for a dataset including all resources.

    Args:
        dataset_id: Dataset ID or URL-friendly name (e.g. "ontario-covid-19-cases")
    """
    ckan, cache = _get_deps(ctx)
    ds = await ckan.package_show(dataset_id)
    cache.store_dataset_metadata(ds["id"], ds)

    resources = []
    for r in ds.get("resources", []):
        resources.append({
            "id": r["id"],
            "name": r.get("name"),
            "format": r.get("format"),
            "size_bytes": r.get("size"),
            "url": r.get("url"),
            "last_modified": r.get("last_modified") or r.get("data_last_updated"),
            "datastore_active": r.get("datastore_active", False),
        })

    return json.dumps({
        "id": ds["id"],
        "name": ds.get("name"),
        "title": ds.get("title"),
        "description": ds.get("notes"),
        "organization": ds.get("organization", {}).get("title"),
        "maintainer": ds.get("maintainer_translated", {}).get("en") or ds.get("maintainer"),
        "license": ds.get("license_title"),
        "tags": [t["name"] for t in ds.get("tags", [])],
        "update_frequency": ds.get("update_frequency"),
        "created": ds.get("metadata_created"),
        "last_modified": ds.get("metadata_modified"),
        "access_level": ds.get("access_level"),
        "geographic_coverage": ds.get("geographic_coverage"),
        "resources": resources,
    }, indent=2, default=str)


@mcp.tool
async def list_resources(
    dataset_id: str,
    ctx: Context = None,
) -> str:
    """List all resources (files) in a dataset with their formats and sizes.

    Args:
        dataset_id: Dataset ID or name
    """
    ckan, _ = _get_deps(ctx)
    ds = await ckan.package_show(dataset_id)
    resources = []
    for r in ds.get("resources", []):
        resources.append({
            "id": r["id"],
            "name": r.get("name"),
            "format": r.get("format"),
            "size_bytes": r.get("size"),
            "url": r.get("url"),
            "last_modified": r.get("last_modified") or r.get("data_last_updated"),
            "datastore_active": r.get("datastore_active", False),
            "data_range": f"{r.get('data_range_start', '?')} to {r.get('data_range_end', '?')}",
        })
    return json.dumps({
        "dataset": ds.get("title"),
        "num_resources": len(resources),
        "resources": resources,
    }, indent=2, default=str)


@mcp.tool
async def get_resource_schema(
    resource_id: str,
    sample_size: int = 5,
    ctx: Context = None,
) -> str:
    """Get the column schema and sample values for a datastore resource.

    Args:
        resource_id: Resource ID (the resource must have datastore_active=True)
        sample_size: Number of sample rows to include
    """
    ckan, _ = _get_deps(ctx)
    result = await ckan.datastore_search(resource_id, limit=sample_size)

    fields = []
    for f in result.get("fields", []):
        if f["id"].startswith("_"):
            continue
        sample_values = [str(r.get(f["id"], "")) for r in result.get("records", [])]
        fields.append({
            "name": f["id"],
            "type": f.get("type", "unknown"),
            "sample_values": sample_values[:sample_size],
        })

    return json.dumps({
        "resource_id": resource_id,
        "total_records": result.get("total", 0),
        "num_columns": len(fields),
        "fields": fields,
    }, indent=2)


@mcp.tool
async def get_update_history(
    dataset_id: str,
    ctx: Context = None,
) -> str:
    """Check when a dataset was created, last modified, and its update frequency.

    Args:
        dataset_id: Dataset ID or name
    """
    ckan, _ = _get_deps(ctx)
    ds = await ckan.package_show(dataset_id)

    resource_updates = []
    for r in ds.get("resources", []):
        resource_updates.append({
            "name": r.get("name"),
            "format": r.get("format"),
            "created": r.get("created"),
            "last_modified": r.get("last_modified") or r.get("data_last_updated"),
        })

    return json.dumps({
        "dataset": ds.get("title"),
        "created": ds.get("metadata_created"),
        "last_modified": ds.get("metadata_modified"),
        "update_frequency": ds.get("update_frequency"),
        "current_as_of": ds.get("current_as_of"),
        "resource_updates": resource_updates,
    }, indent=2, default=str)


@mcp.tool
async def compare_datasets(
    dataset_ids: list[str],
    ctx: Context = None,
) -> str:
    """Compare metadata side-by-side for multiple datasets.

    Args:
        dataset_ids: List of dataset IDs or names to compare (2-5)
    """
    ckan, _ = _get_deps(ctx)
    comparisons = []
    for ds_id in dataset_ids[:5]:
        ds = await ckan.package_show(ds_id)
        resources = ds.get("resources", [])
        formats = sorted(set(r.get("format", "").upper() for r in resources if r.get("format")))
        comparisons.append({
            "id": ds["id"],
            "title": ds.get("title"),
            "organization": ds.get("organization", {}).get("title"),
            "num_resources": len(resources),
            "formats": formats,
            "update_frequency": ds.get("update_frequency"),
            "last_modified": ds.get("metadata_modified"),
            "tags": [t["name"] for t in ds.get("tags", [])],
            "license": ds.get("license_title"),
            "geographic_coverage": ds.get("geographic_coverage"),
        })

    # Find shared and unique tags
    all_tags = [set(c["tags"]) for c in comparisons]
    shared_tags = list(set.intersection(*all_tags)) if all_tags else []

    return json.dumps({
        "datasets": comparisons,
        "shared_tags": shared_tags,
    }, indent=2, default=str)
