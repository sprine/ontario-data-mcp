from __future__ import annotations

from fastmcp import Context

from ontario_data.server import READONLY, mcp
from ontario_data.utils import (
    _lifespan_state,
    fan_out,
    get_deps,
    json_response,
    parse_portal_id,
)


@mcp.tool(annotations=READONLY)
async def get_dataset_info(
    dataset_id: str,
    ctx: Context = None,
) -> str:
    """Get full metadata for a dataset including all resources.

    Args:
        dataset_id: Prefixed dataset ID (e.g. "toronto:ttc-ridership") or bare ID
    """
    configs = _lifespan_state(ctx)["portal_configs"]
    portal, bare_id = parse_portal_id(dataset_id, set(configs.keys()))

    async def _show(pk: str):
        ckan, _ = get_deps(ctx, pk)
        return await ckan.package_show(bare_id)

    if portal:
        ckan, cache = get_deps(ctx, portal)
        ds = await ckan.package_show(bare_id)
    else:
        results = await fan_out(ctx, None, _show, first_match=True)
        if not results or results[0][2] is not None:
            errors = "; ".join(f"{pk}: {err}" for pk, _, err in results) if results else "no portals available"
            raise ValueError(
                f"Dataset '{bare_id}' not found. Tried: {errors}. "
                f"Use search_datasets(query='{bare_id}') to find the correct prefixed ID."
            )
        portal = results[0][0]
        ds = results[0][1]
        _, cache = get_deps(ctx, portal)

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

    return json_response(
        id=f"{portal}:{ds['id']}",
        name=ds.get("name"),
        title=ds.get("title"),
        description=ds.get("notes"),
        organization=ds.get("organization", {}).get("title"),
        maintainer=ds.get("maintainer_translated", {}).get("en") or ds.get("maintainer"),
        license=ds.get("license_title"),
        tags=[t["name"] for t in ds.get("tags", [])],
        update_frequency=ds.get("update_frequency"),
        created=ds.get("metadata_created"),
        last_modified=ds.get("metadata_modified"),
        access_level=ds.get("access_level"),
        geographic_coverage=ds.get("geographic_coverage"),
        resources=resources,
    )


@mcp.tool(annotations=READONLY)
async def list_resources(
    dataset_id: str,
    ctx: Context = None,
) -> str:
    """List all resources (files) in a dataset with their formats and sizes.

    Args:
        dataset_id: Prefixed dataset ID (e.g. "toronto:ttc-ridership") or bare ID
    """
    configs = _lifespan_state(ctx)["portal_configs"]
    portal, bare_id = parse_portal_id(dataset_id, set(configs.keys()))

    async def _show(pk: str):
        ckan, _ = get_deps(ctx, pk)
        return await ckan.package_show(bare_id)

    if portal:
        ckan, _ = get_deps(ctx, portal)
        ds = await ckan.package_show(bare_id)
    else:
        results = await fan_out(ctx, None, _show, first_match=True)
        if not results or results[0][2] is not None:
            errors = "; ".join(f"{pk}: {err}" for pk, _, err in results) if results else "no portals available"
            raise ValueError(
                f"Dataset '{bare_id}' not found. Tried: {errors}. "
                f"Use search_datasets(query='{bare_id}') to find the correct prefixed ID."
            )
        portal = results[0][0]
        ds = results[0][1]

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
    return json_response(
        dataset=ds.get("title"),
        dataset_id=f"{portal}:{ds['id']}",
        num_resources=len(resources),
        resources=resources,
    )


@mcp.tool(annotations=READONLY)
async def get_resource_schema(
    resource_id: str,
    sample_size: int = 5,
    ctx: Context = None,
) -> str:
    """Get the column schema and sample values for a datastore resource.

    Args:
        resource_id: Prefixed resource ID (e.g. "toronto:abc123") or bare ID
        sample_size: Number of sample rows to include
    """
    configs = _lifespan_state(ctx)["portal_configs"]
    portal, bare_id = parse_portal_id(resource_id, set(configs.keys()))

    async def _schema(pk: str):
        ckan, _ = get_deps(ctx, pk)
        resource = await ckan.resource_show(bare_id)
        if not resource.get("datastore_active"):
            fmt = (resource.get("format") or "unknown").upper()
            return {
                "resource_id": bare_id,
                "datastore_active": False,
                "format": fmt,
                "hint": f"This {fmt} resource has no datastore. Use download_resource to cache it locally, then query_cached to analyze.",
            }
        result = await ckan.datastore_search(bare_id, limit=sample_size)
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
        return {
            "resource_id": bare_id,
            "total_records": result.get("total", 0),
            "num_columns": len(fields),
            "fields": fields,
        }

    if portal:
        data = await _schema(portal)
    else:
        results = await fan_out(ctx, None, _schema, first_match=True)
        if not results or results[0][2] is not None:
            errors = "; ".join(f"{pk}: {err}" for pk, _, err in results) if results else "no portals available"
            raise ValueError(
                f"Resource '{bare_id}' not found. Tried: {errors}. "
                f"Use search_datasets to find the correct prefixed ID."
            )
        data = results[0][1]

    return json_response(**data)


@mcp.tool(annotations=READONLY)
async def compare_datasets(
    dataset_ids: list[str],
    ctx: Context = None,
) -> str:
    """Compare metadata side-by-side for multiple datasets (can be cross-portal).

    Args:
        dataset_ids: List of prefixed dataset IDs (e.g. ["toronto:abc", "ontario:def"]) to compare (2-5)
    """
    configs = _lifespan_state(ctx)["portal_configs"]
    known = set(configs.keys())

    comparisons = []
    for ds_id in dataset_ids[:5]:
        portal, bare_id = parse_portal_id(ds_id, known)

        async def _show(pk: str, bid=bare_id):
            ckan, _ = get_deps(ctx, pk)
            return await ckan.package_show(bid)

        if portal:
            ckan, _ = get_deps(ctx, portal)
            ds = await ckan.package_show(bare_id)
        else:
            results = await fan_out(ctx, None, _show, first_match=True)
            if not results or results[0][2] is not None:
                errors = "; ".join(f"{pk}: {err}" for pk, _, err in results) if results else "no portals available"
                raise ValueError(
                    f"Dataset '{bare_id}' not found. Tried: {errors}. "
                    f"Use search_datasets(query='{bare_id}') to find the correct prefixed ID."
                )
            portal = results[0][0]
            ds = results[0][1]

        resources = ds.get("resources", [])
        formats = sorted(set(r.get("format", "").upper() for r in resources if r.get("format")))
        comparisons.append({
            "id": f"{portal}:{ds['id']}",
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

    all_tags = [set(c["tags"]) for c in comparisons]
    shared_tags = list(set.intersection(*all_tags)) if all_tags else []

    return json_response(datasets=comparisons, shared_tags=shared_tags)
