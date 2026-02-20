from __future__ import annotations

import json
import logging

from fastmcp import Context

from ontario_data.portals import PORTALS
from ontario_data.server import READONLY, mcp
from ontario_data.utils import (
    _lifespan_state,
    fan_out,
    get_deps,
    json_response,
    parse_portal_id,
)

logger = logging.getLogger("ontario_data.discovery")


@mcp.tool(annotations=READONLY)
async def search_datasets(
    query: str,
    organization: str | None = None,
    resource_format: str | None = None,
    update_frequency: str | None = None,
    sort_by: str = "relevance asc, metadata_modified desc",
    limit: int = 5,
    portal: str | None = None,
    ctx: Context = None,
) -> str:
    """Search for datasets across all open data portals.

    Args:
        query: Search terms (e.g. "covid cases", "housing prices", "school enrollment")
        organization: Filter by ministry/org (e.g. "health", "education")
        resource_format: Filter by file format (e.g. "CSV", "JSON", "SHP")
        update_frequency: Filter by frequency (e.g. "yearly", "monthly", "daily")
        sort_by: Sort order (default: relevance)
        limit: Max results per portal (1-50)
        portal: Narrow to one portal (e.g. "ontario", "toronto"). Default: all portals.
    """
    configs = _lifespan_state(ctx)["portal_configs"]
    filters = {}
    if organization:
        filters["organization"] = organization
    if resource_format:
        filters["res_format"] = resource_format
    if update_frequency:
        filters["update_frequency"] = update_frequency

    async def _search_one(portal_key: str) -> dict:
        ckan, _ = get_deps(ctx, portal_key)
        result = await ckan.package_search(
            query=query, filters=filters or None, sort=sort_by, rows=min(limit, 50),
        )
        datasets = []
        for ds in result["results"]:
            resources = ds.get("resources", [])
            formats = sorted(set(r.get("format", "").upper() for r in resources if r.get("format")))
            datasets.append({
                "id": f"{portal_key}:{ds['id']}",
                "name": ds.get("name"),
                "title": ds.get("title"),
                "organization": ds.get("organization", {}).get("title", "Unknown"),
                "description": (ds.get("notes") or "")[:200],
                "formats": formats,
                "num_resources": len(resources),
                "last_modified": ds.get("metadata_modified"),
                "update_frequency": ds.get("update_frequency", "unknown"),
            })
        return {
            "portal": portal_key,
            "portal_name": configs[portal_key].name,
            "total_count": result["count"],
            "returned": len(datasets),
            "datasets": datasets,
        }

    raw = await fan_out(ctx, portal, _search_one)

    results = []
    skipped = []
    for portal_key, result, error in raw:
        if error:
            skipped.append({"portal": portal_key, "portal_name": configs[portal_key].name, "reason": error})
        else:
            results.append(result)

    return json_response(
        query=query,
        portals_searched=len(results),
        results=results,
        skipped=skipped,
    )


@mcp.tool(annotations=READONLY)
async def list_organizations(
    include_counts: bool = True,
    portal: str | None = None,
    ctx: Context = None,
) -> str:
    """List government ministries and organizations with dataset counts across all portals.

    Args:
        include_counts: Include dataset counts per organization
        portal: Narrow to one portal. Default: all portals.
    """

    async def _list_orgs(portal_key: str) -> list[dict]:
        ckan, _ = get_deps(ctx, portal_key)
        orgs = await ckan.organization_list(all_fields=True, include_dataset_count=include_counts)
        result = []
        for org in orgs:
            result.append({
                "portal": portal_key,
                "name": org.get("name"),
                "title": org.get("title"),
                "dataset_count": org.get("package_count", 0),
                "description": (org.get("description") or "")[:150],
            })
        result.sort(key=lambda x: x["dataset_count"], reverse=True)
        return result

    raw = await fan_out(ctx, portal, _list_orgs)
    all_orgs = []
    for _, result, error in raw:
        if result and not error:
            all_orgs.extend(result)
    return json.dumps(all_orgs, indent=2)


@mcp.tool(annotations=READONLY)
async def list_topics(
    query: str | None = None,
    portal: str | None = None,
    ctx: Context = None,
) -> str:
    """List all tags/topics used across data portals.

    Args:
        query: Optional filter to match tag names
        portal: Narrow to one portal. Default: all portals.
    """

    async def _list_tags(portal_key: str) -> list[dict]:
        ckan, _ = get_deps(ctx, portal_key)
        tags = await ckan.tag_list(query=query, all_fields=True)
        if isinstance(tags, list) and tags and isinstance(tags[0], dict):
            return [{"portal": portal_key, "name": t["name"], "count": t.get("count", 0)} for t in tags]
        return [{"portal": portal_key, "name": t} for t in tags]

    raw = await fan_out(ctx, portal, _list_tags)
    all_tags = []
    for _, result, error in raw:
        if result and not error:
            all_tags.extend(result)
    return json.dumps(all_tags, indent=2)


@mcp.tool(annotations=READONLY)
async def get_popular_datasets(
    sort: str = "recent",
    limit: int = 10,
    portal: str | None = None,
    ctx: Context = None,
) -> str:
    """Get popular or recently updated datasets across all portals.

    Args:
        sort: "recent" for recently modified, "name" for alphabetical
        limit: Number of results per portal (1-50)
        portal: Narrow to one portal. Default: all portals.
    """
    sort_map = {
        "recent": "metadata_modified desc",
        "name": "title asc",
    }
    sort_str = sort_map.get(sort, "metadata_modified desc")

    async def _popular(portal_key: str) -> dict:
        ckan, _ = get_deps(ctx, portal_key)
        result = await ckan.package_search(sort=sort_str, rows=min(limit, 50))
        datasets = []
        for ds in result["results"]:
            datasets.append({
                "id": f"{portal_key}:{ds['id']}",
                "name": ds.get("name"),
                "title": ds.get("title"),
                "organization": ds.get("organization", {}).get("title", "Unknown"),
                "last_modified": ds.get("metadata_modified"),
                "update_frequency": ds.get("update_frequency", "unknown"),
            })
        return {"portal": portal_key, "total": result["count"], "datasets": datasets}

    raw = await fan_out(ctx, portal, _popular)
    results = [result for _, result, error in raw if result and not error]
    return json_response(results=results)


@mcp.tool(annotations=READONLY)
async def search_by_location(
    region: str,
    limit: int = 10,
    portal: str | None = None,
    ctx: Context = None,
) -> str:
    """Find datasets covering a specific geographic area.

    Args:
        region: Geographic area (e.g. "Toronto", "Northern Ontario", "Ottawa", "province-wide")
        limit: Max results per portal
        portal: Narrow to one portal. Default: all portals.
    """

    async def _search_location(portal_key: str) -> dict:
        ckan, _ = get_deps(ctx, portal_key)
        result = await ckan.package_search(query=region, filters=None, rows=min(limit, 50))
        datasets = []
        for ds in result["results"]:
            datasets.append({
                "id": f"{portal_key}:{ds['id']}",
                "title": ds.get("title"),
                "organization": ds.get("organization", {}).get("title", "Unknown"),
                "geographic_coverage": ds.get("geographic_coverage", "Not specified"),
                "description": (ds.get("notes") or "")[:200],
            })
        return {"portal": portal_key, "total": result["count"], "datasets": datasets}

    raw = await fan_out(ctx, portal, _search_location)
    results = [result for _, result, error in raw if result and not error]
    return json_response(results=results)


@mcp.tool(annotations=READONLY)
async def find_related_datasets(
    dataset_id: str,
    limit: int = 10,
    ctx: Context = None,
) -> str:
    """Find datasets related to a given dataset by shared tags and organization.

    Searches within the same portal as the source dataset only.

    Args:
        dataset_id: Prefixed dataset ID (e.g. "toronto:ttc-ridership") or bare ID
        limit: Max related datasets to return
    """
    configs = _lifespan_state(ctx)["portal_configs"]
    portal, bare_id = parse_portal_id(dataset_id, set(configs.keys()))

    async def _show(pk: str):
        ckan, _ = get_deps(ctx, pk)
        return await ckan.package_show(bare_id)

    if portal:
        ckan, _ = get_deps(ctx, portal)
        source = await ckan.package_show(bare_id)
    else:
        results = await fan_out(ctx, None, _show, first_match=True)
        if not results or results[0][2] is not None:
            errors = "; ".join(f"{pk}: {err}" for pk, _, err in results) if results else "no portals available"
            raise ValueError(
                f"Dataset '{bare_id}' not found. Tried: {errors}. "
                f"Use search_datasets(query='{bare_id}') to find the correct prefixed ID."
            )
        portal = results[0][0]
        source = results[0][1]

    ckan, _ = get_deps(ctx, portal)
    tags = [t["name"] for t in source.get("tags", [])]
    org = source.get("organization", {}).get("name", "")

    related = []
    if tags:
        tag_query = " OR ".join(tags[:5])
        result = await ckan.package_search(query=tag_query, rows=min(limit + 5, 50))
        for ds in result["results"]:
            if ds["id"] != source["id"]:
                shared_tags = [t["name"] for t in ds.get("tags", []) if t["name"] in tags]
                related.append({
                    "id": f"{portal}:{ds['id']}",
                    "title": ds.get("title"),
                    "organization": ds.get("organization", {}).get("title", "Unknown"),
                    "shared_tags": shared_tags,
                    "relevance": "tags",
                })

    if org:
        result = await ckan.package_search(filters={"organization": org}, rows=min(limit, 50))
        seen_ids = {r["id"] for r in related}
        for ds in result["results"]:
            if ds["id"] != source["id"] and f"{portal}:{ds['id']}" not in seen_ids:
                related.append({
                    "id": f"{portal}:{ds['id']}",
                    "title": ds.get("title"),
                    "organization": ds.get("organization", {}).get("title", "Unknown"),
                    "shared_tags": [],
                    "relevance": "same_organization",
                })

    return json_response(
        source={"id": f"{portal}:{source['id']}", "title": source.get("title"), "tags": tags},
        related=related[:limit],
    )


@mcp.tool(annotations=READONLY)
async def list_portals(
    ctx: Context = None,
) -> str:
    """List all available data portals with their platform type and descriptions."""
    state = _lifespan_state(ctx)
    configs = state["portal_configs"]

    portals = []
    for key, config in configs.items():
        portals.append({
            "key": key,
            "name": config.name,
            "base_url": config.base_url,
            "portal_type": str(config.portal_type),
            "description": config.description,
        })

    return json_response(portals=portals)
