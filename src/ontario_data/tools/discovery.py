from __future__ import annotations

import json

from fastmcp import Context

from ontario_data.server import READONLY, mcp
from ontario_data.utils import get_deps, json_response


@mcp.tool(annotations=READONLY)
async def search_datasets(
    query: str,
    organization: str | None = None,
    resource_format: str | None = None,
    update_frequency: str | None = None,
    sort_by: str = "relevance asc, metadata_modified desc",
    limit: int = 10,
    ctx: Context = None,
) -> str:
    """Search for datasets in Ontario's Open Data Catalogue.

    Args:
        query: Search terms (e.g. "covid cases", "housing prices", "school enrollment")
        organization: Filter by ministry/org (e.g. "health", "education")
        resource_format: Filter by file format (e.g. "CSV", "JSON", "SHP")
        update_frequency: Filter by frequency (e.g. "yearly", "monthly", "daily")
        sort_by: Sort order (default: relevance)
        limit: Max results to return (1-50)
    """
    ckan, _ = get_deps(ctx)
    filters = {}
    if organization:
        filters["organization"] = organization
    if resource_format:
        filters["res_format"] = resource_format
    if update_frequency:
        filters["update_frequency"] = update_frequency

    result = await ckan.package_search(
        query=query, filters=filters or None, sort=sort_by, rows=min(limit, 50),
    )

    datasets = []
    for ds in result["results"]:
        resources = ds.get("resources", [])
        formats = sorted(set(r.get("format", "").upper() for r in resources if r.get("format")))
        datasets.append({
            "id": ds["id"],
            "name": ds.get("name"),
            "title": ds.get("title"),
            "organization": ds.get("organization", {}).get("title", "Unknown"),
            "description": (ds.get("notes") or "")[:200],
            "formats": formats,
            "num_resources": len(resources),
            "last_modified": ds.get("metadata_modified"),
            "update_frequency": ds.get("update_frequency", "unknown"),
        })

    return json_response(
        total_count=result["count"],
        returned=len(datasets),
        datasets=datasets,
    )


@mcp.tool(annotations=READONLY)
async def list_organizations(
    include_counts: bool = True,
    ctx: Context = None,
) -> str:
    """List all Ontario government ministries and organizations with dataset counts.

    Use this to discover which ministries publish data and how much.
    """
    ckan, _ = get_deps(ctx)
    orgs = await ckan.organization_list(all_fields=True, include_dataset_count=include_counts)
    result = []
    for org in orgs:
        result.append({
            "name": org.get("name"),
            "title": org.get("title"),
            "dataset_count": org.get("package_count", 0),
            "description": (org.get("description") or "")[:150],
        })
    result.sort(key=lambda x: x["dataset_count"], reverse=True)
    return json.dumps(result, indent=2)


@mcp.tool(annotations=READONLY)
async def list_topics(
    query: str | None = None,
    ctx: Context = None,
) -> str:
    """List all tags/topics used in the Ontario Data Catalogue.

    Args:
        query: Optional filter to match tag names
    """
    ckan, _ = get_deps(ctx)
    tags = await ckan.tag_list(query=query, all_fields=True)
    if isinstance(tags, list) and tags and isinstance(tags[0], dict):
        result = [{"name": t["name"], "count": t.get("count", 0)} for t in tags]
    else:
        result = [{"name": t} for t in tags]
    return json.dumps(result, indent=2)


@mcp.tool(annotations=READONLY)
async def get_popular_datasets(
    sort: str = "recent",
    limit: int = 10,
    ctx: Context = None,
) -> str:
    """Get popular or recently updated datasets.

    Args:
        sort: "recent" for recently modified, "name" for alphabetical
        limit: Number of results (1-50)
    """
    ckan, _ = get_deps(ctx)
    sort_map = {
        "recent": "metadata_modified desc",
        "name": "title asc",
    }
    sort_str = sort_map.get(sort, "metadata_modified desc")
    result = await ckan.package_search(sort=sort_str, rows=min(limit, 50))

    datasets = []
    for ds in result["results"]:
        datasets.append({
            "id": ds["id"],
            "name": ds.get("name"),
            "title": ds.get("title"),
            "organization": ds.get("organization", {}).get("title", "Unknown"),
            "last_modified": ds.get("metadata_modified"),
            "update_frequency": ds.get("update_frequency", "unknown"),
        })
    return json_response(total=result["count"], datasets=datasets)


@mcp.tool(annotations=READONLY)
async def search_by_location(
    region: str,
    limit: int = 10,
    ctx: Context = None,
) -> str:
    """Find datasets covering a specific geographic area in Ontario.

    Args:
        region: Geographic area (e.g. "Toronto", "Northern Ontario", "Ottawa", "province-wide")
        limit: Max results
    """
    ckan, _ = get_deps(ctx)
    result = await ckan.package_search(
        query=region,
        filters=None,
        rows=min(limit, 50),
    )

    datasets = []
    for ds in result["results"]:
        datasets.append({
            "id": ds["id"],
            "title": ds.get("title"),
            "organization": ds.get("organization", {}).get("title", "Unknown"),
            "geographic_coverage": ds.get("geographic_coverage", "Not specified"),
            "description": (ds.get("notes") or "")[:200],
        })
    return json_response(total=result["count"], datasets=datasets)


@mcp.tool(annotations=READONLY)
async def find_related_datasets(
    dataset_id: str,
    limit: int = 10,
    ctx: Context = None,
) -> str:
    """Find datasets related to a given dataset by shared tags and organization.

    Args:
        dataset_id: The ID or name of the source dataset
        limit: Max related datasets to return
    """
    ckan, _ = get_deps(ctx)
    source = await ckan.package_show(dataset_id)
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
                    "id": ds["id"],
                    "title": ds.get("title"),
                    "organization": ds.get("organization", {}).get("title", "Unknown"),
                    "shared_tags": shared_tags,
                    "relevance": "tags",
                })

    if org:
        result = await ckan.package_search(filters={"organization": org}, rows=min(limit, 50))
        seen_ids = {r["id"] for r in related}
        for ds in result["results"]:
            if ds["id"] != source["id"] and ds["id"] not in seen_ids:
                related.append({
                    "id": ds["id"],
                    "title": ds.get("title"),
                    "organization": ds.get("organization", {}).get("title", "Unknown"),
                    "shared_tags": [],
                    "relevance": "same_organization",
                })

    return json_response(
        source={"id": source["id"], "title": source.get("title"), "tags": tags},
        related=related[:limit],
    )
