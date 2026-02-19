from __future__ import annotations

import io
import logging
import httpx
from fastmcp import Context

from ontario_data.server import READONLY, mcp
from ontario_data.utils import (
    SpatialExtensionError,
    get_active_portal,
    get_cache,
    get_deps,
    json_response,
    make_geo_table_name,
    require_cached,
)

logger = logging.getLogger("ontario_data.geospatial")


@mcp.tool(annotations=READONLY)
async def load_geodata(
    resource_id: str,
    force_refresh: bool = False,
    portal: str | None = None,
    ctx: Context = None,
) -> str:
    """Download and cache a geospatial resource (SHP, KML, GeoJSON) into DuckDB with spatial support.

    Args:
        resource_id: Resource ID for a geospatial file
        force_refresh: Re-download even if cached
    """
    import geopandas as gpd
    import pandas as pd

    ckan, cache = get_deps(ctx, portal=portal)

    if cache.is_cached(resource_id) and not force_refresh:
        table_name = cache.get_table_name(resource_id)
        return json_response(status="already_cached", table_name=table_name)

    resource = await ckan.resource_show(resource_id)
    dataset_id = resource.get("package_id", "")
    dataset = await ckan.package_show(dataset_id) if dataset_id else {}
    fmt = (resource.get("format") or "").upper()
    url = resource.get("url", "")

    await ctx.report_progress(0, 100, "Downloading geospatial data...")

    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
        response = await client.get(url)
        response.raise_for_status()
        content = response.content

    await ctx.report_progress(50, 100, "Parsing geospatial data...")

    if fmt == "GEOJSON":
        gdf = gpd.read_file(io.BytesIO(content), driver="GeoJSON")
    elif fmt == "KML":
        gdf = gpd.read_file(io.BytesIO(content), driver="KML")
    elif fmt in ("SHP", "ZIP"):
        import tempfile
        import zipfile
        with tempfile.TemporaryDirectory() as tmpdir:
            if fmt == "ZIP" or content[:4] == b"PK\x03\x04":
                with zipfile.ZipFile(io.BytesIO(content)) as zf:
                    zf.extractall(tmpdir)
                gdf = gpd.read_file(tmpdir)
            else:
                raise ValueError("SHP files must be provided as ZIP archives")
    else:
        raise ValueError(f"Unsupported geospatial format: {fmt}")

    await ctx.report_progress(80, 100, "Storing in DuckDB...")

    # Convert geometry to WKT for DuckDB storage
    df = gdf.copy()
    if "geometry" in df.columns:
        df["geometry_wkt"] = df["geometry"].apply(lambda g: g.wkt if g else None)
        df["geometry_type"] = df["geometry"].apply(lambda g: g.geom_type if g else None)
        if hasattr(gdf, "crs") and gdf.crs:
            df["crs"] = str(gdf.crs)
        bounds = gdf.total_bounds  # [minx, miny, maxx, maxy]
        df = df.drop(columns=["geometry"])
    else:
        bounds = None

    active = portal or get_active_portal(ctx)
    table_name = make_geo_table_name(dataset.get("name", ""), resource_id, portal=active)

    cache.store_resource(
        resource_id=resource_id,
        dataset_id=dataset_id,
        table_name=table_name,
        df=pd.DataFrame(df),
        source_url=url,
    )

    await ctx.report_progress(100, 100, "Done")

    return json_response(
        status="loaded",
        table_name=table_name,
        row_count=len(df),
        columns=list(df.columns),
        geometry_types=df["geometry_type"].unique().tolist() if "geometry_type" in df.columns else [],
        bounds={"minx": bounds[0], "miny": bounds[1], "maxx": bounds[2], "maxy": bounds[3]} if bounds is not None else None,
        crs=str(gdf.crs) if hasattr(gdf, "crs") and gdf.crs else None,
        hint=f'Query with: SELECT * FROM "{table_name}" LIMIT 10',
    )


@mcp.tool(annotations=READONLY)
async def spatial_query(
    resource_id: str,
    operation: str,
    latitude: float | None = None,
    longitude: float | None = None,
    radius_km: float | None = None,
    bbox: list[float] | None = None,
    limit: int = 100,
    ctx: Context = None,
) -> str:
    """Run spatial queries against cached geospatial data.

    Args:
        resource_id: Resource ID (must be cached via load_geodata)
        operation: "contains_point", "within_bbox", or "within_radius"
        latitude: Latitude for point queries
        longitude: Longitude for point queries
        radius_km: Radius in kilometers (for within_radius)
        bbox: Bounding box as [min_lng, min_lat, max_lng, max_lat] (for within_bbox)
        limit: Max results
    """
    cache = get_cache(ctx)
    table_name = require_cached(cache, resource_id)

    if not cache.has_spatial_extension:
        raise SpatialExtensionError(
            "DuckDB spatial extension is not available. "
            "Spatial queries require the spatial extension to be installed."
        )

    if operation == "contains_point" and latitude is not None and longitude is not None:
        sql = f"""
            SELECT *, ST_Distance(
                ST_GeomFromText(geometry_wkt),
                ST_Point({longitude}, {latitude})
            ) as distance
            FROM "{table_name}"
            WHERE geometry_wkt IS NOT NULL
            AND ST_Contains(ST_GeomFromText(geometry_wkt), ST_Point({longitude}, {latitude}))
            LIMIT {limit}
        """
    elif operation == "within_radius" and latitude is not None and longitude is not None and radius_km is not None:
        degree_radius = radius_km / 111.0
        sql = f"""
            SELECT *, ST_Distance(
                ST_GeomFromText(geometry_wkt),
                ST_Point({longitude}, {latitude})
            ) * 111.0 as distance_km
            FROM "{table_name}"
            WHERE geometry_wkt IS NOT NULL
            AND ST_DWithin(
                ST_GeomFromText(geometry_wkt),
                ST_Point({longitude}, {latitude}),
                {degree_radius}
            )
            ORDER BY distance_km
            LIMIT {limit}
        """
    elif operation == "within_bbox" and bbox and len(bbox) == 4:
        min_lng, min_lat, max_lng, max_lat = bbox
        sql = f"""
            SELECT *
            FROM "{table_name}"
            WHERE geometry_wkt IS NOT NULL
            AND ST_Intersects(
                ST_GeomFromText(geometry_wkt),
                ST_MakeEnvelope({min_lng}, {min_lat}, {max_lng}, {max_lat})
            )
            LIMIT {limit}
        """
    else:
        raise ValueError(
            f"Invalid operation '{operation}' or missing parameters. "
            f"Valid: contains_point (lat, lng), within_radius (lat, lng, radius_km), within_bbox (bbox)"
        )

    # Execute directly since spatial SQL contains functions not in allowed prefixes
    records = cache.execute_sql_dict(sql)

    return json_response(operation=operation, result_count=len(records), records=records)


@mcp.tool(annotations=READONLY)
async def list_geo_datasets(
    format_filter: str | None = None,
    limit: int = 50,
    portal: str | None = None,
    ctx: Context = None,
) -> str:
    """Find all datasets that contain geospatial resources (SHP, KML, GeoJSON).

    Args:
        format_filter: Filter to specific format: "SHP", "KML", "GEOJSON", or None for all
        limit: Max results
    """
    ckan, _ = get_deps(ctx, portal=portal)
    geo_formats = [format_filter.upper()] if format_filter else ["SHP", "KML", "GEOJSON"]

    all_datasets = []
    seen_ids = set()
    for fmt in geo_formats:
        result = await ckan.package_search(filters={"res_format": fmt}, rows=min(limit, 50))
        for ds in result["results"]:
            if ds["id"] not in seen_ids:
                seen_ids.add(ds["id"])
                geo_resources = [
                    {"id": r["id"], "name": r.get("name"), "format": r.get("format"), "size": r.get("size")}
                    for r in ds.get("resources", [])
                    if (r.get("format") or "").upper() in ("SHP", "KML", "GEOJSON", "ZIP")
                ]
                all_datasets.append({
                    "id": ds["id"],
                    "title": ds.get("title"),
                    "organization": ds.get("organization", {}).get("title"),
                    "geo_resources": geo_resources,
                })

    return json_response(total=len(all_datasets), datasets=all_datasets[:limit])
