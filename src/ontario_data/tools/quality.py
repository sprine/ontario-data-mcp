from __future__ import annotations

import json
from datetime import datetime, timezone

from fastmcp import Context

from ontario_data.server import READONLY, mcp
from ontario_data.staleness import FREQUENCY_DAYS
from ontario_data.formatting import md_response
from ontario_data.utils import (
    get_cache,
    require_cached,
    resolve_dataset,
)


@mcp.tool(annotations=READONLY)
async def check_freshness(
    dataset_id: str,
    ctx: Context = None,
) -> str:
    """Check if a dataset is current by comparing its update frequency to its last modification date.

    Args:
        dataset_id: Prefixed dataset ID (e.g. "toronto:ttc-ridership") or bare ID
    """
    portal, bare_id, ds = await resolve_dataset(ctx, dataset_id)

    last_modified = ds.get("metadata_modified", "")
    frequency = ds.get("update_frequency", "unknown")
    current_as_of = ds.get("current_as_of", "")

    try:
        modified_dt = datetime.fromisoformat(last_modified.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        days_since_update = (now - modified_dt).days
    except (ValueError, AttributeError):
        days_since_update = None

    expected = FREQUENCY_DAYS.get(frequency)
    is_stale = days_since_update > expected if (days_since_update is not None and expected) else None

    resource_freshness = []
    for r in ds.get("resources", []):
        r_modified = r.get("last_modified") or r.get("data_last_updated")
        resource_freshness.append({
            "name": r.get("name"),
            "format": r.get("format"),
            "last_modified": r_modified,
        })

    return md_response(
        dataset=ds.get("title"),
        update_frequency=frequency,
        last_modified=last_modified,
        current_as_of=current_as_of,
        days_since_update=days_since_update,
        is_stale=is_stale,
        resources=resource_freshness,
    )


@mcp.tool(annotations=READONLY)
async def profile_data(
    resource_id: str,
    ctx: Context = None,
) -> str:
    """Statistical profile and quality check of a cached dataset.

    Uses DuckDB SUMMARIZE for column-level statistics (min, max, avg, std,
    nulls, unique counts). Also checks for duplicate rows and reports
    type warnings for VARCHAR columns that contain numeric values.

    Args:
        resource_id: Resource ID (must be cached)
    """
    cache = get_cache(ctx)
    table_name = require_cached(cache, resource_id)

    # Use DuckDB's SUMMARIZE command — one query for all column stats
    summary = cache.execute_sql_dict(f'SUMMARIZE SELECT * FROM "{table_name}"')

    # Get row count
    row_count = cache.execute_sql(f'SELECT COUNT(*) FROM "{table_name}"')[0][0]

    # Duplicate row check
    columns = cache.execute_sql(f'DESCRIBE "{table_name}"')
    col_names = ", ".join(f'"{col[0]}"' for col in columns)
    dup_result = cache.execute_sql(
        f'SELECT count(*) FROM ('
        f'SELECT {col_names}, count(*) OVER (PARTITION BY {col_names}) as _cnt '
        f'FROM "{table_name}"'
        f') WHERE _cnt > 1'
    )
    duplicate_rows = dup_result[0][0] if dup_result else 0

    # Read type warnings from cache metadata (detected at download time)
    type_warnings = []
    try:
        meta_rows = cache.execute_sql(
            "SELECT type_warnings FROM _cache_metadata WHERE table_name = ?",
            [table_name],
        )
        if meta_rows and meta_rows[0][0]:
            type_warnings = json.loads(meta_rows[0][0])
    except Exception:
        pass

    result = dict(
        resource_id=resource_id,
        table_name=table_name,
        row_count=row_count,
        duplicate_rows=duplicate_rows,
        columns=summary,
    )
    if type_warnings:
        result["type_warnings"] = type_warnings
        result["hint"] = (
            f"Columns {type_warnings} are VARCHAR but contain numbers. "
            f"Use TRY_CAST(col AS DOUBLE) for comparisons."
        )

    return md_response(**result)
