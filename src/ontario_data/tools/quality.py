from __future__ import annotations

import re
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

_NUMERIC_RE = re.compile(r"^-?\d+\.?\d*$")


@mcp.tool(annotations=READONLY)
async def check_data_quality(
    resource_id: str,
    ctx: Context = None,
) -> str:
    """Analyze data quality: null counts, type consistency, duplicates, and outliers.

    Resource must be cached locally first (use download_resource).
    Reports numeric statistics (min, max, mean, stddev) for numeric columns and
    for VARCHAR columns that contain numeric values (detected automatically).

    Only reports numeric statistics for columns with numeric DuckDB types. VARCHAR
    columns containing numbers will show null/distinct counts plus numeric stats
    via TRY_CAST — use TRY_CAST in your queries too.

    Args:
        resource_id: Resource ID
    """
    cache = get_cache(ctx)
    table_name = require_cached(cache, resource_id)

    # Get total rows
    total = cache.execute_sql(f'SELECT count(*) FROM "{table_name}"')[0][0]

    # Get column info
    columns = cache.execute_sql(f'DESCRIBE "{table_name}"')

    quality_report = []
    for col in columns:
        col_name, col_type = col[0], col[1]
        stats = {"name": col_name, "type": col_type}

        # Null count
        null_count = cache.execute_sql(
            f'SELECT count(*) FROM "{table_name}" WHERE "{col_name}" IS NULL'
        )[0][0]
        stats["null_count"] = null_count
        stats["null_pct"] = round(null_count / total * 100, 1) if total > 0 else 0

        # Distinct values
        distinct = cache.execute_sql(
            f'SELECT count(DISTINCT "{col_name}") FROM "{table_name}"'
        )[0][0]
        stats["distinct_count"] = distinct
        stats["cardinality_pct"] = round(distinct / total * 100, 1) if total > 0 else 0

        # For numeric columns: min, max, mean, stddev
        if any(t in col_type.upper() for t in ("INT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC")):
            num_stats = cache.execute_sql(
                f'SELECT min("{col_name}"), max("{col_name}"), avg("{col_name}"), stddev("{col_name}") FROM "{table_name}"'
            )[0]
            stats["min"] = num_stats[0]
            stats["max"] = num_stats[1]
            stats["mean"] = round(float(num_stats[2]), 4) if num_stats[2] is not None else None
            stats["stddev"] = round(float(num_stats[3]), 4) if num_stats[3] is not None else None
        elif "VARCHAR" in col_type.upper():
            # Check if VARCHAR values look numeric
            sample = cache.execute_sql(
                f'SELECT DISTINCT "{col_name}" FROM "{table_name}" '
                f'WHERE "{col_name}" IS NOT NULL LIMIT 100'
            )
            values = [str(r[0]) for r in sample if r[0] is not None and str(r[0]).strip()]
            if values:
                numeric_count = sum(1 for v in values if _NUMERIC_RE.match(v.strip()))
                if numeric_count / len(values) > 0.8:
                    stats["type_note"] = "VARCHAR but values appear numeric — use TRY_CAST for queries"
                    try:
                        num_stats = cache.execute_sql(
                            f'SELECT min(TRY_CAST("{col_name}" AS DOUBLE)), '
                            f'max(TRY_CAST("{col_name}" AS DOUBLE)), '
                            f'avg(TRY_CAST("{col_name}" AS DOUBLE)) '
                            f'FROM "{table_name}"'
                        )[0]
                        stats["min"] = num_stats[0]
                        stats["max"] = num_stats[1]
                        stats["mean"] = round(float(num_stats[2]), 4) if num_stats[2] is not None else None
                    except Exception:
                        pass

        quality_report.append(stats)

    # Duplicate row check using COLUMNS(*)
    col_names = ", ".join(f'"{col[0]}"' for col in columns)
    dup_count = cache.execute_sql(
        f'SELECT count(*) FROM (SELECT {col_names}, count(*) OVER (PARTITION BY {col_names}) as _cnt FROM "{table_name}") WHERE _cnt > 1'
    )[0]

    return md_response(
        resource_id=resource_id,
        table_name=table_name,
        total_rows=total,
        duplicate_rows=dup_count[0] if dup_count else 0,
        columns=quality_report,
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
    """Statistical profile of a cached dataset using DuckDB SUMMARIZE.

    Provides column-level statistics (min, max, avg, std, nulls, unique counts).

    Args:
        resource_id: Resource ID (must be cached)
    """
    cache = get_cache(ctx)
    table_name = require_cached(cache, resource_id)

    # Use DuckDB's SUMMARIZE command
    summary = cache.execute_sql_dict(f'SUMMARIZE SELECT * FROM "{table_name}"')

    # Get row count
    row_count = cache.execute_sql(f'SELECT COUNT(*) as cnt FROM "{table_name}"')[0][0]

    return md_response(
        resource_id=resource_id,
        table_name=table_name,
        row_count=row_count,
        columns=summary,
    )
