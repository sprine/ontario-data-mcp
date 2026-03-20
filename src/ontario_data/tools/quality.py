"""Data quality tools: freshness, profiling, and statistical summaries.

These two tools answer "is this data good?" separately from "what does this
data say?" — a distinction that matters for data journalism and policy analysis
where stale or unreliable data can lead to incorrect conclusions.

Why separate from the querying tools?

- ``check_freshness`` — compares a dataset's last-modified timestamp against its
  declared update frequency to flag stale data. This operates on dataset metadata
  (no download required) and uses a frequency-to-days mapping (staleness.py).
  Kept separate from get_dataset_info because: (1) it computes a derived
  staleness verdict rather than surfacing raw metadata; (2) it should be called
  *before* downloading to avoid caching stale data.
- ``profile_data`` — runs DuckDB SUMMARIZE over a cached table, returning min,
  max, mean, null counts, and approximate distinct values per column. Requires
  a prior download_resource call. Kept separate from query_cached because:
  (1) SUMMARIZE syntax is non-obvious and model-generated SUMMARIZE queries are
  often malformed; (2) profile_data adds null-percentage and duplicate-row
  detection on top of raw SUMMARIZE output; (3) it's a distinct task
  (understand the shape of data) vs. query_cached (answer a specific question).

Consolidation considered: profile_data could theoretically be expressed as a
query_cached call with SUMMARIZE. In practice, models frequently write incorrect
SUMMARIZE syntax. A dedicated tool that handles this correctly is more reliable
than hoping the model writes the right incantation every time.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastmcp import Context

logger = logging.getLogger("ontario_data.quality")

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
    nulls, unique counts). Also checks for duplicate rows.

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

    return md_response(
        resource_id=resource_id,
        table_name=table_name,
        row_count=row_count,
        duplicate_rows=duplicate_rows,
        columns=summary,
    )
