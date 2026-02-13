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
async def check_data_quality(
    resource_id: str,
    ctx: Context = None,
) -> str:
    """Analyze data quality: null counts, type consistency, duplicates, and outliers.

    Resource must be cached locally first (use download_resource).

    Args:
        resource_id: Resource ID
    """
    _, cache = _get_deps(ctx)
    table_name = cache.get_table_name(resource_id)
    if not table_name:
        return json.dumps({"error": f"Resource {resource_id} not cached. Use download_resource first."})

    # Get total rows
    total = cache.conn.execute(f'SELECT count(*) FROM "{table_name}"').fetchone()[0]

    # Get column info
    columns = cache.conn.execute(f"DESCRIBE \"{table_name}\"").fetchall()

    quality_report = []
    for col in columns:
        col_name, col_type = col[0], col[1]
        stats = {}
        stats["name"] = col_name
        stats["type"] = col_type

        # Null count
        null_count = cache.conn.execute(
            f'SELECT count(*) FROM "{table_name}" WHERE "{col_name}" IS NULL'
        ).fetchone()[0]
        stats["null_count"] = null_count
        stats["null_pct"] = round(null_count / total * 100, 1) if total > 0 else 0

        # Distinct values
        distinct = cache.conn.execute(
            f'SELECT count(DISTINCT "{col_name}") FROM "{table_name}"'
        ).fetchone()[0]
        stats["distinct_count"] = distinct
        stats["cardinality_pct"] = round(distinct / total * 100, 1) if total > 0 else 0

        # For numeric columns: min, max, mean, stddev
        if "INT" in col_type.upper() or "FLOAT" in col_type.upper() or "DOUBLE" in col_type.upper() or "DECIMAL" in col_type.upper() or "NUMERIC" in col_type.upper():
            num_stats = cache.conn.execute(
                f'SELECT min("{col_name}"), max("{col_name}"), avg("{col_name}"), stddev("{col_name}") FROM "{table_name}"'
            ).fetchone()
            stats["min"] = num_stats[0]
            stats["max"] = num_stats[1]
            stats["mean"] = round(float(num_stats[2]), 4) if num_stats[2] is not None else None
            stats["stddev"] = round(float(num_stats[3]), 4) if num_stats[3] is not None else None

        quality_report.append(stats)

    # Duplicate row check
    dup_count = cache.conn.execute(
        f'SELECT count(*) FROM (SELECT *, count(*) OVER (PARTITION BY * ) as _cnt FROM "{table_name}") WHERE _cnt > 1'
    ).fetchone()

    return json.dumps({
        "resource_id": resource_id,
        "table_name": table_name,
        "total_rows": total,
        "duplicate_rows": dup_count[0] if dup_count else 0,
        "columns": quality_report,
    }, indent=2, default=str)


@mcp.tool
async def check_freshness(
    dataset_id: str,
    ctx: Context = None,
) -> str:
    """Check if a dataset is current by comparing its update frequency to its last modification date.

    Args:
        dataset_id: Dataset ID or name
    """
    ckan, _ = _get_deps(ctx)
    ds = await ckan.package_show(dataset_id)

    from datetime import datetime, timezone

    last_modified = ds.get("metadata_modified", "")
    frequency = ds.get("update_frequency", "unknown")
    current_as_of = ds.get("current_as_of", "")

    # Parse last modified
    try:
        modified_dt = datetime.fromisoformat(last_modified.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        days_since_update = (now - modified_dt).days
    except (ValueError, AttributeError):
        days_since_update = None

    # Expected update intervals
    freq_days = {
        "daily": 2,
        "weekly": 10,
        "monthly": 45,
        "quarterly": 120,
        "biannually": 200,
        "yearly": 400,
    }
    expected = freq_days.get(frequency)
    is_stale = days_since_update > expected if (days_since_update is not None and expected) else None

    resource_freshness = []
    for r in ds.get("resources", []):
        r_modified = r.get("last_modified") or r.get("data_last_updated")
        resource_freshness.append({
            "name": r.get("name"),
            "format": r.get("format"),
            "last_modified": r_modified,
        })

    return json.dumps({
        "dataset": ds.get("title"),
        "update_frequency": frequency,
        "last_modified": last_modified,
        "current_as_of": current_as_of,
        "days_since_update": days_since_update,
        "is_stale": is_stale,
        "resources": resource_freshness,
    }, indent=2, default=str)


@mcp.tool
async def validate_schema(
    resource_id: str,
    ctx: Context = None,
) -> str:
    """Compare the schema of a cached resource with the current live version.

    Detects added/removed/changed columns.

    Args:
        resource_id: Resource ID (must be cached)
    """
    ckan, cache = _get_deps(ctx)
    table_name = cache.get_table_name(resource_id)
    if not table_name:
        return json.dumps({"error": f"Resource {resource_id} not cached."})

    # Get cached schema
    cached_cols = cache.conn.execute(f"DESCRIBE \"{table_name}\"").fetchall()
    cached_schema = {col[0]: col[1] for col in cached_cols}

    # Get live schema
    live_result = await ckan.datastore_search(resource_id, limit=0)
    live_fields = {f["id"]: f.get("type", "unknown") for f in live_result.get("fields", []) if not f["id"].startswith("_")}

    cached_names = set(cached_schema.keys())
    live_names = set(live_fields.keys())

    added = list(live_names - cached_names)
    removed = list(cached_names - live_names)
    common = cached_names & live_names
    type_changes = []
    for col in common:
        if cached_schema[col] != live_fields[col]:
            type_changes.append({"column": col, "cached_type": cached_schema[col], "live_type": live_fields[col]})

    has_changes = bool(added or removed or type_changes)

    return json.dumps({
        "resource_id": resource_id,
        "schema_changed": has_changes,
        "columns_added": added,
        "columns_removed": removed,
        "type_changes": type_changes,
        "recommendation": "Use download_resource with force_refresh=True to update" if has_changes else "Schema is consistent",
    }, indent=2)


@mcp.tool
async def profile_dataset(
    resource_id: str,
    ctx: Context = None,
) -> str:
    """Generate a comprehensive statistical profile of a cached dataset.

    Includes distributions, cardinality, correlations for numeric columns.

    Args:
        resource_id: Resource ID (must be cached)
    """
    _, cache = _get_deps(ctx)
    table_name = cache.get_table_name(resource_id)
    if not table_name:
        return json.dumps({"error": f"Resource {resource_id} not cached. Use download_resource first."})

    df = cache.query_df(f'SELECT * FROM "{table_name}"')

    profile = {
        "resource_id": resource_id,
        "table_name": table_name,
        "shape": {"rows": len(df), "columns": len(df.columns)},
        "memory_usage_mb": round(df.memory_usage(deep=True).sum() / (1024 * 1024), 2),
        "columns": {},
    }

    for col in df.columns:
        col_profile: dict[str, Any] = {
            "dtype": str(df[col].dtype),
            "null_count": int(df[col].isna().sum()),
            "null_pct": round(df[col].isna().mean() * 100, 1),
            "unique_count": int(df[col].nunique()),
        }

        if df[col].dtype in ("int64", "float64", "Int64", "Float64"):
            desc = df[col].describe()
            col_profile["stats"] = {
                "mean": round(float(desc["mean"]), 4) if "mean" in desc else None,
                "std": round(float(desc["std"]), 4) if "std" in desc else None,
                "min": float(desc["min"]) if "min" in desc else None,
                "25%": float(desc["25%"]) if "25%" in desc else None,
                "50%": float(desc["50%"]) if "50%" in desc else None,
                "75%": float(desc["75%"]) if "75%" in desc else None,
                "max": float(desc["max"]) if "max" in desc else None,
            }
        elif df[col].dtype == "object":
            top_values = df[col].value_counts().head(10)
            col_profile["top_values"] = {str(k): int(v) for k, v in top_values.items()}
            col_profile["avg_length"] = round(df[col].dropna().str.len().mean(), 1) if not df[col].dropna().empty else 0

        profile["columns"][col] = col_profile

    # Correlation matrix for numeric columns
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns.tolist()
    if len(numeric_cols) >= 2:
        corr = df[numeric_cols].corr()
        profile["correlations"] = {
            col: {col2: round(float(corr.loc[col, col2]), 3) for col2 in numeric_cols}
            for col in numeric_cols
        }

    return json.dumps(profile, indent=2, default=str)
