from __future__ import annotations

import json
from typing import Any

import pandas as pd
from fastmcp import Context

from ontario_data.server import mcp
from ontario_data.cache import CacheManager


def _get_cache(ctx: Context) -> CacheManager:
    return ctx.lifespan_context["cache"]


def _require_cached(cache: CacheManager, resource_id: str) -> str:
    """Get table name or raise helpful error."""
    table_name = cache.get_table_name(resource_id)
    if not table_name:
        raise ValueError(f"Resource {resource_id} not cached. Use download_resource first.")
    return table_name


@mcp.tool
async def summarize(
    resource_id: str,
    columns: list[str] | None = None,
    ctx: Context = None,
) -> str:
    """Get descriptive statistics for numeric columns in a cached dataset.

    Args:
        resource_id: Resource ID (must be cached)
        columns: Specific columns to summarize (default: all numeric)
    """
    cache = _get_cache(ctx)
    table_name = _require_cached(cache, resource_id)
    df = cache.query_df(f'SELECT * FROM "{table_name}"')

    if columns:
        df = df[columns]

    numeric_df = df.select_dtypes(include=["int64", "float64", "Int64", "Float64"])
    if numeric_df.empty:
        return json.dumps({"error": "No numeric columns found", "available_columns": list(df.columns)})

    stats = numeric_df.describe().round(4)
    result = {}
    for col in stats.columns:
        result[col] = {str(k): float(v) if pd.notna(v) else None for k, v in stats[col].items()}

    return json.dumps({"resource_id": resource_id, "statistics": result}, indent=2, default=str)


@mcp.tool
async def time_series_analysis(
    resource_id: str,
    date_column: str,
    value_column: str,
    frequency: str = "auto",
    ctx: Context = None,
) -> str:
    """Analyze trends and patterns in time-indexed data.

    Args:
        resource_id: Resource ID (must be cached)
        date_column: Column containing dates
        value_column: Column containing values to analyze
        frequency: Aggregation frequency: "daily", "weekly", "monthly", "quarterly", "yearly", or "auto"
    """
    cache = _get_cache(ctx)
    table_name = _require_cached(cache, resource_id)
    df = cache.query_df(f'SELECT "{date_column}", "{value_column}" FROM "{table_name}" ORDER BY "{date_column}"')

    df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
    df = df.dropna(subset=[date_column, value_column])
    df = df.sort_values(date_column)

    # Auto-detect frequency
    if frequency == "auto":
        date_range = (df[date_column].max() - df[date_column].min()).days
        if date_range > 365 * 3:
            frequency = "yearly"
        elif date_range > 365:
            frequency = "quarterly"
        elif date_range > 90:
            frequency = "monthly"
        elif date_range > 14:
            frequency = "weekly"
        else:
            frequency = "daily"

    freq_map = {"daily": "D", "weekly": "W", "monthly": "ME", "quarterly": "QE", "yearly": "YE"}
    pd_freq = freq_map.get(frequency, "ME")

    df = df.set_index(date_column)
    resampled = df[value_column].resample(pd_freq).agg(["mean", "sum", "count", "min", "max"])
    resampled = resampled.round(4)

    # Trend calculation
    values = resampled["mean"].dropna()
    if len(values) >= 2:
        first_half = values.iloc[: len(values) // 2].mean()
        second_half = values.iloc[len(values) // 2 :].mean()
        pct_change = ((second_half - first_half) / first_half * 100) if first_half != 0 else 0
        trend = "increasing" if pct_change > 5 else "decreasing" if pct_change < -5 else "stable"
    else:
        pct_change = 0
        trend = "insufficient data"

    periods = []
    for idx, row in resampled.iterrows():
        periods.append({
            "period": str(idx.date()) if hasattr(idx, "date") else str(idx),
            "mean": float(row["mean"]) if pd.notna(row["mean"]) else None,
            "sum": float(row["sum"]) if pd.notna(row["sum"]) else None,
            "count": int(row["count"]),
            "min": float(row["min"]) if pd.notna(row["min"]) else None,
            "max": float(row["max"]) if pd.notna(row["max"]) else None,
        })

    return json.dumps({
        "resource_id": resource_id,
        "date_column": date_column,
        "value_column": value_column,
        "frequency": frequency,
        "trend": trend,
        "pct_change": round(pct_change, 1),
        "total_periods": len(periods),
        "date_range": {
            "start": str(values.index.min()) if not values.empty else None,
            "end": str(values.index.max()) if not values.empty else None,
        },
        "periods": periods,
    }, indent=2, default=str)


@mcp.tool
async def cross_tabulate(
    resource_id: str,
    row_field: str,
    col_field: str,
    value_field: str | None = None,
    aggregation: str = "count",
    ctx: Context = None,
) -> str:
    """Create a cross-tabulation (pivot table) from cached data.

    Args:
        resource_id: Resource ID (must be cached)
        row_field: Column for rows
        col_field: Column for columns
        value_field: Column to aggregate (required for sum/mean/min/max)
        aggregation: "count", "sum", "mean", "min", "max"
    """
    cache = _get_cache(ctx)
    table_name = _require_cached(cache, resource_id)
    df = cache.query_df(f'SELECT * FROM "{table_name}"')

    if aggregation == "count":
        ct = pd.crosstab(df[row_field], df[col_field])
    else:
        if not value_field:
            return json.dumps({"error": f"value_field required for aggregation={aggregation}"})
        ct = pd.crosstab(df[row_field], df[col_field], values=df[value_field], aggfunc=aggregation)

    ct = ct.round(4)

    return json.dumps({
        "resource_id": resource_id,
        "row_field": row_field,
        "col_field": col_field,
        "aggregation": aggregation,
        "shape": {"rows": ct.shape[0], "columns": ct.shape[1]},
        "table": json.loads(ct.to_json()),
    }, indent=2, default=str)


@mcp.tool
async def correlation_matrix(
    resource_id: str,
    columns: list[str] | None = None,
    method: str = "pearson",
    ctx: Context = None,
) -> str:
    """Compute pairwise correlations between numeric columns.

    Args:
        resource_id: Resource ID (must be cached)
        columns: Specific columns (default: all numeric)
        method: "pearson", "spearman", or "kendall"
    """
    cache = _get_cache(ctx)
    table_name = _require_cached(cache, resource_id)
    df = cache.query_df(f'SELECT * FROM "{table_name}"')

    if columns:
        df = df[columns]

    numeric_df = df.select_dtypes(include=["int64", "float64", "Int64", "Float64"])
    if len(numeric_df.columns) < 2:
        return json.dumps({"error": "Need at least 2 numeric columns", "available": list(df.columns)})

    corr = numeric_df.corr(method=method).round(4)

    # Find strongest correlations (excluding self-correlations)
    strong = []
    for i, col1 in enumerate(corr.columns):
        for col2 in corr.columns[i + 1 :]:
            val = float(corr.loc[col1, col2])
            if abs(val) > 0.5:
                strong.append({"col1": col1, "col2": col2, "correlation": val})
    strong.sort(key=lambda x: abs(x["correlation"]), reverse=True)

    return json.dumps({
        "resource_id": resource_id,
        "method": method,
        "matrix": json.loads(corr.to_json()),
        "strong_correlations": strong,
    }, indent=2, default=str)


@mcp.tool
async def compare_periods(
    resource_id: str,
    date_column: str,
    period1_start: str,
    period1_end: str,
    period2_start: str,
    period2_end: str,
    metrics: list[str] | None = None,
    ctx: Context = None,
) -> str:
    """Compare metrics between two time periods.

    Args:
        resource_id: Resource ID (must be cached)
        date_column: Column containing dates
        period1_start: Start date of first period (YYYY-MM-DD)
        period1_end: End date of first period
        period2_start: Start date of second period
        period2_end: End date of second period
        metrics: Numeric columns to compare (default: all numeric)
    """
    cache = _get_cache(ctx)
    table_name = _require_cached(cache, resource_id)
    df = cache.query_df(f'SELECT * FROM "{table_name}"')

    df[date_column] = pd.to_datetime(df[date_column], errors="coerce")

    p1 = df[(df[date_column] >= period1_start) & (df[date_column] <= period1_end)]
    p2 = df[(df[date_column] >= period2_start) & (df[date_column] <= period2_end)]

    if metrics:
        numeric_cols = metrics
    else:
        numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns.tolist()

    comparisons = {}
    for col in numeric_cols:
        p1_mean = float(p1[col].mean()) if not p1[col].empty else None
        p2_mean = float(p2[col].mean()) if not p2[col].empty else None
        p1_sum = float(p1[col].sum()) if not p1[col].empty else None
        p2_sum = float(p2[col].sum()) if not p2[col].empty else None

        pct_change_mean = None
        if p1_mean and p2_mean and p1_mean != 0:
            pct_change_mean = round((p2_mean - p1_mean) / p1_mean * 100, 2)

        comparisons[col] = {
            "period1": {"mean": round(p1_mean, 4) if p1_mean else None, "sum": round(p1_sum, 4) if p1_sum else None, "count": len(p1)},
            "period2": {"mean": round(p2_mean, 4) if p2_mean else None, "sum": round(p2_sum, 4) if p2_sum else None, "count": len(p2)},
            "pct_change_mean": pct_change_mean,
        }

    return json.dumps({
        "resource_id": resource_id,
        "period1": f"{period1_start} to {period1_end}",
        "period2": f"{period2_start} to {period2_end}",
        "comparisons": comparisons,
    }, indent=2, default=str)
