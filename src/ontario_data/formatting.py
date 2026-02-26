"""Markdown formatters for MCP tool responses.

Replaces json_response() with human-readable markdown output.
The primary consumer is an LLM — markdown is both human-readable
in terminals and natively parsed by language models.
"""
from __future__ import annotations

from typing import Any


def _escape_cell(value: Any) -> str:
    """Stringify and escape characters that break markdown table cells."""
    if value is None:
        return ""
    s = str(value)
    return s.replace("|", "\\|").replace("\n", " ")


def md_table(headers: list[str], rows: list[list[Any]]) -> str:
    """Build a plain markdown table. No alignment tricks, no truncation."""
    if not headers:
        return ""

    num_cols = len(headers)

    # Pad short rows, truncate long rows to match header count
    str_rows = []
    for row in rows:
        padded = list(row) + [""] * (num_cols - len(row))
        str_rows.append([_escape_cell(v) for v in padded[:num_cols]])

    sep = "| " + " | ".join("---" for _ in headers) + " |"
    header_line = "| " + " | ".join(headers) + " |"
    data_lines = ["| " + " | ".join(row) + " |" for row in str_rows]

    return "\n".join([header_line, sep] + data_lines)


def md_response(**kwargs: Any) -> str:
    """Render tool output as human-readable markdown.

    Drop-in replacement for json_response(**kwargs). Same call sites,
    same kwargs — but outputs markdown instead of JSON.

    Rendering rules:
    - list[dict] → markdown table (using md_table)
    - dict → nested bullet list (one level of nesting only)
    - list → comma-separated inline
    - scalar → key: value line
    """
    if not kwargs:
        return ""

    parts: list[str] = []
    for key, value in kwargs.items():
        if isinstance(value, list) and all(isinstance(r, dict) for r in value) and value:
            # Non-empty list of dicts → table
            headers = list(value[0].keys())
            rows = [[r.get(h) for h in headers] for r in value]
            parts.append(f"\n**{key}** ({len(value)}):\n")
            parts.append(md_table(headers, rows))
        elif isinstance(value, dict):
            # Nested dict → sub-bullets
            parts.append(f"- **{key}:**")
            for k, v in value.items():
                parts.append(f"  - {k}: {v}")
        elif isinstance(value, list):
            # Plain list or empty list[dict] → inline
            if value:
                parts.append(f"- **{key}:** {', '.join(str(v) for v in value)}")
            else:
                parts.append(f"- **{key}:** (none)")
        else:
            parts.append(f"- **{key}:** {value}")

    return "\n".join(parts)


def format_records(
    records: list[dict],
    row_count: int,
    *,
    source: str | None = None,
    total: int | None = None,
    preview: bool = False,
    fields: list[dict] | None = None,
) -> str:
    """Format tabular query results with a count header + markdown table.

    Used by: query_cached, query_resource, sql_query, preview_data, spatial_query.
    Always renders a table — no compact/bullet heuristic.
    """
    parts: list[str] = []

    # Header line
    if preview and total is not None:
        header = f"Previewing **{row_count}** of {total} total records"
    elif total is not None and total != row_count:
        header = f"**{row_count} rows** returned ({total} total)"
    else:
        header = f"**{row_count} rows**"

    if source:
        header += f" from `{source}`"
    parts.append(header)

    # Schema line from fields metadata
    if fields:
        col_info = ", ".join(f"{f['name']} ({f.get('type', '?')})" for f in fields)
        parts.append(f"Columns: {col_info}")

    if not records:
        return "\n".join(parts)

    headers = list(records[0].keys())
    rows = [[rec.get(h) for h in headers] for rec in records]
    parts.append("")
    parts.append(md_table(headers, rows))

    return "\n".join(parts)


def format_search_results(
    query: str,
    portals_searched: int,
    results: list[dict],
    skipped: list[dict],
) -> str:
    """Format search_datasets results grouped by portal."""
    total_returned = sum(len(r.get("datasets", [])) for r in results)
    total_available = sum(r.get("total_count", 0) for r in results)
    portal_count = len(results)

    if total_available > total_returned:
        header = f'Found **{total_returned}** datasets for "{query}" ({total_available} available across {portal_count} portals)'
    else:
        header = f'Found **{total_returned}** datasets across {portal_count} portal{"s" if portal_count != 1 else ""} for "{query}"'
    parts = [header]

    for portal_result in results:
        datasets = portal_result.get("datasets", [])
        if not datasets:
            continue
        parts.append(f"\n**{portal_result['portal_name']}:** {portal_result['total_count']} results")
        for ds in datasets:
            formats = ", ".join(ds.get("formats", []))
            org = ds.get("organization", "")
            line = f"- **{ds['title']}** (`{ds['id']}`) — {formats}"
            if org:
                line += f" — {org}"
            parts.append(line)

    for skip in skipped:
        parts.append(f"\n*Skipped {skip['portal_name']}:* {skip['reason']}")

    if not results and not skipped:
        parts.append("\nNo results found.")

    return "\n".join(parts)
