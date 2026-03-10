from __future__ import annotations

from fastmcp import Context

from ontario_data.server import READONLY, mcp
from ontario_data.utils import get_cache
from ontario_data.validate_results import validate


@mcp.tool(annotations=READONLY)
async def validate_result(
    sql: str,
    claim: str,
    ctx: Context = None,
) -> str:
    """Validate that a claim is supported by query results.

    Call this after making a data claim to verify it against the source.
    Re-executes the SQL, extracts numbers and terms from the claim, and
    checks them against the actual data.

    Args:
        sql: The SQL query that produced the data backing the claim
        claim: The natural-language claim to verify (e.g. "Toronto had
               12,345 building permits in 2023")
    """
    cache = get_cache(ctx)
    result = validate(sql, claim, cache)

    # Format as narrative markdown
    if result.valid is True:
        icon = "PASS"
    elif result.valid is False:
        icon = "FAIL"
    else:
        icon = "UNVERIFIABLE"

    lines = [f"**Validation: {icon}**", "", result.summary, ""]
    if result.steps:
        lines.append("**Details:**")
        lines.extend(result.steps)

    return "\n".join(lines)
