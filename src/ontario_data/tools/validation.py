"""Validation tool: verify data claims against query results.

Why a standalone validate_result tool rather than folding into query_cached?

The validate_result tool was considered for merging into query_cached as an
optional ``claim=`` parameter. Rejected because:

1. Validation is a *second pass* — it re-executes the SQL and checks the result
   against a completed claim. The model formulates the claim *after* seeing query
   results, not before. An optional parameter on query_cached would require the
   model to predict its claim before seeing the data, which it can't do reliably.
2. validate_result can be called on *any* past SQL, not just the most recent
   query_cached call. The model can validate a claim made earlier in a
   conversation by passing the original SQL. This retrospective use case is
   incompatible with folding it into query_cached.
3. Keeping it separate preserves the MCP resource boundary: query_cached returns
   data, validate_result returns a verdict. Mixing them into one tool conflates
   two distinct steps in the analysis workflow.

Usage note: this tool is most effective when the SQL is kept tight — one query
that directly produces the number being claimed. Complex CTEs with intermediate
results are harder to validate because the claim extractor matches numbers
against final output rows only.
"""
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
