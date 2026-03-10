# Result Validation: Backtrace from Claim to Source

## Problem

When the LLM makes a claim based on query results, there is no verification
that (a) the claim matches the data, or (b) the data is reproducible.
The system currently has post-query heuristic warnings but no structured
validation of the final answer.

## Design

A standalone module `src/ontario_data/validate_results.py` with one public
function and one MCP tool wrapper.

### Function signature

```python
def validate(
    sql: str,
    results: list[dict],
    claim: str,
    cache: CacheManager,
) -> ValidationResult
```

### Return type

```python
@dataclass
class ValidationResult:
    valid: bool | None       # True, False, or None (unverifiable)
    summary: str             # one-liner for the user
    steps: list[dict]        # [{"check": str, "passed": bool|None, "detail": str}]
```

- `valid` is `True` only if all steps pass.
- `False` if any step fails.
- `None` if no extractable facts found (unverifiable).

### Two checks, executed in order (both always run)

**Check 1: Claim vs Results**

1. Extract verifiable facts from the claim string:
   - Numbers via regex: integers, floats, comma-formatted (12,345), percentages (45.2%)
   - Short string fragments: quoted strings in the claim ("Toronto", 'residential')
2. For each extracted fact, scan all cell values in `results`:
   - Numeric: match within 0.5% relative tolerance (handles rounding in prose)
   - Strings: case-insensitive substring match against cell values
3. Verdict per fact: found, not_found, or close_match (within tolerance but not exact)
4. If zero facts extracted, step returns None (unverifiable), not a failure

**Check 2: Results vs Re-execution**

1. Re-execute the SQL via `cache.query_with_meta(sql)`
2. Compare row count: must match exactly
3. Compare cell values row-by-row, column-by-column:
   - Numeric: exact match (same query, same data, deterministic)
   - Strings: exact match
   - NULLs: both must be NULL
4. If any difference, fail with diff detail
5. If re-execution raises an exception, fail with the error

### Output formatting

The MCP tool renders a one-line summary followed by a plain-English details
section. The prose explains the verification the way a staff data scientist
would explain a discrepancy to an executive.

**Pass:**
```
Summary: ✓ Auto-verified to the best of our ability. Always perform
your own checks if this result will be used for a serious matter.
See details below.

Details:

The claim mentions 12,345 permits in Toronto. We checked the query
results and confirmed both values — 12,345 appears in the permit_count
column and Toronto appears in the city column.

We also re-ran the query from scratch and got identical results.
```

**Fail:**
```
Summary: ✗ Verification failed — see details below. Do not rely on
this result without manual review.

Details:

The claim says 15,000 permits, but the data shows 12,345. That's a
significant discrepancy — the number doesn't appear anywhere in the
query results. The closest match is 12,345 in the permit_count column.

Toronto does appear in the data as expected.

We re-ran the query and got the same 12,345 figure, so the data itself
is consistent — the issue is between the claim and what the data says.
```

**Unverifiable:**
```
Summary: ? Could not extract verifiable facts from the claim. The query
re-executed successfully but the claim itself was not checkable.

Details:

The claim doesn't contain specific numbers or values we can cross-check
against the data. We re-ran the query and confirmed the results are
stable, but we can't verify whether the claim accurately represents
what the data shows.
```

### MCP tool wrapper

```python
@mcp.tool(annotations=READONLY)
async def validate_result(
    sql: str,
    results: list[dict],
    claim: str,
    ctx: Context = None,
) -> str:
    """Validate that a claim is supported by query results.

    Call this after making a data claim to verify it against the source.
    Extracts numbers and terms from the claim, checks them against the
    results, and re-runs the SQL to confirm reproducibility.

    Args:
        sql: The SQL query that produced the results
        results: The query result rows (list of dicts)
        claim: The natural-language claim to verify
    """
```

### Dependencies

Uses only existing library functions:
- `CacheManager.query_with_meta()` for re-execution
- `CacheManager.get_tables_metadata()` for provenance (if needed later)
- Standard library `re` for fact extraction
- No LLM calls, no external dependencies

### File layout

```
src/ontario_data/validate_results.py   # validate() + ValidationResult + helpers
src/ontario_data/tools/validation.py   # MCP tool wrapper (validate_result)
tests/test_validate_results.py         # unit tests
```

### Out of scope (for now)

- SQL vs schema checks (correct columns, correct types)
- Data freshness checks (stale cache detection)
- Unit checking (mg/L vs ug/L)
- Semantic extrapolation detection ("all of Ontario" from Toronto data)

These are natural extensions but not part of this iteration.
