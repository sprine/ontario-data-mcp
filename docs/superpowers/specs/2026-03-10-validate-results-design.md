# Result Validation: Backtrace from Claim to Source

## Problem

When the LLM makes a claim based on query results, there is no verification
that (a) the claim matches the data, or (b) the data is stable.
The system currently has post-query heuristic warnings but no structured
validation of the final answer.

## Design

A standalone module `src/ontario_data/validate_results.py` with one public
function and one MCP tool wrapper.

### Function signature

```python
def validate(
    sql: str,
    claim: str,
    cache: CacheManager,
) -> ValidationResult
```

**Sync function.** CacheManager is entirely synchronous (no async methods).
The MCP tool wrapper is async (required by FastMCP) but calls this directly,
matching the pattern used by `profile_data` in `tools/quality.py`.

**No `results` parameter.** The validator executes the SQL itself via
`cache.query_with_meta(sql)` to get real structured data. This eliminates:
- The impedance mismatch (LLM can't reconstruct `list[dict]` from markdown)
- The confused-deputy risk (LLM could pass fabricated results)

### Return type

```python
@dataclass(frozen=True, slots=True)
class ValidationResult:
    valid: bool | None       # True, False, or None (unverifiable)
    summary: str             # one-liner for the user
    steps: list[str]         # human-readable detail per check

    def __bool__(self):
        raise TypeError(
            "Do not use ValidationResult in a boolean context. "
            "Check .valid explicitly."
        )
```

- `valid` is `True` only if all checks pass.
- `False` if any check fails.
- `None` if no extractable facts found (unverifiable).
- `frozen=True, slots=True` matches `PortalConfig` precedent in `portals.py`.
- `steps: list[str]` — the consumer is a human reading prose, not code parsing
  structured fields.
- `__bool__` raises `TypeError` to prevent accidental `if result:` bugs.

### Two checks, executed in order (both always run)

**Check 1: Claim vs Query Results**

The validator executes the SQL, then extracts verifiable facts from the claim
and matches them against the real results.

1. Execute SQL via `cache.query_with_meta(sql, max_rows=2000)`. Handle
   `InvalidQueryError` distinctly — return `valid=False` with "SQL failed
   validation" (this is a real failure, not an infrastructure issue).

2. Extract verifiable facts from the claim string via three sequential regex
   passes (each pass removes matched text to prevent double-extraction):
   - **Pass 1:** Percentages (`45.2%` → 45.2, kind=percentage)
   - **Pass 2:** Remaining numbers — comma-formatted (`1,234,567`), floats
     (`3.14`), plain integers (`42`)
   - **Pass 3:** Quoted strings (`"Toronto"`, `'residential'`)

   Sequential passes eliminate the ordering-sensitivity of a single combined
   alternation pattern. No subtle match-priority bugs.

3. Build a value index from the results: `{normalized_value: set_of_column_names}`.
   O(rows * cols) to build, O(1) per lookup. Tells us *which column* matched,
   producing better prose output.

4. For each extracted fact, look it up in the index:
   - **Integers**: exact match. "12,345 permits" means exactly 12,345.
   - **Floats**: `math.isclose(rel_tol=0.005, abs_tol=0.5)` per PEP 485.
     `rel_tol=0.005` catches display rounding (45.2% vs 45.1876%).
     `abs_tol=0.5` catches sub-unit rounding ($500 vs 499.7).
   - **Percentages**: try both N and N/100. Claim "45.2%" matches data value
     0.452 or 45.2.
   - **Strings**: case-insensitive, whitespace-normalized (`" ".join(s.lower().split())`).

5. Verdict per fact: `found` or `not_found`. Binary outcome — nuance goes in
   the prose.

6. If zero facts extracted → `valid=None` (unverifiable), not a failure.

7. If a number is not found but at least one other extracted fact *was* found
   in the data, report the unmatched number as "possibly a derived or computed
   value" rather than a hard fail. This handles "grew by 20%" where 20 isn't
   in cells but the raw numbers 100 and 120 are. No keyword list to maintain.

**Check 2: Data Stability**

Verify the underlying data hasn't changed since it was cached, without
re-executing the full query.

1. Extract table names from the SQL using the existing `_TABLE_RE` pattern
   (`FROM\s+"([^"]+)"`). For JOINs, capture all tables.

2. For each table, fetch `downloaded_at` from `cache.get_tables_metadata()`.

3. If all tables have `downloaded_at` values and none have changed since
   Check 1's execution began, report "data is stable."

4. If a table is missing from metadata (was removed between calls), report
   "table no longer cached" and set check to failed.

5. If `downloaded_at` changed (concurrent `download_resource` refreshed the
   table), report "data was refreshed during validation — re-run the full
   analysis."

This replaces the previous design's full SQL re-execution. DuckDB is a local
embedded database — same query against same data produces same results. The
real risk is cache mutation, which a timestamp check catches at zero cost.

### Output formatting

The MCP tool renders a one-line summary followed by plain-English details.
The prose explains the verification the way a staff data scientist would
explain a discrepancy to an executive. We do not use `md_response()` here
because the output requires narrative prose, not key-value structure.

The exact wording is an implementation detail — the spec defines the three
states and their tone:

- **Pass**: reassuring but with a caveat ("always perform your own checks
  if this result will be used for a serious matter")
- **Fail**: direct about the discrepancy, names the specific numbers and
  which column has the real value
- **Unverifiable**: honest about what couldn't be checked and why

### MCP tool wrapper

```python
# src/ontario_data/tools/validation.py

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
    # Format ValidationResult as narrative markdown
    ...
```

### Dependencies

Uses only existing library functions:
- `CacheManager.query_with_meta()` for SQL execution
- `CacheManager.get_tables_metadata()` for stability check
- Standard library `re` for fact extraction
- Standard library `math.isclose` for numeric comparison (PEP 485)
- No LLM calls, no external dependencies

### File layout

```
src/ontario_data/validate_results.py   # validate() + ValidationResult + helpers
src/ontario_data/tools/validation.py   # MCP tool wrapper (validate_result)
tests/test_validate_results.py         # unit tests
```

Follows the existing separation: `staleness.py` (logic) + `tools/quality.py`
(MCP wrapper). Register in `server.py` imports alongside other tool modules.
All files start with `from __future__ import annotations`.

### Error handling

- `InvalidQueryError` from `query_with_meta()` → `valid=False`, step explains
  "SQL failed validation: {message}". This is a stronger signal than a generic
  failure.
- Other `Exception` → `valid=None`, step explains "Query execution failed:
  {message}". Wrapped in `try/except Exception` with
  `logger.debug(..., exc_info=True)` matching `_generate_query_warnings`.
- A check failure never crashes the tool.
- Guard: `if len(claim) > 10_000: return ValidationResult(valid=None, ...)`.

### Relation to existing `_generate_query_warnings`

These serve different purposes and remain separate:
- `_generate_query_warnings`: automatic, every `query_cached` call, catches
  SQL mistakes (COUNT vs SUM, empty results)
- `validate()`: on-demand, explicit tool invocation, verifies claim-to-data
  fidelity

They should not be merged. Different invocation times, different inputs.

### Testing strategy

Use **real CacheManager** with `tmp_path` (matching `test_tools_unit.py`
convention). No mocking of DuckDB. Store test DataFrames that trigger
specific validation conditions.

Key test cases:
- Correct claim, single number → pass
- Wrong number in claim → fail with closest match
- No numbers in claim → unverifiable
- Percentage claim vs fraction in data (45.2% vs 0.452) → pass
- Derived value ("grew by 20%") with other facts found → unverifiable, not fail
- Comma-formatted number in claim → correct extraction
- Empty result set → unverifiable
- InvalidQueryError → valid=False with clear message
- Table removed between query and stability check → reported
- JOIN query: stability check covers all tables

### Known limitations (v1)

- **Single-query claims only.** Multi-query claims (LLM combines results from
  2-3 queries) must be validated one query at a time.
- **No derived-value arithmetic.** "grew by 20%" is unverifiable when data has
  raw numbers 100 and 120 — the validator doesn't compute percentages.
- **No currency multipliers.** "$1.2M" is unverifiable — an honest result.
  Add when real usage demands it.
- **No semantic extrapolation detection.** Claim says "all of Ontario" when
  data is Toronto-only.
- **No unit checking** (mg/L vs ug/L).
- **Inherits P1 security gap:** `_validate_sql` doesn't block DuckDB
  file-reading functions. Should be fixed in `cache.py` independently.

### Security notes

- SQL re-validation via `query_with_meta()` is NOT redundant — the SQL
  arrives via a separate MCP tool call with no binding to the original
  `query_cached` invocation.
- The `claim` string is only processed by regex, never executed. No injection
  vector.
- Claim length bounded at 10,000 chars to prevent regex abuse.
