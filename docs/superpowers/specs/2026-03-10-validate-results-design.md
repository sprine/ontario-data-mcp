# Result Validation: Backtrace from Claim to Source

## Enhancement Summary

**Deepened on:** 2026-03-10
**Research agents used:** 8 (best-practices-researcher, architecture-strategist,
performance-oracle, security-sentinel, agent-native-reviewer, code-simplicity-reviewer,
pattern-recognition-specialist, spec-flow-analyzer)

### Key Changes from Research

1. **Dropped `results` parameter** — LLM receives markdown from `query_cached`, cannot
   reconstruct `list[dict]`. Validator executes SQL itself. (agent-native, security,
   performance, flow-analyzer — 4 agents converged independently)
2. **Simplified signature** to `validate(sql, claim, cache)` — two params, not four
3. **Percentage dual-check** — try both N and N/100 when `%` detected (flow-analyzer,
   best-practices)
4. **Unordered multiset comparison** for Check 2 — DuckDB doesn't guarantee row order
   without ORDER BY (flow-analyzer, best-practices)
5. **Derived values → unverifiable** not fail — "grew by 20%" where 20 isn't in cells
6. **Non-deterministic SQL detection** — skip Check 2 for RANDOM(), NOW(), etc.

---

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
- The redundancy (re-execution was already planned for Check 2)

### Return type

```python
@dataclass(frozen=True, slots=True)
class ValidationResult:
    valid: bool | None       # True, False, or None (unverifiable)
    summary: str             # one-liner for the user
    steps: list[str]         # human-readable detail per check
```

- `valid` is `True` only if all checks pass.
- `False` if any check fails.
- `None` if no extractable facts found (unverifiable).
- `frozen=True, slots=True` matches `PortalConfig` precedent in `portals.py`.
- `steps: list[str]` — the consumer is a human reading prose, not code parsing
  structured fields. Nuance goes in the prose, not in enum values.

### Two checks, executed in order (both always run)

**Check 1: Claim vs Query Results**

The validator executes the SQL, then extracts verifiable facts from the claim
and matches them against the real results.

1. Execute SQL via `cache.query_with_meta(sql, max_rows=2000)`.

2. Extract verifiable facts from the claim string via regex:
   - Integers and comma-formatted: `12,345` → 12345
   - Floats: `3.14` → 3.14
   - Percentages: `45.2%` → 45.2 (also check 0.452)
   - Currency with multipliers: `$1.2M` → 1200000
   - Quoted strings: `"Toronto"`, `'residential'`

3. Build a value index from the results: `{normalized_value: set_of_column_names}`.
   This is O(rows * cols) to build and O(1) per lookup, and tells us *which
   column* matched — producing better prose output.

4. For each extracted fact, look it up in the index:
   - **Integers**: exact match. "12,345 permits" means exactly 12,345.
   - **Floats/currency**: `math.isclose(rel_tol=0.005, abs_tol=0.5)` per PEP 485.
   - **Percentages**: try both N and N/100. Claim "45.2%" matches data value
     0.452 or 45.2.
   - **Strings**: case-insensitive, whitespace-normalized, NFKC unicode normalized.

5. Verdict per fact: `found` or `not_found`. No `close_match` — binary outcome,
   nuance goes in the prose ("matches at 2 significant figures").

6. If zero facts extracted → step returns `None` (unverifiable), not a failure.

7. If a number is not found but the claim contains mathematical language
   ("grew by", "increased", "percent change", "ratio"), report as
   "unverifiable computed value" rather than fail.

**Check 2: Reproducibility**

Execute the SQL a second time and compare with Check 1's results to confirm
determinism.

1. Detect non-deterministic SQL via regex scan for `RANDOM`, `NOW`,
   `CURRENT_TIMESTAMP`, `CURRENT_DATE`, `UUID`, `SETSEED`. If found, skip
   Check 2 and report "re-execution skipped: query contains non-deterministic
   functions."

2. Re-execute the SQL via `cache.query_with_meta(sql, max_rows=2000)`.

3. Compare row counts. If different, fail with explanation.

4. Compare as **unordered multisets** — sort both result sets by all column
   values before comparing. DuckDB does not guarantee row order without
   ORDER BY, so row-by-row comparison would produce false negatives.

5. For numeric cells, use epsilon comparison (`abs(a-b) / max(abs(a), abs(b),
   1e-15) < 1e-9`) to absorb IEEE 754 floating-point accumulation differences
   across DuckDB thread counts.

6. If re-execution raises an exception, fail with the error but don't crash —
   return `valid=None` with explanatory detail (e.g. "table was removed from
   cache between query and validation").

### Fact extraction regex

Pre-compiled at module level following codebase convention (`_UPPER_SNAKE_RE`).
Ordered from most specific to least specific so earlier patterns match first:

```
1. Currency with multiplier: $1.2M, $500K, $3.5B
2. Currency plain: $1,234,567.89
3. Percentage: 45.2%, -3.1%
4. Comma-formatted: 1,234,567
5. Plain float: 3.14
6. Plain integer: 42
7. Quoted strings: "Toronto", 'residential'
```

Multiplier suffixes: K=1000, M=1000000, B=1000000000.

**Edge case: year numbers.** Four-digit numbers 1900-2099 near date-context
words ("in", "during", "since", "from", "year") are deprioritized — they
still match but are checked last and flagged as "likely a year, not a quantity"
in the prose.

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
    claim: str,
    ctx: Context = None,
) -> str:
    """Validate that a claim is supported by query results.

    Call this after making a data claim to verify it against the source.
    Re-executes the SQL, extracts numbers and terms from the claim,
    checks them against the actual data, and re-runs the query a second
    time to confirm reproducibility.

    Args:
        sql: The SQL query that produced the data backing the claim
        claim: The natural-language claim to verify (e.g. "Toronto had
               12,345 building permits in 2023")
    """
```

### Dependencies

Uses only existing library functions:
- `CacheManager.query_with_meta()` for SQL execution
- `CacheManager.get_tables_metadata()` for TOCTOU detection (optional)
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

### Error handling

Each check wraps its cache queries in `try/except Exception` with
`logger.debug(..., exc_info=True)` — matching the pattern established by
`_generate_query_warnings` in `querying.py`. A check failure never crashes
the tool; it returns a step explaining what went wrong.

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

Key test cases from flow analysis:
- Correct claim, single number → pass
- Wrong number in claim → fail with closest match
- No numbers in claim → unverifiable
- Percentage claim vs fraction in data (45.2% vs 0.452) → pass
- Derived value ("grew by 20%") → unverifiable, not fail
- Comma-formatted number in claim → correct extraction
- Currency with multiplier ($1.2M) → correct extraction
- Empty result set → unverifiable
- Non-deterministic SQL → Check 2 skipped
- Re-execution returns same data → Check 2 pass
- Year numbers deprioritized

### Known limitations (v1)

- **Single-query claims only.** Multi-query claims (LLM combines results from
  2-3 queries) must be validated one query at a time.
- **No derived-value arithmetic.** "grew by 20%" is unverifiable when data has
  raw numbers 100 and 120 — the validator doesn't compute percentages.
- **No semantic extrapolation detection.** Claim says "all of Ontario" when
  data is Toronto-only.
- **No unit checking** (mg/L vs ug/L).
- **No SQL-to-schema checks** (correct columns, correct types).
- **No data freshness checks** (stale cache detection).
- **Inherits P1 security gap:** `_validate_sql` doesn't block DuckDB
  file-reading functions. Should be fixed in `cache.py` independently.

### Security notes

- SQL re-validation via `query_with_meta()` is NOT redundant — the SQL
  arrives via a separate MCP tool call with no binding to the original
  `query_cached` invocation.
- The `claim` string is only processed by regex, never executed. No injection
  vector.
- Add `max_claim_length` guard (10,000 chars) to bound regex execution time.
- TOCTOU: if `downloaded_at` changes between executions, report "data was
  refreshed during validation" rather than a generic mismatch.
