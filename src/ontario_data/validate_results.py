from __future__ import annotations

import logging
import math
import re
from dataclasses import dataclass, field
from typing import Any

from ontario_data.cache import CacheManager, InvalidQueryError

logger = logging.getLogger("ontario_data.validate_results")

# Regex for extracting table names from SQL — captures FROM and JOIN targets
_TABLE_RE = re.compile(r'(?:FROM|JOIN)\s+"([^"]+)"', re.IGNORECASE)

# --- Fact extraction regexes (applied sequentially) ---
_PERCENTAGE_RE = re.compile(r"(\d[\d,]*\.?\d*)\s*%")
_NUMBER_RE = re.compile(r"(?<![%\w])(\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+\.\d+|\d+)(?![%\w])")
_QUOTED_STRING_RE = re.compile(r"""["']([^"']+)["']""")

_MAX_CLAIM_LENGTH = 10_000


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """Outcome of validating a claim against query results."""

    valid: bool | None
    summary: str
    steps: list[str] = field(default_factory=list)

    def __bool__(self):
        raise TypeError(
            "Do not use ValidationResult in a boolean context. "
            "Check .valid explicitly."
        )


@dataclass(frozen=True, slots=True)
class _ExtractedFact:
    """A single verifiable fact extracted from a claim string."""

    raw: str
    value: float | str
    kind: str  # "percentage", "integer", "float", or "string"


def _extract_facts(claim: str) -> list[_ExtractedFact]:
    """Extract verifiable facts from a claim via three sequential regex passes.

    Each pass removes matched text to prevent double-extraction.
    """
    facts: list[_ExtractedFact] = []
    remaining = claim

    # Pass 1: Percentages
    for m in _PERCENTAGE_RE.finditer(remaining):
        raw_num = m.group(1).replace(",", "")
        facts.append(_ExtractedFact(raw=m.group(0), value=float(raw_num), kind="percentage"))
    remaining = _PERCENTAGE_RE.sub("", remaining)

    # Pass 2: Remaining numbers
    for m in _NUMBER_RE.finditer(remaining):
        raw_num = m.group(1).replace(",", "")
        if "." in m.group(1):
            facts.append(_ExtractedFact(raw=m.group(1), value=float(raw_num), kind="float"))
        else:
            facts.append(_ExtractedFact(raw=m.group(1), value=int(raw_num), kind="integer"))
    remaining = _NUMBER_RE.sub("", remaining)

    # Pass 3: Quoted strings
    for m in _QUOTED_STRING_RE.finditer(remaining):
        facts.append(_ExtractedFact(raw=m.group(0), value=m.group(1), kind="string"))

    return facts


def _normalize_string(s: str) -> str:
    """Case-insensitive, whitespace-normalized string comparison."""
    return " ".join(str(s).lower().split())


def _build_value_index(
    rows: list[dict[str, Any]],
) -> dict[str, set[str]]:
    """Build {normalized_value: set_of_column_names} from query results.

    Numeric values are keyed by their float representation.
    String values are keyed by their normalized form.
    """
    index: dict[str, set[str]] = {}
    for row in rows:
        for col_name, val in row.items():
            if val is None:
                continue
            # Index numeric values by their float repr
            try:
                fval = float(val)
                key = f"num:{fval}"
                index.setdefault(key, set()).add(col_name)
            except (ValueError, TypeError):
                pass
            # Index string values by normalized form
            sval = _normalize_string(val)
            if sval:
                key = f"str:{sval}"
                index.setdefault(key, set()).add(col_name)
    return index


def _lookup_number(
    value: float, index: dict[str, set[str]], is_integer: bool
) -> tuple[bool, str | None]:
    """Look up a numeric value in the index. Returns (found, column_name)."""
    for key, cols in index.items():
        if not key.startswith("num:"):
            continue
        data_val = float(key[4:])
        if is_integer:
            # Exact match for integers
            if data_val == value:
                return True, next(iter(cols))
        else:
            # Float tolerance per PEP 485
            if math.isclose(data_val, value, rel_tol=0.005, abs_tol=0.5):
                return True, next(iter(cols))
    return False, None


def _lookup_percentage(
    value: float, index: dict[str, set[str]]
) -> tuple[bool, str | None]:
    """Look up a percentage in the index. Try both N and N/100."""
    # Try the raw percentage value (e.g., 45.2)
    found, col = _lookup_number(value, index, is_integer=False)
    if found:
        return True, col
    # Try as fraction (e.g., 0.452)
    found, col = _lookup_number(value / 100, index, is_integer=False)
    if found:
        return True, col
    return False, None


def _lookup_string(
    value: str, index: dict[str, set[str]]
) -> tuple[bool, str | None]:
    """Look up a string in the index (case-insensitive, whitespace-normalized)."""
    key = f"str:{_normalize_string(value)}"
    cols = index.get(key)
    if cols:
        return True, next(iter(cols))
    return False, None


def _check_claim_vs_results(
    sql: str, claim: str, cache: CacheManager
) -> tuple[bool | None, list[str]]:
    """Check 1: Verify claim facts against query results."""
    steps: list[str] = []

    # Execute SQL
    try:
        rows, fields = cache.query_with_meta(sql, max_rows=2000)
    except InvalidQueryError as exc:
        steps.append(f"SQL failed validation: {exc}")
        return False, steps
    except Exception as exc:
        logger.debug("Query execution failed", exc_info=True)
        steps.append(f"Query execution failed: {exc}")
        return None, steps

    steps.append(f"Query returned {len(rows)} rows with {len(fields)} columns.")

    if not rows:
        steps.append("Result set is empty — cannot verify claim against data.")
        return None, steps

    # Extract facts
    facts = _extract_facts(claim)
    if not facts:
        steps.append("No verifiable facts (numbers or quoted strings) found in claim.")
        return None, steps

    steps.append(f"Extracted {len(facts)} verifiable facts from claim.")

    # Build index and check each fact
    index = _build_value_index(rows)
    found_count = 0
    not_found_facts: list[_ExtractedFact] = []

    for fact in facts:
        if fact.kind == "percentage":
            ok, col = _lookup_percentage(fact.value, index)
        elif fact.kind == "integer":
            ok, col = _lookup_number(fact.value, index, is_integer=True)
        elif fact.kind == "float":
            ok, col = _lookup_number(fact.value, index, is_integer=False)
        else:  # string
            ok, col = _lookup_string(fact.value, index)

        if ok:
            found_count += 1
            steps.append(f"  {fact.raw} — found in column \"{col}\".")
        else:
            not_found_facts.append(fact)

    # Determine verdict
    if not not_found_facts:
        return True, steps

    # If some facts matched and unmatched are numbers, they may be derived
    for fact in not_found_facts:
        if found_count > 0 and fact.kind in ("percentage", "integer", "float"):
            steps.append(
                f"  {fact.raw} — not found in data; "
                f"possibly a derived or computed value."
            )
        else:
            steps.append(f"  {fact.raw} — not found in query results.")

    # If all unmatched facts are possibly derived (some others were found), unverifiable
    if found_count > 0 and all(
        f.kind in ("percentage", "integer", "float") for f in not_found_facts
    ):
        return None, steps

    return False, steps


def _check_data_stability(
    sql: str, cache: CacheManager, query_started_at: float
) -> tuple[bool | None, list[str]]:
    """Check 2: Verify underlying data hasn't changed since query execution."""
    steps: list[str] = []
    table_names = _TABLE_RE.findall(sql)

    if not table_names:
        steps.append("No table names found in SQL — skipping stability check.")
        return None, steps

    metadata_list = cache.get_tables_metadata(table_names)
    meta_by_table = {m["table_name"]: m for m in metadata_list}

    all_stable = True
    for table in table_names:
        meta = meta_by_table.get(table)
        if meta is None:
            steps.append(f"Table \"{table}\" is no longer cached.")
            all_stable = False
            continue

        downloaded_at = meta.get("downloaded_at")
        if downloaded_at is None:
            steps.append(f"Table \"{table}\" has no download timestamp.")
            all_stable = False
            continue

        # Convert to epoch for comparison.  DuckDB returns TIMESTAMP as
        # a Python datetime (naive, UTC).  list_cached() stringifies it,
        # but get_tables_metadata() returns the raw datetime.
        try:
            from datetime import datetime, timezone

            if isinstance(downloaded_at, datetime):
                dt = downloaded_at
            else:
                dt = datetime.fromisoformat(str(downloaded_at).replace("Z", "+00:00"))
            # Treat naive timestamps as UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dl_epoch = dt.timestamp()
        except (ValueError, AttributeError, TypeError):
            steps.append(f"Table \"{table}\" has unparseable download timestamp.")
            all_stable = False
            continue

        if dl_epoch > query_started_at:
            steps.append(
                f"Table \"{table}\" was refreshed during validation — "
                f"re-run the full analysis."
            )
            all_stable = False
        else:
            steps.append(f"Table \"{table}\": data is stable (not refreshed since query).")

    if all_stable:
        return True, steps
    return False, steps


def validate(sql: str, claim: str, cache: CacheManager) -> ValidationResult:
    """Validate that a claim is supported by query results.

    Executes the SQL, extracts verifiable facts from the claim, and checks
    them against the actual data. Also verifies data stability.
    """
    # Guard against oversized claims
    if len(claim) > _MAX_CLAIM_LENGTH:
        return ValidationResult(
            valid=None,
            summary="Claim too long to validate.",
            steps=[f"Claim length ({len(claim)}) exceeds maximum ({_MAX_CLAIM_LENGTH})."],
        )

    import time

    query_started_at = time.time()

    # Check 1: Claim vs query results
    claim_valid, claim_steps = _check_claim_vs_results(sql, claim, cache)

    # Check 2: Data stability (always runs)
    stability_valid, stability_steps = _check_data_stability(sql, cache, query_started_at)

    # Combine results
    all_steps = ["**Check 1 — Claim vs Query Results:**"] + claim_steps
    all_steps += ["", "**Check 2 — Data Stability:**"] + stability_steps

    # Overall verdict
    if claim_valid is False or stability_valid is False:
        valid = False
        if claim_valid is False and stability_valid is False:
            summary = "Claim does not match data, and underlying data may have changed."
        elif claim_valid is False:
            summary = "Claim does not match the query results."
        else:
            summary = "Data was modified during validation — results may be stale."
    elif claim_valid is True and stability_valid is not False:
        valid = True
        summary = (
            "All extracted facts match the query results. "
            "Always perform your own checks if this result will be used for a serious matter."
        )
    else:
        valid = None
        summary = "Could not fully verify the claim against the data."

    return ValidationResult(valid=valid, summary=summary, steps=all_steps)
