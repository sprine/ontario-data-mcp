"""Tests for validate_results module."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from ontario_data.cache import CacheManager, InvalidQueryError
from ontario_data.portals import PORTALS
from ontario_data.validate_results import (
    ValidationResult,
    _extract_facts,
    validate,
)


@pytest.fixture
def cache(tmp_path):
    """Create a fresh CacheManager for testing."""
    c = CacheManager(db_path=str(tmp_path / "test.duckdb"))
    c.initialize()
    yield c


def _store(cache, table_name, df, resource_id="r1", dataset_id="ds1"):
    """Helper to store a DataFrame in the cache."""
    cache.store_resource(
        resource_id=resource_id,
        dataset_id=dataset_id,
        table_name=table_name,
        df=df,
        source_url="http://example.com/data.csv",
    )


def make_mock_context(cache: CacheManager):
    ctx = MagicMock()
    ctx.lifespan_context = {
        "cache": cache,
        "http_client": MagicMock(),
        "portal_configs": PORTALS,
        "portal_clients": {"ontario": AsyncMock()},
    }
    ctx.report_progress = AsyncMock()
    return ctx


# ---------------------------------------------------------------------------
# ValidationResult __bool__ guard
# ---------------------------------------------------------------------------


class TestValidationResultBool:
    def test_bool_raises_type_error(self):
        r = ValidationResult(valid=True, summary="ok")
        with pytest.raises(TypeError, match="Do not use ValidationResult"):
            bool(r)

    def test_if_raises_type_error(self):
        r = ValidationResult(valid=False, summary="nope")
        with pytest.raises(TypeError):
            if r:
                pass


# ---------------------------------------------------------------------------
# Fact extraction
# ---------------------------------------------------------------------------


class TestExtractFacts:
    def test_percentage(self):
        facts = _extract_facts("Usage increased by 45.2% last year.")
        assert len(facts) == 1
        assert facts[0].kind == "percentage"
        assert facts[0].value == 45.2

    def test_comma_formatted_number(self):
        facts = _extract_facts("Toronto had 12,345 building permits.")
        assert any(f.value == 12345 and f.kind == "integer" for f in facts)

    def test_float(self):
        facts = _extract_facts("Average was 3.14 units.")
        assert any(f.value == pytest.approx(3.14) and f.kind == "float" for f in facts)

    def test_plain_integer(self):
        facts = _extract_facts("There were 42 incidents.")
        assert any(f.value == 42 and f.kind == "integer" for f in facts)

    def test_quoted_string(self):
        facts = _extract_facts('The top category was "residential".')
        assert any(f.value == "residential" and f.kind == "string" for f in facts)

    def test_no_double_extraction_percent(self):
        """Percentage value should not also appear as a plain number."""
        facts = _extract_facts("Growth was 20%.")
        assert len(facts) == 1
        assert facts[0].kind == "percentage"

    def test_no_facts(self):
        facts = _extract_facts("Data looks good overall.")
        assert len(facts) == 0


# ---------------------------------------------------------------------------
# Check 1: Claim vs Query Results
# ---------------------------------------------------------------------------


class TestClaimVsResults:
    def test_correct_claim_single_number(self, cache):
        _store(cache, "t_permits", pd.DataFrame({
            "city": ["Toronto"],
            "permits": [12345],
        }))
        result = validate(
            sql='SELECT * FROM "t_permits"',
            claim="Toronto had 12,345 permits.",
            cache=cache,
        )
        assert result.valid is True

    def test_wrong_number(self, cache):
        _store(cache, "t_permits", pd.DataFrame({
            "city": ["Toronto"],
            "permits": [12345],
        }))
        result = validate(
            sql='SELECT * FROM "t_permits"',
            claim="Toronto had 99,999 permits.",
            cache=cache,
        )
        assert result.valid is False

    def test_no_numbers_in_claim(self, cache):
        _store(cache, "t_permits", pd.DataFrame({
            "city": ["Toronto"],
            "permits": [12345],
        }))
        result = validate(
            sql='SELECT * FROM "t_permits"',
            claim="Toronto had many permits.",
            cache=cache,
        )
        assert result.valid is None

    def test_percentage_vs_fraction(self, cache):
        """45.2% in claim should match 0.452 in data."""
        _store(cache, "t_rates", pd.DataFrame({
            "category": ["residential"],
            "rate": [0.452],
        }))
        result = validate(
            sql='SELECT * FROM "t_rates"',
            claim='The "residential" rate was 45.2%.',
            cache=cache,
        )
        assert result.valid is True

    def test_percentage_as_whole_number(self, cache):
        """45.2% in claim should also match 45.2 in data."""
        _store(cache, "t_rates", pd.DataFrame({
            "category": ["A"],
            "pct": [45.2],
        }))
        result = validate(
            sql='SELECT * FROM "t_rates"',
            claim="Category A had 45.2% share.",
            cache=cache,
        )
        assert result.valid is True

    def test_derived_value_not_hard_fail(self, cache):
        """A derived number (e.g. 'grew by 20%') with other facts found → unverifiable."""
        _store(cache, "t_growth", pd.DataFrame({
            "year": [2022, 2023],
            "count": [100, 120],
        }))
        result = validate(
            sql='SELECT * FROM "t_growth"',
            claim="Count grew by 20% from 100 to 120.",
            cache=cache,
        )
        # 100 and 120 found, 20% is derived → should not be a hard fail
        assert result.valid is not False

    def test_empty_result_set(self, cache):
        _store(cache, "t_empty", pd.DataFrame({
            "city": pd.Series([], dtype=str),
            "permits": pd.Series([], dtype=int),
        }))
        result = validate(
            sql='SELECT * FROM "t_empty"',
            claim="Toronto had 100 permits.",
            cache=cache,
        )
        assert result.valid is None

    def test_invalid_query_error(self, cache):
        result = validate(
            sql="DROP TABLE foo",
            claim="Some claim.",
            cache=cache,
        )
        assert result.valid is False
        assert any("SQL failed validation" in s for s in result.steps)

    def test_string_case_insensitive(self, cache):
        _store(cache, "t_cities", pd.DataFrame({
            "city": ["Toronto"],
            "count": [500],
        }))
        result = validate(
            sql='SELECT * FROM "t_cities"',
            claim='"toronto" had 500 incidents.',
            cache=cache,
        )
        assert result.valid is True

    def test_float_tolerance(self, cache):
        """Display rounding should still pass (499.7 vs 500)."""
        _store(cache, "t_vals", pd.DataFrame({
            "metric": ["cost"],
            "value": [499.7],
        }))
        result = validate(
            sql='SELECT * FROM "t_vals"',
            claim="The cost was 500.",
            cache=cache,
        )
        # 500 is an integer, exact match required
        # 499.7 != 500 exactly → fail
        assert result.valid is False

    def test_float_claim_with_tolerance(self, cache):
        """Float claim 499.9 should match 499.7 within tolerance."""
        _store(cache, "t_vals", pd.DataFrame({
            "metric": ["cost"],
            "value": [499.7],
        }))
        result = validate(
            sql='SELECT * FROM "t_vals"',
            claim="The cost was 499.9.",
            cache=cache,
        )
        assert result.valid is True


# ---------------------------------------------------------------------------
# Check 2: Data Stability
# ---------------------------------------------------------------------------


class TestDataStability:
    def test_stable_data(self, cache):
        _store(cache, "t_stable", pd.DataFrame({"x": [1]}))
        result = validate(
            sql='SELECT * FROM "t_stable"',
            claim="x is 1.",
            cache=cache,
        )
        assert result.valid is True
        assert any("stable" in s for s in result.steps)

    def test_table_removed(self, cache):
        """If table is removed between query and stability check, report it."""
        _store(cache, "t_removed", pd.DataFrame({"x": [1]}))
        # Validate will query successfully, then we need the table gone for stability.
        # Since validate runs both checks sequentially and the table exists throughout,
        # we can't easily test removal without mocking. Test via direct function call.
        from ontario_data.validate_results import _check_data_stability

        # Check stability for a table that was never stored
        valid, steps = _check_data_stability(
            sql='SELECT * FROM "nonexistent_table"',
            cache=cache,
            query_started_at=0,
        )
        assert valid is False
        assert any("no longer cached" in s for s in steps)

    def test_join_covers_all_tables(self, cache):
        _store(cache, "t_a", pd.DataFrame({"id": [1], "val": [10]}),
               resource_id="ra", dataset_id="dsa")
        _store(cache, "t_b", pd.DataFrame({"id": [1], "name": ["foo"]}),
               resource_id="rb", dataset_id="dsb")
        result = validate(
            sql='SELECT * FROM "t_a" JOIN "t_b" ON "t_a".id = "t_b".id',
            claim="id 1 has val 10 and name is \"foo\".",
            cache=cache,
        )
        # Both tables should be checked for stability
        stability_steps = [s for s in result.steps if "stable" in s or "t_a" in s or "t_b" in s]
        assert any("t_a" in s for s in stability_steps)
        assert any("t_b" in s for s in stability_steps)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_oversized_claim(self, cache):
        _store(cache, "t_data", pd.DataFrame({"x": [1]}))
        long_claim = "x" * 11_000
        result = validate(
            sql='SELECT * FROM "t_data"',
            claim=long_claim,
            cache=cache,
        )
        assert result.valid is None
        assert "too long" in result.summary.lower()


# ---------------------------------------------------------------------------
# MCP tool wrapper
# ---------------------------------------------------------------------------


class TestValidateResultTool:
    @pytest.mark.asyncio
    async def test_tool_returns_string(self, cache):
        _store(cache, "t_tool", pd.DataFrame({
            "city": ["Ottawa"],
            "count": [200],
        }))
        from ontario_data.tools.validation import validate_result

        ctx = make_mock_context(cache)
        output = await validate_result(
            sql='SELECT * FROM "t_tool"',
            claim="Ottawa had 200 incidents.",
            ctx=ctx,
        )
        assert isinstance(output, str)
        assert "PASS" in output

    @pytest.mark.asyncio
    async def test_tool_fail(self, cache):
        _store(cache, "t_tool2", pd.DataFrame({
            "city": ["Ottawa"],
            "count": [200],
        }))
        from ontario_data.tools.validation import validate_result

        ctx = make_mock_context(cache)
        output = await validate_result(
            sql='SELECT * FROM "t_tool2"',
            claim="Ottawa had 999 incidents.",
            ctx=ctx,
        )
        assert "FAIL" in output
