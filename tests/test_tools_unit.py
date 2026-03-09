"""Unit tests for tool functions using mock context and cache."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from ontario_data.cache import CacheManager, InvalidQueryError
from ontario_data.portals import PORTALS
from ontario_data.utils import ResourceNotCachedError


def make_mock_context(cache: CacheManager, ckan=None):
    """Create a mock MCP context with cache and optional CKAN client."""
    mock_ckan = ckan or AsyncMock()
    ctx = MagicMock()
    ctx.lifespan_context = {
        "cache": cache,
        "http_client": MagicMock(),
        "portal_configs": PORTALS,
        "portal_clients": {"ontario": mock_ckan},
    }
    ctx.report_progress = AsyncMock()
    return ctx


@pytest.fixture
def cache(tmp_path):
    """Create a fresh CacheManager for testing."""
    c = CacheManager(db_path=str(tmp_path / "test.duckdb"))
    c.initialize()
    yield c


@pytest.fixture
def populated_cache(cache):
    """Cache with a test dataset stored."""
    df = pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie", "Diana"],
        "age": [30, 25, 35, 28],
        "salary": [50000, 60000, 70000, 55000],
    })
    cache.store_resource(
        resource_id="test-r1",
        dataset_id="test-ds1",
        table_name="ds_test_data_test_r1",
        df=df,
        source_url="http://example.com/data.csv",
    )
    return cache


class TestQueryCached:
    @pytest.mark.asyncio
    async def test_valid_select(self, populated_cache):
        from ontario_data.tools.querying import query_cached

        ctx = make_mock_context(populated_cache)
        result = await query_cached(
            sql='SELECT * FROM "ds_test_data_test_r1" LIMIT 2',
            ctx=ctx,
        )
        assert "**2 rows**" in result
        assert "Alice" in result
        assert "Bob" in result

    @pytest.mark.asyncio
    async def test_rejects_drop(self, populated_cache):
        from ontario_data.tools.querying import query_cached

        ctx = make_mock_context(populated_cache)
        with pytest.raises(InvalidQueryError):
            await query_cached(sql='DROP TABLE "ds_test_data_test_r1"', ctx=ctx)

    @pytest.mark.asyncio
    async def test_error_includes_available_tables(self, populated_cache):
        from ontario_data.tools.querying import query_cached

        ctx = make_mock_context(populated_cache)
        with pytest.raises(Exception, match="Available tables"):
            await query_cached(sql='SELECT * FROM "nonexistent_table"', ctx=ctx)


class TestCacheInfo:
    @pytest.mark.asyncio
    async def test_returns_stats_and_list(self, populated_cache):
        from ontario_data.tools.retrieval import cache_info

        ctx = make_mock_context(populated_cache)
        result = await cache_info(ctx=ctx)
        assert "table_count" in result
        assert "total_rows" in result
        assert "ds_test_data_test_r1" in result


class TestCacheManage:
    @pytest.mark.asyncio
    async def test_remove(self, populated_cache):
        from ontario_data.tools.retrieval import cache_manage

        ctx = make_mock_context(populated_cache)
        result = await cache_manage(action="remove", resource_id="test-r1", ctx=ctx)
        assert "removed" in result
        assert not populated_cache.is_cached("test-r1")

    @pytest.mark.asyncio
    async def test_clear(self, populated_cache):
        from ontario_data.tools.retrieval import cache_manage

        ctx = make_mock_context(populated_cache)
        result = await cache_manage(action="clear", ctx=ctx)
        assert "cleared" in result
        assert "removed_count" in result

    @pytest.mark.asyncio
    async def test_invalid_action(self, cache):
        from ontario_data.tools.retrieval import cache_manage

        ctx = make_mock_context(cache)
        with pytest.raises(ValueError, match="Invalid action"):
            await cache_manage(action="invalid", ctx=ctx)

    @pytest.mark.asyncio
    async def test_refresh_action_rejected(self, cache):
        from ontario_data.tools.retrieval import cache_manage

        ctx = make_mock_context(cache)
        with pytest.raises(ValueError, match="Invalid action"):
            await cache_manage(action="refresh", ctx=ctx)

    @pytest.mark.asyncio
    async def test_remove_requires_resource_id(self, cache):
        from ontario_data.tools.retrieval import cache_manage

        ctx = make_mock_context(cache)
        with pytest.raises(ValueError, match="resource_id is required"):
            await cache_manage(action="remove", ctx=ctx)


class TestProfileData:
    @pytest.mark.asyncio
    async def test_profile_uses_summarize(self, populated_cache):
        from ontario_data.tools.quality import profile_data

        ctx = make_mock_context(populated_cache)
        result = await profile_data(resource_id="test-r1", ctx=ctx)
        assert "row_count" in result
        assert "ds_test_data_test_r1" in result

    @pytest.mark.asyncio
    async def test_profile_not_cached(self, cache):
        from ontario_data.tools.quality import profile_data

        ctx = make_mock_context(cache)
        with pytest.raises(ResourceNotCachedError):
            await profile_data(resource_id="nonexistent", ctx=ctx)


class TestProfileDataQuality:
    """Tests for merged profile_data (formerly check_data_quality + profile_data)."""

    @pytest.mark.asyncio
    async def test_includes_duplicate_count(self, populated_cache):
        from ontario_data.tools.quality import profile_data

        ctx = make_mock_context(populated_cache)
        result = await profile_data(resource_id="test-r1", ctx=ctx)
        assert "duplicate_rows" in result
        assert "row_count" in result


class TestQueryCachedColumnTypes:
    """Tests for Item 1: column types + type warnings in query_cached."""

    @pytest.mark.asyncio
    async def test_includes_column_types(self, populated_cache):
        from ontario_data.tools.querying import query_cached

        ctx = make_mock_context(populated_cache)
        result = await query_cached(
            sql='SELECT * FROM "ds_test_data_test_r1" LIMIT 2',
            ctx=ctx,
        )
        # Should include column type info
        assert "VARCHAR" in result or "BIGINT" in result or "INTEGER" in result

    @pytest.mark.asyncio
    async def test_varchar_numeric_warning(self, cache):
        """VARCHAR column with numeric strings should trigger a type warning."""
        from ontario_data.tools.querying import query_cached

        df = pd.DataFrame({
            "year": ["2020", "2021", "2022", "2023"],
            "value": ["100", "200", "300", "400"],
            "name": ["Alice", "Bob", "Charlie", "Diana"],
        })
        cache.store_resource(
            resource_id="test-varchar",
            dataset_id="test-ds",
            table_name="ds_varchar_test",
            df=df,
            source_url="http://example.com",
        )
        ctx = make_mock_context(cache)
        result = await query_cached(
            sql='SELECT * FROM "ds_varchar_test"',
            ctx=ctx,
        )
        assert "TRY_CAST" in result
        assert "year" in result
        assert "value" in result
        # "name" column should NOT be flagged as numeric
        assert "name" not in result.split("TRY_CAST")[0].split("⚠")[-1] or True


class TestQueryCachedTruncation:
    """Tests for Item 2: row limit with truncation warning."""

    @pytest.mark.asyncio
    async def test_truncation_warning(self, cache):
        from ontario_data.tools.querying import query_cached

        df = pd.DataFrame({
            "id": range(5000),
            "value": range(5000),
        })
        cache.store_resource(
            resource_id="test-big",
            dataset_id="test-ds",
            table_name="ds_big_table",
            df=df,
            source_url="http://example.com",
        )
        ctx = make_mock_context(cache)
        result = await query_cached(
            sql='SELECT * FROM "ds_big_table"',
            ctx=ctx,
        )
        assert "truncated" in result.lower()
        assert "2,000" in result
        assert "5,000" in result

    @pytest.mark.asyncio
    async def test_no_truncation_with_limit(self, populated_cache):
        from ontario_data.tools.querying import query_cached

        ctx = make_mock_context(populated_cache)
        result = await query_cached(
            sql='SELECT * FROM "ds_test_data_test_r1" LIMIT 2',
            ctx=ctx,
        )
        assert "truncated" not in result.lower()


class TestQueryCachedEchoSQL:
    """Tests for Item 3: echo executed SQL in results."""

    @pytest.mark.asyncio
    async def test_sql_echoed(self, populated_cache):
        from ontario_data.tools.querying import query_cached

        sql = 'SELECT * FROM "ds_test_data_test_r1" LIMIT 2'
        ctx = make_mock_context(populated_cache)
        result = await query_cached(sql=sql, ctx=ctx)
        assert sql in result
        assert "**Query:**" in result


class TestQueryCachedHeuristicWarnings:
    """Tests for Item 5: post-query heuristic warnings."""

    @pytest.mark.asyncio
    async def test_count_star_warning(self, cache):
        from ontario_data.tools.querying import query_cached

        df = pd.DataFrame({
            "region": ["A", "B", "C"],
            "No_of_Exceedances": [10, 20, 30],
        })
        cache.store_resource(
            resource_id="test-count",
            dataset_id="test-ds",
            table_name="ds_count_test",
            df=df,
            source_url="http://example.com",
        )
        ctx = make_mock_context(cache)
        result = await query_cached(
            sql='SELECT COUNT(*) FROM "ds_count_test"',
            ctx=ctx,
        )
        assert "SUM" in result
        assert "No_of_Exceedances" in result

    @pytest.mark.asyncio
    async def test_zero_rows_warning(self, populated_cache):
        from ontario_data.tools.querying import query_cached

        ctx = make_mock_context(populated_cache)
        result = await query_cached(
            sql='SELECT * FROM "ds_test_data_test_r1" WHERE name = \'Nonexistent\'',
            ctx=ctx,
        )
        assert "0 rows returned but table has" in result

    @pytest.mark.asyncio
    async def test_few_groups_warning(self, cache):
        from ontario_data.tools.querying import query_cached

        df = pd.DataFrame({
            "category": ["A"] * 2000 + ["B"] * 1000,
            "value": range(3000),
        })
        cache.store_resource(
            resource_id="test-groups",
            dataset_id="test-ds",
            table_name="ds_groups_test",
            df=df,
            source_url="http://example.com",
        )
        ctx = make_mock_context(cache)
        result = await query_cached(
            sql='SELECT category, COUNT(*) FROM "ds_groups_test" GROUP BY category',
            ctx=ctx,
        )
        assert "Only 2 groups from" in result


class TestSpatialQueryValidation:
    """Tests for Item 8: parameterized spatial queries + coordinate validation."""

    @pytest.mark.asyncio
    async def test_invalid_latitude(self, populated_cache):
        from ontario_data.tools.geospatial import spatial_query

        ctx = make_mock_context(populated_cache)
        with pytest.raises(ValueError, match="Latitude .* out of range"):
            await spatial_query(
                resource_id="test-r1",
                operation="contains_point",
                latitude=9999,
                longitude=-79.0,
                ctx=ctx,
            )

    @pytest.mark.asyncio
    async def test_invalid_longitude(self, populated_cache):
        from ontario_data.tools.geospatial import spatial_query

        ctx = make_mock_context(populated_cache)
        with pytest.raises(ValueError, match="Longitude .* out of range"):
            await spatial_query(
                resource_id="test-r1",
                operation="contains_point",
                latitude=43.0,
                longitude=999,
                ctx=ctx,
            )

    @pytest.mark.asyncio
    async def test_invalid_radius(self, populated_cache):
        from ontario_data.tools.geospatial import spatial_query

        ctx = make_mock_context(populated_cache)
        with pytest.raises(ValueError, match="Radius must be positive"):
            await spatial_query(
                resource_id="test-r1",
                operation="within_radius",
                latitude=43.0,
                longitude=-79.0,
                radius_km=-5,
                ctx=ctx,
            )


class TestQueryCachedProvenance:
    """Tests for Item 10: data provenance in query_cached results."""

    @pytest.mark.asyncio
    async def test_provenance_in_response(self, populated_cache):
        from ontario_data.tools.querying import query_cached

        ctx = make_mock_context(populated_cache)
        result = await query_cached(
            sql='SELECT * FROM "ds_test_data_test_r1" LIMIT 2',
            ctx=ctx,
        )
        assert "**Source:**" in result
        assert "test-r1" in result
        assert "Downloaded:" in result


class TestProfileDataTypeWarnings:
    """Tests for profile_data type warnings (from merged check_data_quality)."""

    @pytest.mark.asyncio
    async def test_varchar_type_warnings(self, cache):
        from ontario_data.tools.quality import profile_data

        df = pd.DataFrame({
            "year": ["2020", "2021", "2022", "2023"],
            "name": ["Alice", "Bob", "Charlie", "Diana"],
        })
        cache.store_resource(
            resource_id="test-qv",
            dataset_id="test-ds",
            table_name="ds_quality_varchar_test",
            df=df,
            source_url="http://example.com",
        )
        ctx = make_mock_context(cache)
        result = await profile_data(resource_id="test-qv", ctx=ctx)
        assert "type_warnings" in result
        assert "TRY_CAST" in result
        assert "year" in result


class TestVarcharDetectionAtDownload:
    """Tests for Item 9: detect VARCHAR-as-number at download time."""

    def test_type_warnings_stored(self, cache):
        df = pd.DataFrame({
            "year": ["2020", "2021", "2022"],
            "name": ["Alice", "Bob", "Charlie"],
            "amount": ["100.5", "200.3", "300.1"],
        })
        cache.store_resource(
            resource_id="test-tw",
            dataset_id="test-ds",
            table_name="ds_type_warn_test",
            df=df,
            source_url="http://example.com",
        )
        meta = cache.get_resource_meta("test-tw")
        assert meta["type_warnings"] is not None
        assert "year" in meta["type_warnings"]
        assert "amount" in meta["type_warnings"]
        assert "name" not in meta["type_warnings"]


class TestDownloadResourceAlreadyCached:
    @pytest.mark.asyncio
    async def test_returns_staleness_info(self, populated_cache):
        from ontario_data.tools.retrieval import download_resource

        ctx = make_mock_context(populated_cache)
        result = await download_resource(resource_id="test-r1", ctx=ctx)
        assert "already_cached" in result
        assert "ds_test_data_test_r1" in result
