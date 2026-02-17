"""Unit tests for tool functions using mock context and cache."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from ontario_data.cache import CacheManager, InvalidQueryError
from ontario_data.utils import ResourceNotCachedError


def make_mock_context(cache: CacheManager, ckan=None):
    """Create a mock MCP context with cache and optional CKAN client."""
    ctx = MagicMock()
    ctx.fastmcp._lifespan_result = {
        "cache": cache,
        "ckan": ckan or AsyncMock(),
    }
    ctx.report_progress = AsyncMock()
    return ctx


@pytest.fixture
def cache(tmp_path):
    """Create a fresh CacheManager for testing."""
    c = CacheManager(db_path=str(tmp_path / "test.duckdb"))
    c.initialize()
    yield c
    c.close()


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
        result = json.loads(await query_cached.fn(
            sql='SELECT * FROM "ds_test_data_test_r1" LIMIT 2',
            ctx=ctx,
        ))
        assert result["row_count"] == 2
        assert len(result["records"]) == 2

    @pytest.mark.asyncio
    async def test_rejects_drop(self, populated_cache):
        from ontario_data.tools.querying import query_cached

        ctx = make_mock_context(populated_cache)
        with pytest.raises(InvalidQueryError):
            await query_cached.fn(sql='DROP TABLE "ds_test_data_test_r1"', ctx=ctx)

    @pytest.mark.asyncio
    async def test_error_includes_available_tables(self, populated_cache):
        from ontario_data.tools.querying import query_cached

        ctx = make_mock_context(populated_cache)
        with pytest.raises(Exception, match="Available tables"):
            await query_cached.fn(sql='SELECT * FROM "nonexistent_table"', ctx=ctx)


class TestCacheInfo:
    @pytest.mark.asyncio
    async def test_returns_stats_and_list(self, populated_cache):
        from ontario_data.tools.retrieval import cache_info

        ctx = make_mock_context(populated_cache)
        result = json.loads(await cache_info.fn(ctx=ctx))
        assert result["table_count"] == 1
        assert result["total_rows"] == 4
        assert len(result["datasets"]) == 1
        assert result["datasets"][0]["table_name"] == "ds_test_data_test_r1"


class TestCacheManage:
    @pytest.mark.asyncio
    async def test_remove(self, populated_cache):
        from ontario_data.tools.retrieval import cache_manage

        ctx = make_mock_context(populated_cache)
        result = json.loads(await cache_manage.fn(action="remove", resource_id="test-r1", ctx=ctx))
        assert result["status"] == "removed"
        assert not populated_cache.is_cached("test-r1")

    @pytest.mark.asyncio
    async def test_clear(self, populated_cache):
        from ontario_data.tools.retrieval import cache_manage

        ctx = make_mock_context(populated_cache)
        result = json.loads(await cache_manage.fn(action="clear", ctx=ctx))
        assert result["status"] == "cleared"
        assert result["removed_count"] == 1

    @pytest.mark.asyncio
    async def test_invalid_action(self, cache):
        from ontario_data.tools.retrieval import cache_manage

        ctx = make_mock_context(cache)
        with pytest.raises(ValueError, match="Invalid action"):
            await cache_manage.fn(action="invalid", ctx=ctx)

    @pytest.mark.asyncio
    async def test_remove_requires_resource_id(self, cache):
        from ontario_data.tools.retrieval import cache_manage

        ctx = make_mock_context(cache)
        with pytest.raises(ValueError, match="resource_id is required"):
            await cache_manage.fn(action="remove", ctx=ctx)


class TestProfileData:
    @pytest.mark.asyncio
    async def test_profile_uses_summarize(self, populated_cache):
        from ontario_data.tools.quality import profile_data

        ctx = make_mock_context(populated_cache)
        result = json.loads(await profile_data.fn(resource_id="test-r1", ctx=ctx))
        assert result["row_count"] == 4
        assert result["table_name"] == "ds_test_data_test_r1"
        assert len(result["columns"]) > 0

    @pytest.mark.asyncio
    async def test_profile_not_cached(self, cache):
        from ontario_data.tools.quality import profile_data

        ctx = make_mock_context(cache)
        with pytest.raises(ResourceNotCachedError):
            await profile_data.fn(resource_id="nonexistent", ctx=ctx)


class TestCheckDataQuality:
    @pytest.mark.asyncio
    async def test_quality_report(self, populated_cache):
        from ontario_data.tools.quality import check_data_quality

        ctx = make_mock_context(populated_cache)
        result = json.loads(await check_data_quality.fn(resource_id="test-r1", ctx=ctx))
        assert result["total_rows"] == 4
        assert len(result["columns"]) == 3  # name, age, salary

    @pytest.mark.asyncio
    async def test_quality_not_cached(self, cache):
        from ontario_data.tools.quality import check_data_quality

        ctx = make_mock_context(cache)
        with pytest.raises(ResourceNotCachedError):
            await check_data_quality.fn(resource_id="nonexistent", ctx=ctx)


class TestDownloadResourceAlreadyCached:
    @pytest.mark.asyncio
    async def test_returns_staleness_info(self, populated_cache):
        from ontario_data.tools.retrieval import download_resource

        ctx = make_mock_context(populated_cache)
        result = json.loads(await download_resource.fn(resource_id="test-r1", ctx=ctx))
        assert result["status"] == "already_cached"
        assert result["table_name"] == "ds_test_data_test_r1"
        assert "staleness" in result
