import json
from unittest.mock import AsyncMock

import pytest

from ontario_data.cache import CacheManager
from ontario_data.utils import (
    ResourceNotCachedError,
    json_response,
    make_table_name,
    require_cached,
    resolve_dataset,
    resolve_resource_portal,
    strip_internal_fields,
    unwrap_first_match,
)


class TestStripInternalFields:
    def test_removes_underscore_fields(self):
        records = [{"_id": 1, "name": "Alice", "_full_text": "..."}]
        result = strip_internal_fields(records)
        assert result == [{"name": "Alice"}]

    def test_empty_records(self):
        assert strip_internal_fields([]) == []

    def test_no_internal_fields(self):
        records = [{"name": "Bob", "age": 30}]
        assert strip_internal_fields(records) == records


class TestMakeTableName:
    def test_basic(self):
        result = make_table_name("My Dataset", "abcd1234-ef56")
        assert result == "ds_ontario_my_dataset_abcd1234"

    def test_special_characters(self):
        result = make_table_name("Health & Safety (2024)", "12345678-abcd")
        assert result == "ds_ontario_health_safety_2024_12345678"

    def test_none_dataset_name(self):
        result = make_table_name(None, "abcd1234")
        assert result.startswith("ds_ontario_unknown_")

    def test_portal_prefix(self):
        result = make_table_name("TTC Routes", "abcd1234", portal="toronto")
        assert result == "ds_toronto_ttc_routes_abcd1234"

    def test_long_name_truncated(self):
        long_name = "a" * 100
        result = make_table_name(long_name, "abcd1234")
        # ds_ + portal_ + slug(40) + _ + prefix(8)
        assert len(result) <= 3 + 8 + 1 + 40 + 1 + 8


class TestRequireCached:
    def test_found(self, tmp_path):
        cache = CacheManager(db_path=str(tmp_path / "test.duckdb"))
        cache.initialize()
        # Insert a fake cached entry
        cache.execute_sql(
            "INSERT INTO _cache_metadata (resource_id, table_name) VALUES (?, ?)",
            ["r1", "ds_test_r1"],
        )
        result = require_cached(cache, "r1")
        assert result == "ds_test_r1"

    def test_not_found(self, tmp_path):
        cache = CacheManager(db_path=str(tmp_path / "test.duckdb"))
        cache.initialize()
        with pytest.raises(ResourceNotCachedError, match="not cached"):
            require_cached(cache, "nonexistent")


class TestJsonResponse:
    def test_basic(self):
        result = json_response(status="ok", count=42)
        parsed = json.loads(result)
        assert parsed["status"] == "ok"
        assert parsed["count"] == 42

    def test_handles_non_serializable(self):
        from datetime import datetime, timezone
        result = json_response(timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc))
        parsed = json.loads(result)
        assert "2024" in parsed["timestamp"]


class TestUnwrapFirstMatch:
    def test_success_extraction(self):
        results = [("toronto", {"id": "ds1", "title": "Test"}, None)]
        portal, data = unwrap_first_match(results, "ds1")
        assert portal == "toronto"
        assert data["id"] == "ds1"

    def test_all_errors_raises(self):
        results = [
            ("ontario", None, "not found"),
            ("toronto", None, "timeout"),
        ]
        with pytest.raises(ValueError, match="not found"):
            unwrap_first_match(results, "abc123")

    def test_empty_results(self):
        with pytest.raises(ValueError, match="no portals available"):
            unwrap_first_match([], "abc123")

    def test_entity_type_in_message(self):
        results = [("ontario", None, "404")]
        with pytest.raises(ValueError, match="Resource 'r1' not found"):
            unwrap_first_match(results, "r1", "Resource")

    def test_default_entity_type_is_dataset(self):
        results = [("ontario", None, "404")]
        with pytest.raises(ValueError, match="Dataset 'ds1' not found"):
            unwrap_first_match(results, "ds1")


class TestResolveDataset:
    @pytest.mark.asyncio
    async def test_prefixed_id_direct_call(self, make_portal_context):
        ckan = AsyncMock()
        ckan.package_show.return_value = {"id": "ds1", "title": "Test"}
        ctx = make_portal_context(portal_clients={"toronto": ckan})

        portal, bare_id, ds = await resolve_dataset(ctx, "toronto:ds1")
        assert portal == "toronto"
        assert bare_id == "ds1"
        assert ds["title"] == "Test"
        ckan.package_show.assert_called_once_with("ds1")

    @pytest.mark.asyncio
    async def test_bare_id_fans_out(self, make_portal_context):
        ontario_ckan = AsyncMock()
        ontario_ckan.package_show.side_effect = ValueError("not found")
        toronto_ckan = AsyncMock()
        toronto_ckan.package_show.return_value = {"id": "ds1", "title": "Found"}
        ctx = make_portal_context(
            portal_clients={"ontario": ontario_ckan, "toronto": toronto_ckan},
        )

        portal, bare_id, ds = await resolve_dataset(ctx, "ds1")
        assert portal == "toronto"
        assert bare_id == "ds1"
        assert ds["title"] == "Found"

    @pytest.mark.asyncio
    async def test_not_found_raises(self, make_portal_context):
        ckan = AsyncMock()
        ckan.package_show.side_effect = ValueError("not found")
        ctx = make_portal_context(
            portal_clients={"ontario": ckan, "toronto": ckan},
        )

        with pytest.raises(ValueError, match="Dataset 'nonexistent' not found"):
            await resolve_dataset(ctx, "nonexistent")


class TestResolveResourcePortal:
    @pytest.mark.asyncio
    async def test_prefixed_id_returns_immediately(self, make_portal_context):
        ckan = AsyncMock()
        ctx = make_portal_context(portal_clients={"toronto": ckan})

        portal, bare_id = await resolve_resource_portal(ctx, "toronto:r1")
        assert portal == "toronto"
        assert bare_id == "r1"
        # No API call made — prefix is enough
        ckan.resource_show.assert_not_called()

    @pytest.mark.asyncio
    async def test_bare_id_fans_out(self, make_portal_context):
        ontario_ckan = AsyncMock()
        ontario_ckan.resource_show.side_effect = ValueError("not found")
        toronto_ckan = AsyncMock()
        toronto_ckan.resource_show.return_value = {"id": "r1"}
        ctx = make_portal_context(
            portal_clients={"ontario": ontario_ckan, "toronto": toronto_ckan},
        )

        portal, bare_id = await resolve_resource_portal(ctx, "r1")
        assert portal == "toronto"
        assert bare_id == "r1"

    @pytest.mark.asyncio
    async def test_not_found_raises(self, make_portal_context):
        ckan = AsyncMock()
        ckan.resource_show.side_effect = ValueError("not found")
        ctx = make_portal_context(
            portal_clients={"ontario": ckan, "toronto": ckan},
        )

        with pytest.raises(ValueError, match="Resource 'nonexistent' not found"):
            await resolve_resource_portal(ctx, "nonexistent")
