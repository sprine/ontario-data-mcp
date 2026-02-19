import json

import pytest

from ontario_data.cache import CacheManager
from ontario_data.utils import (
    ResourceNotCachedError,
    json_response,
    make_table_name,
    require_cached,
    strip_internal_fields,
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
