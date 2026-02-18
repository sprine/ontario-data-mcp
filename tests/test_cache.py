import pytest
import pandas as pd
from ontario_data.cache import CacheManager, InvalidQueryError, _has_semicolons_outside_strings


@pytest.fixture
def cache(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    mgr = CacheManager(db_path)
    mgr.initialize()
    return mgr


class TestInitialization:
    def test_creates_metadata_tables(self, cache):
        tables = cache.conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
        table_names = [t[0] for t in tables]
        assert "_cache_metadata" in table_names
        assert "_dataset_metadata" in table_names

    def test_idempotent_init(self, cache):
        cache.initialize()  # Should not raise
        tables = cache.conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name='_cache_metadata'"
        ).fetchone()
        assert tables[0] == 1


class TestStoreDataFrame:
    def test_store_and_retrieve(self, cache):
        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
        cache.store_resource("r1", "ds1", "test_table", df, "http://example.com/data.csv")

        result = cache.conn.execute("SELECT * FROM test_table ORDER BY name").fetchdf()
        assert len(result) == 2
        assert list(result["name"]) == ["Alice", "Bob"]

    def test_metadata_recorded(self, cache):
        df = pd.DataFrame({"x": [1, 2, 3]})
        cache.store_resource("r1", "ds1", "tbl", df, "http://example.com")

        meta = cache.conn.execute(
            "SELECT * FROM _cache_metadata WHERE resource_id='r1'"
        ).fetchone()
        assert meta is not None
        assert meta[4] == 3  # row_count

    def test_force_refresh_replaces(self, cache):
        df1 = pd.DataFrame({"x": [1]})
        df2 = pd.DataFrame({"x": [1, 2, 3]})
        cache.store_resource("r1", "ds1", "tbl", df1, "http://example.com")
        cache.store_resource("r1", "ds1", "tbl", df2, "http://example.com")

        result = cache.conn.execute("SELECT count(*) FROM tbl").fetchone()
        assert result[0] == 3


class TestCacheQueries:
    def test_list_cached(self, cache):
        df = pd.DataFrame({"x": [1]})
        cache.store_resource("r1", "ds1", "tbl1", df, "http://example.com/1")
        cache.store_resource("r2", "ds2", "tbl2", df, "http://example.com/2")

        cached = cache.list_cached()
        assert len(cached) == 2

    def test_is_cached(self, cache):
        assert not cache.is_cached("r1")
        df = pd.DataFrame({"x": [1]})
        cache.store_resource("r1", "ds1", "tbl", df, "http://example.com")
        assert cache.is_cached("r1")

    def test_get_table_name(self, cache):
        df = pd.DataFrame({"x": [1]})
        cache.store_resource("r1", "ds1", "my_table", df, "http://example.com")
        assert cache.get_table_name("r1") == "my_table"

    def test_remove_resource(self, cache):
        df = pd.DataFrame({"x": [1]})
        cache.store_resource("r1", "ds1", "tbl", df, "http://example.com")
        cache.remove_resource("r1")
        assert not cache.is_cached("r1")

    def test_cache_stats(self, cache):
        df = pd.DataFrame({"x": range(100)})
        cache.store_resource("r1", "ds1", "tbl", df, "http://example.com")
        stats = cache.get_stats()
        assert stats["table_count"] == 1
        assert stats["total_rows"] == 100


class TestSQLQuery:
    def test_run_sql(self, cache):
        df = pd.DataFrame({"name": ["Alice", "Bob"], "score": [90, 85]})
        cache.store_resource("r1", "ds1", "scores", df, "http://example.com")

        result = cache.query("SELECT name, score FROM scores WHERE score > 87")
        assert len(result) == 1
        assert result[0]["name"] == "Alice"


class TestSemicolonParser:
    def test_bare_semicolon_detected(self):
        assert _has_semicolons_outside_strings("SELECT 1; DROP TABLE x") is True

    def test_semicolon_in_string_allowed(self):
        assert _has_semicolons_outside_strings("SELECT * FROM t WHERE name = 'Phosphorus; total'") is False

    def test_no_semicolons(self):
        assert _has_semicolons_outside_strings("SELECT * FROM t") is False

    def test_escaped_quote_with_semicolon(self):
        # Semicolon after escaped quote — still inside string
        assert _has_semicolons_outside_strings(r"SELECT * WHERE x = 'it\'s; here'") is False

    def test_semicolon_after_closing_string(self):
        assert _has_semicolons_outside_strings("SELECT * WHERE x = 'safe';") is True

    def test_query_cached_allows_semicolons_in_strings(self, cache):
        df = pd.DataFrame({"name": ["Phosphorus; total", "Nitrogen"], "value": [1.0, 2.0]})
        cache.store_resource("r1", "ds1", "params", df, "http://example.com")
        result = cache.query("SELECT * FROM params WHERE name = 'Phosphorus; total'")
        assert len(result) == 1
        assert result[0]["name"] == "Phosphorus; total"

    def test_query_cached_rejects_bare_semicolons(self, cache):
        df = pd.DataFrame({"x": [1]})
        cache.store_resource("r1", "ds1", "tbl", df, "http://example.com")
        with pytest.raises(InvalidQueryError):
            cache.query("SELECT * FROM tbl; DROP TABLE tbl")


class TestDatasetMetadata:
    def test_store_and_get_metadata(self, cache):
        meta = {"id": "ds1", "title": "Test", "organization": {"name": "health"}}
        cache.store_dataset_metadata("ds1", meta)
        result = cache.get_dataset_metadata("ds1")
        assert result["title"] == "Test"
