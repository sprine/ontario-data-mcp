import pytest

from ontario_data.cache import CacheManager, _has_semicolons_outside_strings, _validate_sql
from ontario_data.utils import InvalidQueryError


class TestSemicolonDetection:
    """Test SQL-standard '' escaping in semicolon detection."""

    def test_no_semicolon(self):
        assert _has_semicolons_outside_strings("SELECT 1") is False

    def test_semicolon_outside_string(self):
        assert _has_semicolons_outside_strings("SELECT 1; DROP TABLE x") is True

    def test_semicolon_inside_string(self):
        assert _has_semicolons_outside_strings("SELECT 'a;b'") is False

    def test_escaped_quote_sql_standard(self):
        # SQL-standard: '' is an escaped single quote inside a string
        # 'O''Brien' is the string O'Brien — semicolons after should be detected
        assert _has_semicolons_outside_strings("SELECT 'O''Brien'; DROP TABLE x") is True

    def test_escaped_quote_no_semicolon(self):
        assert _has_semicolons_outside_strings("SELECT 'O''Brien'") is False

    def test_backslash_does_not_escape(self):
        # Backslash is NOT an escape character in standard SQL
        # '\' is a complete string containing a backslash
        # The semicolon after is OUTSIDE the string
        assert _has_semicolons_outside_strings(r"SELECT '\'; DROP TABLE x") is True

    def test_multiple_strings(self):
        assert _has_semicolons_outside_strings("SELECT 'a', 'b'") is False

    def test_multiple_strings_with_semicolon(self):
        assert _has_semicolons_outside_strings("SELECT 'a', 'b'; DROP TABLE x") is True

    def test_empty_string_literal(self):
        assert _has_semicolons_outside_strings("SELECT ''") is False

    def test_empty_sql(self):
        assert _has_semicolons_outside_strings("") is False


class TestValidateSQL:
    def test_select_allowed(self):
        _validate_sql("SELECT * FROM my_table")

    def test_select_lowercase(self):
        _validate_sql("select * from my_table")

    def test_cte_allowed(self):
        _validate_sql("WITH cte AS (SELECT 1) SELECT * FROM cte")

    def test_explain_allowed(self):
        _validate_sql("EXPLAIN SELECT * FROM my_table")

    def test_describe_allowed(self):
        _validate_sql("DESCRIBE my_table")

    def test_show_allowed(self):
        _validate_sql("SHOW TABLES")

    def test_pragma_allowed(self):
        _validate_sql("PRAGMA table_info('my_table')")

    def test_summarize_allowed(self):
        _validate_sql("SUMMARIZE SELECT * FROM my_table")

    def test_drop_rejected(self):
        with pytest.raises(InvalidQueryError, match="read-only"):
            _validate_sql("DROP TABLE my_table")

    def test_delete_rejected(self):
        with pytest.raises(InvalidQueryError, match="read-only"):
            _validate_sql("DELETE FROM my_table")

    def test_insert_rejected(self):
        with pytest.raises(InvalidQueryError, match="read-only"):
            _validate_sql("INSERT INTO my_table VALUES (1)")

    def test_update_rejected(self):
        with pytest.raises(InvalidQueryError, match="read-only"):
            _validate_sql("UPDATE my_table SET x = 1")

    def test_create_rejected(self):
        with pytest.raises(InvalidQueryError, match="read-only"):
            _validate_sql("CREATE TABLE my_table (id INT)")

    def test_semicolon_injection_rejected(self):
        with pytest.raises(InvalidQueryError, match="semicolons"):
            _validate_sql("SELECT 1; DROP TABLE x")

    def test_comment_prefixed_mutation_rejected(self):
        with pytest.raises(InvalidQueryError, match="read-only"):
            _validate_sql("/* harmless */ DROP TABLE x")

    def test_line_comment_prefixed_mutation_rejected(self):
        with pytest.raises(InvalidQueryError, match="read-only"):
            _validate_sql("-- just a comment\nDROP TABLE x")

    def test_comment_embedded_quote_injection_rejected(self):
        # A quote inside a line comment must not open a "string" that hides a semicolon
        with pytest.raises(InvalidQueryError, match="semicolons"):
            _validate_sql("SELECT 1 --'\n; DROP TABLE x")

    def test_block_comment_embedded_quote_injection_rejected(self):
        with pytest.raises(InvalidQueryError, match="semicolons"):
            _validate_sql("SELECT 1 /* ' */ ; DROP TABLE x")


class TestCacheManagerQuerySafety:
    def test_select_works(self, tmp_path):
        cache = CacheManager(db_path=str(tmp_path / "test.duckdb"))
        cache.initialize()
        result = cache.query("SELECT 1 AS n")
        assert result == [{"n": 1}]

    def test_drop_rejected(self, tmp_path):
        cache = CacheManager(db_path=str(tmp_path / "test.duckdb"))
        cache.initialize()
        with pytest.raises(InvalidQueryError):
            cache.query("DROP TABLE _cache_metadata")

    def test_semicolon_rejected(self, tmp_path):
        cache = CacheManager(db_path=str(tmp_path / "test.duckdb"))
        cache.initialize()
        with pytest.raises(InvalidQueryError):
            cache.query("SELECT 1; DROP TABLE _cache_metadata")

    def test_query_df_validates(self, tmp_path):
        cache = CacheManager(db_path=str(tmp_path / "test.duckdb"))
        cache.initialize()
        with pytest.raises(InvalidQueryError):
            cache.query_df("DELETE FROM _cache_metadata")
