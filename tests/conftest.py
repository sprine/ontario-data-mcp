import pytest
import duckdb


@pytest.fixture(autouse=True)
def isolate_cache_dir(tmp_path, monkeypatch):
    """Ensure all tests use an isolated cache directory, never production."""
    test_cache = str(tmp_path / "ontario_test_cache")
    monkeypatch.setenv("ONTARIO_DATA_CACHE_DIR", test_cache)


@pytest.fixture
def tmp_cache_dir(tmp_path):
    """Temporary cache directory for tests."""
    return str(tmp_path / "ontario_data_cache")


@pytest.fixture
def duckdb_conn(tmp_path):
    """In-memory DuckDB connection for tests."""
    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    yield conn
    conn.close()
