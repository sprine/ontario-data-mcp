import pytest
import duckdb


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
