import pytest
import duckdb


def pytest_addoption(parser):
    parser.addoption("--live", action="store_true", default=False, help="Run live API tests")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--live"):
        # --live passed: remove the 'live' skip so live tests run
        return
    skip_live = pytest.mark.skip(reason="needs --live option to run")
    for item in items:
        if "live" in item.keywords:
            item.add_marker(skip_live)


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
