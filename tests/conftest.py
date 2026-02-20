from unittest.mock import AsyncMock, MagicMock

import duckdb
import pytest

from ontario_data.portals import PORTALS


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


@pytest.fixture
def make_portal_context():
    """Factory fixture: create a mock MCP context with full portal state."""
    def _make(portal_clients=None):
        ctx = MagicMock()
        ctx.fastmcp._lifespan_result = {
            "cache": MagicMock(),
            "http_client": MagicMock(),
            "portal_configs": PORTALS,
            "portal_clients": portal_clients or {},
        }
        ctx.report_progress = AsyncMock()
        return ctx
    return _make
