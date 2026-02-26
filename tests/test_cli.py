"""Tests for CLI cache management commands."""
from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ontario_data.cache import CacheManager


@pytest.fixture
def cache_with_resources(tmp_path):
    """Cache with Toronto and Ottawa resources for testing."""
    cache = CacheManager(db_path=str(tmp_path / "test.duckdb"))
    cache.initialize()

    df = pd.DataFrame({"col": [1, 2, 3]})
    cache.store_resource(
        resource_id="abc123",
        dataset_id="ds1",
        table_name="ds_toronto_ttc_routes_abc12345",
        df=df,
        source_url="http://example.com/data.csv",
    )
    cache.store_resource(
        resource_id="def456",
        dataset_id="ds2",
        table_name="ds_ottawa_bus_stops_def45678",
        df=df,
        source_url="http://example.com/data2.csv",
    )
    return cache


class TestCmdRemovePrefixStripping:
    def test_strips_portal_prefix(self, cache_with_resources):
        from ontario_data.cli import cmd_remove

        assert cache_with_resources.is_cached("abc123")
        args = argparse.Namespace(resource_id="toronto:abc123")
        with patch("ontario_data.cli._make_cache", return_value=cache_with_resources):
            cmd_remove(args)
        assert not cache_with_resources.is_cached("abc123")

    def test_bare_id_still_works(self, cache_with_resources):
        from ontario_data.cli import cmd_remove

        args = argparse.Namespace(resource_id="abc123")
        with patch("ontario_data.cli._make_cache", return_value=cache_with_resources):
            cmd_remove(args)
        assert not cache_with_resources.is_cached("abc123")


class TestCmdRefreshPortalInference:
    def test_infers_toronto_portal(self, cache_with_resources):
        """Verify cmd_refresh correctly infers toronto from table name."""
        from ontario_data.cli import cmd_refresh
        from ontario_data.utils import infer_portal_from_table

        meta = cache_with_resources.get_resource_meta("abc123")
        portal = infer_portal_from_table(meta["table_name"])
        assert portal == "toronto"

    def test_infers_ottawa_portal(self, cache_with_resources):
        """Verify cmd_refresh correctly infers ottawa from table name."""
        from ontario_data.utils import infer_portal_from_table

        meta = cache_with_resources.get_resource_meta("def456")
        portal = infer_portal_from_table(meta["table_name"])
        assert portal == "ottawa"

    def test_refresh_not_cached_exits(self, cache_with_resources, capsys):
        from ontario_data.cli import cmd_refresh

        args = argparse.Namespace(resource_id="nonexistent")
        with patch("ontario_data.cli._make_cache", return_value=cache_with_resources):
            with pytest.raises(SystemExit):
                cmd_refresh(args)
