from datetime import datetime, timedelta, timezone


from ontario_data.cache import CacheManager
from ontario_data.staleness import compute_expires_at, get_staleness_info, is_stale


class TestComputeExpiresAt:
    def test_daily(self):
        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        result = compute_expires_at(base, "daily")
        assert result == base + timedelta(days=2)

    def test_weekly(self):
        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        result = compute_expires_at(base, "weekly")
        assert result == base + timedelta(days=10)

    def test_monthly(self):
        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        result = compute_expires_at(base, "monthly")
        assert result == base + timedelta(days=45)

    def test_quarterly(self):
        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        result = compute_expires_at(base, "quarterly")
        assert result == base + timedelta(days=120)

    def test_yearly(self):
        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        result = compute_expires_at(base, "yearly")
        assert result == base + timedelta(days=400)

    def test_unknown_defaults_30(self):
        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        result = compute_expires_at(base, "unknown_freq")
        assert result == base + timedelta(days=30)

    def test_none_defaults_30(self):
        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        result = compute_expires_at(base, None)
        assert result == base + timedelta(days=30)

    def test_case_insensitive(self):
        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        result = compute_expires_at(base, "Daily")
        assert result == base + timedelta(days=2)


class TestIsStale:
    def test_stale_resource(self, tmp_path):
        cache = CacheManager(db_path=str(tmp_path / "test.duckdb"))
        cache.initialize()
        # Insert old cached entry
        old_time = datetime.now(timezone.utc) - timedelta(days=60)
        cache.execute_sql(
            "INSERT INTO _cache_metadata (resource_id, table_name, downloaded_at) VALUES (?, ?, ?)",
            ["r1", "ds_test", old_time],
        )
        assert is_stale(cache, "r1") is True

    def test_fresh_resource(self, tmp_path):
        cache = CacheManager(db_path=str(tmp_path / "test.duckdb"))
        cache.initialize()
        # Insert recent cached entry with future expiry
        now = datetime.now(timezone.utc)
        future = now + timedelta(days=30)
        cache.execute_sql(
            "INSERT INTO _cache_metadata (resource_id, table_name, downloaded_at, expires_at) VALUES (?, ?, ?, ?)",
            ["r1", "ds_test", now, future],
        )
        assert is_stale(cache, "r1") is False

    def test_uncached_resource(self, tmp_path):
        cache = CacheManager(db_path=str(tmp_path / "test.duckdb"))
        cache.initialize()
        assert is_stale(cache, "nonexistent") is False


class TestGetStalenessInfo:
    def test_returns_none_for_uncached(self, tmp_path):
        cache = CacheManager(db_path=str(tmp_path / "test.duckdb"))
        cache.initialize()
        assert get_staleness_info(cache, "nonexistent") is None

    def test_returns_info_for_cached(self, tmp_path):
        cache = CacheManager(db_path=str(tmp_path / "test.duckdb"))
        cache.initialize()
        now = datetime.now(timezone.utc)
        future = now + timedelta(days=30)
        cache.execute_sql(
            "INSERT INTO _cache_metadata (resource_id, table_name, downloaded_at, expires_at) VALUES (?, ?, ?, ?)",
            ["r1", "ds_test", now, future],
        )
        info = get_staleness_info(cache, "r1")
        assert info is not None
        assert info["resource_id"] == "r1"
        assert info["is_stale"] is False
        assert info["age_hours"] >= 0
