from __future__ import annotations

from datetime import datetime, timedelta, timezone

from ontario_data.cache import CacheManager

# Map CKAN update_frequency values to expected update intervals
FREQUENCY_DAYS = {
    "daily": 2,
    "weekly": 10,
    "monthly": 45,
    "quarterly": 120,
    "biannually": 200,
    "yearly": 400,
    "as_required": 365,
    "on_demand": 365,
}


def compute_expires_at(downloaded_at: datetime, update_frequency: str | None) -> datetime:
    """Compute when a cached resource should be considered stale."""
    freq = (update_frequency or "").lower().strip()
    days = FREQUENCY_DAYS.get(freq, 30)  # default 30 days
    return downloaded_at + timedelta(days=days)


def is_stale(cache: CacheManager, resource_id: str) -> bool:
    """Check if a cached resource is stale based on its expires_at."""
    info = get_staleness_info(cache, resource_id)
    if info is None:
        return False
    return info["is_stale"]


def get_staleness_info(cache: CacheManager, resource_id: str) -> dict | None:
    """Get staleness information for a cached resource.

    Returns None if the resource is not cached.
    """
    result = cache.conn.execute(
        "SELECT downloaded_at, expires_at FROM _cache_metadata WHERE resource_id = ?",
        [resource_id],
    ).fetchone()
    if result is None:
        return None

    downloaded_at = result[0]
    expires_at = result[1]

    if downloaded_at is None:
        return None

    # Ensure timezone-aware comparison
    now = datetime.now(timezone.utc)
    if downloaded_at.tzinfo is None:
        downloaded_at = downloaded_at.replace(tzinfo=timezone.utc)

    if expires_at is None:
        # No expires_at set — compute default (30 days)
        expires_at = downloaded_at + timedelta(days=30)

    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    return {
        "resource_id": resource_id,
        "downloaded_at": str(downloaded_at),
        "expires_at": str(expires_at),
        "is_stale": now > expires_at,
        "age_hours": round((now - downloaded_at).total_seconds() / 3600, 1),
    }
