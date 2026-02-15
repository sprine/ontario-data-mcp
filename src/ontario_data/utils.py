from __future__ import annotations

import json
import re
from typing import Any

from fastmcp import Context

from ontario_data.cache import CacheManager, InvalidQueryError  # noqa: F401
from ontario_data.ckan_client import CKANClient


class ResourceNotCachedError(Exception):
    """Raised when a tool requires cached data that doesn't exist."""
    pass


class DatastoreNotAvailableError(Exception):
    """Raised when a resource has no datastore."""
    pass


class SpatialExtensionError(Exception):
    """Raised when DuckDB spatial extension is not available."""
    pass


def get_deps(ctx: Context) -> tuple[CKANClient, CacheManager]:
    """Extract CKAN client and cache manager from MCP context."""
    return ctx.lifespan_context["ckan"], ctx.lifespan_context["cache"]


def get_cache(ctx: Context) -> CacheManager:
    """Extract cache manager from MCP context."""
    return ctx.lifespan_context["cache"]


def strip_internal_fields(records: list[dict]) -> list[dict]:
    """Remove CKAN internal fields (prefixed with _) from records."""
    return [{k: v for k, v in r.items() if not k.startswith("_")} for r in records]


def make_table_name(dataset_name: str, resource_id: str) -> str:
    """Generate a safe DuckDB table name from dataset name and resource ID."""
    slug = re.sub(r"[^a-z0-9]", "_", (dataset_name or "unknown").lower())
    slug = re.sub(r"_+", "_", slug).strip("_")[:40]
    prefix = resource_id[:8]
    return f"ds_{slug}_{prefix}"


def require_cached(cache: CacheManager, resource_id: str) -> str:
    """Get table name for a cached resource, or raise ResourceNotCachedError."""
    table_name = cache.get_table_name(resource_id)
    if not table_name:
        raise ResourceNotCachedError(
            f"Resource {resource_id} is not cached. "
            f"Use download_resource(resource_id='{resource_id}') first."
        )
    return table_name


def json_response(**kwargs) -> str:
    """Serialize a response dict to JSON with consistent formatting."""
    return json.dumps(kwargs, indent=2, default=str)
