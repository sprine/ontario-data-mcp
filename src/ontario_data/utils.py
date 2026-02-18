from __future__ import annotations

import json
import re

from fastmcp import Context

from ontario_data.cache import CacheManager, InvalidQueryError  # noqa: F401
from ontario_data.ckan_client import CKANClient
from ontario_data.portals import PortalType


class ResourceNotCachedError(Exception):
    """Raised when a tool requires cached data that doesn't exist."""
    pass


class DatastoreNotAvailableError(Exception):
    """Raised when a resource has no datastore."""
    pass


class SpatialExtensionError(Exception):
    """Raised when DuckDB spatial extension is not available."""
    pass


def _lifespan_state(ctx: Context) -> dict:
    """Access the lifespan state dict from the MCP context."""
    return ctx.fastmcp._lifespan_result


def get_deps(ctx: Context, portal: str | None = None) -> tuple[CKANClient, CacheManager]:
    """Extract portal client and cache manager from MCP context.

    Lazily creates the client for the requested portal on first use.
    """
    state = _lifespan_state(ctx)
    portal = portal or state["active_portal"]
    configs = state["portal_configs"]

    if portal not in configs:
        available = list(configs.keys())
        raise ValueError(f"Unknown portal '{portal}'. Available: {available}")

    clients = state["portal_clients"]
    if portal not in clients:
        config = configs[portal]
        if config.portal_type == PortalType.CKAN:
            clients[portal] = CKANClient(
                base_url=config.base_url,
                http_client=state["http_client"],
            )
        else:
            raise ValueError(
                f"Portal '{portal}' uses {config.portal_type} which is not yet supported. "
                f"ArcGIS Hub support is coming in a future release."
            )

    return clients[portal], state["cache"]


def get_active_portal(ctx: Context) -> str:
    """Get the name of the currently active portal."""
    return _lifespan_state(ctx)["active_portal"]


def get_cache(ctx: Context) -> CacheManager:
    """Extract cache manager from MCP context."""
    return _lifespan_state(ctx)["cache"]


def strip_internal_fields(records: list[dict]) -> list[dict]:
    """Remove CKAN internal fields (prefixed with _) from records."""
    return [{k: v for k, v in r.items() if not k.startswith("_")} for r in records]


def make_table_name(dataset_name: str, resource_id: str, portal: str = "ontario") -> str:
    """Generate a safe DuckDB table name from dataset name, resource ID, and portal."""
    slug = re.sub(r"[^a-z0-9]", "_", (dataset_name or "unknown").lower())
    slug = re.sub(r"_+", "_", slug).strip("_")[:40]
    prefix = resource_id[:8]
    return f"ds_{portal}_{slug}_{prefix}"


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
