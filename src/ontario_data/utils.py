from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Awaitable, Callable
from typing import TypeVar

from fastmcp import Context

from ontario_data.cache import CacheManager, InvalidQueryError  # noqa: F401
from ontario_data.ckan_client import CKANClient
from ontario_data.portals import PORTALS, PortalType

T = TypeVar("T")


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
    # fastmcp stores lifespan yield value here (not part of public API)
    return ctx.fastmcp._lifespan_result


def get_deps(ctx: Context, portal: str) -> tuple[CKANClient, CacheManager]:
    """Extract portal client and cache manager from MCP context.

    Lazily creates the client for the requested portal on first use.
    portal is required — there is no default.
    """
    state = _lifespan_state(ctx)
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


def parse_portal_id(id_str: str, known_portals: set[str]) -> tuple[str | None, str]:
    """Split 'portal:bare_id'. Returns (None, id_str) if no valid prefix."""
    if ":" in id_str:
        prefix, rest = id_str.split(":", 1)
        if prefix in known_portals:
            return prefix, rest
    return None, id_str


async def fan_out(
    ctx: Context,
    portal: str | None,
    fn: Callable[[str], Awaitable[T]],
    *,
    first_match: bool = False,
) -> list[tuple[str, T | None, str | None]]:
    """Run fn(portal_key) across portals.

    first_match=False (default): asyncio.gather all portals, collect all results.
      Returns [(portal_key, result_or_None, error_or_None), ...].
    first_match=True: try portals sequentially in PORTALS dict order.
      Return on first success. Swallow errors and continue.
      On all-fail, return all errors so caller can build a diagnostic message.

    If portal is specified, only run against that one portal.
    Only runs against CKAN portals; silently skips non-CKAN.
    No timeout — relies on each CKANClient's own 30s timeout + retry.
    """
    configs = _lifespan_state(ctx)["portal_configs"]

    if portal:
        keys = [portal]
    else:
        keys = [k for k, c in configs.items() if c.portal_type == PortalType.CKAN]

    if first_match:
        errors: list[tuple[str, None, str]] = []
        for key in keys:
            try:
                result = await fn(key)
                return [(key, result, None)]
            except Exception as exc:
                errors.append((key, None, str(exc)))
        return errors

    # Parallel fan-out
    async def _safe(key: str) -> tuple[str, T | None, str | None]:
        try:
            result = await fn(key)
            return (key, result, None)
        except Exception as exc:
            return (key, None, str(exc))

    return list(await asyncio.gather(*[_safe(k) for k in keys]))


def get_cache(ctx: Context) -> CacheManager:
    return _lifespan_state(ctx)["cache"]


def strip_internal_fields(records: list[dict]) -> list[dict]:
    """Strip CKAN bookkeeping columns (_id, _full_text, etc.) that clutter
    results returned to the LLM."""
    return [{k: v for k, v in r.items() if not k.startswith("_")} for r in records]


def _slugify(name: str, fallback: str = "unknown", max_len: int = 40) -> str:
    """Lowercase, collapse non-alphanumerics to underscores, truncate.
    e.g. 'Ontario COVID-19 Cases' → 'ontario_covid_19_cases'."""
    slug = re.sub(r"[^a-z0-9]", "_", (name or fallback).lower())
    return re.sub(r"_+", "_", slug).strip("_")[:max_len]


def make_table_name(dataset_name: str, resource_id: str, portal: str = "ontario") -> str:
    """Build a deterministic table name like 'ds_ontario_covid_cases_a1b2c3d4'
    so the same resource always lands in the same table."""
    slug = _slugify(dataset_name)
    return f"ds_{portal}_{slug}_{resource_id[:8]}"


def make_geo_table_name(dataset_name: str, resource_id: str, portal: str = "ontario") -> str:
    """Like make_table_name but prefixed 'geo_' so spatial tables are
    distinguishable in cache listings."""
    slug = _slugify(dataset_name, fallback="geo")
    return f"geo_{portal}_{slug}_{resource_id[:8]}"


def require_cached(cache: CacheManager, resource_id: str) -> str:
    """Return table name, or raise with a user-friendly message that includes
    the download_resource command to run."""
    table_name = cache.get_table_name(resource_id)
    if not table_name:
        raise ResourceNotCachedError(
            f"Resource {resource_id} is not cached. "
            f"Use download_resource(resource_id='{resource_id}') first."
        )
    return table_name


def json_response(**kwargs) -> str:
    """Serialize to JSON with default=str so datetimes and other
    non-native types don't blow up."""
    return json.dumps(kwargs, indent=2, default=str)
