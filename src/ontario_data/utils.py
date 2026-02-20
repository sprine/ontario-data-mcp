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
    return ctx.lifespan_context


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
        elif config.portal_type == PortalType.ARCGIS_HUB:
            from ontario_data.arcgis_client import ArcGISHubClient
            clients[portal] = ArcGISHubClient(
                base_url=config.base_url,
                http_client=state["http_client"],
                org_name=portal,
                org_title=config.name.replace(" Open Data", ""),
            )
        else:
            raise ValueError(f"Portal '{portal}' uses unknown type {config.portal_type}.")

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
    No timeout — relies on each client's own 30s timeout + retry.
    """
    configs = _lifespan_state(ctx)["portal_configs"]

    if portal:
        keys = [portal]
    else:
        keys = list(configs.keys())

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


def unwrap_first_match(
    results: list[tuple[str, T | None, str | None]],
    bare_id: str,
    entity_type: str = "Dataset",
) -> tuple[str, T]:
    """Extract the first success from ``fan_out(first_match=True)`` results.

    Returns ``(portal, result)``.  Raises :class:`ValueError` with a
    consistent, user-friendly message when every portal failed.
    """
    if results and results[0][2] is None:
        return results[0][0], results[0][1]
    errors = (
        "; ".join(f"{pk}: {err}" for pk, _, err in results)
        if results
        else "no portals available"
    )
    raise ValueError(
        f"{entity_type} '{bare_id}' not found. Tried: {errors}. "
        f"Use search_datasets to find the correct prefixed ID."
    )


async def resolve_dataset(
    ctx: Context, dataset_id: str
) -> tuple[str, str, dict]:
    """Resolve a (possibly bare) dataset ID to ``(portal, bare_id, ds_dict)``.

    If *dataset_id* carries a portal prefix the call goes directly to that
    portal; otherwise every configured portal is tried sequentially via
    :func:`fan_out`.
    """
    configs = _lifespan_state(ctx)["portal_configs"]
    portal, bare_id = parse_portal_id(dataset_id, set(configs.keys()))

    async def _show(pk: str):
        client, _ = get_deps(ctx, pk)
        return await client.package_show(bare_id)

    if portal:
        client, _ = get_deps(ctx, portal)
        ds = await client.package_show(bare_id)
    else:
        results = await fan_out(ctx, None, _show, first_match=True)
        portal, ds = unwrap_first_match(results, bare_id, "Dataset")

    return portal, bare_id, ds


async def resolve_resource_portal(
    ctx: Context, resource_id: str
) -> tuple[str, str]:
    """Resolve a (possibly bare) resource ID to ``(portal, bare_id)``.

    Unlike :func:`resolve_dataset` this does **not** return the resource
    dict — callers typically need to make their own follow-up API call
    (e.g. ``resource_show`` or ``datastore_search``) after knowing the portal.
    """
    configs = _lifespan_state(ctx)["portal_configs"]
    portal, bare_id = parse_portal_id(resource_id, set(configs.keys()))

    if portal:
        return portal, bare_id

    async def _try(pk: str):
        client, _ = get_deps(ctx, pk)
        await client.resource_show(bare_id)
        return pk

    results = await fan_out(ctx, None, _try, first_match=True)
    unwrap_first_match(results, bare_id, "Resource")
    return results[0][0], bare_id


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
