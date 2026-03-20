"""Async client for ArcGIS Hub portals (duck-types CKANClient)."""
from __future__ import annotations

import asyncio
import logging
import random
import re
from typing import Any

import httpx

logger = logging.getLogger("ontario_data.arcgis")


class ArcGISHubClient:
    """ArcGIS Hub client with CKANClient-compatible method signatures.

    Uses the OGC Records API for search, Hub v3 API for metadata,
    and Downloads API for data retrieval.
    """

    def __init__(
        self,
        base_url: str = "https://open.ottawa.ca",
        http_client: httpx.AsyncClient | None = None,
        org_name: str = "ottawa",
        org_title: str = "City of Ottawa",
        owner_filter: str | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self._http = http_client
        self._owns_client = http_client is None
        self._org = {"title": org_title, "name": org_name}
        self._owner_filter = owner_filter

    async def _get_client(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=30.0)
            self._owns_client = True
        return self._http

    async def close(self):
        if self._owns_client and self._http is not None:
            await self._http.aclose()
            self._http = None

    # ── Search (OGC Records API) ───────────────────────────────────

    async def package_search(
        self,
        query: str = "",
        filters: dict[str, str] | None = None,
        sort: str | None = None,
        rows: int = 10,
        start: int = 0,
    ) -> dict[str, Any]:
        """Search datasets via OGC Records API.

        Returns {"count": int, "results": [...]} matching CKANClient shape.
        """
        if filters:
            logger.debug(
                "ArcGIS OGC Records API does not support CKAN-style filters; "
                "ignoring filters=%s",
                filters,
            )

        client = await self._get_client()
        # OGC Records API uses 1-based startindex
        params: dict[str, Any] = {"limit": rows}
        if start > 0:
            params["startindex"] = start
        if query and query != "*:*":
            params["q"] = query
        if self._owner_filter:
            params["filter"] = f"owner={self._owner_filter}"

        resp = await client.get(
            f"{self.base_url}/api/search/v1/collections/all/items",
            params=params,
        )
        resp.raise_for_status()
        try:
            data = resp.json()
        except ValueError:
            raise httpx.DecodingError(
                f"package_search returned non-JSON response "
                f"(HTTP {resp.status_code}, "
                f"content-type: {resp.headers.get('content-type', 'unknown')})"
            )

        results = []
        for feature in data.get("features", []):
            props = feature.get("properties", {})
            raw_id = props.get("id") or feature.get("id", "")
            item_type = props.get("type", "")

            # Hub v3 indexes Feature Service datasets as {itemId}_0;
            # non-layered items (Excel, CSV, etc.) use the bare itemId.
            ds_id = f"{raw_id}_0" if _is_layered_type(item_type) else raw_id

            tags_raw = props.get("tags") or []
            tags = [{"name": t} for t in tags_raw] if isinstance(tags_raw, list) else []

            url = props.get("url", "")
            fmt = item_type or "Feature Service"
            resources = []
            if url:
                resources.append({
                    "id": ds_id,
                    "name": props.get("title", ""),
                    "format": fmt,
                    "url": url,
                    "datastore_active": False,
                    "download_hint": "Use download_resource — CSV download is typically available.",
                })

            results.append({
                "id": ds_id,
                "name": _slugify_name(props.get("title", "")),
                "title": props.get("title", ""),
                "notes": props.get("description") or props.get("snippet") or "",
                "metadata_modified": props.get("modified", ""),
                "organization": dict(self._org),
                "tags": tags,
                "resources": resources,
                "update_frequency": "unknown",
            })

        return {
            "count": data.get("numberMatched", len(results)),
            "results": results,
        }

    # ── Dataset metadata (Hub v3 API) ──────────────────────────────

    async def package_show(self, id: str) -> dict[str, Any]:
        """Get dataset metadata via Hub v3 API.

        Returns a CKAN-like package dict.
        """
        client = await self._get_client()
        resp = await client.get(f"{self.base_url}/api/v3/datasets/{id}")

        if resp.status_code == 404:
            # Bare item ID → try appending _0 (Feature Service layer convention)
            if "_" not in id:
                resp = await client.get(f"{self.base_url}/api/v3/datasets/{id}_0")
                if resp.is_success:
                    id = f"{id}_0"
            # Layer-suffixed ID → the expected layer index may not exist;
            # try other indices (0–4) before giving up.
            elif re.search(r"_\d+$", id):
                base = re.sub(r"_\d+$", "", id)
                for layer_idx in range(5):
                    candidate = f"{base}_{layer_idx}"
                    if candidate == id:
                        continue
                    alt = await client.get(f"{self.base_url}/api/v3/datasets/{candidate}")
                    if alt.is_success:
                        logger.info(
                            "Dataset layer '%s' not found; using '%s' instead",
                            id, candidate,
                        )
                        resp = alt
                        id = candidate
                        break

        resp.raise_for_status()
        try:
            body = resp.json()
        except ValueError:
            raise httpx.DecodingError(
                f"package_show returned non-JSON response for '{id}' "
                f"(HTTP {resp.status_code}, "
                f"content-type: {resp.headers.get('content-type', 'unknown')})"
            )
        try:
            attrs = body["data"]["attributes"]
        except (KeyError, TypeError):
            raise httpx.DecodingError(
                f"package_show for '{id}' returned unexpected structure "
                f"(missing data.attributes)"
            )

        tags_raw = attrs.get("tags") or []
        tags = [{"name": t} for t in tags_raw] if isinstance(tags_raw, list) else []

        url = attrs.get("url", "")
        ds_id = attrs.get("id", id)
        resources = []
        if url:
            resources.append({
                "id": ds_id,
                "name": attrs.get("title", ""),
                "format": "Feature Service",
                "url": url,
                "datastore_active": False,
                "download_hint": "Use download_resource — CSV download is typically available.",
            })

        return {
            "id": ds_id,
            "name": attrs.get("name") or _slugify_name(attrs.get("title", "")),
            "title": attrs.get("title", ""),
            "notes": attrs.get("description") or "",
            "metadata_modified": attrs.get("modified", ""),
            "metadata_created": attrs.get("created", ""),
            "organization": dict(self._org),
            "tags": tags,
            "resources": resources,
            "update_frequency": attrs.get("updateFrequency") or "unknown",
            "license_title": attrs.get("license") or "",
            "geographic_coverage": f"{self._org['title']}",
        }

    # ── Resource (synthesized from dataset) ────────────────────────

    async def resource_show(self, id: str) -> dict[str, Any]:
        """Synthesize a CKAN-like resource dict from dataset metadata.

        For ArcGIS, resource_id == dataset_id (itemId_layerIndex).
        """
        ds = await self.package_show(id)
        for r in ds.get("resources", []):
            if r["id"] == id:
                r["package_id"] = ds["id"]
                return r
        return {
            "id": id,
            "package_id": ds["id"],
            "name": ds.get("title", ""),
            "format": "Feature Service",
            "url": "",
            "datastore_active": False,
        }

    # ── Compat stubs (single-org portal, no tags endpoint) ─────────

    async def organization_list(
        self,
        sort: str = "package_count desc",
        all_fields: bool = False,
        include_dataset_count: bool = True,
    ) -> list[dict]:
        package_count = 0
        if include_dataset_count:
            try:
                result = await self.package_search(rows=0)
                package_count = result.get("count", 0)
            except Exception:
                logger.debug("Failed to fetch dataset count for %s", self._org["name"], exc_info=True)
        return [{
            "name": self._org["name"],
            "title": self._org["title"],
            "description": f"Single-org portal — all datasets belong to {self._org['title']}.",
            "package_count": package_count,
        }]

    async def tag_list(self, query: str | None = None, all_fields: bool = False) -> list:
        return []

    # ── Download support ───────────────────────────────────────────

    _RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

    async def get_download_url(self, dataset_id: str, fmt: str = "csv") -> str | None:
        """Try to get a bulk download URL from the Downloads API.

        Retries on 429/5xx and transient network errors with exponential
        backoff. Returns the URL string, or None if not available.
        """
        client = await self._get_client()
        max_retries = 3
        for attempt in range(max_retries + 1):
            try:
                resp = await client.get(
                    f"{self.base_url}/api/v3/datasets/{dataset_id}/downloads",
                    params={"spatialRefId": "4326", "format": fmt},
                )
                if resp.status_code == 404:
                    return None
                if resp.status_code in self._RETRYABLE_STATUS_CODES:
                    if attempt < max_retries:
                        delay = 1.0 * (2 ** attempt) + random.uniform(0, 0.5)
                        logger.warning(
                            "Retryable HTTP %d from Downloads API for %s "
                            "(attempt %d/%d), retrying in %.1fs",
                            resp.status_code, dataset_id, attempt + 1, max_retries, delay,
                        )
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.warning(
                            "Downloads API returned %d for %s after %d attempts; "
                            "no download URL available",
                            resp.status_code, dataset_id, max_retries + 1,
                        )
                        return None
                resp.raise_for_status()
                try:
                    data = resp.json().get("data", [])
                except ValueError:
                    logger.warning(
                        "Downloads API returned non-JSON for %s (HTTP %d)",
                        dataset_id, resp.status_code,
                    )
                    return None
                for d in data:
                    attrs = d.get("attributes", {})
                    if attrs.get("format") == fmt and attrs.get("url"):
                        return attrs["url"]
                return None
            except (httpx.ConnectError, httpx.TimeoutException) as exc:
                if attempt < max_retries:
                    delay = 1.0 * (2 ** attempt)
                    logger.warning(
                        "Network error from Downloads API for %s "
                        "(attempt %d/%d): %s — retrying in %.1fs",
                        dataset_id, attempt + 1, max_retries, exc, delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.warning(
                        "Downloads API unreachable for %s after %d attempts: %s",
                        dataset_id, max_retries + 1, exc,
                    )
                    return None
        return None


_LAYERED_TYPES = {"Feature Service", "Map Service"}


def _is_layered_type(item_type: str) -> bool:
    """Return True for ArcGIS item types that have layers (need _0 suffix)."""
    return item_type in _LAYERED_TYPES


def _slugify_name(title: str) -> str:
    slug = re.sub(r"[^a-z0-9]", "-", title.lower())
    return re.sub(r"-+", "-", slug).strip("-")[:80]
