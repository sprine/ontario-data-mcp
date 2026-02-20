"""Async client for ArcGIS Hub portals (duck-types CKANClient)."""
from __future__ import annotations

import logging
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
    ):
        self.base_url = base_url.rstrip("/")
        self._http = http_client
        self._owns_client = http_client is None
        self._org = {"title": org_title, "name": org_name}

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
        facet_fields: list[str] | None = None,
    ) -> dict[str, Any]:
        """Search datasets via OGC Records API.

        Returns {"count": int, "results": [...]} matching CKANClient shape.
        """
        client = await self._get_client()
        # OGC Records API uses 1-based startindex
        params: dict[str, Any] = {"limit": rows}
        if start > 0:
            params["startindex"] = start
        if query and query != "*:*":
            params["q"] = query

        resp = await client.get(
            f"{self.base_url}/api/search/v1/collections/all/items",
            params=params,
        )
        resp.raise_for_status()
        data = resp.json()

        results = []
        for feature in data.get("features", []):
            props = feature.get("properties", {})
            ds_id = props.get("id") or feature.get("id", "")
            tags_raw = props.get("tags") or []
            tags = [{"name": t} for t in tags_raw] if isinstance(tags_raw, list) else []

            url = props.get("url", "")
            resources = []
            if url:
                resources.append({
                    "id": ds_id,
                    "name": props.get("title", ""),
                    "format": "Feature Service",
                    "url": url,
                    "datastore_active": False,
                    "download_hint": "Use download_resource — CSV download is typically available.",
                })

            results.append({
                "id": ds_id,
                "name": _slugify(props.get("title", "")),
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
        resp.raise_for_status()
        attrs = resp.json()["data"]["attributes"]

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
            "name": attrs.get("name") or _slugify(attrs.get("title", "")),
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
        return [{
            "name": self._org["name"],
            "title": self._org["title"],
            "description": f"Single-org portal — all datasets belong to {self._org['title']}.",
        }]

    async def tag_list(self, query: str | None = None, all_fields: bool = False) -> list:
        return []

    # ── Download support ───────────────────────────────────────────

    async def get_download_url(self, dataset_id: str, fmt: str = "csv") -> str | None:
        """Try to get a bulk download URL from the Downloads API.

        Returns the URL string, or None if not available.
        """
        client = await self._get_client()
        try:
            resp = await client.get(
                f"{self.base_url}/api/v3/datasets/{dataset_id}/downloads",
                params={"spatialRefId": "4326", "format": fmt},
            )
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json().get("data", [])
            for d in data:
                attrs = d.get("attributes", {})
                if attrs.get("format") == fmt and attrs.get("url"):
                    return attrs["url"]
            return None
        except (httpx.HTTPStatusError, httpx.ConnectError):
            return None


def _slugify(title: str) -> str:
    slug = re.sub(r"[^a-z0-9]", "-", title.lower())
    return re.sub(r"-+", "-", slug).strip("-")[:80]
