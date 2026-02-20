"""Live smoke tests for Ottawa ArcGIS Hub — requires network.

Run: python -m pytest tests/test_arcgis_smoke.py -v --live
"""
from __future__ import annotations

import httpx
import pytest

from ontario_data.arcgis_client import ArcGISHubClient


pytestmark = pytest.mark.live


@pytest.fixture
async def client():
    async with httpx.AsyncClient(timeout=30.0) as http:
        yield ArcGISHubClient(base_url="https://open.ottawa.ca", http_client=http)


class TestOttawaSmoke:
    async def test_search_returns_results(self, client):
        result = await client.package_search(query="parking", rows=3)
        assert result["count"] > 0
        assert len(result["results"]) > 0
        ds = result["results"][0]
        assert "title" in ds
        assert "id" in ds
        assert "organization" in ds

    async def test_package_show(self, client):
        search = await client.package_search(query="parking", rows=1)
        ds_id = search["results"][0]["id"]

        ds = await client.package_show(ds_id)
        assert ds["id"] == ds_id
        assert "title" in ds
        assert "notes" in ds
        assert "resources" in ds

    async def test_download_url_returns_valid_csv(self, client):
        search = await client.package_search(query="parking", rows=1)
        ds_id = search["results"][0]["id"]

        url = await client.get_download_url(ds_id, fmt="csv")
        assert url is None or url.startswith("http")

        if url:
            async with httpx.AsyncClient(timeout=30.0) as http:
                resp = await http.get(url, follow_redirects=True)
                assert resp.status_code == 200
                first_line = resp.text.split("\n")[0]
                assert "," in first_line
